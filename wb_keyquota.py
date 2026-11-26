"""每 Key 累计积分统计（配合 API Key 的 credit_limit 额度）。

积分来自 usage.jsonl 里每行的 ``credit``（上游按模型倍率实际扣的分数），
按 ``key``（API Key 的 id）折叠累加，用于「这个 Key 一共只能用 N 积分」的额度判断。

实现采用「增量续读 + 必要时整份重建」：
- 首次访问、文件被轮转替换（dev/ino 变化或变短）时，读完整逻辑日志重建，
  已封存的段（usage.jsonl.segments/）通过 wb_usage_io.open_log 一起读到；
- 其余情况只读上次偏移之后新追加的字节，单个请求的开销与日志总长无关。

环境变量 ``WB_KEY_CREDIT_TOTALS=0`` 可整体关闭统计（此时额度判断视为不限）。
"""

import json
import os
import threading
import time

import wb_usage_io

_LOCK = threading.Lock()
_STATE = {"log": None, "dev": 0, "ino": 0, "offset": 0, "carry": b"", "totals": {}, "at": 0.0}
_REFRESH_SECONDS = 1.0


def enabled():
    raw = (os.environ.get("WB_KEY_CREDIT_TOTALS") or "1").strip().lower()
    return raw not in ("0", "false", "off", "no")


def log_path(usage_dir):
    return os.path.join(usage_dir, "usage.jsonl")


def _apply(chunk, totals):
    if not chunk:
        return
    for line in chunk.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        key = row.get("key")
        if not key:
            continue
        try:
            credit = float(row.get("credit") or 0)
        except (TypeError, ValueError):
            continue
        if credit:
            key = str(key)
            totals[key] = totals.get(key, 0.0) + credit


def _read_all(path):
    """整份逻辑日志（含已封存段）读成 bytes。"""
    try:
        with wb_usage_io.open_log(path, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        return b""
    except Exception:
        try:
            with open(path, "rb") as handle:
                return handle.read()
        except OSError:
            return b""


def _stamp(path):
    try:
        st = os.stat(path)
        return st.st_dev, st.st_ino, st.st_size
    except OSError:
        return 0, 0, 0


def refresh(usage_dir, force=False):
    """返回 {key_id: 累计积分}。默认 1 秒内复用上次结果。"""
    if not enabled():
        return {}
    path = log_path(usage_dir)
    now = time.time()
    with _LOCK:
        dev, ino, size = _stamp(path)
        rotated = (_STATE["log"] != path
                   or (dev, ino) != (_STATE["dev"], _STATE["ino"])
                   or size < _STATE["offset"])
        if not force and not rotated and (now - _STATE["at"]) < _REFRESH_SECONDS:
            return dict(_STATE["totals"])
        if rotated:
            totals = {}
            data = _read_all(path)
            _apply(data, totals)
            _STATE.update(log=path, dev=dev, ino=ino, offset=len(data),
                          carry=b"", totals=totals, at=now)
            return dict(totals)
        try:
            with open(path, "rb") as handle:
                handle.seek(_STATE["offset"])
                chunk = handle.read()
        except OSError:
            chunk = b""
        if chunk:
            # 只消费到最后一个换行：尾部的半行留在下一轮连同续写一起读，
            # 偏移量停在半行起点，所以同一批字节不会被计两次（carry 会重复计）。
            cut = chunk.rfind(b"\n")
            if cut >= 0:
                _apply(chunk[:cut + 1], _STATE["totals"])
                _STATE["offset"] += cut + 1
        _STATE["at"] = now
        return dict(_STATE["totals"])


def used(usage_dir, key_id):
    """这个 Key 从建库以来的累计积分（拿不到就当 0）。"""
    if not key_id or not enabled():
        return 0.0
    try:
        return float(refresh(usage_dir).get(str(key_id), 0.0))
    except Exception:
        return 0.0


def totals(usage_dir):
    return refresh(usage_dir)


def reset():
    """丢掉缓存，下次访问整份重建（换日志目录、手工修数据后可用）。"""
    with _LOCK:
        _STATE.update(log=None, dev=0, ino=0, offset=0, carry=b"", totals={}, at=0.0)
