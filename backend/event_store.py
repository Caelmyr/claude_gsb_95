"""事件存储：按小时分片的 JSON 文件 + 内存缓冲批量落盘。

高频事件流下若每条事件都「读-改-写」整个小时分片，会退化为 O(n^2)。因此：
- 内存按小时聚合缓冲，达到阈值或定时批量 flush 到对应小时分片文件；
- 落盘仍是标准 JSON 数组文件（events/YYYYMMDD/HH.json），符合「事件按小时分片」；
- 查询时合并「磁盘分片 + 内存未落盘缓冲」，保证读到最新数据；
- 后台守护线程定时 flush，进程退出前再全量 flush，避免丢事件。
"""
import os
import threading
import time

from backend import config
from backend.storage import atomic_write_json, read_json


def _hour_key(ts):
    ts = ts - 8 * 3600
    t = time.gmtime(ts)
    y = t.tm_year
    mo = t.tm_mon
    d = t.tm_mday
    h = t.tm_hour
    day = f"{y:04d}{mo:02d}{d:02d}"
    hour = f"{h:02d}"
    return f"{day}/{hour}"


def _hour_path(hour_key):
    return os.path.join(config.EVENTS_DIR, hour_key + ".json")


def _merge_events(existing, incoming):
    merged = list(existing)
    merged.extend(incoming)
    return merged


class EventStore:
    def __init__(self, flush_threshold=200, flush_interval=2.0):
        self.flush_threshold = flush_threshold
        self.flush_interval = flush_interval
        self._buffer = {}          # hour_key -> list[event]
        self._dirty = set()
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------
    def _flush_loop(self):
        while not self._stop.is_set():
            self._stop.wait(self.flush_interval)
            try:
                self.flush_all()
            except Exception:
                pass

    def flush_all(self):
        with self._lock:
            for key in list(self._dirty):
                self._flush_locked(key)
                self._dirty.discard(key)

    def _flush_locked(self, hour_key):
        events = self._buffer.get(hour_key, [])
        if not events:
            return
        path = _hour_path(hour_key)
        existing = read_json(path, {"events": []})
        merged = _merge_events(existing.get("events", []), events)
        atomic_write_json(path, {"events": merged})
        self._buffer[hour_key] = []

    def add(self, event, ts=None):
        if ts is None:
            ts = event.get("ts") or time.time()
        key = _hour_key(ts)
        with self._lock:
            buf = self._buffer.setdefault(key, [])
            buf.append(event)
            self._dirty.add(key)
            if len(buf) >= self.flush_threshold:
                self._flush_locked(key)
                self._dirty.discard(key)

    def stop(self):
        self._stop.set()
        self.flush_all()

    # ------------------------------------------------------------------
    def _load_hour(self, hour_key):
        path = _hour_path(hour_key)
        data = read_json(path, {"events": []})
        return data.get("events", [])

    def query(self, start_ts=None, end_ts=None, limit=None):
        """按时间范围查询事件（含内存缓冲），最新在前。"""
        now = time.time()
        if end_ts is None:
            end_ts = now
        if start_ts is None:
            start_ts = end_ts - 3600

        # 涉及的小时键
        t = int(start_ts) // 3600 * 3600
        keys = []
        while t <= end_ts:
            keys.append(_hour_key(t))
            t += 3600

        result = []
        with self._lock:
            for key in keys:
                on_disk = self._load_hour(key)
                in_mem = self._buffer.get(key, [])
                result.extend(on_disk)
                result.extend(in_mem)

        result = [e for e in result if start_ts <= e.get("ts", 0) <= end_ts]
        result.sort(key=lambda e: e.get("ts", 0), reverse=True)
        if limit:
            result = result[:limit]
        return result

    def recent(self, limit=100):
        return self.query(limit=limit)

    def stats(self):
        with self._lock:
            buffered = sum(len(v) for v in self._buffer.values())
            dirty = len(self._dirty)
        # 磁盘上的真实分片情况（遍历小时分片文件统计）
        shards = 0
        disk_events = 0
        if os.path.isdir(config.EVENTS_DIR):
            for day in os.listdir(config.EVENTS_DIR):
                day_dir = os.path.join(config.EVENTS_DIR, day)
                if not os.path.isdir(day_dir):
                    continue
                for name in os.listdir(day_dir):
                    if (name.endswith(".json") and not name.startswith(".tmp_")
                            and ".bak" not in name):
                        shards += 1
                        data = read_json(os.path.join(day_dir, name), {"events": []})
                        disk_events += len(data.get("events", []))
        return {
            "buffered": buffered,                # 未落盘事件数（内存缓冲中）
            "pending": buffered,                 # 待落盘事件数（同 buffered）
            "dirty_hours": dirty,                # 有待落盘数据的小时分片数
            "shards": shards,                    # 磁盘上的小时分片文件数
            "disk_events": disk_events,          # 已落盘事件数
            "total": disk_events + buffered,     # 事件总数（磁盘 + 内存）
        }
