import heapq
import threading

from ._base import BaseStorage
from ._shared import EmptyData


class MemoryStorage(BaseStorage):
    def __init__(self, *args, **kwargs):
        super(MemoryStorage, self).__init__(*args, **kwargs)
        self._c = 0  # Counter to ensure FIFO behavior for queue.
        self._queue = []
        self._results = {}
        self._schedule = []
        self._counters = {}
        self._lock = threading.RLock()

    def enqueue(self, data, priority=None):
        with self._lock:
            self._c += 1
            priority = 0 if priority is None else -priority
            heapq.heappush(self._queue, (priority, self._c, data))

    def dequeue(self):
        with self._lock:
            try:
                _, _, data = heapq.heappop(self._queue)
            except IndexError:
                pass
            else:
                return data

    def queue_size(self):
        return len(self._queue)

    def enqueued_items(self, limit=None):
        items = [data for _, _, data in sorted(self._queue)]
        if limit:
            items = items[:limit]
        return items

    def flush_queue(self):
        self._queue = []

    def add_to_schedule(self, data, ts):
        with self._lock:
            heapq.heappush(self._schedule, (ts, data))

    def read_schedule(self, ts):
        with self._lock:
            accum = []
            while self._schedule:
                sts, data = heapq.heappop(self._schedule)
                if sts <= ts:
                    accum.append(data)
                else:
                    heapq.heappush(self._schedule, (sts, data))
                    break

        return accum

    def schedule_size(self):
        return len(self._schedule)

    def scheduled_items(self, limit=None):
        items = [data for _, data in sorted(self._schedule)]
        if limit:
            items = items[:limit]
        return items

    def flush_schedule(self):
        self._schedule = []

    def put_data(self, key, value, is_result=False):
        self._results[key] = value

    def peek_data(self, key):
        return self._results.get(key, EmptyData)

    def pop_data(self, key):
        return self._results.pop(key, EmptyData)

    def has_data_for_key(self, key):
        return key in self._results

    def put_if_empty(self, key, value):
        with self._lock:
            if key in self._results:
                return False
            self._results[key] = value
            return True

    def incr(self, key, amount=1):
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount
        return self._counters[key]

    def delete_counter(self, key):
        with self._lock:
            self._counters.pop(key, None)

    def result_store_size(self):
        return len(self._results)

    def result_items(self):
        return dict(self._results)

    def flush_results(self):
        self._results = {}

    def flush_counters(self):
        self._counters = {}
