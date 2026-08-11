from ._base import BaseStorage
from ._shared import EmptyData


class BlackHoleStorage(BaseStorage):
    def enqueue(self, data, priority=None): pass
    def dequeue(self): pass
    def queue_size(self): return 0
    def enqueued_items(self, limit=None): return []
    def flush_queue(self): pass
    def add_to_schedule(self, data, ts): pass
    def read_schedule(self, ts): return []
    def schedule_size(self): return 0
    def scheduled_items(self, limit=None): return []
    def flush_schedule(self): pass
    def put_data(self, key, value, is_result=False): pass
    def peek_data(self, key): return EmptyData
    def pop_data(self, key): return EmptyData
    def has_data_for_key(self, key): return False
    def incr(self, key, amount=1): return amount
    def delete_counter(self, key): pass
    def result_store_size(self): return 0
    def result_items(self): return {}
    def flush_results(self): pass
    def flush_counters(self): pass
