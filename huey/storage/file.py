import hashlib
import itertools
import os
import shutil
import struct
import threading
import time

from huey.utils import FileLock

from ._base import BaseStorage
from ._shared import EmptyData


class FileStorage(BaseStorage):
    """
    Simple file-system storage implementation.

    This storage implementation should NOT be used in production as it utilizes
    exclusive locks around all file-system operations. This is done to prevent
    race-conditions when reading from the file-system.
    """
    MAX_PRIORITY = 0xffff

    def __init__(self, name, path, levels=2, use_thread_lock=False,
                 **storage_kwargs):
        super(FileStorage, self).__init__(name, **storage_kwargs)

        self.path = path
        if os.path.exists(self.path) and not os.path.isdir(self.path):
            raise ValueError('path "%s" is not a directory' % path)
        if levels < 0 or levels > 4:
            raise ValueError('%s levels must be between 0 and 4' % self)

        self.queue_path = os.path.join(self.path, 'queue')
        self.schedule_path = os.path.join(self.path, 'schedule')
        self.result_path = os.path.join(self.path, 'results')
        self.counter_path = os.path.join(self.path, 'counters')
        self.levels = levels

        if use_thread_lock:
            self.lock = threading.Lock()
        else:
            self.lock_file = os.path.join(self.path, '.lock')
            self.lock = FileLock(self.lock_file)

    def _flush_dir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
            os.makedirs(path)

    def enqueue(self, data, priority=None):
        priority = int(priority or 0)
        if priority < 0: raise ValueError('priority must be a positive number')
        if priority > self.MAX_PRIORITY:
            raise ValueError('priority must be <= %s' % self.MAX_PRIORITY)

        with self.lock:
            if not os.path.exists(self.queue_path):
                os.makedirs(self.queue_path)

            # Encode the filename so that tasks are sorted by priority (desc) and
            # timestamp (asc).
            prefix = '%04x-%012x' % (
                self.MAX_PRIORITY - priority,
                int(time.time() * 1000))

            base = filename = os.path.join(self.queue_path, prefix)
            conflict = 0
            while os.path.exists(filename):
                conflict += 1
                filename = '%s.%03d' % (base, conflict)

            with open(filename, 'wb') as fh:
                fh.write(data)

    def _get_sorted_filenames(self, path):
        if not os.path.exists(path):
            return ()
        return [f for f in sorted(os.listdir(path)) if not f.endswith('.tmp')]

    def dequeue(self):
        with self.lock:
            filenames = self._get_sorted_filenames(self.queue_path)
            if not filenames:
                return

            filename = os.path.join(self.queue_path, filenames[0])
            tmp_dest = filename + '.tmp'
            os.rename(filename, tmp_dest)

            with open(tmp_dest, 'rb') as fh:
                data = fh.read()
            os.unlink(tmp_dest)
        return data

    def queue_size(self):
        return len(self._get_sorted_filenames(self.queue_path))

    def enqueued_items(self, limit=None):
        filenames = self._get_sorted_filenames(self.queue_path)[:limit]
        accum = []
        for filename in filenames:
            with open(os.path.join(self.queue_path, filename), 'rb') as fh:
                accum.append(fh.read())
        return accum

    def flush_queue(self):
        self._flush_dir(self.queue_path)

    def _timestamp_to_prefix(self, ts):
        ts = time.mktime(ts.timetuple()) + (ts.microsecond * 1e-6)
        return '%012x' % int(ts * 1000)

    def add_to_schedule(self, data, ts):
        with self.lock:
            if not os.path.exists(self.schedule_path):
                os.makedirs(self.schedule_path)

            ts_prefix = self._timestamp_to_prefix(ts)
            base = filename = os.path.join(self.schedule_path, ts_prefix)
            conflict = 0
            while os.path.exists(filename):
                conflict += 1
                filename = '%s.%03d' % (base, conflict)

            with open(filename, 'wb') as fh:
                fh.write(data)

    def read_schedule(self, ts):
        with self.lock:
            prefix = self._timestamp_to_prefix(ts)
            accum = []
            for basename in self._get_sorted_filenames(self.schedule_path):
                if basename[:12] > prefix:
                    break
                filename = os.path.join(self.schedule_path, basename)
                new_filename = filename + '.tmp'
                os.rename(filename, new_filename)
                accum.append(new_filename)

            tasks = []
            for filename in accum:
                with open(filename, 'rb') as fh:
                    tasks.append(fh.read())
                    os.unlink(filename)

        return tasks

    def schedule_size(self):
        return len(self._get_sorted_filenames(self.schedule_path))

    def scheduled_items(self, limit=None):
        filenames = self._get_sorted_filenames(self.schedule_path)[:limit]
        accum = []
        for filename in filenames:
            with open(os.path.join(self.schedule_path, filename), 'rb') as fh:
                accum.append(fh.read())
        return accum

    def flush_schedule(self):
        self._flush_dir(self.schedule_path)

    def path_for_key(self, key):
        if isinstance(key, str):
            key = key.encode('utf8')
        checksum = hashlib.md5(key).hexdigest()
        prefix = checksum[:self.levels]
        prefix_filename = itertools.chain(prefix, (checksum,))
        return os.path.join(self.result_path, *prefix_filename)

    def put_data(self, key, value, is_result=False):
        with self.lock:
            self._put_data(key, value)

    def _put_data(self, key, value):
        # Write a key/value pair. The lock must already be held.
        if isinstance(key, str):
            key = key.encode('utf8')

        filename = self.path_for_key(key)
        dirname = os.path.dirname(filename)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(filename, 'wb') as fh:
            key_len = len(key)
            fh.write(struct.pack('>I', key_len))
            fh.write(key)
            fh.write(value)

    def put_if_empty(self, key, value):
        with self.lock:
            if os.path.exists(self.path_for_key(key)):
                return False
            self._put_data(key, value)
            return True

    def _unpack_result(self, data):
        key_len, = struct.unpack('>I', data[:4])
        key = data[4:4 + key_len]
        if len(key) != key_len:
            return None, None
        return key, data[4 + key_len:]

    def peek_data(self, key):
        filename = self.path_for_key(key)
        if not os.path.exists(filename):
            return EmptyData

        with open(filename, 'rb') as fh:
            _, value = self._unpack_result(fh.read())

        # If file is corrupt or has been tampered with, return EmptyData.
        return value if value is not None else EmptyData

    def pop_data(self, key):
        filename = self.path_for_key(key)

        with self.lock:
            if not os.path.exists(filename):
                return EmptyData

            with open(filename, 'rb') as fh:
                _, value = self._unpack_result(fh.read())

            os.unlink(filename)

        # If file is corrupt or has been tampered with, return EmptyData.
        return value if value is not None else EmptyData

    def has_data_for_key(self, key):
        return os.path.exists(self.path_for_key(key))

    def _counter_filename(self, key):
        if isinstance(key, str):
            key = key.encode('utf8')
        return os.path.join(self.counter_path, hashlib.md5(key).hexdigest())

    def incr(self, key, amount=1):
        filename = self._counter_filename(key)
        with self.lock:
            if not os.path.exists(self.counter_path):
                os.makedirs(self.counter_path)
            try:
                with open(filename, 'rt') as fh:
                    value = int(fh.read()) + amount
            except Exception:
                value = amount
            with open(filename, 'wt') as fh:
                fh.write(str(value))

        return value

    def delete_counter(self, key):
        filename = self._counter_filename(key)
        with self.lock:
            if os.path.exists(filename):
                os.unlink(filename)

    def result_store_size(self):
        return sum(len(filenames) for _, _, filenames
                   in os.walk(self.result_path))

    def result_items(self):
        accum = {}
        for root, _, filenames in os.walk(self.result_path):
            for filename in filenames:
                path = os.path.join(root, filename)
                with open(path, 'rb') as fh:
                    key, value = self._unpack_result(fh.read())
                accum[key] = value
        return accum

    def flush_results(self):
        self._flush_dir(self.result_path)

    def flush_counters(self):
        self._flush_dir(self.counter_path)
