import re
import struct

from functools import cached_property

try:
    from redis import ConnectionPool
    from redis import Redis
    from redis.exceptions import ConnectionError
    from redis.exceptions import TimeoutError
except ImportError:
    ConnectionPool = Redis = ConnectionError = TimeoutError = None

from ._base import BaseStorage
from ._shared import ConfigurationError, EmptyData, convert_ts, int_time


# A custom lua script to pass to redis that will read tasks from the schedule
# and atomically pop them from the sorted set and return them. It won't return
# anything if it isn't able to remove the items it reads.
SCHEDULE_POP_LUA = """\
local unix_ts = tonumber(ARGV[1])
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end"""


class RedisStorage(BaseStorage):
    priority = False  # Use PriorityRedisStorage instead. Requires Redis>=5.0.
    redis_client = Redis

    def __init__(self, name='huey', blocking=True, read_timeout=1,
                 connection_pool=None, url=None, client_name=None,
                 notify_result=False, notify_result_ttl=86400,
                 **connection_params):

        if Redis is None:
            raise ConfigurationError('"redis" python module not found, cannot '
                                     'use Redis storage backend. Run "pip '
                                     'install redis" to install.')

        # Drop common empty values from the connection_params.
        for p in ('host', 'port', 'db'):
            if p in connection_params and connection_params[p] is None:
                del connection_params[p]

        if sum(1 for p in (url, connection_pool, connection_params) if p) > 1:
            raise ConfigurationError(
                'The connection configuration is over-determined. '
                'Please specify only one of the following: '
                '"url", "connection_pool", or "connection_params"')

        if url:
            connection_pool = ConnectionPool.from_url(url)
        elif connection_pool is None:
            connection_pool = ConnectionPool(**connection_params)

        self.pool = connection_pool
        self.conn = self.redis_client(connection_pool=connection_pool)
        self.connection_params = connection_params
        self._pop = self.conn.register_script(SCHEDULE_POP_LUA)

        self.name = self.clean_name(name)
        self.queue_key = 'huey.redis.%s' % self.name
        self.schedule_key = 'huey.schedule.%s' % self.name
        self.result_key = 'huey.results.%s' % self.name
        self.counter_key = 'huey.counters.%s' % self.name
        self.notify_prefix = 'huey.notify.%s.' % self.name
        self.notify_result = notify_result  # Use result notification.
        self.notify_result_ttl = notify_result_ttl

        if client_name is not None:
            self.conn.client_setname(client_name)

        self.blocking = blocking
        self.read_timeout = read_timeout

    @cached_property
    def redis_version(self):
        # Server version, used only to clamp BLPOP timeouts for redis < 6.
        try:
            version = str(self.conn.info()['redis_version'])
        except Exception:
            version = '0.0.0'  # Assume old, int timeouts always work.
        return tuple(int(i) if i.isdigit() else 999
                     for i in version.split('.'))

    def clean_name(self, name):
        return re.sub('[^A-Za-z0-9_]', '', name)

    def convert_ts(self, ts):
        return convert_ts(ts)

    def enqueue(self, data, priority=None):
        if priority:
            raise NotImplementedError('Task priorities are not supported by '
                                      'this storage.')
        self.conn.lpush(self.queue_key, data)

    def dequeue(self):
        if self.blocking:
            try:
                return self.conn.brpop(
                    self.queue_key,
                    timeout=self.read_timeout)[1]
            except (TimeoutError, TypeError, IndexError):
                # Unfortunately, there is no way to differentiate a socket
                # timing out and a host being unreachable. ConnectionError is
                # allowed to propagate, however, so the worker logs the error
                # and applies backoff, rather than busy-looping silently.
                return None
        else:
            return self.conn.rpop(self.queue_key)

    def queue_size(self):
        return self.conn.llen(self.queue_key)

    def enqueued_items(self, limit=None):
        if limit:
            # Take items from the consumption end of the list, e.g. the next
            # `limit` tasks to be dequeued.
            return self.conn.lrange(self.queue_key, -limit, -1)[::-1]
        return self.conn.lrange(self.queue_key, 0, -1)[::-1]

    def flush_queue(self):
        self.conn.delete(self.queue_key)

    def add_to_schedule(self, data, ts):
        self.conn.zadd(self.schedule_key, {data: self.convert_ts(ts)})

    def read_schedule(self, ts):
        unix_ts = self.convert_ts(ts)
        # invoke the redis lua script that will atomically pop off
        # all the tasks older than the given timestamp
        tasks = self._pop(keys=[self.schedule_key], args=[unix_ts])
        return [] if tasks is None else tasks

    def schedule_size(self):
        return self.conn.zcard(self.schedule_key)

    def scheduled_items(self, limit=None):
        stop = limit - 1 if limit else -1
        return self.conn.zrange(self.schedule_key, 0, stop, withscores=False)

    def flush_schedule(self):
        self.conn.delete(self.schedule_key)

    def _notify(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf8')
        nkey = self.notify_prefix + key
        pipe = self.conn.pipeline()
        pipe.lpush(nkey, b'1')
        pipe.expire(nkey, self.notify_result_ttl)
        pipe.execute()

    def put_data(self, key, value, is_result=False):
        self.conn.hset(self.result_key, key, value)
        if is_result and self.notify_result:
            self._notify(key)

    def peek_data(self, key):
        pipe = self.conn.pipeline()
        pipe.hexists(self.result_key, key)
        pipe.hget(self.result_key, key)
        exists, val = pipe.execute()
        return EmptyData if not exists else val

    def pop_data(self, key):
        pipe = self.conn.pipeline()
        pipe.hexists(self.result_key, key)
        pipe.hget(self.result_key, key)
        pipe.hdel(self.result_key, key)
        exists, val, n = pipe.execute()
        return EmptyData if not exists else val

    def wait_result(self, key, timeout=None, backoff=1.15, max_delay=1.0):
        if not self.notify_result:
            return super(RedisStorage, self).wait_result(key, timeout,
                                                         backoff, max_delay)

        if self.has_data_for_key(key):
            return True
        nkey = self.notify_prefix + key
        timeout = timeout or 0
        if timeout > 0 and self.redis_version[0] < 6:
            timeout = max(1, int(timeout))  # Timeout must be int for R < 6.
        try:
            result = self.conn.blpop(nkey, timeout=timeout)
        except (ConnectionError, TimeoutError):
            return False

        if result is not None:
            self.conn.delete(nkey)
            return True

        return False

    def has_data_for_key(self, key):
        return self.conn.hexists(self.result_key, key)

    def put_if_empty(self, key, value):
        return self.conn.hsetnx(self.result_key, key, value)

    def incr(self, key, amount=1):
        return self.conn.hincrby(self.counter_key, key, amount)

    def delete_counter(self, key):
        self.conn.hdel(self.counter_key, key)

    def result_store_size(self):
        return self.conn.hlen(self.result_key)

    def result_items(self):
        return self.conn.hgetall(self.result_key)

    def flush_results(self):
        self.conn.delete(self.result_key)

    def flush_counters(self):
        self.conn.delete(self.counter_key)


class RedisExpireStorage(RedisStorage):
    # Redis storage subclass that adds expiration to task result values. Since
    # the Redis server handles deleting our results after the expiration time,
    # this storage layer will not delete the results when they are read.
    def __init__(self, name='huey', expire_time=86400, *args, **kwargs):
        super(RedisExpireStorage, self).__init__(name, *args, **kwargs)

        self._expire_time = expire_time

        self.result_prefix = rp = b'huey.r.%s.' % self.name.encode('utf8')
        self.counter_prefix = cp = b'huey.c.%s.' % self.name.encode('utf8')

        encode = lambda s: s if isinstance(s, bytes) else s.encode('utf8')
        self.result_key = lambda k: rp + encode(k)
        self.counter_key = lambda k: cp + encode(k)

    def put_data(self, key, value, is_result=False):
        if is_result:
            # We only want to expire task result data. If we are storing an
            # important metadata like a revocation key, we need to preserve it.
            self.conn.set(self.result_key(key), value, ex=self._expire_time)
            if self.notify_result:
                self._notify(key)
        else:
            self.conn.set(self.result_key(key), value)

    def peek_data(self, key):
        pipe = self.conn.pipeline()
        pipe.exists(self.result_key(key))
        pipe.get(self.result_key(key))
        exists, val = pipe.execute()
        return EmptyData if not exists else val

    # Here we explicitly prevent result items from being removed by using the
    # same implementation for "pop" (get and delete) as we do for "peek"
    # (non-destructive read).
    pop_data = peek_data

    def delete_data(self, key):
        return self.conn.delete(self.result_key(key))

    def has_data_for_key(self, key):
        return self.conn.exists(self.result_key(key)) != 0

    def put_if_empty(self, key, value):
        return self.conn.setnx(self.result_key(key), value)

    def incr(self, key, amount=1):
        res = self.conn.incr(self.counter_key(key), amount)
        self.conn.expire(self.counter_key(key), self._expire_time)
        return res

    def delete_counter(self, key):
        self.conn.delete(self.counter_key(key))

    def _result_keys(self):
        return self.conn.scan_iter(match=self.result_prefix + b'*')

    def result_store_size(self):
        return len(list(self._result_keys()))

    def result_items(self):
        keys = list(self._result_keys())
        accum = {}
        if keys:
            pfx_len = len(self.result_prefix)
            for key, value in zip(keys, self.conn.mget(keys)):
                accum[key[pfx_len:]] = value
        return accum

    def _counter_keys(self):
        return self.conn.scan_iter(match=self.counter_prefix + b'*')

    def flush_results(self):
        keys = list(self._result_keys())
        if keys:
            self.conn.delete(*keys)

    def flush_counters(self):
        keys = list(self._counter_keys())
        if keys:
            self.conn.delete(*keys)


class RedisPriorityQueue(object):
    priority = True

    def enqueue(self, data, priority=None):
        priority = 0 if priority is None else -priority
        # Prefix the message with an encoded timestamp to ensure that messages
        # created with the same priority are stored in the correct order. Since
        # the underlying data-type is a sorted-set, this also prevents multiple
        # identical messages, except they are enqueued on the same microsecond,
        # from being treated as a single item.
        prefix = struct.pack('>Q', int_time(1e6))
        self.conn.zadd(self.queue_key, {prefix + data: priority})

    def dequeue(self):
        if self.blocking:
            try:
                # BZPOPMIN returns (key, data, score).
                _, res, _ = self.conn.bzpopmin(
                    self.queue_key,
                    timeout=self.read_timeout)
            except (TimeoutError, TypeError, IndexError):
                # Unfortunately, there is no way to differentiate a socket
                # timing out and a host being unreachable. ConnectionError is
                # allowed to propagate, however, so the worker logs the error
                # and applies backoff, rather than busy-looping silently.
                return
            else:
                return res[8:]
        else:
            # ZPOPMIN returns a list of (data, score) 2-tuples.
            items = self.conn.zpopmin(self.queue_key, count=1)
            if items:
                return items[0][0][8:]  # [(prefix+data, score)].

    def queue_size(self):
        return self.conn.zcard(self.queue_key)

    def enqueued_items(self, limit=None):
        items = self.conn.zrange(self.queue_key, 0, limit - 1 if limit else -1)
        return [item[8:] for item in items]  # Unprefix the data.


class PriorityRedisStorage(RedisPriorityQueue, RedisStorage): pass


class PriorityRedisExpireStorage(RedisPriorityQueue, RedisExpireStorage): pass
