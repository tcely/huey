import hashlib
import os
import re
import threading
import time

try:
    import psycopg
except ImportError:
    psycopg = None

from ._shared import ConfigurationError, EmptyData
from ._sql_base import BaseSqlStorage


class PostgresStorage(BaseSqlStorage):
    def __init__(self, name='huey', dsn=None, connection=None, blocking=True,
                 read_timeout=1, table_prefix='huey', create_tables=True,
                 **connection_params):
        if psycopg is None:
            raise ConfigurationError('"psycopg" (version 3.2 or newer) not '
                                     'found, cannot use Postgres storage '
                                     'backend. Run "pip install psycopg" to '
                                     'install.')
        self.dsn = dsn
        self.connection = connection  # Zero-arg callable returning conn.
        self.blocking = blocking
        self.read_timeout = read_timeout
        self.connection_params = connection_params

        prefix = re.sub('[^A-Za-z0-9_]', '', table_prefix)
        self.table_kv = prefix + '_kv'
        self.table_schedule = prefix + '_schedule'
        self.table_task = prefix + '_task'
        self.table_counter = prefix + '_counter'

        # Postgres channel names longer than 63 bytes raise "channel name
        # too long" from pg_notify(), which would break every enqueue.
        channel = '%s.q.%s' % (prefix, name)
        if len(channel.encode('utf-8')) > 63:
            digest = hashlib.md5(channel.encode('utf-8')).hexdigest()
            channel = 'huey.q.%s' % digest
        self.channel = channel

        self.ddl = tuple(q.format(p=prefix) for q in (
            'create table if not exists {p}_kv ('
            'queue text not null, key text not null, value bytea not null, '
            'primary key(queue, key))',

            'create table if not exists {p}_schedule ('
            'id bigserial primary key, queue text not null, '
            'data bytea not null, timestamp double precision not null)',

            'create index if not exists {p}_schedule_queue_timestamp '
            'on {p}_schedule (queue, timestamp)',

            'create table if not exists {p}_task ('
            'id bigserial primary key, queue text not null, '
            'data bytea not null, '
            'priority double precision not null default 0.0)',

            'create index if not exists {p}_task_queue_priority_id '
            'on {p}_task (queue, priority desc, id)',

            'create table if not exists {p}_counter ('
            'queue text not null, key text not null, '
            'value bigint not null default 0, primary key(queue, key))'))

        # Do not reuse conns across fork!
        self._inherited = []
        self._conn_pid = None

        # Each worker thread gets its own LISTEN connection on first
        # dequeue. A dead thread's connection is released by GC: psycopg
        # only sends the protocol Terminate from the creating process, so
        # this is safe on both sides of a fork.
        self._listen_local = threading.local()

        super(PostgresStorage, self).__init__(name, create_tables=create_tables)

    def _connect(self):
        if self.connection is not None:
            conn = self.connection()
        else:
            conn = psycopg.connect(self.dsn or '', **self.connection_params)
        conn.autocommit = True
        return conn
    _create_connection = _connect

    @property
    def conn(self):
        if self._conn is not None:
            if self._conn_pid != os.getpid():
                self._inherited.append(self._conn)
                self._conn = None
            elif self._conn.closed or self._conn.broken:
                self._close_quiet(self._conn)
                self._conn = None
        if self._conn is None:
            self._conn = self._connect()
            self._conn_pid = os.getpid()
        return self._conn

    def _close_quiet(self, conn):
        try:
            conn.close()
        except Exception:
            pass

    def close(self):
        local = self._listen_local
        conn = getattr(local, 'conn', None)
        if conn is not None:
            if local.pid == os.getpid():
                self._close_quiet(conn)
            else:
                self._inherited.append(conn)
            local.conn = None
        return super(PostgresStorage, self).close()

    def _listen_conn(self):
        local = self._listen_local
        conn = getattr(local, 'conn', None)
        if conn is not None and (local.pid != os.getpid() or conn.closed or
                                 conn.broken):
            if local.pid != os.getpid():
                self._inherited.append(conn)
            else:
                self._close_quiet(conn)
            conn = local.conn = None
        if conn is None:
            conn = self._connect()
            conn.execute('listen "%s"' % self.channel.replace('"', '""'))
            local.conn, local.pid = conn, os.getpid()
        return conn

    def enqueue(self, data, priority=None):
        with self.db(commit=True) as curs:
            curs.execute('insert into {} (queue, data, priority) '
                         'values (%s, %s, %s)'.format(self.table_task),
                         (self.name, data, priority or 0))
            curs.execute('select pg_notify(%s, %s)', (self.channel, ''))

    def _dequeue(self):
        with self.db() as curs:
            curs.execute('delete from {t} where id = ('
                         'select id from {t} where queue = %s '
                         'order by priority desc, id limit 1 '
                         'for update skip locked) '
                         'returning data'.format(t=self.table_task),
                         (self.name,))
            row = curs.fetchone()
        if row is not None:
            return bytes(row[0])

    def dequeue(self):
        data = self._dequeue()
        if data is not None or not self.blocking:
            return data

        conn = self._listen_conn()
        deadline = time.monotonic() + self.read_timeout
        while True:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                return None
            if not list(conn.notifies(timeout=timeout, stop_after=1)):
                return None
            data = self._dequeue()
            if data is not None:
                return data  # Otherwise another worker won, keep waiting.

    def queue_size(self):
        return self.sql('select count(*) from {} where queue = %s'.format(
            self.table_task), (self.name,), results=True)[0][0]

    def enqueued_items(self, limit=None):
        sql = ('select data from {} where queue = %s '
               'order by priority desc, id'.format(self.table_task))
        params = (self.name,)
        if limit is not None:
            sql += ' limit %s'
            params = (self.name, limit)

        return [bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_queue(self):
        self.sql('delete from {} where queue = %s'.format(self.table_task),
                 (self.name,))

    def add_to_schedule(self, data, ts):
        self.sql('insert into {} (queue, data, timestamp) '
                 'values (%s, %s, %s)'.format(self.table_schedule),
                 (self.name, data, ts.timestamp()))

    def read_schedule(self, ts):
        with self.db() as curs:
            curs.execute('delete from {t} where id in ('
                         'select id from {t} where queue = %s and '
                         'timestamp <= %s for update skip locked) '
                         'returning timestamp, id, data'.format(
                             t=self.table_schedule),
                         (self.name, ts.timestamp()))
            rows = curs.fetchall()
        return [bytes(data) for _, _, data in
                sorted(rows, key=lambda row: row[:2])]

    def schedule_size(self):
        return self.sql('select count(*) from {} where queue = %s'.format(
            self.table_schedule), (self.name,), results=True)[0][0]

    def scheduled_items(self, limit=None):
        sql = ('select data from {} where queue = %s '
               'order by timestamp'.format(self.table_schedule))
        params = (self.name,)
        if limit is not None:
            sql += ' limit %s'
            params = (self.name, limit)

        return [bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_schedule(self):
        self.sql('delete from {} where queue = %s'.format(
            self.table_schedule), (self.name,))

    def _key(self, key):
        return key.decode('utf-8') if isinstance(key, bytes) else key

    def put_data(self, key, value, is_result=False):
        self.sql('insert into {} (queue, key, value) values (%s, %s, %s) '
                 'on conflict (queue, key) do update set '
                 'value = excluded.value'.format(self.table_kv),
                 (self.name, self._key(key), value))

    def peek_data(self, key):
        res = self.sql('select value from {} where queue = %s and '
                       'key = %s'.format(self.table_kv),
                       (self.name, self._key(key)), results=True)
        return bytes(res[0][0]) if res else EmptyData

    def pop_data(self, key):
        with self.db() as curs:
            curs.execute('delete from {} where queue = %s and key = %s '
                         'returning value'.format(self.table_kv),
                         (self.name, self._key(key)))
            row = curs.fetchone()
        return bytes(row[0]) if row is not None else EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from {} where queue = %s and '
                             'key = %s'.format(self.table_kv),
                             (self.name, self._key(key)), results=True))

    def put_if_empty(self, key, value):
        with self.db() as curs:
            curs.execute('insert into {} (queue, key, value) '
                         'values (%s, %s, %s) '
                         'on conflict do nothing'.format(self.table_kv),
                         (self.name, self._key(key), value))
            return curs.rowcount == 1

    def incr(self, key, amount=1):
        with self.db() as curs:
            curs.execute('insert into {t} as c (queue, key, value) '
                         'values (%s, %s, %s) '
                         'on conflict (queue, key) do update set '
                         'value = c.value + excluded.value '
                         'returning value'.format(t=self.table_counter),
                         (self.name, self._key(key), amount))
            return curs.fetchone()[0]

    def delete_counter(self, key):
        self.sql('delete from {} where queue = %s and key = %s'.format(
            self.table_counter), (self.name, self._key(key)))

    def result_store_size(self):
        return self.sql('select count(*) from {} where queue = %s'.format(
            self.table_kv), (self.name,), results=True)[0][0]

    def result_items(self):
        res = self.sql('select key, value from {} where queue = %s'.format(
            self.table_kv), (self.name,), results=True)
        return dict((k, bytes(v)) for k, v in res)

    def flush_results(self):
        self.sql('delete from {} where queue = %s'.format(self.table_kv),
                 (self.name,))

    def flush_counters(self):
        self.sql('delete from {} where queue = %s'.format(
            self.table_counter), (self.name,))
