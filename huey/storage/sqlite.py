try:
    import sqlite3
except ImportError:
    sqlite3 = None

from ._base import BaseStorage
from ._shared import ConfigurationError, EmptyData
from ._sql_base import BaseSqlStorage


class SqliteStorage(BaseSqlStorage):
    begin_sql = 'begin exclusive'
    integrity_error = getattr(sqlite3, 'IntegrityError', None)
    sqlite_version_info = getattr(sqlite3, 'sqlite_version_info', None)
    table_kv = ('create table if not exists kv ('
                'queue text not null, key text not null, value blob not null, '
                'primary key(queue, key))')
    table_sched = ('create table if not exists schedule ('
                   'id integer not null primary key, queue text not null, '
                   'data blob not null, timestamp real not null)')
    index_sched = ('create index if not exists schedule_queue_timestamp '
                   'on schedule (queue, timestamp)')
    table_task = ('create table if not exists task ('
                  'id integer not null primary key, queue text not null, '
                  'data blob not null, priority real not null default 0.0)')
    index_task = ('create index if not exists task_priority_id on task '
                  '(priority desc, id asc)')
    table_counter = ('create table if not exists counter ('
                     'queue text not null, key text not null, '
                     'value integer not null default 0, '
                     'primary key(queue, key))')
    ddl = (table_kv, table_sched, index_sched, table_task, index_task,
           table_counter)

    def __init__(self, name='huey', filename='huey.db', cache_mb=8,
                 fsync=None, journal_mode='wal', timeout=5, strict_fifo=False,
                 create_tables=True, **kwargs):
        if sqlite3 is None:
            raise ConfigurationError('"sqlite3" not found. '
                                     'Python 3 should have included this.')
        self.filename = filename
        self._cache_mb = cache_mb
        self._fsync = fsync
        self._journal_mode = journal_mode
        self._timeout = timeout  # Busy timeout in seconds, default is 5.
        self._conn_kwargs = kwargs

        # By default Sqlite may reuse rowids when rows are removed. This means
        # that SqliteHuey may not strictly be a FIFO. If strict FIFO ordering
        # is needed, then we will utilize Sqlite's AUTOINCREMENT functionality,
        # which prevents deleted rowids from being reused.
        # NOTE: changing an existing database is not supported, so you will
        # need to delete and re-create it to change this value.
        if strict_fifo:
            ddl = list(self.ddl)
            ddl[3] = self.table_task.replace(
                'primary key',
                'primary key autoincrement')
            self.ddl = tuple(ddl)

        self.to_blob = memoryview

        super(SqliteStorage, self).__init__(name, create_tables=create_tables)

    def _create_connection(self):
        conn = sqlite3.connect(self.filename, timeout=self._timeout,
                               check_same_thread=False,
                               **self._conn_kwargs)
        conn.isolation_level = None  # Autocommit mode.
        conn.execute('pragma journal_mode="%s"' % self._journal_mode)
        if self._cache_mb:
            conn.execute('pragma cache_size=%s' % (-1000 * self._cache_mb))
        if self._fsync is not None:
            conn.execute('pragma synchronous=%s' % (2 if self._fsync else 0))
        return conn

    def enqueue(self, data, priority=None):
        self.sql('insert into task (queue, data, priority) values (?, ?, ?)',
                 (self.name, self.to_blob(data), priority or 0), commit=True)

    def dequeue(self):
        with self.db(commit=True) as curs:
            curs.execute('select id, data from task where queue = ? '
                         'order by priority desc, id limit 1', (self.name,))
            result = curs.fetchone()
            if result is not None:
                tid, data = result
                curs.execute('delete from task where id = ?', (tid,))
                if curs.rowcount == 1:
                    return data

    def queue_size(self):
        return self._first(self.sql('select count(id) from task where queue=?',
                                    (self.name,), results=True))

    def enqueued_items(self, limit=None):
        sql = 'select data from task where queue=? order by priority desc, id'
        params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params += (limit,)

        return self._flatten(self.sql(sql, params, results=True))

    def flush_queue(self):
        self.sql('delete from task where queue=?', (self.name,), commit=True)

    def add_to_schedule(self, data, ts):
        params = (self.name, self.to_blob(data), ts.timestamp())
        self.sql('insert into schedule (queue, data, timestamp) '
                 'values (?, ?, ?)', params, commit=True)

    def read_schedule(self, ts):
        with self.db(commit=True) as curs:
            params = (self.name, ts.timestamp())
            curs.execute('select id, data from schedule where '
                         'queue = ? and timestamp <= ?', params)
            id_list, data = [], []
            for task_id, task_data in curs.fetchall():
                id_list.append(task_id)
                data.append(task_data)
            if id_list:
                plist = ','.join('?' * len(id_list))
                curs.execute('delete from schedule where id IN (%s)' % plist,
                             id_list)
            return data

    def schedule_size(self):
        return self._first(self.sql('select count(id) from schedule where queue=?',
                                    (self.name,), results=True))

    def scheduled_items(self, limit=None):
        sql = 'select data from schedule where queue=? order by timestamp'
        params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params += (limit,)

        return self._flatten(self.sql(sql, params, results=True))

    def flush_schedule(self):
        self.sql('delete from schedule where queue = ?', (self.name,), True)

    def put_data(self, key, value, is_result=False):
        self.sql('insert or replace into kv (queue, key, value) '
                 'values (?, ?, ?)',
                 (self.name, key, self.to_blob(value)), True)

    def peek_data(self, key):
        res = self.sql('select value from kv where queue = ? and key = ?',
                       (self.name, key), results=True)
        return self._first(res) if res else EmptyData

    def pop_data(self, key):
        with self.db(commit=True) as curs:
            if self.sqlite_version_info >= (3, 35, 0):
                curs.execute('delete from kv where queue = ? and key = ? '
                             'returning value', (self.name, key))
                result = curs.fetchone()
                if result is not None:
                    return result[0]
            else:
                curs.execute('select value from kv where queue = ? and key = ?',
                             (self.name, key))
                result = curs.fetchone()
                if result is not None:
                    curs.execute('delete from kv where queue=? and key=?',
                                 (self.name, key))
                    if curs.rowcount == 1:
                        return result[0]
            return EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from kv where queue=? and key=?',
                             (self.name, key), results=True))

    def put_if_empty(self, key, value):
        try:
            with self.db(commit=True) as curs:
                curs.execute('insert or abort into kv '
                             '(queue, key, value) values (?, ?, ?)',
                             (self.name, key, self.to_blob(value)))
        except self.integrity_error:
            return False
        else:
            return True

    def incr(self, key, amount=1):
        if not self.sqlite_version_info >= (3, 24, 0):
            raise NotImplementedError('SQLite 3.24 or newer is required.')
        insert_sql = (
            'insert into counter (queue, key, value) '
            'values (?, ?, ?) on conflict (queue, key) '
            'do update set value = value + ?'
        )
        select_counter = True
        if self.sqlite_version_info >= (3, 35, 0):
            insert_sql += ' returning value'
            select_counter = False
        with self.db(commit=True) as curs:
            curs.execute(insert_sql, (self.name, key, amount, amount))
            if select_counter:
                curs.execute('select value from counter '
                             'where queue = ? and key = ?',
                             (self.name, key))
            value, = curs.fetchone()

        return value

    def delete_counter(self, key):
        self.sql('delete from counter where queue = ? and key = ?',
                 (self.name, key), commit=True)

    def result_store_size(self):
        return self._first(self.sql('select count(*) from kv where queue=?', (self.name,),
                                    results=True))

    def result_items(self):
        res = self.sql('select key, value from kv where queue=?', (self.name,),
                       results=True)
        return dict((k, v) for k, v in res)

    def flush_results(self):
        self.sql('delete from kv where queue=?', (self.name,), True)

    def flush_counters(self):
        self.sql('delete from counter where queue=?', (self.name,), True)
