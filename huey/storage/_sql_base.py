import contextlib
import threading

from ._base import BaseStorage


class BaseSqlStorage(BaseStorage):
    begin_sql = 'begin'
    ddl = []

    def __init__(self, *args, **kwargs):
        create_tables = kwargs.pop('create_tables', True)
        super(BaseSqlStorage, self).__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self._conn = None
        if create_tables:
            self.initialize_schema()

    def close(self):
        if self._conn is None:
            return False
        with self.lock:
            self._conn.close()
            self._conn = None
        return True

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self._create_connection()
        return self._conn

    def _create_connection(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def db(self, commit=False, close=False):
        with self.lock:
            conn = self.conn
            cursor = conn.cursor()
            try:
                if commit: cursor.execute(self.begin_sql)
                yield cursor
            except Exception:
                if commit: conn.rollback()
                raise
            else:
                if commit: conn.commit()
            finally:
                cursor.close()
                if close:
                    conn.close()
                    self._conn = None

    def initialize_schema(self):
        with self.db(commit=True, close=True) as curs:
            for sql in self.ddl:
                curs.execute(sql)

    def sql(self, query, params=None, commit=False, results=False):
        with self.db(commit=commit) as curs:
            curs.execute(query, params or ())
            if results:
                return curs.fetchall()
