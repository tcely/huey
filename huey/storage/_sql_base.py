import collections
import contextlib
import threading

from ._base import BaseStorage


def _value_error_msg(name, /, expected, actual, example=None):
    def suffix(length):
        return '' if 1 == length else 's'

    if expected != actual:
        if example is None:
            example = ''
        return (
            f'SQL Execution Error: {name} expected exactly '
            f'{expected} column{suffix(expected)}{example}, '
            f'but rows contain {actual} column{suffix(actual)}. '
            f'Check your SELECT clause.'
        )


class BaseSqlStorage(BaseStorage):
    begin_sql = 'begin'
    ddl = ()

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

    def _first(self, results):
        error_msg = (
            'SQL Execution Error: _first expected at least 1 {}. '
            'Check your SELECT clause.'
        )
        if not results:
            raise ValueError(error_msg.format('row'))

        actual = len(results[0])
        if 1 > actual:
            raise ValueError(error_msg.format('column'))

        return results[0][0]

    def _flatten(self, results):
        """Safely flattens a database result of 1-column rows into a 1D list.

        Loudly fails if rows do not have exactly 1 column.
        """
        if not results:
            return []

        actual = len(results[0])
        error_msg = _value_error_msg('_flatten', expected=1, actual=actual, example=' (value,)')
        if error_msg:
            raise ValueError(error_msg)

        return [item for (item,) in results]

    def _to_dict(self, results):
        """Safely converts a database result of 2-column rows (key, value) into a dict.

        Loudly fails if rows do not have exactly 2 columns.
        """
        if not results:
            return {}

        actual = len(results[0])
        error_msg = _value_error_msg('_to_dict', expected=2, actual=actual, example=' (key, value)')
        if error_msg:
            raise ValueError(error_msg)

        return dict(results)

    def _to_dict_of_lists(self, results):
        """Safely groups a database result of 2-column rows into a dict of lists.

        Loudly fails if rows do not have exactly 2 columns.
        """
        if not results:
            return {}

        actual = len(results[0])
        error_msg = _value_error_msg('_to_dict_of_lists', expected=2, actual=actual, example=' (key, value)')
        if error_msg:
            raise ValueError(error_msg)

        grouped = collections.defaultdict(list)
        for key, value in results:
            grouped[key].append(value)
        return dict(grouped)
