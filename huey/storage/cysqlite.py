try:
    import cysqlite
except ImportError:
    cysqlite = None

from ._shared import ConfigurationError
from .sqlite import SqliteStorage


class CySqliteStorage(SqliteStorage):
    def __init__(self, name='huey', filename='huey.db', pragmas=None,
                 timeout=5, strict_fifo=False, create_tables=True, **kwargs):
        if cysqlite is None:
            raise ConfigurationError('"cysqlite" not found. Run "pip install '
                                     'cysqlite" to install.')
        self.integrity_error = cysqlite.IntegrityError
        self.sqlite_version_info = cysqlite.sqlite_version_info

        # Normalize hard-coded params to generic pragmas.
        pragmas = dict(pragmas or {})
        pragmas.setdefault('journal_mode', 'wal')
        if 'journal_mode' in kwargs:
            pragmas['journal_mode'] = kwargs.pop('journal_mode') or 'wal'
        if 'cache_mb' in kwargs:
            pragmas['cache_size'] = kwargs.pop('cache_mb') * -1000
        if 'fsync' in kwargs:
            pragmas['synchronous'] = 2 if kwargs.pop('fsync') else 0

        super(CySqliteStorage, self).__init__(
            name,
            filename,
            timeout=timeout,
            strict_fifo=strict_fifo,
            create_tables=create_tables,
            pragmas=pragmas,
            **kwargs)

    def _create_connection(self):
        return cysqlite.connect(self.filename, timeout=self._timeout,
                                **self._conn_kwargs)
