from ._base import BaseStorage

from .discard import BlackHoleStorage
from .file import FileStorage
from .memory import MemoryStorage


__all__ = [
    'BaseStorage',
    'BlackHoleStorage',
    'FileStorage',
    'MemoryStorage',
]

from .cysqlite import cysqlite
from .cysqlite import CySqliteStorage
if cysqlite is not None:
    __all__.append('CySqliteStorage')

from .postgres import psycopg
from .postgres import PostgresStorage
if psycopg is not None:
    __all__.append('PostgresStorage')

from .redis import Redis
from .redis import (
    RedisStorage,
    RedisPriorityQueue,
    PriorityRedisStorage,
    PriorityRedisExpireStorage,
    RedisExpireStorage,
)
if Redis is not None:
    __all__.extend([
        'RedisStorage',
        'RedisPriorityQueue',
        'PriorityRedisStorage',
        'PriorityRedisExpireStorage',
        'RedisExpireStorage',
    ])

from .sqlite import sqlite3
from .sqlite import SqliteStorage
if sqlite3 is not None:
    __all__.append('SqliteStorage')
