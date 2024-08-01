"""Original code taken from here:
 https://github.com/gilesbrown/gsqlite3/blob/fef400f1c5bcbc546772c827d3992e578ea5f905/gsqlite3.py
but heavily modified"""

import argparse
import shutil
import logging
import random
import sqlite3
from enum import Enum, auto
from pathlib import Path
from multiprocessing import Lock as process_lock
from multiprocessing.managers import SyncManager
from multiprocessing.synchronize import Lock
from pysqlcipher3 import dbapi2 as sqlcipher
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeAlias
from collections.abc import Sequence

from rotkehlchen.args import _positive_int_or_zero
from rotkehlchen.config import default_data_directory
from rotkehlchen.constants.misc import DEFAULT_SQL_VM_INSTRUCTIONS_CB, GLOBALDB_NAME, GLOBALDIR_NAME
from rotkehlchen.logging import TRACE, add_logging_level

if TYPE_CHECKING:
    from rotkehlchen.logging import RotkehlchenLogger

UnderlyingCursor: TypeAlias = sqlite3.Cursor | sqlcipher.Cursor  # pylint: disable=no-member
UnderlyingConnection: TypeAlias = sqlite3.Connection | sqlcipher.Connection  # pylint: disable=no-member
logger: 'RotkehlchenLogger' = logging.getLogger(__name__)  # type: ignore


class DBCursor:

    def __init__(self, connection: 'DBConnection', cursor: UnderlyingCursor) -> None:
        self._cursor = cursor
        self.connection = connection

    def __iter__(self) -> 'DBCursor':
        if __debug__:
            logger.trace(f'Getting iterator for cursor {self._cursor}')
        return self

    def __next__(self) -> Any:
        """
        We type this and other function returning Any since anything else has
        too many false positives. Same as typeshed:
        https://github.com/python/typeshed/blob/a750a42c65b77963ff097b6cbb6d36cef5912eb7/stdlib/sqlite3/dbapi2.pyi#L397
        """
        if __debug__:
            logger.trace(f'Get next item for cursor {self._cursor}')
        result = next(self._cursor, None)
        if result is None:
            if __debug__:
                logger.trace(f'Stopping iteration for cursor {self._cursor}')
            raise StopIteration

        if __debug__:
            logger.trace(f'Got next item for cursor {self._cursor}')
        return result

    def __enter__(self) -> 'DBCursor':
        return self

    def __exit__(
            self,
            exctype: type[BaseException] | None,
            value: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool:
        self.close()
        return True

    def execute(self, statement: str, *bindings: Sequence) -> 'DBCursor':
        if __debug__:
            logger.trace(f'EXECUTE {statement}')
        try:
            self._cursor.execute(statement, *bindings)
        except (sqlcipher.InterfaceError, sqlite3.InterfaceError):  # pylint: disable=no-member
            # Long story. Don't judge me. https://github.com/rotki/rotki/issues/5432
            logger.debug(f'{statement} with {bindings} failed due to https://github.com/rotki/rotki/issues/5432. Retrying')  # noqa: E501
            self._cursor.execute(statement, *bindings)

        if __debug__:
            logger.trace(f'FINISH EXECUTE {statement}')
        return self

    def executemany(self, statement: str, *bindings: Sequence[Sequence]) -> 'DBCursor':
        if __debug__:
            logger.trace(f'EXECUTEMANY {statement}')
        self._cursor.executemany(statement, *bindings)
        if __debug__:
            logger.trace(f'FINISH EXECUTEMANY {statement}')
        return self

    def executescript(self, script: str) -> 'DBCursor':
        """Remember this always issues a COMMIT before
        https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.executescript
        """
        if __debug__:
            logger.trace(f'EXECUTESCRIPT {script}')
        self._cursor.executescript(script)
        if __debug__:
            logger.trace(f'FINISH EXECUTESCRIPT {script}')
        return self

    def switch_foreign_keys(
            self,
            on_or_off: Literal['ON', 'OFF'],
            restart_transaction: bool = True,
    ) -> None:
        """
        Switches foreign keys ON or OFF depending on `on_or_off`. Important! When switching
        foreign keys a commit always happens which means that if you had a transaction, it might
        need to be restarted which this function does if `restart_transaction` is True.
        """
        self.executescript(f'PRAGMA foreign_keys={on_or_off};')
        if restart_transaction is True:
            self.execute('BEGIN TRANSACTION')

    def fetchone(self) -> Any:
        if __debug__:
            logger.trace('CURSOR FETCHONE')
        result = self._cursor.fetchone()
        if __debug__:
            logger.trace('FINISH CURSOR FETCHONE')
        return result

    def fetchmany(self, size: int | None = None) -> list[Any]:
        if __debug__:
            logger.trace(f'CURSOR FETCHMANY with {size=}')
        if size is None:
            size = self._cursor.arraysize
        result = self._cursor.fetchmany(size)
        if __debug__:
            logger.trace('FINISH CURSOR FETCHMANY')
        return result

    def fetchall(self) -> list[Any]:
        if __debug__:
            logger.trace('CURSOR FETCHALL')
        result = self._cursor.fetchall()
        if __debug__:
            logger.trace('FINISH CURSOR FETCHALL')
        return result

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int:
        return self._cursor.lastrowid  # type: ignore

    def close(self) -> None:
        self._cursor.close()


class DBConnectionType(Enum):
    USER = auto()
    TRANSIENT = auto()
    GLOBAL = auto()


# This is a global connection map to be able to get the connection from inside the
# progress handler. Having a global mapping and 3 different progress callbacks is
# a sort of ugly hack. If anybody knows of a better way to make it work let's improve it.
# With this approach we have named connections and a different progress callback per connection.
CONNECTION_MAP: dict[DBConnectionType, 'DBConnection'] = {}


def _progress_callback(connection: Optional['DBConnection']) -> int:
    """Needs to be a static function. Cannot be a connection class method
    or sqlite breaks in funny ways. Raises random Operational errors.
    """
    if __debug__:
        identifier = random.random()
        conn_type = connection._connection_type if connection else 'no connection'
        logger.trace(f'START progress callback for {conn_type} with id {identifier}')

    if connection is None:
        return 0

    if connection.in_callback.locked():
        # This solves the bug described in test_callback_segfault_complex. This works
        # since we are single threaded and if we get here and it's locked we know that
        # we should not wait since this is an edge case that can hit us if the connection gets
        # modified before we exit the callback. So we immediately exit the callback
        # without any sleep that would lead to context switching
        return 0

    # # without this rotkehlchen/tests/db/test_async.py::test_async_segfault fails
    # with connection.in_callback:
    #     if __debug__:
    #         logger.trace(f'Got in locked section of the progress callback for {connection.connection_type} with id {identifier}')  # noqa: E501
    #     gevent.sleep(0)
    #     if __debug__:
    #         logger.trace(f'Going out of the progress callback for {connection.connection_type} with id {identifier}')  # noqa: E501
    #     return 0


def user_callback() -> int:
    connection = CONNECTION_MAP.get(DBConnectionType.USER)
    return _progress_callback(connection)


def transient_callback() -> int:
    connection = CONNECTION_MAP.get(DBConnectionType.TRANSIENT)
    return _progress_callback(connection)


def global_callback() -> int:
    connection = CONNECTION_MAP.get(DBConnectionType.GLOBAL)
    return _progress_callback(connection)


CALLBACK_MAP = {
    DBConnectionType.USER: user_callback,
    DBConnectionType.TRANSIENT: transient_callback,
    DBConnectionType.GLOBAL: global_callback,
}


class DBConnection(SyncManager):
    def __init__(
            self,
            path: str | Path,
            connection_type: DBConnectionType,
            sql_vm_instructions_cb: int,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        CONNECTION_MAP[connection_type] = self
        self._conn: UnderlyingConnection
        self.in_callback = process_lock()
        self.transaction_lock = process_lock()
        self._connection_type = connection_type
        self.sql_vm_instructions_cb = sql_vm_instructions_cb
        self.write_greenlet_id: str | None = None
        if connection_type == DBConnectionType.GLOBAL:
            self._conn = sqlite3.connect(
                database=path,
                check_same_thread=False,
                isolation_level=None,
            )
        else:
            self._conn = sqlcipher.connect(  # pylint: disable=no-member
                database=str(path),
                check_same_thread=False,
                isolation_level=None,
            )
        self.set_progress_handler()

    def set_progress_handler(self) -> None:
        callback = CALLBACK_MAP.get(self._connection_type)
        self._conn.set_progress_handler(callback, self.sql_vm_instructions_cb)

    def execute(self, statement: str, *bindings: Sequence) -> DBCursor:
        if __debug__:
            logger.trace(f'DB CONNECTION EXECUTE {statement}')
        underlying_cursor = self._conn.execute(statement, *bindings)
        if __debug__:
            logger.trace(f'FINISH DB CONNECTION EXECUTEMANY {statement}')
        return DBCursor(connection=self, cursor=underlying_cursor)

    def executemany(self, statement: str, *bindings: Sequence[Sequence]) -> DBCursor:
        if __debug__:
            logger.trace(f'DB CONNECTION EXECUTEMANY {statement}')
        underlying_cursor = self._conn.executemany(statement, *bindings)
        if __debug__:
            logger.trace(f'FINISH DB CONNECTION EXECUTEMANY {statement}')
        return DBCursor(connection=self, cursor=underlying_cursor)

    def executescript(self, script: str) -> DBCursor:
        """Remember this always issues a COMMIT before
        https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.executescript
        """
        if __debug__:
            logger.trace(f'DB CONNECTION EXECUTESCRIPT {script}')
        underlying_cursor = self._conn.executescript(script)
        if __debug__:
            logger.trace(f'DB CONNECTION EXECUTESCRIPT {script}')
        return DBCursor(connection=self, cursor=underlying_cursor)

    def commit(self) -> None:
        with self.in_callback:
            if __debug__:
                logger.trace('START DB CONNECTION COMMIT')
            try:
                self._conn.commit()
            finally:
                if __debug__:
                    logger.trace('FINISH DB CONNECTION COMMIT')

    def rollback(self) -> None:
        with self.in_callback:
            if __debug__:
                logger.trace('START DB CONNECTION ROLLBACK')
            try:
                self._conn.rollback()
            finally:
                if __debug__:
                    logger.trace('FINISH DB CONNECTION ROLLBACK')

    def cursor(self) -> DBCursor:
        return DBCursor(connection=self, cursor=self._conn.cursor())

    def close(self) -> None:
        self._conn.close()
        CONNECTION_MAP.pop(self._connection_type, None)

    def get_lock(self):
        return self.transaction_lock

    def get_conn(self) -> UnderlyingConnection:
        return self._conn

    def get_conn_type(self) -> DBConnectionType:
        return self._connection_type

    def get_callback_lock(self) -> Lock:
        return self.in_callback


def app_args(prog: str, description: str) -> argparse.ArgumentParser:
    """Add the rotki db process arguments to the argument parser and return it"""
    p = argparse.ArgumentParser(
        prog=prog,
        description=description,
    )

    p.add_argument(
        '--data-dir',
        help='The directory where all data and configs are placed',
        type=str,
        default=None,
    )
    p.add_argument(
        '--sqlite-instructions',
        help='Instructions per sqlite context switch. Should be a positive integer or zero to disable.',  # noqa: E501
        default=DEFAULT_SQL_VM_INSTRUCTIONS_CB,
        type=_positive_int_or_zero,
    )
    return p


if __name__ == '__main__':
    add_logging_level('TRACE', TRACE)
    # Creates and run a server instance.
    args = app_args(prog='rotki', description="rotki's db process").parse_args()
    if args.data_dir is None:
        data_dir = default_data_directory()
    else:
        data_dir = Path(args.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)

    global_dir = data_dir / GLOBALDIR_NAME
    global_dir.mkdir(parents=True, exist_ok=True)
    dbname = global_dir / GLOBALDB_NAME
    if not dbname.is_file():
        # if no global db exists, copy the built-in file
        root_dir = Path(__file__).resolve().parent.parent
        builtin_data_dir = root_dir / 'data'
        shutil.copyfile(builtin_data_dir / GLOBALDB_NAME, global_dir / GLOBALDB_NAME)

    server = DBConnection(
        path=global_dir / GLOBALDB_NAME,
        connection_type=DBConnectionType.GLOBAL,
        sql_vm_instructions_cb=args.sqlite_instructions,
        address=('', 50000),
        authkey=b'123',
    )
    server.register('set_progress_handler', server.set_progress_handler)
    server.register('execute', server.execute)
    server.register('executemany', server.executemany)
    server.register('executescript', server.executescript)
    server.register('commit', server.commit)
    server.register('rollback', server.rollback)
    server.register('cursor', server.cursor)
    server.register('close', server.close)
    server.register('get_lock', server.get_lock)
    server.register('get_conn', server.get_conn)
    server.register('get_conn_type', server.get_conn_type)
    server.register('get_callback_lock', server.get_callback_lock)
    import threading
    print(threading.get_native_id())

    server.get_server().serve_forever()
