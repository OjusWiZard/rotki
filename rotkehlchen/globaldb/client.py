from gevent import monkey
monkey.patch_all()

import gevent
import logging
from abc import abstractmethod
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from multiprocessing.managers import BaseManager
from multiprocessing.synchronize import Lock
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from rotkehlchen.db.checks import sanity_check_impl
from rotkehlchen.db.minimized_schema import MINIMIZED_USER_DB_SCHEMA
from rotkehlchen.globaldb.minimized_schema import MINIMIZED_GLOBAL_DB_SCHEMA
from rotkehlchen.greenlets.utils import get_greenlet_name
from rotkehlchen.utils.misc import ts_now
from rotkehlchen.db.drivers.server import DBConnectionType, DBCursor, UnderlyingConnection

if TYPE_CHECKING:
    from rotkehlchen.logging import RotkehlchenLogger

logger: 'RotkehlchenLogger' = logging.getLogger(__name__)  # type: ignore
CONTEXT_SWITCH_WAIT = 1  # seconds to wait for a status change in a DB context switch


class ContextError(Exception):
    """Intended to be raised when something is wrong with db context management"""


# Manager
class DBConnection(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # We need an ordered set. Python doesn't have such thing as a standalone object, but has
        # `dict` which preserves the order of its keys. So we use dict with None values.
        self.savepoints: dict[str, None] = {}
        # These will hold the id of the greenlet where write tx/savepoints are active
        # https://www.gevent.org/api/gevent.greenlet.html#gevent.Greenlet.minimal_ident
        self.savepoint_greenlet_id: str | None = None
        self.minimized_schema = None

    def set_minimized_schema(self):
        if (connection_type := self.get_conn_type()) == DBConnectionType.USER:
            self.minimized_schema = MINIMIZED_USER_DB_SCHEMA
        elif connection_type == DBConnectionType.GLOBAL:
            self.minimized_schema = MINIMIZED_GLOBAL_DB_SCHEMA

    @abstractmethod
    def set_progress_handler(self) -> None:
        ...

    @abstractmethod
    def execute(self, statement, *bindings) -> DBCursor:
        ...

    @abstractmethod
    def executemany(self, statement: str, bindings: Sequence[Sequence]) -> DBCursor:
        ...

    @abstractmethod
    def executescript(self, script: str) -> DBCursor:
        ...

    @abstractmethod
    def commit(self) -> None:
        ...

    @abstractmethod
    def rollback(self) -> None:
        ...

    @abstractmethod
    def cursor(self) -> DBCursor:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def get_lock(self) -> Lock:
        ...

    @abstractmethod
    def get_conn(self) -> UnderlyingConnection:
        ...

    @abstractmethod
    def get_conn_type(self) -> DBConnectionType:
        ...

    @abstractmethod
    def get_callback_lock(self) -> Lock:
        ...

    @contextmanager
    def write_cursor(self):
        self.get_lock().acquire()
        try:
            self.execute('BEGIN TRANSACTION')
            yield
        except Exception as e:
            print(f'ERROR {e}')
            self.rollback()
        else:
            self.commit()
        finally:
            self.get_lock().release()

    @contextmanager
    def read_ctx(self) -> Generator['DBCursor', None, None]:
        cursor = self.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @contextmanager
    def write_ctx(self, commit_ts: bool = False) -> Generator['DBCursor', None, None]:
        """Opens a transaction to the database. This should be used kept open for
        as little time as possible.

        It's possible that a write transaction tries to be opened when savepoints are being used.
        In order for savepoints to work then, we will need to open a savepoint instead of a write
        transaction in that case. This should be used sparingly.
        """
        if len(self.savepoints) != 0:
            current_id = get_greenlet_name(gevent.getcurrent())
            if current_id != self.savepoint_greenlet_id:
                # savepoint exists but in other greenlet. Wait till it's done.
                while self.savepoint_greenlet_id is not None:
                    gevent.sleep(CONTEXT_SWITCH_WAIT)
                # and now continue with the normal write context logic
            else:  # open another savepoint instead of a write transaction
                with self.savepoint_ctx() as cursor:
                    yield cursor
                    return
        # else
        with self.critical_section(), self.get_lock():
            cursor = self.cursor()
            self.write_greenlet_id = get_greenlet_name(gevent.getcurrent())
            cursor.execute('BEGIN TRANSACTION')
            try:
                yield cursor
            except Exception:
                self.get_conn().rollback()
                raise
            else:
                if commit_ts is True:
                    cursor.execute(
                        'INSERT OR REPLACE INTO settings(name, value) VALUES(?, ?)',
                        ('last_write_ts', str(ts_now())),
                    )
                    # last_write_ts in not cached to cached settings. This is a critical section
                    # and adding even one more function call can have very ugly and
                    # detrimental effects in the entire codebase as everything calls this.
                self.get_conn().commit()
            finally:
                cursor.close()
                self.write_greenlet_id = None

    @contextmanager
    def savepoint_ctx(
            self,
            savepoint_name: str | None = None,
    ) -> Generator['DBCursor', None, None]:
        """
        Creates a savepoint context with the provided name. If the code inside the savepoint fails,
        rolls back this savepoint, otherwise releases it (aka forgets it -- this is not commited to the DB).
        Savepoints work like nested transactions, more information here: https://www.sqlite.org/lang_savepoint.html
        """    # noqa: E501
        cursor, savepoint_name = self._enter_savepoint(savepoint_name)
        try:
            yield cursor
        except Exception:
            self.rollback_savepoint(savepoint_name)
            raise
        finally:
            self.release_savepoint(savepoint_name)
            cursor.close()

    def _enter_savepoint(self, savepoint_name: str | None = None) -> tuple['DBCursor', str]:
        """
        Creates an sqlite savepoint with the given name. If None is given, a uuid is created.
        Returns cursor and savepoint's name.

        Should only be used inside a savepoint_ctx

        May raise:
        - ContextError if a savepoint with the same name already exists. Can only happen in case of
        manually specified name.
        """
        if savepoint_name is None:
            savepoint_name = str(uuid4())

        current_id = get_greenlet_name(gevent.getcurrent())
        if self.get_conn().in_transaction is True and self.write_greenlet_id != current_id:
            # a transaction is open in a different greenlet
            while self.write_greenlet_id is not None:
                gevent.sleep(CONTEXT_SWITCH_WAIT)  # wait until that transaction ends

        if self.savepoint_greenlet_id is not None:
            # savepoints exist but in other greenlet
            while self.savepoint_greenlet_id is not None and current_id != self.savepoint_greenlet_id:  # noqa: E501
                gevent.sleep(CONTEXT_SWITCH_WAIT)  # wait until no other savepoint exists
        if savepoint_name in self.savepoints:
            raise ContextError(
                f'Wanted to enter savepoint {savepoint_name} but a savepoint with the same name '
                f'already exists. Current savepoints: {list(self.savepoints)}',
            )
        cursor = self.cursor()
        cursor.execute(f'SAVEPOINT "{savepoint_name}"')
        self.savepoints[savepoint_name] = None
        self.savepoint_greenlet_id = current_id
        return cursor, savepoint_name

    def _modify_savepoint(
            self,
            rollback_or_release: Literal['ROLLBACK TO', 'RELEASE'],
            savepoint_name: str | None,
    ) -> None:
        if len(self.savepoints) == 0:
            raise ContextError(
                f'Incorrect use of savepoints! Wanted to {rollback_or_release.lower()} savepoint '
                f'{savepoint_name}, but the stack is empty.',
            )
        list_savepoints = list(self.savepoints)
        if savepoint_name is None:
            savepoint_name = list_savepoints[-1]
        elif savepoint_name not in self.savepoints:
            raise ContextError(
                f'Incorrect use of savepoints! Wanted to {rollback_or_release.lower()} savepoint '
                f'{savepoint_name}, but it is not present in the stack: {list_savepoints}',
            )
        self.execute(f'{rollback_or_release} SAVEPOINT "{savepoint_name}"')

        # Release all savepoints until, and including, the one with name `savepoint_name`.
        # For rollback we don't remove the savepoints since they are not released yet.
        if rollback_or_release == 'RELEASE':
            self.savepoints = dict.fromkeys(list_savepoints[:list_savepoints.index(savepoint_name)])  # noqa: E501
            if len(self.savepoints) == 0:  # mark if we are out of all savepoints
                self.savepoint_greenlet_id = None

    def rollback_savepoint(self, savepoint_name: str | None = None) -> None:
        """
        Rollbacks to `savepoint_name` if given and to the latest savepoint otherwise.
        May raise:
        - ContextError if savepoints stack is empty or given savepoint name is not in the stack
        """
        self._modify_savepoint(rollback_or_release='ROLLBACK TO', savepoint_name=savepoint_name)

    def release_savepoint(self, savepoint_name: str | None = None) -> None:
        """
        Releases (aka forgets) `savepoint_name` if given and the latest savepoint otherwise.
        May raise:
        - ContextError if savepoints stack is empty or given savepoint name is not in the stack
        """
        self._modify_savepoint(rollback_or_release='RELEASE', savepoint_name=savepoint_name)

    @contextmanager
    def critical_section(self) -> Generator[None, None, None]:
        callback_lock = self.get_callback_lock()
        with callback_lock:
            if __debug__:
                logger.trace(f'entering critical section for {(connection_type := self.get_conn_type())}')  # noqa: E501
            self.get_conn().set_progress_handler(None, 0)
        yield

        with callback_lock:
            if __debug__:
                logger.trace(f'exiting critical section for {connection_type}')
            self.set_progress_handler()

    @contextmanager
    def critical_section_and_transaction_lock(self) -> Generator[None, None, None]:
        with self.critical_section(), self.get_lock():
            yield

    @property
    def total_changes(self) -> int:
        """total number of database rows that have been modified, inserted,
        or deleted since the database connection was opened"""
        return self.get_conn().total_changes

    def schema_sanity_check(self) -> None:
        """Ensures that database schema is not broken.

        If you need to regenerate the schema that is being checked run:
        tools/scripts/generate_minimized_db_schema.py

        Raises DBSchemaError if anything is off.
        """
        connection_type = self.get_conn_type()
        assert (
            connection_type != DBConnectionType.TRANSIENT and
            self.minimized_schema is not None
        )

        with self.read_ctx() as cursor:
            sanity_check_impl(
                cursor=cursor,
                db_name=connection_type.name.lower(),
                minimized_schema=self.minimized_schema,
            )


DBConnection.register('set_progress_handler')
DBConnection.register('execute')
DBConnection.register('executemany')
DBConnection.register('executescript')
DBConnection.register('commit')
DBConnection.register('rollback')
DBConnection.register('cursor')
DBConnection.register('close')
DBConnection.register('get_lock')
DBConnection.register('get_conn')
DBConnection.register('get_conn_type')
DBConnection.register('get_callback_lock')


if __name__ == '__main__':
    import threading
    print(threading.get_native_id())

    client = DBConnection(address=('', 50000), authkey=b'123')
    client.connect()
    client.set_minimized_schema()

    print('pre enter')
    with client.write_cursor():
        print(client.execute('SELECT * FROM assets LIMIT 1').fetchone())

    print('exited')
