from pymongo.collection import Collection
from typing import Callable

from .errors import DBConnectionError


class InsertBuffer:
    def __init__(self, collection: Collection, buffer_size: int = 100, log_func: Callable = None):
        self._collection = collection
        self._buffer_size = buffer_size
        self._buffer = []
        self._log_func = log_func

    def __len__(self):
        return len(self._buffer)

    def flush(self):
        if len(self._buffer) > 0:
            try:
                self._collection.insert_many(self._buffer)
            except Exception:
                raise DBConnectionError('insertion in database failed') from None
            if isinstance(self._log_func, Callable):
                self._log_func(f'flushed {len(self._buffer)} elements in collection {self._collection.name}')
            self._buffer = []

    def add(self, obj: dict):
        self._buffer.append(obj)
        if len(self._buffer) >= self._buffer_size:
            self.flush()
