from pymongo.collection import Collection
from typing import Callable

from .errors import DBConnectionError


class InsertBuffer:
    def __init__(self, collection: Collection, buffer_size: int = 100, log_func: Callable = None):
        self.collection = collection
        self.buffer_size = buffer_size
        self.buffer = []
        self.log_func = log_func

    def __len__(self):
        return len(self.buffer)

    def flush(self):
        if len(self.buffer) > 0:
            try:
                self.collection.insert_many(self.buffer)
            except Exception:
                raise DBConnectionError('insertion in database failed') from None
            if isinstance(self.log_func, Callable):
                self.log_func(f'flushed {len(self.buffer)} elements in collection {self.collection.name}')
            self.buffer = []

    def add(self, obj: dict):
        self.buffer.append(obj)
        if len(self.buffer) >= self.buffer_size:
            self.flush()
