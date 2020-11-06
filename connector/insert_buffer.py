from pymongo.collection import Collection
from typing import Callable


class InsertBuffer:
    def __init__(self, collection: Collection, buffer_size: int = 100, log_func: Callable = None):
        self.collection = collection
        self.buffer_size = buffer_size
        self.buffer = []
        self.log_func = log_func

    def flush(self):
        if len(self.buffer) > 0:
            self.collection.insert_many(self.buffer)
            if isinstance(self.log_func, Callable):
                self.log_func(f'flushed {len(self.buffer)} elements in collection {self.collection.name}')
            self.buffer = []

    def add(self, obj: dict):
        self.buffer.append(obj)
        if len(self.buffer) >= self.buffer_size:
            self.flush()
