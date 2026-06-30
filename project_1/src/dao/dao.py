from abc import ABC, abstractmethod
from contextlib import _GeneratorContextManager
from typing import Optional

from psycopg import Cursor
from src.database import Database


class Dao[IT, RT](ABC):
    """

    """

    #generally this would be through injection
    def __init__(self, row_type: type[RT], database: Database = Database()) -> None:
        self.database = database
        self._row_type = row_type

    #https://www.psycopg.org/psycopg3/docs/advanced/rows.html
    def get_cursor(self) -> _GeneratorContextManager[Cursor[RT], None, None]:
        return self.database.get_connection_cursor(self._row_type)

    @abstractmethod
    def get_all(self) -> list[RT]:
        pass

    @abstractmethod
    def get_by_id(self, record_id: IT) -> Optional[RT]:
        pass

    @abstractmethod
    def save(self, record: Optional[RT] = None, **kwargs) -> RT:
        pass

    @abstractmethod
    def update(self, record: Optional[RT] = None, **kwargs) -> Optional[RT]:
        pass

    @abstractmethod
    def delete(self, record_id: IT) -> Optional[RT]:
        pass