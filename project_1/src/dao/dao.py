"""
Defines the DAO class
"""
from abc import ABC, abstractmethod
from contextlib import _GeneratorContextManager
from typing import Optional

from psycopg import Cursor
from src.database import Database


class Dao[IT, RT](ABC):
    """
    Base DAO class. Defines the expected CRUD operations
    IT - The ID type
    RT - The record type
    """

    #generally this would be through injection
    def __init__(self, row_type: type[RT], database: Database) -> None:
        """
        Creates the class and defines row return type
        :param row_type: The class of the RowType [RT]
        :param database: The database to use
        """
        self.database = database
        self._row_type = row_type

    #https://www.psycopg.org/psycopg3/docs/advanced/rows.html
    def get_cursor(self) -> _GeneratorContextManager[Cursor[RT], None, None]:
        """
        Helper method to create a cursor from the database based off of the row type
        :return: The cursor with the row type for this DAO
        """
        return self.database.get_connection_cursor(self._row_type)

    @abstractmethod
    def get_all(self) -> list[RT]:
        """
        Gets all the records
        :returns: list of Records
        """
        raise NotImplementedError

    @abstractmethod
    def get_by_id(self, record_id: IT) -> Optional[RT]:
        """
        Gets the specified record by the ID or None if it does not exist
        :param record_id: the ID of type IT
        :returns: the specified record or none if the specified record does not exist
        """
        raise NotImplementedError

    @abstractmethod
    def save(self, record: Optional[RT] = None, **kwargs) -> RT:
        """
        Saves the record provided or creates a savable record from the kwargs if the record is not provided.
        :param record: The record to save or none
        :param kwargs: KWord args to create a savable record of type RT if record is None
        :return: the created record
        """
        raise NotImplementedError

    @abstractmethod
    def update(self, record_id: IT, record: Optional[RT] = None, **kwargs) -> Optional[RT]:
        """
        Updates the record with either the record provided or from the kwargs
        Returns the updated record or None if no record by that ID is found
        :param record_id: the id of the record to update
        :param record: The updated record or None
        :param kwargs: kwargs to create the updated record if record is None
        :return: None if no record that matches the id, otherwise the updated record
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, record_id: IT) -> Optional[RT]:
        """
        Deletes the specified record if it exists
        :param record_id: the record id to delete
        :return: The deleted record or None if no record was deleted
        """
        raise NotImplementedError