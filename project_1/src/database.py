"""
Handles database connection and upload
"""
import os
from contextlib import contextmanager
from typing import Any, Generator

import psycopg
from psycopg import Connection, Cursor
from psycopg.rows import Row, class_row

#TODO pooling and connection handling
#https://www.psycopg.org/articles/2026/06/24/pydantic-fastapi/
class Database:
    """
    Class for interacting with the database
    """

    def get_connection_string(self):
        """
        Helper method to get the connection string from environment variables
        :return:
        """
        return (
            f"host={os.environ.get('POSTGRES_IP')} "
            f"dbname={os.environ.get('POSTGRES_DB')} "
            f"user={os.environ.get('POSTGRES_USER')} "
            f"password={os.environ.get('POSTGRES_PASSWORD')} "
            f"port={os.environ.get('POSTGRES_PORT')}"
        )

    @contextmanager
    def get_connection(self) -> Generator[Connection[Row], Any, None]:
        """
        Gets the connection to the the database
        :returns: The connection to the database as a context manager
        """
        with psycopg.connect(self.get_connection_string()) as conn:
            yield conn

    #https://www.psycopg.org/psycopg3/docs/advanced/rows.html
    #https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager
    @contextmanager
    def get_connection_cursor[T](self, row_type: type[T]) -> Generator[Cursor[T], None, None]:
        """
        Gets the cursor based off of the type
        :param row_type: the row type
        :returns: The cursor with the specified row_type as a context manager
        """
        with self.get_connection() as conn:
            yield conn.cursor(row_factory=class_row(row_type))








