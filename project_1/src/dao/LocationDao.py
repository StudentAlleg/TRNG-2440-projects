from typing import Optional

from psycopg import ProgrammingError
from src.dao.dao import Dao
from src.database import Database
from src.model import Location, LocationRecord


class LocationDao(Dao[int, LocationRecord]):

    def __init__(self, database: Database = Database()) -> None:
        super().__init__(LocationRecord, database)

    def get_all(self) -> list[LocationRecord]:
        with self.get_cursor() as cursor:
            query: str = """
                        SELECT location_id, name, latitude, longitude FROM Location
                        """
            cursor.execute(query)
            return cursor.fetchall()

    def get_by_id(self, record_id: int) -> Optional[LocationRecord]:
        with self.get_cursor() as cursor:
            query: str = """
            SELECT location_id, name, latitude, longitude FROM Location
            WHERE location_id = %s
            """
            cursor.execute(query, (record_id,))
            try:
                return cursor.fetchone()
            except ProgrammingError:
                return None

    def save(self, record: Optional[LocationRecord] = None, location: Optional[Location] = None) -> LocationRecord:
        if record is None:
            if location is None:
                raise ValueError("If record is None, location must not be None")
            record = LocationRecord(location_id = -1, name=location.name, latitude=location.latitude, longitude=location.longitude)
        with self.get_cursor() as cursor:
            update: str = """
            INSERT INTO Location(location_id, name, latitude, longitude) VALUES (DEFAULT, %s, %s, %s)
            ON CONFLICT (latitude, longitude) DO UPDATE SET name = EXCLUDED.name
            RETURNING location_id, name, latitude, longitude
            """
            cursor.execute(update, (record.name, record.latitude, record.longitude))
            return next(cursor)

    def update(self, record: Optional[LocationRecord] = None, **kwargs) -> Optional[LocationRecord]:
        raise NotImplementedError

    def delete(self, record_id: int) -> Optional[LocationRecord]:
        raise NotImplementedError