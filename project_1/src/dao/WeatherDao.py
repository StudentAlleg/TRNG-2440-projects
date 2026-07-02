from typing import Optional

from psycopg import ProgrammingError
from src.dao.dao import Dao
from src.database import Database
from src.model import WeatherRecord

WEATHER_COLUMNS: list[str] = list(WeatherRecord.model_fields.keys())
WEATHER_UPDATE_COLUMNS: list[str] = [c for c in WEATHER_COLUMNS if c not in ("location_id", "day")]

class WeatherDao(Dao[int, WeatherRecord]):

    def __init__(self, database: Database = Database()) -> None:
        super().__init__(WeatherRecord, database)

    def get_all(self) -> list[WeatherRecord]:
        with self.get_cursor() as cursor:
            query: str = f"SELECT {", ".join(WEATHER_COLUMNS)} FROM Weather"
            cursor.execute(query)

            return cursor.fetchall()

    def get_by_id(self, record_id: int) -> Optional[WeatherRecord]:
        with self.get_cursor() as cursor:
            query: str = f"""
            SELECT {", ".join(WEATHER_COLUMNS)} FROM Weather
            WHERE location_id = %s
            """
            cursor.execute(query, (record_id,))
            try:
                return cursor.fetchone()
            except ProgrammingError:
                return None

    def save(self, record: Optional[WeatherRecord] = None, **kwargs) -> WeatherRecord:
        if record is None:
            raise ValueError("record must not be None")
        with self.get_cursor() as cursor:
            update: str = f"""
            INSERT INTO Weather({", ".join(WEATHER_COLUMNS)})
            VALUES ({", ".join(["%s" for _ in range(0, len(WEATHER_COLUMNS))])})
            ON CONFLICT (location_id, day) DO UPDATE SET
            {", ".join(f"{col} = EXCLUDED.{col}" for col in WEATHER_UPDATE_COLUMNS)}
            RETURNING {", ".join(WEATHER_COLUMNS)}
            """
            values: dict = record.model_dump()
            cursor.execute(update, tuple(values[col] for col in WEATHER_COLUMNS))
            return next(cursor)

    def update(self, record: Optional[WeatherRecord] = None, **kwargs) -> Optional[WeatherRecord]:
        raise NotImplementedError

    def delete(self, record_id: int) -> Optional[WeatherRecord]:
        raise NotImplementedError