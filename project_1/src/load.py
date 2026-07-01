#TODO load infrastructure with daos
from pandas import DataFrame
from pydantic import BaseModel

from src.dao.WeatherDao import WeatherDao
from src.dao.LocationDao import LocationDao
from src.database import Database
from src.model import Location, LocationRecord, WeatherRecord


def load(dataframe: DataFrame, database: Database) -> None:
    location_dao: LocationDao = LocationDao(database)
    weather_dao: WeatherDao = WeatherDao(database)

    #locations
    location_table: DataFrame = dataframe[["location", "latitude", "longitude"]].drop_duplicates()
    locations: list[Location] = [Location(name=row["location"], latitude=row["latitude"], longitude=row["longitude"]) for row in location_table.to_dict(orient="records")]
    record_locations: dict[str, LocationRecord] = {}
    for location in locations:
        record_locations[location.name] = location_dao.save(location = location)
    location_to_id: dict[str, int] = {key: value.location_id for key, value in record_locations.items()}
    weather_table: DataFrame = (dataframe.copy(deep=True)
                                .drop(columns=["latitude", "longitude"]))
    weather_table["location"] = weather_table["location"].replace(location_to_id)
    weather_table.rename(columns={"location": "location_id"}, inplace=True)
    weather = [WeatherRecord.model_validate(row) for row in weather_table.to_dict(orient="records")]
    for data in weather:
        weather_dao.save(record=data)
    return
    
