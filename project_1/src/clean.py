"""
Class for cleaning and formatting data
"""
from typing import Any

import argparse
import json
import pandas as pd
from pandas import DataFrame

from model import MeteoResponse, Location


def from_file(api_out_path: str, locations_path: str) -> pd.DataFrame:
    with open(api_out_path, "r") as api_out, open(locations_path, "r") as locations:
        return from_json(api_out.read(), locations.read())

def from_json(api_json_str: str, location_json_str: str) -> pd.DataFrame:
    api_json: list[dict[str, Any]] = json.loads(api_json_str)
    model_data: list[MeteoResponse] = [MeteoResponse.model_validate(res) for res in api_json]

    location_json: list[dict[str, Any]] = json.loads(location_json_str)
    locations: list[Location] = [Location.model_validate(location) for location in location_json]

    return to_table(model_data, locations)


def to_table(weather_data: list[MeteoResponse], city_data: list[Location]) -> pd.DataFrame:
    """

    :param data:
    :return:
    """

    mapped_weather_data: dict[tuple[float, float], MeteoResponse] = {(value.latitude, value.longitude): value for value in weather_data}

    print(mapped_weather_data.keys())

    tables: list[DataFrame] = []
    for city in city_data:
        weather_data_key: tuple[float, float] = (city.latitude, city.longitude)
        print(weather_data_key)
        if weather_data_key not in mapped_weather_data:
            print(f"no data for {weather_data_key}")
            continue
        w_data: MeteoResponse = mapped_weather_data[weather_data_key]
        table: pd.DataFrame = pd.DataFrame.from_dict(w_data.daily.model_dump())
        #TODO FIX TIME, make it a date or datetime
        #TODO ALSO LOOK AT SUNRISE/SUNSET
        table.insert(0, "location", [city.name for _ in range(0, len(w_data.daily.time))])
        table.insert(1, "latitude", [w_data.latitude for _ in range(0, len(w_data.daily.time))])
        table.insert(2,"longitude", [w_data.longitude for _ in range(0, len(w_data.daily.time))])

        tables.append(table)

    full_table: DataFrame = pd.concat(tables, ignore_index=True)
    full_table.info()
    full_table.describe()
    return full_table


if __name__ == "__main__":
    # https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(
        prog="Loads data to a json table",
        description="Loads data from a table"
    )

    parser.add_argument("api_out_file")
    parser.add_argument("locations_file")

    args: argparse.Namespace = parser.parse_args()

    data_file: str = args.api_out_file
    location_file: str = args.locations_file
    from_file(data_file, location_file)
