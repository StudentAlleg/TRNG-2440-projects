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

    tables: list[DataFrame] = []
    for city in data:
        pass
    table: DataFrame = DataFrame.fr(data)

    print(table)


if __name__ == "__main__":
    # https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(
        prog="Loads data to a json table",
        description="Loads data from a table"
    )

    parser.add_argument("data_file")

    args: argparse.Namespace = parser.parse_args()

    data_file: str = args.data_file

    
    with open(data_file, "r") as f:
        data: DataFrame = json.read_json(data_file)
        #to_table(Json)
        to_table(data)
        print(data.columns)