"""
Class for cleaning and formatting data
"""
from typing import Any

import argparse
import json
import pandas as pd
from pandas import DataFrame

from model import MeteoResponse, Location


def from_file(filepath: str) -> pd.DataFrame:
    with open(filepath, "r") as f:
        return from_json(f.read())

def from_json(json_str: str) -> pd.DataFrame:
    json_data: list[dict[str, Any]] = json.loads(json_str)
    model_data: list[MeteoResponse] = []
    for res in json_data:
        data: MeteoResponse = MeteoResponse.model_validate(res)
        model_data.append(data)

    return to_table(model_data)


def to_table(weather_data: list[MeteoResponse], city_data: list[Location]) -> pd.DataFrame:
    """

    :param data:
    :return:
    """

    tables: list[DataFrame] = []
    for city in data:

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
        to_table(Json)
        to_table(data)
        print(data.columns)