"""
Class for cleaning and formatting data
"""
from typing import Any

import argparse
import pandas
from pandas import DataFrame
from pandas.io import json


def to_table(data: dict[str, Any]):
    """

    :param data:
    :return:
    """

    table: DataFrame = DataFrame.from_dict(data)

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
        print(data.columns)