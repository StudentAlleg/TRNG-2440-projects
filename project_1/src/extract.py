"""
open-meteo api - access
"""
import argparse
import json
import logging
import os
from typing import Any, Optional

import requests
from src.model import DailyUnits, Location

API_URL: str = "https://archive-api.open-meteo.com/v1/archive"

def extract(start_date: str = "2026-05-01",
            end_date: str = "2026-06-01",
            locations_file: str = os.path.join(os.path.dirname(__file__), "..", "data", "locations.json"),
            out_file: str = os.path.join(os.path.dirname(__file__), "..", "data", "api_out.json")
            ) -> None:
    """
    Extracts data from the open-meteo API for the given locations and saves it to the out_file
    :param start_date: 
    :param end_date: 
    :param locations_file: 
    :param out_file: 
    :return: 
    """

    locations: list[Location] = []
    
    with open(locations_file, "r") as f:
        locations_dict: list[dict[str, Any]] = json.load(f)
        for location_data in locations_dict:
            locations.append(Location.model_validate(location_data))
    logging.info("Requesting Data")
    request_data(start_date, end_date, locations, out_file)
    logging.info("Data requested successfully")
    logging.info(f"find values at {out_file}")
    

def request_data(start_date: str, end_date: str, locations: list[Location], out_file: str) -> None:
    """
    Requests the data and stores it in out_file
    :type locations: list[Location]
    :param locations: list of locations
    :param start_date: YYYY-MM-DD
    :param end_date: YYYY-MM-DD
    :return:
    """
    daily_params: list[str] = [key for key in DailyUnits.model_fields.keys() if key != "time"]
    latitudes: list[float] = [location.latitude for location in locations]
    longitudes: list[float] = [location.longitude for location in locations]

    params: dict[str, Any] = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": start_date,
        "end_date": end_date,
        "daily": daily_params
    }
    logging.info("Extracting with params " +str(params))

    with requests.get(url=API_URL, params= params) as response:
        with open(out_file, "w") as f:
            json.dump(response.json(), f)

if __name__ == "__main__":
    """
    Runs the extract stage
    """
    # https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(
        prog="Open Meteo API",
        description="Gets the json from the api arguments"
    )

    parser.add_argument("start_date")
    parser.add_argument("end_date")
    parser.add_argument("locations_file")
    parser.add_argument("out_file")

    args: argparse.Namespace = parser.parse_args()

    start_date: str = args.start_date
    end_date: str = args.end_date
    locations_file: str = args.locations_file
    out_file: str = args.out_file

    logging.info("Requesting Data")
    extract(start_date, end_date, locations_file, out_file)
    logging.info("Data requested successfully")
    if out_file:
        logging.info(f"find values at {out_file}")
