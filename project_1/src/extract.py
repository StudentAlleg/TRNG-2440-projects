"""
open-meteo api - access
"""
import os
import argparse
import json
from typing import Any, Optional
import logging
import requests

from src.model import Location, DailyUnits

API_URL: str = "https://archive-api.open-meteo.com/v1/archive"

def extract(start_date: str = "2026-05-01",
            end_date: str = "2026-06-01",
            locations_file: str = os.path.join(os.path.dirname(__file__), "..", "data", "locations.json"),
            out_file: str = os.path.join(os.path.dirname(__file__), "..", "data", "api_out.json")
            ) -> None:

    locations: list[Location] = []
    
    with open(locations_file, "r") as f:
        locations_dict: list[dict[str, Any]] = json.load(f)
        for location_data in locations_dict:
            locations.append(Location.model_validate(location_data))
    logging.info("Requesting Data")
    request_data(start_date, end_date, locations, out_file)
    
    request_data(start_date, end_date, locations, out_file)
    logging.info("Data requested successfully")
    logging.info(f"find values at {out_file}")
    

#TODO remove out file?
def request_data(start_date: str, end_date: str, locations: list[Location], out_file: str) -> None:
    """
    Requests the data ans stores it in out_file
    :type locations: list[Location]
    :param locations: list of locations
    :param start_date: YYYY-MM-DD
    :param end_date: YYYY-MM-DD
    :return:
    """
    #TODO FIX LOCATIONS, SCHEMA CHANGE
    daily_params: list[str] = list(DailyUnits.model_fields.keys())
    latitudes: list[float] = [location.latitude for location in locations]
    longitudes: list[float] = [location.longitude for location in locations]

    params: dict[str, Any] = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": start_date,
        "end_date": end_date,
        "daily": daily_params
    }

    with requests.get(url=API_URL, params= params) as response:
        with open(out_file, "w") as f:
            json.dump(response.json(), f)

if __name__ == "__main__":
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
    out_file: Optional[str] = args.out_file if args.out_file != "" else None

    locations: list[Location] = []
    with open(locations_file, "r") as f:
        locations_dict: dict[str, float] = json.load(f)
        for name, data in locations_dict.items():
            locations.append(Location(name, data["latitude"], data["longitude"]))
    logging.info("Requesting Data")
    request_data(start_date, end_date, locations, out_file)
    logging.info("Data requested successfully")
    if out_file:
        logging.info(f"find values at {out_file}")
