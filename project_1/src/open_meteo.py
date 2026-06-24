"""
open-meteo api - access
"""
import argparse
import json
from typing import Any, Optional
from Location import Location
import logging
import requests

API_URL: str = "https://archive-api.open-meteo.com/v1/archive"

#TODO remove out file?
def request_data(start_date: str, end_date: str, locations: list[Location], out_file: Optional[str] = None) -> dict[str, Any]:
    """

    :type locations: list[Location]
    :param locations: list of locations
    :param start_date: YYYY-MM-DD
    :param end_date: YYYY-MM-DD
    :return:
    """
    #TODO FIX LOCATIONS, SCHEMA CHANGE
    daily_params: list[str] = ["weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "apparent_temperature_mean", "apparent_temperature_max", "apparent_temperature_min", "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration", "wind_gusts_10m_max", "sunshine_duration", "daylight_duration", "sunset", "sunrise"]
    latitudes: list[str] = [location.get_latitude() for location in locations]
    longitudes: list[str] = [location.get_longitude() for location in locations]

    params: dict[str, Any] = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": start_date,
        "end_date": end_date,
        "daily": daily_params
    }

    with requests.get(url=API_URL, params= params) as response:
        if out_file:
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
