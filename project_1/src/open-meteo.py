"""
open-meteo api - access
"""

import requests

API_URL: str = "https://archive-api.open-meteo.com/v1/archive"

def request_data(locations: dict[dict[str, float]], start_date: str, end_date: str) -> dict:
    daily_params: list[str] = ["weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "apparent_temperature_mean", "apparent_temperature_max", "apparent_temperature_min", "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration", "wind_gusts_10m_max", "sunshine_duration", "daylight_duration", "sunset", "sunrise"]