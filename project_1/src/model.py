import datetime
from datetime import date

from pydantic import BaseModel

class DailyUnits(BaseModel):
    time: str
    weather_code: str
    temperature_2m_mean: str
    temperature_2m_max: str
    temperature_2m_min: str
    apparent_temperature_mean: str
    apparent_temperature_max: str
    apparent_temperature_min: str
    precipitation_sum: str
    rain_sum: str
    snowfall_sum: str
    precipitation_hours: str
    wind_speed_10m_max: str
    wind_direction_10m_dominant: str
    shortwave_radiation_sum: str
    et0_fao_evapotranspiration: str
    wind_gusts_10m_max: str
    sunshine_duration: str
    daylight_duration: str
    sunset: str
    sunrise: str
    
class Daily(BaseModel):
    time: list[datetime.date]
    weather_code: list[int]
    temperature_2m_mean: list[float]
    temperature_2m_max: list[float]
    temperature_2m_min: list[float]
    apparent_temperature_mean: list[float]
    apparent_temperature_max: list[float]
    apparent_temperature_min: list[float]
    precipitation_sum: list[float]
    rain_sum: list[float]
    snowfall_sum: list[float]
    precipitation_hours: list[float]
    wind_speed_10m_max: list[float]
    wind_direction_10m_dominant: list[float]
    shortwave_radiation_sum: list[float]
    et0_fao_evapotranspiration: list[float]
    wind_gusts_10m_max: list[float]
    sunshine_duration: list[float]
    daylight_duration: list[float]
    sunset: datetime.datetime
    sunrise: datetime.datetime

class MeteoResponse(BaseModel):
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: float
    timezone: str
    timezone_abbreviation: str
    elevation: float
    daily_units: DailyUnits
    daily: Daily
    
class Location(BaseModel):
    name: str
    latitude: float
    longitude: float