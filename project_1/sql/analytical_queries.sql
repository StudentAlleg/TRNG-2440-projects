--hottest city for each day + that cities temp
SELECT day, name, apparent_temperature_max
  FROM (
      SELECT
          Location.name,
          Weather.day,
          Weather.apparent_temperature_max,
          RANK() OVER
            (PARTITION BY Weather.day
            ORDER BY Weather.apparent_temperature_max DESC)
            AS rnk
      FROM Weather
      JOIN Location ON Weather.location_id = Location.location_id
  )
  WHERE rnk = 1
;

--Rainfall from the previous 7 days, plus current rain sum for that day

SELECT Location.name, Weather.day, Weather.rain_sum, SUM(Weather.rain_sum) OVER (
    PARTITION BY Weather.location_id
    ORDER BY Weather.day
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) as past_seven_day_total FROM Weather
JOIN Location
    ON Weather.location_id = Location.location_id
WHERE Weather.day > '2026-05-06'
ORDER BY Weather.day ASC, Location.latitude ASC
;

--total daylight per day per location,
--https://collectingwisdom.com/postgresql-convert-seconds-to-hhmmss/
SELECT Location.name, Weather.day, Weather.sunset - Weather.sunrise as total_daylight,
Weather.sunshine_duration * interval '1 sec' AS sunshine_time, Weather.daylight_duration * interval '1 sec' AS daylight_time
FROM Weather
JOIN Location
    ON Weather.location_id = Location.location_id
ORDER BY Weather.day ASC, Location.latitude ASC
;

--total precipitation hours and total rain_fall
SELECT Location.name, SUM(Weather.precipitation_hours) as total_precipitation_hours, SUM(Weather.rain_sum) as total_rain_sum
FROM Weather
JOIN Location
    ON Weather.location_id = Location.location_id
GROUP BY Location.location_id, Location.name
ORDER BY Location.latitude ASC
;


--avg windspeed direction
SELECT Location.name, AVG(wind_direction_10m_dominant) as avg_wind_direction FROM Weather
JOIN Location
    ON Weather.location_id = Location.location_id
GROUP BY Location.location_id, Location.name
ORDER BY Location.latitude ASC
;

--Frequency of weather codes per location
SELECT Location.name, Weather.weather_code, COUNT(*) as weather_code_frequency FROM Weather
JOIN Location
    ON Weather.location_id = Location.location_id
GROUP BY Location.location_id, Location.name, Weather.weather_code
ORDER BY Location.latitude ASC, weather_code_frequency DESC
;

