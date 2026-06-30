CREATE TABLE IF NOT EXISTS Location (
    id          int           AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(32)        NOT NULL,
    latitude    DOUBLE             NOT NULL,
    longitude   DOUBLE             NOT NULL,
    UNIQUE(latitude, longitude)
);

CREATE INDEX idx_latlong_locations on Location (latitude, longitude);
CREATE INDEX idx_id_locations on Location (id);

CREATE TABLE IF NOT EXISTS Weather (
    location_id                 int         NOT NULL,
    day                         DATE   NOT NULL,
    weather_code                int         NOT NULL,
    temperature_2m_mean         float       NOT NULL,
    temperature_2m_max          float       NOT NULL,
    temperature_2m_min          float       NOT NULL,
    apparent_temperature_mean   float       NOT NULL,
    apparent_temperature_max    float       NOT NULL,
    apparent_temperature_min    float       NOT NULL,
    precipitation_sum           float       NOT NULL,
    rain_sum                    float       NOT NULL,
    snowfall_sum                float       NOT NULL,
    precipitation_hours         float       NOT NULL,
    wind_speed_10m_max          float       NOT NULL,
    wind_direction_10m_dominant float       NOT NULL,
    shortwave_radiation_sum     float       NOT NULL,
    et0_fao_evapotranspiration  float       NOT NULL,
    wind_gusts_10m_max          float       NOT NULL,
    sunshine_duration           float       NOT NULL,
    daylight_duration           float       NOT NULL,
    sunset                      timestamp   NOT NULL,
    sunrise                     timestamp   NOT NULL,
    PRIMARY KEY (location_id, day),
    CONSTRAINT FK_location_id
    FOREIGN KEY (location_id)
    REFERENCES Location(id)
);

CREATE INDEX idx_location_weather on Weather(id);
CREATE INDEX idx_day_weather on Weather(day);
