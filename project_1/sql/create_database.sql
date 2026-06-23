--TODO
CREATE TABLE IF NOT EXISTS Locations (
    id          int           AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(32)        NOT NULL,
    latitude    DOUBLE             NOT NULL,
    longitude   DOUBLE             NOT NULL,
    UNIQUE(latitude, longitude)
)

CREATE INDEX idx_latlong_locations on Locations (latitude, longitude)
CREATE INDEX idx_id_locations on Locations (id)

CREATE TABLE IF NOT EXISTS LocationData (
    location_id                 int         NOT NULL,
    day                         timestamp   NOT NULL,
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

    FOREIGN KEY locationid REFERENCES Locations(ID),
    PRIMARY KEY (locationid, day)
)

CREATE INDEX idx_location_locationdata on LocationData (id)
CREATE INDEX idx_day_locationdata on Locations (id)