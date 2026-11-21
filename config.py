
#   AZURE_STORAGE_ACCOUNT not set  -> local folder ./data   (development)
#  AZURE_STORAGE_ACCOUNT is set   -> Azure Data Lake Storage Gen2 (cloud)


import os
from pathlib import Path

# Time period (monthly)
START_MONTH = "2022-01"
END_MONTH = "2024-12"

# Countries. The key is the Eurostat country code (Greece is "EL" in Eurostat not "GR")
COUNTRIES = {
    "HU": {"name": "Hungary", "capital": "Budapest", "lat": 47.4979, "lon": 19.0402},
    "AT": {"name": "Austria", "capital": "Vienna", "lat": 48.2082, "lon": 16.3738},
    "CZ": {"name": "Czechia", "capital": "Prague", "lat": 50.0755, "lon": 14.4378},
    "DE": {"name": "Germany", "capital": "Berlin", "lat": 52.5200, "lon": 13.4050},
    "HR": {"name": "Croatia", "capital": "Zagreb", "lat": 45.8150, "lon": 15.9819},
    "IT": {"name": "Italy", "capital": "Rome", "lat": 41.9028, "lon": 12.4964},
    "ES": {"name": "Spain", "capital": "Madrid", "lat": 40.4168, "lon": -3.7038},
    "EL": {"name": "Greece", "capital": "Athens", "lat": 37.9838, "lon": 23.7275},
}

# Storage: local folder or Azure Data Lake (ADLS Gen2)
AZURE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_KEY = os.getenv("AZURE_STORAGE_KEY")
AZURE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "tourism")
USE_AZURE = bool(AZURE_ACCOUNT)

if USE_AZURE:
    DATA_ROOT = f"abfss://{AZURE_CONTAINER}@{AZURE_ACCOUNT}.dfs.core.windows.net"
else:
    DATA_ROOT = str(Path(__file__).parent / "data")

RAW = f"{DATA_ROOT}/raw"
STAGED = f"{DATA_ROOT}/staged"
CURATED = f"{DATA_ROOT}/curated"

# Raw files (fixed names -> re-running overwrites, never duplicates)
RAW_TOURISM_FILE = "eurostat/tour_occ_nim.csv"
RAW_WEATHER_FILE = "weather/{code}.json"

# Staged tables (Parquet)
STAGED_TOURISM = f"{STAGED}/tourism_monthly"
STAGED_WEATHER = f"{STAGED}/weather_daily"
REJECTED_TOURISM = f"{STAGED}/rejected/tourism"
REJECTED_WEATHER = f"{STAGED}/rejected/weather"

# Curated data product (Parquet)
CURATED_TABLE = f"{CURATED}/tourism_weather_monthly"


# Source APIs

# Eurostat tour_occ_nim = "Nights spent at tourist accommodation establishments
# - monthly data". Key order: freq.c_resid.unit.nace_r2.geo
#   M = monthly, TOTAL = all guests, NR = number of nights,
#   I551-I553 = hotels + holiday accommodation + campsites
EUROSTAT_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "tour_occ_nim/M.TOTAL.NR.I551-I553.{geo}"
    "?format=SDMX-CSV&startPeriod={start}&endPeriod={end}"
)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_VARIABLES = ["temperature_2m_mean", "precipitation_sum", "sunshine_duration"]

MIN_TEMP_C, MAX_TEMP_C = -50, 60
RAINY_DAY_MM = 1.0  # a day with >= 1 mm precipitation counts as rainy