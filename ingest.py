# download both sources into the RAW layer, unchanged.

# raw/eurostat/tour_occ_nim.csv   monthly tourist nights per country (CSV)
# raw/weather/<CODE>.json         daily weather of the capital city (JSON)

import calendar
import logging
import time
import requests
import config
from common import save_raw_file, setup_logging

log = logging.getLogger("ingest")


def fetch_with_retry(url, params=None, attempts=3, wait_seconds=5):
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code < 500:
                response.raise_for_status()  # 4xx = the request is wrong -> no retry
                return response
            error = f"server error {response.status_code}"
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = str(exc)
        if attempt == attempts:
            raise RuntimeError(f"giving up after {attempts} attempts: {error}")
        log.warning("attempt %d/%d failed (%s), retrying in %ds",
                    attempt, attempts, error, wait_seconds)
        time.sleep(wait_seconds)


def ingest_tourism():
    geo = "+".join(config.COUNTRIES) 
    url = config.EUROSTAT_URL.format(geo=geo, start=config.START_MONTH, end=config.END_MONTH)
    log.info("Eurostat: downloading tour_occ_nim for %s", geo)
    response = fetch_with_retry(url)
    path = save_raw_file(config.RAW_TOURISM_FILE, response.content)
    log.info("Eurostat: %d rows saved -> %s", response.text.count("\n") - 1, path)


def ingest_weather():
    year, month = map(int, config.END_MONTH.split("-"))
    start_date = f"{config.START_MONTH}-01"
    end_date = f"{config.END_MONTH}-{calendar.monthrange(year, month)[1]:02d}"

    for code, country in config.COUNTRIES.items():
        params = {
            "latitude": country["lat"],
            "longitude": country["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(config.WEATHER_VARIABLES),
            "timezone": "auto",
        }
        response = fetch_with_retry(config.OPEN_METEO_URL, params=params)
        days = len(response.json()["daily"]["time"])
        path = save_raw_file(config.RAW_WEATHER_FILE.format(code=code), response.content)
        log.info("Weather: %s %d days saved -> %s", country["capital"], days, path)


def run():
    ingest_tourism()
    ingest_weather()


if __name__ == "__main__":
    setup_logging()
    run()