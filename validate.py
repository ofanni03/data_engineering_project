# DATA QUALITY checks.

# every check counts "bad rows". Severity:
# ERROR   -> the pipeline stops, the curated data is not published
# WARNING -> logged clearly, the pipeline continues

import calendar
import logging
from datetime import date
from pyspark.sql import functions as F
import config

log = logging.getLogger("validate")


class DataQualityError(Exception):
    pass

def count_nulls(df, columns):
    condition = F.lit(False)
    for c in columns:
        condition = condition | F.col(c).isNull()
    return df.filter(condition).count()


def count_duplicate_keys(df, keys):
    return df.groupBy(*keys).count().filter("count > 1").count()


def count_where(df, condition):
    return df.filter(condition).count()


def expected_months():
    y1, m1 = map(int, config.START_MONTH.split("-"))
    y2, m2 = map(int, config.END_MONTH.split("-"))
    return (y2 - y1) * 12 + (m2 - m1) + 1


def expected_days():
    y1, m1 = map(int, config.START_MONTH.split("-"))
    y2, m2 = map(int, config.END_MONTH.split("-"))
    end = date(y2, m2, calendar.monthrange(y2, m2)[1])
    return (end - date(y1, m1, 1)).days + 1


def run_checks(stage_name, checks):
    log.info("Quality checks: %s", stage_name)
    errors = []
    for description, severity, bad in checks:
        if bad == 0:
            log.info("  PASS  %s", description)
        elif severity == "WARNING":
            log.warning("  WARN  %s  (%d bad)", description, bad)
        else:
            log.error("  FAIL  %s  (%d bad)", description, bad)
            errors.append(description)
    if errors:
        raise DataQualityError(f"{stage_name}: {len(errors)} check(s) failed: {errors}")


def validate_staged(tourism, weather):
    countries = set(config.COUNTRIES)
    tourism_countries = {r.country_code for r in tourism.select("country_code").distinct().collect()}
    weather_countries = {r.country_code for r in weather.select("country_code").distinct().collect()}
    days_per_country = weather.groupBy("country_code").count()

    run_checks("staged", [
        ("tourism: required fields not null", "ERROR",
         count_nulls(tourism, ["country_code", "month", "tourist_nights"])),
        ("tourism: one row per country + month", "ERROR",
         count_duplicate_keys(tourism, ["country_code", "month"])),
        ("tourism: every configured country present", "ERROR",
         len(countries - tourism_countries)),
        ("tourism: no provisional/estimated Eurostat values", "WARNING",
         count_where(tourism, F.col("eurostat_flag").isNotNull())),
        ("weather: one row per country + date", "ERROR",
         count_duplicate_keys(weather, ["country_code", "date"])),
        ("weather: every configured country present", "ERROR",
         len(countries - weather_countries)),
        (f"weather: complete days per country ({expected_days()})", "WARNING",
         count_where(days_per_country, F.col("count") != expected_days())),
    ])


def validate_curated(curated):
    run_checks("curated", [
        ("not empty", "ERROR", 0 if curated.limit(1).count() else 1),
        ("one row per country + month", "ERROR",
         count_duplicate_keys(curated, ["country_code", "month"])),
        ("every tourism month matched with weather (join check)", "ERROR",
         count_nulls(curated, ["avg_temp_c", "total_precip_mm", "sunshine_hours"])),
        ("country name found for every code", "ERROR",
         count_nulls(curated, ["country_name"])),
        ("tourist_nights >= 0", "ERROR",
         count_where(curated, F.col("tourist_nights") < 0)),
        (f"avg_temp_c between {config.MIN_TEMP_C} and {config.MAX_TEMP_C}", "ERROR",
         count_where(curated, ~F.col("avg_temp_c").between(config.MIN_TEMP_C, config.MAX_TEMP_C))),
        ("rainy_days between 0 and weather_days", "ERROR",
         count_where(curated, ~F.col("rainy_days").between(0, F.col("weather_days")))),
        ("full month of weather (28-31 days)", "WARNING",
         count_where(curated, ~F.col("weather_days").between(28, 31))),
        (f"expected row count ({len(config.COUNTRIES)} countries x {expected_months()} months)",
         "WARNING",
         abs(curated.count() - len(config.COUNTRIES) * expected_months())),
    ])