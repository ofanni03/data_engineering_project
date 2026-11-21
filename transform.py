# transform with PySpark.

#stage():          raw  -> staged   (parse, convert types, reject bad rows)
#build_curated():  staged -> curated DataFrame (aggregate, join, enrich)
#publish():        write the curated DataFrame (only after validation passed)


import logging

from pyspark.sql import Window
from pyspark.sql import functions as F

import config

log = logging.getLogger("transform")


def split_valid(df, rules):
    reason = F.coalesce(*[F.when(bad, F.lit(name)) for name, bad in rules])
    df = df.withColumn("reject_reason", reason)
    valid = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = df.filter(F.col("reject_reason").isNotNull())
    return valid, rejected

def stage_tourism(spark):
    raw = spark.read.option("header", True).csv(f"{config.RAW}/{config.RAW_TOURISM_FILE}")

    df = raw.select(
        F.col("geo").alias("country_code"),
        F.to_date(F.concat(F.col("TIME_PERIOD"), F.lit("-01"))).alias("month"),
        F.col("OBS_VALUE").cast("long").alias("tourist_nights"),
        F.col("OBS_FLAG").alias("eurostat_flag"), 
    )
    return split_valid(df, [
        ("missing_country", F.col("country_code").isNull()),
        ("invalid_month", F.col("month").isNull()),
        ("missing_or_non_numeric_nights", F.col("tourist_nights").isNull()),
        ("negative_nights", F.col("tourist_nights") < 0),
    ])


def stage_weather(spark):
    # One JSON document per file, the country code comes from the file name
    raw = (
        spark.read.option("multiLine", True)
        .json(f"{config.RAW}/weather/*.json")
        .withColumn("country_code",
                    F.regexp_extract(F.input_file_name(), r"([A-Z]{2})\.json$", 1))
    )

    # The API returns parallel arrays: daily.time[i] belongs to
    # daily.temperature_2m_mean[i] etc. arrays_zip + explode -> one row per day.
    days = (
        raw.select("country_code", "daily.*")
        .select("country_code", F.explode(F.arrays_zip(
            "time", "temperature_2m_mean", "precipitation_sum", "sunshine_duration"
        )).alias("d"))
    )
    df = days.select(
        "country_code",
        F.to_date("d.time").alias("date"),
        F.col("d.temperature_2m_mean").cast("double").alias("temp_mean_c"),
        F.col("d.precipitation_sum").cast("double").alias("precipitation_mm"),
        (F.col("d.sunshine_duration") / 3600).alias("sunshine_hours"),  # seconds -> hours
    )
    return split_valid(df, [
        ("invalid_date", F.col("date").isNull()),
        ("missing_temperature", F.col("temp_mean_c").isNull()),
        ("temperature_out_of_range",
         ~F.col("temp_mean_c").between(config.MIN_TEMP_C, config.MAX_TEMP_C)),
        ("negative_precipitation", F.col("precipitation_mm") < 0),
        ("sunshine_out_of_range", ~F.col("sunshine_hours").between(0, 24)),
    ])


def stage(spark):
    for name, (valid, rejected), valid_path, rejected_path in [
        ("tourism", stage_tourism(spark), config.STAGED_TOURISM, config.REJECTED_TOURISM),
        ("weather", stage_weather(spark), config.STAGED_WEATHER, config.REJECTED_WEATHER),
    ]:
        valid.write.mode("overwrite").parquet(valid_path)
        rejected.write.mode("overwrite").parquet(rejected_path)

        n_valid = spark.read.parquet(valid_path).count()
        n_rejected = spark.read.parquet(rejected_path).count()
        log.info("%s: %d rows staged -> %s", name, n_valid, valid_path)
        if n_rejected:
            log.warning("%s: %d rows REJECTED -> %s", name, n_rejected, rejected_path)

def build_curated(spark):
    tourism = spark.read.parquet(config.STAGED_TOURISM)
    weather = spark.read.parquet(config.STAGED_WEATHER)

    # 1, Daily weather -> monthly weather per country
    weather_monthly = (
        weather.groupBy("country_code", F.trunc("date", "month").alias("month"))
        .agg(
            F.round(F.avg("temp_mean_c"), 1).alias("avg_temp_c"),
            F.round(F.sum("precipitation_mm"), 1).alias("total_precip_mm"),
            F.sum(F.when(F.col("precipitation_mm") >= config.RAINY_DAY_MM, 1)
                  .otherwise(0)).alias("rainy_days"),
            F.round(F.sum("sunshine_hours"), 1).alias("sunshine_hours"),
            F.count("*").alias("weather_days"),
        )
    )

    # 2, Country names from config 
    countries = spark.createDataFrame(
        [(code, c["name"], c["capital"]) for code, c in config.COUNTRIES.items()],
        ["country_code", "country_name", "capital"],
    )

    # 3, Join both sources on country + month
    #    LEFT join: a tourism month without weather stays visible (nulls) so the validation step can catch it instead of losing it silently
    joined = (tourism.join(weather_monthly, ["country_code", "month"], "left").join(countries, "country_code", "left"))

    # 4, Seasonality index = this month's nights / the countrys average month.
    #    Makes big (DE) and small (HR) countries comparable
    per_country = Window.partitionBy("country_code")
    curated = joined.withColumn(
        "seasonality_index",
        F.round(F.col("tourist_nights") / F.avg("tourist_nights").over(per_country), 3),
    )

    return curated.select(
        "country_code", "country_name", "capital", "month",
        F.year("month").alias("year"),
        F.month("month").alias("month_of_year"),
        "tourist_nights", "seasonality_index",
        "avg_temp_c", "total_precip_mm", "rainy_days", "sunshine_hours", "weather_days",
        "eurostat_flag",
    ).orderBy("country_code", "month")


def publish(curated_df):
    curated_df.write.mode("overwrite").parquet(config.CURATED_TABLE)
    log.info("curated: %d rows published -> %s", curated_df.count(), config.CURATED_TABLE)