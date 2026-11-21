# Data product: `tourism_weather_monthly`

## What is in this table

This is the final table of my pipeline. It has monthly tourism and weather
data for 8 EU countries (HU, AT, CZ, DE, HR, IT, ES, EL), from January 2022 to
December 2024.

In every row there is the number of tourist nights in a country (from
Eurostat) and the weather of the capital city in the same month (from
Open-Meteo). With this table I can check if more tourists come in the warmer
and sunnier months.

- **Where it is:** `curated/tourism_weather_monthly/`, in Parquet format
- **One row is:** one country in one month
- **Number of rows:** 8 countries × 36 months = 288 rows
- **Name in SQL:** `tourism_weather` (`query.py` creates this view)
- **Made by:** `run_pipeline.py`


## Columns

| Column | Type | What it means |
|---|---|---|
| `country_code` | string | Eurostat country code, for example `HU`, `AT` (Greece is `EL`) |
| `country_name` | string | Name of the country in English |
| `capital` | string | The capital city, its weather is used for the whole country |
| `month` | date | First day of the month, for example `2024-07-01` |
| `year` | int | The year |
| `month_of_year` | int | Number of the month, 1 to 12 |
| `tourist_nights` | bigint | How many nights the tourists spent in all accommodations of the country in this month |
| `seasonality_index` | double | Tourist nights of this month divided by the average month of the same country. 1.0 means an average month, 1.8 means 80% more than usual. |
| `avg_temp_c` | double | Average of the daily mean temperatures in the capital (°C) |
| `total_precip_mm` | double | Sum of the precipitation in the month (mm) |
| `rainy_days` | bigint | Number of days with at least 1 mm precipitation |
| `sunshine_hours` | double | Sum of the sunshine hours in the month |
| `weather_days` | bigint | For how many days we have weather data in this month (should be 28 to 31) |
| `eurostat_flag` | string | Flag from Eurostat, for example `p` = provisional, `e` = estimated. If it is NULL, the value is final and there is no remark. |

I added the `seasonality_index` because the countries are very different in
size (Germany has many more tourists than Croatia), so the raw numbers cannot
be compared directly. With the index they can.


## How often it is updated

Now the table has a fixed period (2022 to 2024).

If someone wants to update it, it makes sense to do it **once a month**, after
Eurostat publishes a new month (this usually comes 2 or 3 months later). For
this, change `END_MONTH` in `config.py` and run `python run_pipeline.py`. The
whole table is built again (overwrite), so if Eurostat corrected some older
values, these corrections also get into the table.


## Quality checks

**During staging** the bad rows are not loaded into the staged tables. They go
to `staged/rejected/` with a `reject_reason`, and a WARNING is printed:
- tourism: country, month and number of nights must be there, the number of
  nights must be a number and not negative
- weather: date and temperature must be there, temperature must be between
  −50 and 60 °C, precipitation cannot be negative, sunshine must be between 0
  and 24 hours per day

**Checks on the staged tables** (`validate_staged`):

| Check | If it fails |
|---|---|
| tourism: required fields are not null | ERROR |
| tourism: only one row per country and month | ERROR |
| tourism: every country from the config is there | ERROR |
| tourism: no provisional or estimated Eurostat values | WARNING |
| weather: only one row per country and date | ERROR |
| weather: every country from the config is there | ERROR |
| weather: every day is there for every country | WARNING |

**Checks on the final table** (`validate_curated`, these run **before** the
table is written):

| Check | If it fails |
|---|---|
| the table is not empty | ERROR |
| only one row per country and month | ERROR |
| every tourism month has weather data (join check) | ERROR |
| every country code has a country name | ERROR |
| tourist_nights is not negative | ERROR |
| avg_temp_c is between −50 and 60 | ERROR |
| rainy_days is between 0 and weather_days | ERROR |
| weather data for the full month (28 to 31 days) | WARNING |
| the number of rows is countries × months | WARNING |

If an ERROR check fails, the pipeline stops and the old table stays as it was.
If a WARNING check fails, it is printed in the log but the pipeline goes on.


## Example queries

I run them with `python query.py` (this runs all queries from
`sql/example_queries.sql`), or with own query:
`python query.py "<SQL>"`.

**Do more tourists come in the warmer months?**

```sql
SELECT
  CASE
    WHEN avg_temp_c < 5  THEN '1) cold  (< 5 C)'
    WHEN avg_temp_c < 15 THEN '2) mild  (5-15 C)'
    WHEN avg_temp_c < 22 THEN '3) warm  (15-22 C)'
    ELSE                      '4) hot   (22+ C)'
  END                               AS temperature_band,
  COUNT(*)                          AS months,
  ROUND(AVG(seasonality_index), 2)  AS avg_seasonality_index
FROM tourism_weather
GROUP BY 1
ORDER BY 1;
```

Result:

| temperature_band | months | avg_seasonality_index |
|---|---|---|
| 1) cold (< 5 C) | 45 | 0.60 |
| 2) mild (5-15 C) | 112 | 0.62 |
| 3) warm (15-22 C) | 80 | 1.13 |
| 4) hot (22+ C) | 51 | 1.98 |

So in the hot months there are about 2 times more tourists than in an average
month

**How strong is the connection in each country?**

```sql
SELECT
  country_name,
  ROUND(CORR(avg_temp_c, tourist_nights), 2)  AS corr_temperature,
  ROUND(CORR(rainy_days, tourist_nights), 2)  AS corr_rainy_days
FROM tourism_weather
GROUP BY country_name
ORDER BY corr_temperature DESC;
```

In most countries the correlation with temperature is very high (0.86 to
0.96). In Austria it is only 0.44, I think because many tourists go there in
winter for skiing. The number of rainy days has almost no effect on the
monthly level.


## Limitations

- The weather of one city (the capital) stands for the whole country.
- Monthly averages hide shorter things, for example one rainy week in a
  sunny month.
- Correlation does not mean causation. Summer is also the time of holidays
  and school breaks, not only of the warm weather.