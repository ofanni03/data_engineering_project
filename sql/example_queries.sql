-- 1, Do warmer months bring more tourists? (all countries together)
--    seasonality_index makes big and small countries comparable.
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

-- 2, How strongly does weather go together with tourism in each country?
SELECT
  country_name,
  ROUND(CORR(avg_temp_c, tourist_nights), 2)       AS corr_temperature,
  ROUND(CORR(sunshine_hours, tourist_nights), 2)   AS corr_sunshine,
  ROUND(CORR(rainy_days, tourist_nights), 2)       AS corr_rainy_days
FROM tourism_weather
GROUP BY country_name
ORDER BY corr_temperature DESC;

-- 3, Hungary month by month in 2024
SELECT month, tourist_nights, seasonality_index, avg_temp_c, rainy_days, sunshine_hours
FROM tourism_weather
WHERE country_code = 'HU' AND year = 2024
ORDER BY month