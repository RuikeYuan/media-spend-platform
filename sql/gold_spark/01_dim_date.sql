-- Calendar dimension covering every day with delivered media (Spark SQL).
SELECT
    d                                AS date_key,
    year(d)                          AS year,
    month(d)                         AS month,
    date_format(d, 'yyyy-MM')        AS year_month,
    weekofyear(d)                    AS iso_week,
    ((dayofweek(d) + 5) % 7) + 1     AS iso_day_of_week,
    dayofweek(d) IN (1, 7)           AS is_weekend
FROM (
    SELECT explode(sequence(min(activity_date), max(activity_date), INTERVAL 1 DAY)) AS d
    FROM silver_media_spend
)
