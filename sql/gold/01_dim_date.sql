-- Calendar dimension covering every day with delivered media.
CREATE OR REPLACE TABLE dim_date AS
SELECT
    CAST(d AS DATE)            AS date_key,
    year(d)                    AS year,
    month(d)                   AS month,
    strftime(d, '%Y-%m')       AS year_month,
    weekofyear(d)              AS iso_week,
    isodow(d)                  AS iso_day_of_week,
    isodow(d) >= 6             AS is_weekend
FROM range(
    CAST((SELECT min(activity_date) FROM silver_media_spend) AS TIMESTAMP),
    CAST((SELECT max(activity_date) FROM silver_media_spend) AS TIMESTAMP) + INTERVAL 1 DAY,
    INTERVAL 1 DAY
) AS t(d);
