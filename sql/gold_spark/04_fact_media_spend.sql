-- Fact table. Grain: source x day x campaign x channel x publisher, with lineage (Spark SQL).
SELECT
    activity_date AS date_key,
    campaign_id, channel, publisher, source,
    impressions, clicks, spend_local, currency, spend_eur,
    loaded_file_name, loaded_row_id
FROM silver_media_spend
