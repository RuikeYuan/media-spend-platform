-- Fact table. Grain: one row per source x day x campaign x channel x publisher.
-- Keeps the lineage columns so any reported number traces back to a file + row.
CREATE OR REPLACE TABLE fact_media_spend AS
SELECT
    s.activity_date      AS date_key,
    s.campaign_id,
    s.channel,
    s.publisher,
    s.source,
    s.impressions,
    s.clicks,
    s.spend_local,
    s.currency,
    s.spend_eur,
    s.loaded_file_id,
    s.loaded_file_name,
    s.loaded_row_num
FROM silver_media_spend s;
