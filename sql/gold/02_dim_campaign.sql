-- Campaign dimension from the campaign master, plus "inferred members" for
-- campaign IDs an agency delivered before the master was updated.
-- Facts are never dropped for a missing dimension row; instead the gap is made
-- visible (is_inferred = true) and flagged by the `inferred_campaigns` check.
CREATE OR REPLACE TABLE dim_campaign AS
SELECT
    campaign_id, campaign_name, advertiser, brand, market, agency_source,
    FALSE AS is_inferred
FROM seed_campaigns
UNION ALL
SELECT
    s.campaign_id,
    'UNKNOWN' AS campaign_name,
    'UNKNOWN' AS advertiser,
    'UNKNOWN' AS brand,
    'UNKNOWN' AS market,
    any_value(s.source) AS agency_source,
    TRUE AS is_inferred
FROM silver_media_spend s
WHERE s.campaign_id NOT IN (SELECT campaign_id FROM seed_campaigns)
GROUP BY s.campaign_id;
