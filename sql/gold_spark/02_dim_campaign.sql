-- Campaign master plus inferred members for IDs an agency delivered before the
-- master was updated, so facts are never dropped by an inner join (Spark SQL).
SELECT campaign_id, campaign_name, advertiser, brand, market, agency_source, FALSE AS is_inferred
FROM seed_campaigns
UNION ALL
SELECT
    s.campaign_id,
    'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN',
    first(s.source) AS agency_source,
    TRUE AS is_inferred
FROM silver_media_spend s
LEFT ANTI JOIN seed_campaigns c ON c.campaign_id = s.campaign_id
GROUP BY s.campaign_id
