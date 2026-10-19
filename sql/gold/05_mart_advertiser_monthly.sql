-- Reporting mart: what a consultant or a BI dashboard actually queries.
-- KPIs are computed from summed components (never averaged ratios).
CREATE OR REPLACE TABLE mart_advertiser_monthly AS
SELECT
    c.advertiser,
    c.market,
    d.year_month,
    ch.channel_group,
    f.channel,
    sum(f.spend_eur)                                            AS spend_eur,
    sum(f.impressions)                                          AS impressions,
    sum(f.clicks)                                               AS clicks,
    round(sum(f.spend_eur) / nullif(sum(f.impressions), 0) * 1000, 2) AS cpm_eur,
    round(sum(f.clicks)    / nullif(sum(f.impressions), 0), 5)        AS ctr,
    round(sum(f.spend_eur) / nullif(sum(f.clicks), 0), 2)             AS cpc_eur
FROM fact_media_spend f
JOIN dim_campaign c  ON c.campaign_id = f.campaign_id
JOIN dim_date d      ON d.date_key    = f.date_key
JOIN dim_channel ch  ON ch.channel    = f.channel
GROUP BY ALL
ORDER BY advertiser, market, year_month, channel;
