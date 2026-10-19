-- Reporting mart; KPIs computed from summed components, never averaged ratios (Spark SQL).
SELECT
    c.advertiser,
    c.market,
    d.year_month,
    ch.channel_group,
    f.channel,
    sum(f.spend_eur)                                                        AS spend_eur,
    sum(f.impressions)                                                      AS impressions,
    sum(f.clicks)                                                           AS clicks,
    round(CAST(sum(f.spend_eur) AS DOUBLE) / nullif(sum(f.impressions), 0) * 1000, 2) AS cpm_eur,
    round(CAST(sum(f.clicks) AS DOUBLE) / nullif(sum(f.impressions), 0), 5)           AS ctr,
    round(CAST(sum(f.spend_eur) AS DOUBLE) / nullif(sum(f.clicks), 0), 2)             AS cpc_eur
FROM fact_media_spend f
JOIN dim_campaign c ON c.campaign_id = f.campaign_id
JOIN dim_date d     ON d.date_key    = f.date_key
JOIN dim_channel ch ON ch.channel    = f.channel
GROUP BY c.advertiser, c.market, d.year_month, ch.channel_group, f.channel
