-- Canonical channel taxonomy shared by every agency.
CREATE OR REPLACE TABLE dim_channel AS
SELECT * FROM (VALUES
    ('TV',           'Television',          'Offline'),
    ('OOH',          'Out of home',         'Offline'),
    ('ONLINE_VIDEO', 'Online video',        'Digital'),
    ('SOCIAL',       'Social',              'Digital'),
    ('SEARCH',       'Paid search',         'Digital'),
    ('DISPLAY',      'Programmatic display','Digital')
) AS t(channel, channel_name, channel_group);
