-- Canonical channel taxonomy shared by every agency (Spark SQL).
SELECT * FROM VALUES
    ('TV',           'Television',           'Offline'),
    ('OOH',          'Out of home',          'Offline'),
    ('ONLINE_VIDEO', 'Online video',         'Digital'),
    ('SOCIAL',       'Social',               'Digital'),
    ('SEARCH',       'Paid search',          'Digital'),
    ('DISPLAY',      'Programmatic display', 'Digital')
AS t(channel, channel_name, channel_group)
