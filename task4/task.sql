SELECT
    phrase,
    groupArray((hour, hourly_views)) as views_by_hour
FROM (
    SELECT
        phrase,
        hour,
        hourly_views
    FROM (
        SELECT
            phrase,
            toHour(dt) as hour,
            max(views) - lag(max(views), 1, 0) OVER (
                PARTITION BY phrase
                ORDER BY toHour(dt)
            ) as hourly_views
        FROM phrases_views
        WHERE
            campaign_id = 1111111
            AND toDate(dt) = '2025-01-01'
        GROUP BY phrase, toHour(dt)
    )
    WHERE hourly_views > 0
    ORDER BY phrase, hour DESC
)
GROUP BY phrase