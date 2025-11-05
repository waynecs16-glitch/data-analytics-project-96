WITH all_ads AS (
    SELECT
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent
    FROM
        vk_ads
    UNION ALL
    SELECT
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent
    FROM
        ya_ads
),
attributed_lpc_data AS (
    SELECT
        s_lpc.visit_date::date AS lpc_date, -- Дата LPC-клика
        s_lpc.source AS lpc_source,
        s_lpc.medium AS lpc_medium,
        s_lpc.campaign AS lpc_campaign,bvc
        l.lead_id,
        l.amount,
        CASE
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN 1
            ELSE 0
        END AS is_purchase,
        CASE
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN l.amount
            ELSE 0
        END AS revenue_amount
    FROM
        leads l
    INNER JOIN (
        SELECT
            s_inner.visitor_id,
            s_inner.visit_date,
            s_inner.source,
            s_inner.medium,
            s_inner.campaign,
            l_inner.lead_id,
            ROW_NUMBER() OVER (
                PARTITION BY l_inner.lead_id
                ORDER BY s_inner.visit_date DESC
            ) AS rn
        FROM
            sessions s_inner
        INNER JOIN
            leads l_inner ON s_inner.visitor_id = l_inner.visitor_id
        WHERE
            s_inner.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
            AND s_inner.visit_date <= l_inner.created_at
    ) AS s_lpc ON l.lead_id = s_lpc.lead_id
              AND s_lpc.rn = 1 -- Оставляем ТОЛЬКО LPC-строку
),
session_aggregation AS (
    SELECT
        visit_date::date AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        COUNT(visitor_id) AS visitors_count
    FROM
        sessions
    GROUP BY 1, 2, 3, 4
)
SELECT
    sa.visit_date,
    sa.visitors_count,
    sa.utm_source,
    sa.utm_medium,
    sa.utm_campaign,
    -- Затраты (присоединенные по дате/меткам)
    COALESCE(SUM(ad.daily_spent), 0) AS total_cost,
    COUNT(alpc.lead_id) AS leads_count,
    COALESCE(SUM(alpc.is_purchase), 0) AS purchases_count,
    COALESCE(SUM(alpc.revenue_amount), 0) AS revenue
FROM
    session_aggregation sa -- ВСЕ визиты и их метки
LEFT JOIN
    all_ads ad ON sa.visit_date = ad.campaign_date
              AND sa.utm_source = ad.utm_source
              AND sa.utm_medium = ad.utm_medium
              AND sa.utm_campaign = ad.utm_campaign
LEFT JOIN
    attributed_lpc_data alpc ON sa.visit_date = alpc.lpc_date -- Атрибуция по дате LPC-клика!
                AND sa.utm_source = alpc.lpc_source
                AND sa.utm_medium = alpc.lpc_medium
                AND sa.utm_campaign = alpc.lpc_campaign
WHERE
    (sa.utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') OR alpc.lead_id IS NOT NULL)
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC,
    revenue DESC NULLS LAST
LIMIT 15;