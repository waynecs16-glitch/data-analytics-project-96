WITH RankedPaidSessions AS (
    SELECT
        visitor_id,
        visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign
    FROM sessions
    WHERE
        medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
AttributedLeads AS (
    SELECT
        l.visitor_id,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        (
            SELECT rps.visit_date
            FROM RankedPaidSessions rps
            WHERE
                rps.visitor_id = l.visitor_id
                AND rps.visit_date <= l.created_at
            ORDER BY rps.visit_date DESC
            LIMIT 1
        ) AS last_paid_click_date
    FROM leads l
WITH all_ads AS (
    -- Шаг 1: Объединение рекламных расходов (расходы уже очищены в источнике или здесь не чистятся)
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
attributed_leads AS (
    -- Шаг 2: Атрибуция лидов по Last Click и ЧИСТКА UTM-меток
    SELECT
        l.lead_id,
        l.created_at::date AS lead_date,
        l.amount,
        CASE
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142
            THEN 1
            ELSE 0
        END AS is_purchase,
        CASE
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142
            THEN l.amount
            ELSE 0
        END AS revenue_amount,
        FIRST_VALUE(
            CASE
                WHEN LOWER(s.source) IN ('vk', 'vk.com', 'vkontakte', 'vc') THEN 'vk'
                WHEN LOWER(s.source) LIKE '%telegram%' OR s.source = 'tg' THEN 'telegram'
                WHEN LOWER(s.source) IN ('facebook', 'facebook.com') THEN 'facebook'
                WHEN LOWER(s.source) IN ('twitter', 'twitter.com') THEN 'twitter'
                WHEN LOWER(s.source) IN ('yandex', 'yandex-direct', 'dzen', 'zen') THEN 'yandex'
                ELSE s.source
            END
        ) OVER (
            PARTITION BY l.lead_id
            ORDER BY s.visit_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS attr_source,
        FIRST_VALUE(s.medium) OVER (
            PARTITION BY l.lead_id
            ORDER BY s.visit_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS attr_medium,
        FIRST_VALUE(s.campaign) OVER (
            PARTITION BY l.lead_id
            ORDER BY s.visit_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS attr_campaign
    FROM
        leads l
    INNER JOIN
        sessions s ON l.visitor_id = s.visitor_id AND s.visit_date <= l.created_at
),
session_aggregation AS (
    -- Шаг 3: Агрегация сессий и ЧИСТКА UTM-меток (для visitors_count)
    SELECT
        visit_date::date AS visit_date,
        -- ЧИСТКА: Применяем логику для группировки Source
        CASE
            WHEN LOWER(source) IN ('vk', 'vk.com', 'vkontakte', 'vc') THEN 'vk'
            WHEN LOWER(source) LIKE '%telegram%' OR source = 'tg' THEN 'telegram'
            WHEN LOWER(source) IN ('facebook', 'facebook.com') THEN 'facebook'
            WHEN LOWER(source) IN ('twitter', 'twitter.com') THEN 'twitter'
            WHEN LOWER(source) IN ('yandex', 'yandex-direct', 'dzen', 'zen') THEN 'yandex'
            ELSE source
        END AS utm_source,
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
    COALESCE(SUM(ad.daily_spent), 0) AS total_cost,
    COUNT(al.lead_id) AS leads_count,
    COALESCE(SUM(al.is_purchase), 0) AS purchases_count,
    COALESCE(SUM(al.revenue_amount), 0) AS revenue
FROM
    session_aggregation sa
LEFT JOIN
    all_ads ad ON sa.visit_date = ad.campaign_date
              AND sa.utm_source = ad.utm_source
              AND sa.utm_medium = ad.utm_medium
              AND sa.utm_campaign = ad.utm_campaign
LEFT JOIN
    attributed_leads al ON sa.visit_date = al.lead_date
                       AND sa.utm_source = al.attr_source
                       AND sa.utm_medium = al.attr_medium
                       AND sa.utm_campaign = al.attr_campaign
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
    sa.visit_date ASC,
    sa.visitors_count DESC,
    sa.utm_source ASC,
    sa.utm_medium ASC,
    sa.utm_campaign ASC,
    revenue DESC NULLS last
limit 15;