WITH s_lpc AS (
    SELECT
        s_inner.*,
        l_inner.lead_id AS attributed_lead_id,
        ROW_NUMBER() OVER (
            PARTITION BY l_inner.lead_id
            ORDER BY s_inner.visit_date DESC
        ) AS rn
    FROM
        sessions AS s_inner
    INNER JOIN
        leads AS l_inner
        ON s_inner.visitor_id = l_inner.visitor_id
    WHERE
        s_inner.medium IN (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
        AND s_inner.visit_date <= l_inner.created_at
),

lpc_attribution AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        l.lead_id,
        l.amount,
        l.status_id,
        s_lpc.attributed_lead_id AS lpc_attributed_lead_id,
        COALESCE(s_lpc.source, s.source) AS utm_source,
        COALESCE(s_lpc.medium, s.medium) AS utm_medium,
        -- Флаг атрибуции LPC
        COALESCE(s_lpc.campaign, s.campaign) AS utm_campaign
    FROM
        sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    LEFT JOIN s_lpc
        ON
            s.visitor_id = s_lpc.visitor_id
            AND s.visit_date = s_lpc.visit_date
            AND s_lpc.rn = 1
    WHERE
        s_lpc.attributed_lead_id IS NOT NULL
        OR (l.lead_id IS NULL AND s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'))
),

ad_costs_daily AS (
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost_daily
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM ya_ads
    ) AS combined_ads
    GROUP BY 1, 2, 3, 4
)

SELECT
    DATE(lpc.visit_date) AS visit_date,
    COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    COALESCE(acd.total_cost_daily, 0) AS total_cost,
    COUNT(
        DISTINCT CASE
            WHEN lpc.lpc_attributed_lead_id IS NOT NULL THEN lpc.lead_id
        END
    ) AS leads_count,
    COUNT(
        DISTINCT CASE
            WHEN
                lpc.lpc_attributed_lead_id IS NOT NULL AND lpc.status_id = 142
                THEN lpc.lead_id
        END
    ) AS purchases_count,
    SUM(
        CASE
            WHEN
                lpc.lpc_attributed_lead_id IS NOT NULL AND lpc.status_id = 142
                THEN lpc.amount
            ELSE 0
        END
    ) AS revenue
FROM
    lpc_attribution AS lpc
LEFT JOIN
    ad_costs_daily AS acd
    ON
        DATE(lpc.visit_date) = acd.visit_date
        AND lpc.utm_source = acd.utm_source
        AND lpc.utm_medium = acd.utm_medium
        AND lpc.utm_campaign = acd.utm_campaign
GROUP BY
    1, 3, 4, 5, 6
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC,
    visitors_count DESC
LIMIT 15;
