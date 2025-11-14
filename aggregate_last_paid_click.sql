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
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        COALESCE(s_lpc.source, s.source) AS utm_source,
        COALESCE(s_lpc.medium, s.medium) AS utm_medium,
        COALESCE(s_lpc.campaign, s.campaign) AS utm_campaign
    FROM
        sessions AS s
    LEFT JOIN
        leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    LEFT JOIN s_lpc
        ON
            s.visitor_id = s_lpc.visitor_id
            AND s.visit_date = s_lpc.visit_date
            AND s_lpc.rn = 1
    WHERE
        l.lead_id IS NULL OR s_lpc.attributed_lead_id IS NOT NULL
),
ad_costs AS (
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent AS total_cost
    FROM
        vk_ads
    UNION ALL
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent AS total_cost
    FROM
        ya_ads
)
SELECT
    DATE(lpc.visit_date) AS visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
    COALESCE(SUM(ac.total_cost), 0) AS total_cost,
    COUNT(DISTINCT lpc.lead_id)
        AS leads_count,
    COUNT(
        DISTINCT CASE
            WHEN
                lpc.closing_reason = 'Успешно реализовано'
                OR lpc.status_id = 142
                THEN lpc.lead_id
        END
    ) AS purchases_count,
    SUM(
        CASE
            WHEN
                lpc.closing_reason = 'Успешно реализовано'
                OR lpc.status_id = 142
                THEN lpc.amount
            ELSE 0
        END
    ) AS revenue
FROM
    lpc_attribution AS lpc
LEFT JOIN
    ad_costs AS ac
    ON
        DATE(lpc.visit_date) = ac.visit_date
        AND lpc.utm_source = ac.utm_source
        AND lpc.utm_medium = ac.utm_medium
        AND lpc.utm_campaign = ac.utm_campaign
GROUP BY
    1, 2, 3, 4
ORDER BY
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC,
    revenue DESC NULLS LAST
LIMIT 15;

