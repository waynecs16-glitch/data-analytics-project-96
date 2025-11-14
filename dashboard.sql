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
),

aggregated_performance_daily AS (
    SELECT
        DATE(lpc.visit_date) AS visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
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
    GROUP BY
        1, 2, 3, 4
)

SELECT
    apd.visit_date,
    apd.utm_source,
    apd.utm_medium,
    apd.utm_campaign,
    apd.visitors_count,
    apd.leads_count,
    apd.purchases_count,
    COALESCE(acd.total_cost_daily, 0) AS total_cost
FROM
    aggregated_performance_daily AS apd
LEFT JOIN
    ad_costs_daily AS acd
    ON
        apd.visit_date = acd.visit_date
        AND apd.utm_source = acd.utm_source
        AND apd.utm_medium = acd.utm_medium
        AND apd.utm_campaign = acd.utm_campaign
ORDER BY
    apd.visit_date ASC,
    apd.utm_source ASC;
