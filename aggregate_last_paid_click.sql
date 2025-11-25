WITH lpc AS (
    SELECT
        s_lpc.visitor_id,
        s_lpc.visit_date,
        s_lpc.source,
        s_lpc.medium,
        s_lpc.campaign,
        s_lpc.lead_id,
        s_lpc.created_at,
        s_lpc.amount,
        s_lpc.closing_reason,
        s_lpc.status_id
    FROM (
        SELECT
            s.visitor_id,
            s.visit_date,
            s.source,
            s.medium,
            s.campaign,
            l.lead_id,
            l.created_at,
            l.amount,
            l.closing_reason,
            l.status_id,
            ROW_NUMBER() OVER (
                PARTITION BY s.visitor_id
                ORDER BY s.visit_date DESC
            ) AS rn
        FROM sessions s
        LEFT JOIN leads l 
            ON s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
        WHERE s.medium NOT IN ('organic')
    ) AS s_lpc
    WHERE s_lpc.rn = 1
),
aggregated_data AS (
    SELECT
        DATE(visit_date) AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(CASE 
            WHEN status_id = 142 
            THEN lead_id 
        END) AS purchases_count,
        SUM(CASE 
            WHEN status_id = 142 
            THEN amount 
        END) AS revenue
    FROM lpc
    GROUP BY 
        DATE(visit_date),
        source,
        medium,
        campaign
),
ads_costs AS (
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent 
        FROM vk_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent 
        FROM ya_ads
    ) AS all_ads
    GROUP BY campaign_date, utm_source, utm_medium, utm_campaign
)
SELECT
    ad.visit_date,
    ad.visitors_count,
    ad.utm_source,
    ad.utm_medium,
    ad.utm_campaign,
    COALESCE(ac.total_cost, 0) AS total_cost,
    ad.leads_count,
    ad.purchases_count,
    COALESCE(ad.revenue, 0) AS revenue
FROM aggregated_data ad
LEFT JOIN ads_costs ac
    ON ad.visit_date = ac.visit_date
    AND ad.utm_source = ac.utm_source
    AND ad.utm_medium = ac.utm_medium
    AND ad.utm_campaign = ac.utm_campaign
ORDER BY
    ad.visit_date ASC,
    ad.visitors_count DESC,
    ad.utm_source ASC,
    ad.utm_medium ASC,
    ad.utm_campaign ASC,
    ad.revenue DESC NULLS last
limit 15;


