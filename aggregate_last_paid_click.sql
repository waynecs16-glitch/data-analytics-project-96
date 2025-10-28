WITH last_paid_click AS (
    SELECT 
        s.visitor_id,
        s.visit_date::date AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        s.content AS utm_content,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id 
            ORDER BY 
                CASE WHEN s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') 
                     THEN 0 ELSE 1 END,
                s.visit_date DESC
        ) AS rn
    FROM sessions s
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
attributed_sessions AS (
    SELECT 
        lpc.visitor_id,
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        lpc.utm_content
    FROM last_paid_click lpc
    WHERE lpc.rn = 1
),
ad_costs AS (
    SELECT 
        campaign_date AS cost_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        SUM(daily_spent) AS daily_spent
    FROM (
        SELECT 
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT 
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            daily_spent
        FROM ya_ads
    ) AS all_ads
    GROUP BY 
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content
),
session_leads AS (
    SELECT 
        ases.visitor_id,
        ases.visit_date,
        ases.utm_source,
        ases.utm_medium,
        ases.utm_campaign,
        ases.utm_content,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM attributed_sessions ases
    LEFT JOIN leads l ON ases.visitor_id = l.visitor_id
),
aggregated_data AS (
    SELECT 
        sl.visit_date,
        sl.utm_source,
        sl.utm_medium,
        sl.utm_campaign,
        COUNT(DISTINCT sl.visitor_id) AS visitors_count,
        COALESCE(SUM(ac.daily_spent), 0) AS total_cost,
        COUNT(DISTINCT sl.lead_id) AS leads_count,
        COUNT(DISTINCT CASE 
            WHEN sl.closing_reason = 'Успешно реализовано' OR sl.status_id = 142 
            THEN sl.lead_id 
        END) AS purchases_count,
        SUM(CASE 
            WHEN sl.closing_reason = 'Успешно реализовано' OR sl.status_id = 142 
            THEN sl.amount 
        END) AS revenue
    FROM session_leads sl
    LEFT JOIN ad_costs ac ON 
        sl.visit_date = ac.cost_date AND
        sl.utm_source = ac.utm_source AND
        sl.utm_medium = ac.utm_medium AND
        sl.utm_campaign = ac.utm_campaign AND
        sl.utm_content = ac.utm_content
    GROUP BY 
        sl.visit_date,
        sl.utm_source,
        sl.utm_medium,
        sl.utm_campaign
)
SELECT 
    visit_date,
    visitors_count,
    utm_source,
    utm_medium,
    utm_campaign,
    total_cost,
    leads_count,
    purchases_count,
    revenue
FROM aggregated_data
ORDER BY 
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC,
    revenue DESC NULLS last
LIMIT 15;