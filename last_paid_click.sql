WITH last_paid_click AS (
    SELECT 
        s.visitor_id,
        s.visit_date,
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
)
SELECT 
    ases.visitor_id,
    ases.visit_date,
    ases.utm_source,
    ases.utm_medium,
    ases.utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM attributed_sessions ases
LEFT JOIN leads l ON ases.visitor_id = l.visitor_id
ORDER BY 
    l.amount DESC NULLS LAST,
    ases.visit_date ASC,
    ases.utm_source ASC,
    ases.utm_medium ASC,
    ases.utm_campaign ASC
LIMIT 10;