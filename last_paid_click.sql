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
)

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
ORDER BY
    l.amount DESC NULLS LAST,
    s.visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;
