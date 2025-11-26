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
        FROM sessions AS s
        LEFT JOIN leads AS l
            ON
                s.visitor_id = l.visitor_id
                AND s.visit_date <= l.created_at
        WHERE s.medium NOT IN ('organic')
    ) AS s_lpc
    WHERE s_lpc.rn = 1
)

SELECT
    lpc.visitor_id,
    lpc.visit_date,
    lpc.source AS utm_source,
    lpc.medium AS utm_medium,
    lpc.campaign AS utm_campaign,
    lpc.lead_id,
    lpc.created_at,
    lpc.amount,
    lpc.closing_reason,
    lpc.status_id
FROM lpc
ORDER BY
    lpc.amount DESC NULLS LAST,
    lpc.visit_date ASC,
    lpc.source ASC,
    lpc.medium ASC,
    lpc.campaign ASC
LIMIT 10;
