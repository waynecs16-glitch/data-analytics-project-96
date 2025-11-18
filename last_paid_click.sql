WITH s_lpc AS (
    SELECT
        s_inner.visitor_id,
        s_inner.visit_date,
        s_inner.source,
        s_inner.medium,
        s_inner.campaign,
        l_inner.lead_id,
        l_inner.created_at,
        l_inner.amount,
        l_inner.closing_reason,
        l_inner.status_id,
        ROW_NUMBER() OVER (
            -- ФИКС 1: Партиция по visitor_id
            PARTITION BY s_inner.visitor_id
            ORDER BY s_inner.visit_date DESC
        ) AS rn
    FROM
        sessions AS s_inner
    INNER JOIN
        leads AS l_inner
        ON
            s_inner.visitor_id = l_inner.visitor_id
            AND s_inner.visit_date <= l_inner.created_at
    WHERE
        s_inner.medium NOT IN ('organic')
)

SELECT
    s_lpc.visitor_id,
    s_lpc.visit_date,
    s_lpc.source AS utm_source,
    s_lpc.medium AS utm_medium,
    s_lpc.campaign AS utm_campaign,
    s_lpc.lead_id,
    s_lpc.created_at,
    s_lpc.amount,
    s_lpc.closing_reason,
    s_lpc.status_id
FROM
    s_lpc
WHERE
    s_lpc.rn = 1
ORDER BY
    s_lpc.amount DESC NULLS LAST,
    s_lpc.visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;

