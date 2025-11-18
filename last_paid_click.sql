WITH last_visits AS (
    SELECT
        visitor_id,
        visit_date,
        source,
        medium,
        campaign,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id
            ORDER BY visit_date DESC
        ) AS rn
    FROM sessions
    WHERE medium NOT IN ('organic')
)
SELECT
    lv.visitor_id,
    lv.visit_date,
    lv.source AS utm_source,
    lv.medium AS utm_medium,
    lv.campaign AS utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM last_visits AS lv
LEFT JOIN leads AS l
    ON
        lv.visitor_id = l.visitor_id
        AND lv.visit_date <= l.created_at
WHERE lv.rn = 1
ORDER BY
    l.amount DESC NULLS LAST,
    lv.visit_date ASC,
    lv.source ASC,
    lv.medium ASC,
    lv.campaign ASC
LIMIT 10;

