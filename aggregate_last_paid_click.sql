WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM (
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
    ) AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE s.rn = 1
),

aggregated_data AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        DATE(visit_date) AS visit_date,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(CASE
            WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
                THEN lead_id
        END) AS purchases_count,
        SUM(CASE
            WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
                THEN amount
        END) AS revenue
    FROM last_paid_click
    GROUP BY
        DATE(visit_date),
        utm_source,
        utm_medium,
        utm_campaign
),

ads_costs AS (
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
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
    ) AS all_ads
    GROUP BY campaign_date, utm_source, utm_medium, utm_campaign
)

SELECT
    ad.visit_date,
    ad.visitors_count,
    ad.utm_source,
    ad.utm_medium,
    ad.utm_campaign,
    ad.leads_count,
    ad.purchases_count,
    COALESCE(ac.total_cost, 0) AS total_cost,
    COALESCE(ad.revenue, 0) AS revenue
FROM aggregated_data AS ad
LEFT JOIN ads_costs AS ac
    ON
        ad.visit_date = ac.visit_date
        AND ad.utm_source = ac.utm_source
        AND ad.utm_medium = ac.utm_medium
        AND ad.utm_campaign = ac.utm_campaign
ORDER BY
    ad.visit_date ASC,
    ad.visitors_count DESC,
    ad.utm_source ASC,
    ad.utm_medium ASC,
    ad.utm_campaign ASC,
    ad.revenue DESC NULLS LAST
LIMIT 15;

