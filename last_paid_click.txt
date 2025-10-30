SELECT
    s.visitor_id,
    s.visit_date,
    COALESCE(s_paid.source, s.source) AS utm_source, -- Используем source платного клика, если он есть, иначе - исходный source сессии
    COALESCE(s_paid.medium, s.medium) AS utm_medium, -- Используем medium платного клика, если он есть, иначе - исходный medium сессии
    COALESCE(s_paid.campaign, s.campaign) AS utm_campaign, -- Используем campaign платного клика, если он есть, иначе - исходный campaign сессии
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM
    sessions s -- Основная таблица сессий
LEFT JOIN
    leads l ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at -- Присоединяем лиды, которые произошли ПОСЛЕ или ВО ВРЕМЯ текущей сессии
LEFT JOIN (
    -- Подзапрос для поиска последней платной сессии для каждого лида
    SELECT
        s_inner.visitor_id,
        s_inner.source,
        s_inner.medium,
        s_inner.campaign,
        s_inner.visit_date,
        l_inner.lead_id, -- <--- ДОБАВЛЕНО: Теперь lead_id доступен для связывания
        -- Ранжируем платные сессии для каждого лида:
        -- 1 - это последняя платная сессия ПЕРЕД/ВО ВРЕМЯ создания лида
        ROW_NUMBER() OVER (
            PARTITION BY l_inner.lead_id
            ORDER BY s_inner.visit_date DESC
        ) AS rn
    FROM
        sessions s_inner
    INNER JOIN
        leads l_inner ON s_inner.visitor_id = l_inner.visitor_id
    WHERE
        -- Фильтр для "платных" сессий
        s_inner.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        AND s_inner.visit_date <= l_inner.created_at -- Сессия должна быть до или во время создания лида
) AS s_paid ON l.lead_id = s_paid.lead_id AND s_paid.rn = 1 -- Присоединяем только последнюю платную сессию (rn = 1) для лида
WHERE
    -- Оставляем только сессии, которые не привели к лиду,
    -- ИЛИ сессии, которые совпадают с последним платным кликом
    l.lead_id IS NULL OR s_paid.lead_id IS NOT NULL
ORDER BY
    l.amount DESC NULLS LAST,
    s.visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign asc
limit 10;