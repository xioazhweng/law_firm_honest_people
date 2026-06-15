/*
4. Получить рейтинг услуг по популярности. 
Отчет представить в виде:
Название услуги; 
кол-во договоров, где эта услуга применяется (сортировать по этому полю); 
среднее число раз включения услуги в договор без учета договоров, где услуга не применялась (значение будет больше или равно 1);
текущая стоимость услуги по самому дорогому прайс-листу; 
текущая стоимость услуги по самому дешевому прайс-листу.

*/

WITH services AS (
    SELECT
        pls.id_service,
        pls.client_type,
        MAX(pls.price) AS max_cost,
        MIN(pls.price) AS min_cost
    FROM price_list_service pls
    GROUP BY
        pls.id_service,
        pls.client_type
),
info AS (
    SELECT
        t.id_service,
        ROUND(AVG(t.amount), 0) AS avg_usage,
        COUNT(t.assignment_agreement_no) AS meow
    FROM (
        SELECT
            cs.id_service,
            cs.assignment_agreement_no,
            COUNT(*) AS amount
        FROM contract_service cs
        GROUP BY
            cs.id_service,
            cs.assignment_agreement_no
    ) t
    GROUP BY
        t.id_service
)
SELECT
    s.name_service AS "Название услуги",
    i.meow,
    i.avg_usage AS "Количество договоров, где услуга применяется",
    ss.client_type AS "Тип клиента",
    ss.max_cost AS "Текущая стоимость услуги по самому дорогому прайс-листу",
    ss.min_cost AS "Текущая стоимость услуги по самому дешевому прайс-листу"
FROM service s
JOIN services ss
    ON s.id_service = ss.id_service
JOIN info i
    ON i.id_service = s.id_service
ORDER BY
    i.meow;


/*
-- версия, где типы клиентов выносим в отдельные колонки
WITH services AS (
    SELECT
        pls.id_service,
        pls.client_type,
        MAX(pls.price) as max_cost,
        MIN(pls.price) as min_cost
    FROM price_list_service pls
    GROUP BY pls.id_service, pls.client_type
),
info AS (
    SELECT 
        id_service,
        ROUND(AVG(amount),0) AS avg_usage,
        COUNT(assignment_agreement_no) as meow
    FROM (
        SELECT 
            cs.id_service,
            cs.assignment_agreement_no,
            COUNT(*) AS amount
        FROM contract_service cs
        GROUP BY cs.assignment_agreement_no, cs.id_service
    ) t
    GROUP BY id_service
)
SELECT 
    s.name_service AS "Название услуги", 
    i.meow,
    i.avg_usage AS "Количество договоров, где услуга применяется",
    -- PERSON
    MAX(CASE WHEN ss.client_type = 'PERSON' THEN ss.max_cost END) AS max_person,
    MIN(CASE WHEN ss.client_type = 'PERSON' THEN ss.min_cost END) AS min_person,
    -- ENTREPRENEUR
    MAX(CASE WHEN ss.client_type = 'ENTREPRENEUR' THEN ss.max_cost END) AS max_entrepreneur,
    MIN(CASE WHEN ss.client_type = 'ENTREPRENEUR' THEN ss.min_cost END) AS min_entrepreneur,
    -- LEGAL
    MAX(CASE WHEN ss.client_type = 'LEGAL' THEN ss.max_cost END) AS max_legal,
    MIN(CASE WHEN ss.client_type = 'LEGAL' THEN ss.min_cost END) AS min_legal
FROM service s
JOIN services ss ON s.id_service = ss.id_service 
JOIN info i ON i.id_service = s.id_service
GROUP BY s.name_service, i.meow, i.avg_usage
ORDER BY i.meow;


*/