/*
Получить статистику по пользователям. 
Отчет представить в виде:
ФИО пользователя, 
id, 
дата регистрации, 
число договоров, 
общее число услуг, 
среднее число услуг по договору, 
дата последнего договора для каждого пользователя, 
название самой популярной услуги, 
список счетов пользователя через запятую.
*/


WITH clients_info AS (
    SELECT 
        c.id_client,
        COALESCE(entr.fio, lgl.company_name, prsn.fio) AS name,
        c.created_at
    FROM client c
    LEFT JOIN client_entrepreneur entr ON c.id_client = entr.id_client
    LEFT JOIN client_legal lgl ON c.id_client = lgl.id_client
    LEFT JOIN client_person prsn ON c.id_client = prsn.id_client
),
agreements AS (
    SELECT 
        ca.id_client,
        aa.assignment_agreement_no,
        aa.cooperation_agreement_no,
        aa.completion_date
    FROM cooperation_agreement ca
    LEFT JOIN assignment_agreement aa
        ON ca.cooperation_agreement_no = aa.cooperation_agreement_no
        AND ca.id_client = aa.id_client
),
agreements_stats AS (
    SELECT 
        id_client,
        COUNT(DISTINCT assignment_agreement_no) AS number_of_agreements,
        MAX(completion_date) AS last_agreement_date
    FROM agreements
    GROUP BY id_client
),
services_per_agreement AS (
    SELECT 
        a.id_client,
        a.assignment_agreement_no,
        COUNT(cs.id_service) AS services_count
    FROM agreements a
    LEFT JOIN contract_service cs
        ON cs.assignment_agreement_no = a.assignment_agreement_no
        AND cs.cooperation_agreement_no = a.cooperation_agreement_no
        AND cs.id_client = a.id_client
    GROUP BY a.id_client, a.assignment_agreement_no
),
services_stats AS (
    SELECT 
        id_client,
        SUM(services_count) AS total_services,
        AVG(services_count) AS avg_services_per_agreement
    FROM services_per_agreement
    GROUP BY id_client
),
popular_service AS (
    SELECT id_client, name_service
    FROM (
        SELECT 
            a.id_client,
            s.name_service,
            COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY a.id_client ORDER BY COUNT(*) DESC) AS rn
        FROM agreements a
        JOIN contract_service cs
            ON cs.assignment_agreement_no = a.assignment_agreement_no
            AND cs.cooperation_agreement_no = a.cooperation_agreement_no
            AND cs.id_client = a.id_client
        JOIN service s ON s.id_service = cs.id_service
        GROUP BY a.id_client, s.name_service
    ) t
    WHERE rn = 1
),
accounts AS (
    SELECT pt.cooperation_agreement_no,
	   c.id_client,
		string_agg(pt.account_no::TEXT, ',' order by pt.account_no) as accounts_list
    FROM client c
    LEFT JOIN cooperation_agreement ca 
        ON ca.id_client = c.id_client  
    LEFT JOIN assignment_agreement aa 
        ON ca.cooperation_agreement_no  = aa.cooperation_agreement_no 
        AND ca.id_client = aa.id_client
    LEFT JOIN payment_transaction pt 
        ON pt.cooperation_agreement_no = ca.cooperation_agreement_no 
    GROUP BY pt.cooperation_agreement_no, c.id_client
)
SELECT 
    ci.name,
    ci.id_client AS id,
    ci.created_at AS "Дата регистрации",
    COALESCE(ast.number_of_agreements, 0) AS "Количество договоров",
    COALESCE(ss.total_services, 0) AS "Количество услуг",
    ROUND(COALESCE(ss.avg_services_per_agreement, 0), 0) AS avg_services_per_agreement,
    ast.last_agreement_date AS "Дата заключения последнего договора",
    ps.name_service AS "Популярная услуга",
    acc.accounts_list AS "Номера счетов"
FROM clients_info ci
LEFT JOIN agreements_stats ast ON ci.id_client = ast.id_client
LEFT JOIN services_stats ss ON ci.id_client = ss.id_client
LEFT JOIN popular_service ps ON ci.id_client = ps.id_client
LEFT JOIN accounts acc ON ci.id_client = acc.id_client
ORDER BY ci.name;

