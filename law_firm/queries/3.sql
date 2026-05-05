/*
3. Получить информацию о клиентах-должниках. 
Отчет представить в виде:
ФИО пользователя, 
число договоров, 
сумма всех договоров с учетом услуг, 
сумма всех платежей клиента, 
дата последнего договора, 
дата последнего платежа, 
р/с с которого осуществлялся последний платеж, 
сумма задолженности.
*/

WITH clients_info AS (
    SELECT 
        c.id_client,
        COALESCE(entr.fio, lgl.company_name, prsn.fio) AS name
    FROM client c
    LEFT JOIN client_entrepreneur entr ON c.id_client = entr.id_client
    LEFT JOIN client_legal lgl ON c.id_client = lgl.id_client
    LEFT JOIN client_person prsn ON c.id_client = prsn.id_client
),
services AS ( 
    SELECT 
        c.id_client, 
        pls.price, 
        aa.created_at, 
        aa.assignment_agreement_no 
    FROM client c
    LEFT JOIN cooperation_agreement ca ON c.id_client = ca.id_client 
    LEFT JOIN assignment_agreement aa 
        ON aa.id_client = ca.id_client 
        AND aa.cooperation_agreement_no = ca.cooperation_agreement_no 
    LEFT JOIN contract_service cs 
        ON cs.cooperation_agreement_no = aa.cooperation_agreement_no AND
         cs.id_client = aa.id_client AND
         cs.assignment_agreement_no = aa.assignment_agreement_no 
    LEFT JOIN price_list_service pls 
        ON cs.id_service = pls.id_service 
        AND aa.creation_price_list_date = pls.creation_date 
        AND pls.client_type = c.client_type
),
payments AS (
    SELECT 
        c.id_client, 
        ipd.amount, 
        ipd.payment_date,
        ipd.account_no,
        ipd.bik,
        ROW_NUMBER() OVER(PARTITION BY c.id_client ORDER BY ipd.payment_date DESC) AS rn
    FROM client c
    LEFT JOIN cooperation_agreement ca ON c.id_client = ca.id_client 
    LEFT JOIN assignment_agreement aa 
        ON aa.id_client = ca.id_client 
        AND aa.cooperation_agreement_no = ca.cooperation_agreement_no 
    LEFT JOIN payment_document pd ON 
        pd.id_client = aa.id_client AND 
        pd.cooperation_agreement_no = aa.cooperation_agreement_no AND 
        pd.assignment_agreement_no = aa.assignment_agreement_no
    JOIN payment_bundle pb ON pb.id_payment_bundle = pd.id_payment_bundle
    JOIN income_payment_bundle ipb ON ipb.id_payment_bundle = pb.id_payment_bundle
    JOIN income_pay_document ipd ON ipd.id_income_pay_document = ipb.id_income_pay_document
)
SELECT  
    ci.name,
    ci.id_client,
    COUNT(DISTINCT s.assignment_agreement_no) AS "Число договоров",
    SUM(s.price) AS "Сумма всех договоров",
    SUM(p.amount) AS "Сумма всех платежей клиента",
    MAX(s.created_at) AS "Дата последнего договора",
    MAX(p.payment_date) AS "Дата последнего платежа",
    MAX(CASE WHEN p.rn = 1 THEN p.account_no END) AS "Р/С последнего платежа",
    SUM(s.price) - SUM(p.amount) AS "Сумма задолженности"
FROM services s
JOIN clients_info ci ON ci.id_client = s.id_client
LEFT JOIN payments p ON p.id_client = ci.id_client
GROUP BY ci.name, ci.id_client
HAVING SUM(s.price) - SUM(p.amount) > 0; 