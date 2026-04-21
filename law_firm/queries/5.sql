
/*
5. Получить информацию о сумме выплат сотрудникам за текущий год.
Отчет представить в виде:
ФИО сотрудника; 
количество авансов и з/п, которые должны были быть выплачены за текущий год (сотрудник мог быть принять на работу в середине года); 
сумма всех платежей, которые были выплачены; 
текущая задолженность перед сотрудником; 
дата самой старой запланированной выплаты, по которой выплат не было или было выплачено не полностью.
*/

with current_year as (
        SELECT *
        FROM outgoing_pay_document opd
        WHERE opd.payment_date < CURRENT_DATE AND
                opd.payment_date >= date_trunc('year', CURRENT_DATE)
),
payments_dates as (
        SELECT employee_number, payment_date, payment_type,
        ROW_NUMBER() OVER(PARTITION BY employee_number ORDER BY payment_date) AS rn
        FROM current_year
        WHERE result = FALSE 
),
last_payments as (
        SELECT opd.employee_number,
                MIN(CASE WHEN pd.rn = 1 THEN pd.payment_date END) AS "Р/С последнего платежа"
        FROM outgoing_pay_document opd
        LEFT JOIN payments_dates pd ON opd.employee_number = pd.employee_number
        GROUP BY opd.employee_number
) 
SELECT  e.fio,
        "Р/С последнего платежа",
        SUM(CASE WHEN cy.payment_type = 'ADVANCE' THEN 1 ELSE 0 END) AS "Количество авансов",
        SUM(CASE WHEN cy.payment_type = 'PAYMENT' THEN 1 ELSE 0 END) AS "Количество выплат",
        SUM(CASE WHEN cy.result = TRUE THEN amount ELSE 0 END) AS "Сумма, выплаченных платежей",
        SUM(CASE WHEN cy.result = FALSE THEN amount ELSE 0 END) AS "Сумма, задолжности перед сотрудником"
FROM current_year cy
JOIN employee e ON cy.employee_number = e.employee_number
JOIN last_payments lp ON lp.employee_number = e.employee_number
GROUP BY cy.employee_number, e.fio, "Р/С последнего платежа"
