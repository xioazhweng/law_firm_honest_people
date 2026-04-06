/*
2. Получить статистику по работе сотрудников в разрезе по месяцам прошлого года. 
Отчет представить в следующем виде:
Для каждого сотрудника по одной строке отчета, 
с 24 столбцами – по 2 столбца на месяц. 
В первом столбце указывается число договоров в этом месяце, 
которые оформил сотрудник, во втором – процент изменений относительно прошлого месяца.
*/

WITH info AS (
        SELECT 
            manager_number,
            EXTRACT(MONTH FROM month_start) AS month, 
            TO_CHAR(month_start, 'Month') AS month_name,
            amount,
            COALESCE(
            ((amount - LAG(amount) OVER (PARTITION BY manager_number ORDER BY month_start)) 
            / NULLIF(LAG(amount) OVER (PARTITION BY manager_number ORDER BY month_start), 0)) * 100,
            0
        )  AS relative
        FROM (
            SELECT 
                manager_number,
                DATE_TRUNC('month', start_date) AS month_start,
                COUNT(cooperation_agreement_no) AS amount
            FROM cooperation_agreement
            WHERE start_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'
              AND start_date < DATE_TRUNC('year', CURRENT_DATE)
            GROUP BY manager_number, month_start
        ) AS temp
    ) SELECT manager_number,
            SUM(CASE WHEN month = 1 THEN amount ELSE 0 END) AS January_amount,
            SUM(CASE WHEN month = 1 THEN relative ELSE 0 END) AS January_relative,
            SUM(CASE WHEN month = 2 THEN amount ELSE 0 END) AS February_amount,
            SUM(CASE WHEN month = 2 THEN relative ELSE 0 END) AS February_relative,
            SUM(CASE WHEN month = 3 THEN amount ELSE 0 END) AS March_amount,
            SUM(CASE WHEN month = 3 THEN relative ELSE 0 END) AS March_relative,
            SUM(CASE WHEN month = 4 THEN amount ELSE 0 END) AS April_amount,
            SUM(CASE WHEN month = 4 THEN relative ELSE 0 END) AS April_relative,
            SUM(CASE WHEN month = 5 THEN amount ELSE 0 END) AS May_amount,
            SUM(CASE WHEN month = 5 THEN relative ELSE 0 END) AS May_relative,
            SUM(CASE WHEN month = 6 THEN amount ELSE 0 END) AS June_amount,
            SUM(CASE WHEN month = 6 THEN relative ELSE 0 END) AS June_relative,
            SUM(CASE WHEN month = 7 THEN amount ELSE 0 END) AS July_amount,
            SUM(CASE WHEN month = 7 THEN relative ELSE 0 END) AS July_relative,
            SUM(CASE WHEN month = 8 THEN amount ELSE 0 END) AS August_amount,
            SUM(CASE WHEN month = 8 THEN relative ELSE 0 END) AS August_relative,
            SUM(CASE WHEN month = 9 THEN amount ELSE 0 END) AS September_amount,
            SUM(CASE WHEN month = 9 THEN relative ELSE 0 END) AS September_relative,
            SUM(CASE WHEN month = 10 THEN amount ELSE 0 END) AS October_amount,
            SUM(CASE WHEN month = 10 THEN relative ELSE 0 END) AS October_relative,
            SUM(CASE WHEN month = 11 THEN amount ELSE 0 END) AS November_amount,
            SUM(CASE WHEN month = 11 THEN relative ELSE 0 END) AS November_relative,
            SUM(CASE WHEN month = 12 THEN amount ELSE 0 END) AS December_amount,
            SUM(CASE WHEN month = 12 THEN relative ELSE 0 END) AS December_relative FROM info GROUP BY manager_number ORDER BY manager_number;
