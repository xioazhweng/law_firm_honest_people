
/*
Процедура 1. Оформление з/п сотрудникам.
Процедура предназначена для оформления з/п или аванса для всех сотрудников, 
которые еще работают в фирме. 
Процедура принимает тип выплат (з/п или аванс) и формирует исходящие 
платежки на всех сотрудников. 
Для аванса формируются платежки за текущий месяц, для з/п – за прошлый. 
Если такие платежки уже есть, то выдается ошибка. Если платежки созданы успешно, 
то выводится сумма всех платежек.
*/

CREATE OR REPLACE PROCEDURE pay_employees(
    type           TEXT,
    payment_month  DATE DEFAULT date_trunc('month', CURRENT_DATE)
    )
LANGUAGE plpgsql
AS $$
DECLARE
    doc RECORD;
    pay_date DATE;
    total_amount BIGINT := 0;
BEGIN
    IF type = 'ADVANCE' THEN
        pay_date := CURRENT_DATE;  
    ELSIF type = 'PAYMENT' THEN
        pay_date := payment_month - INTERVAL '1 day'; 
    ELSE
        RAISE EXCEPTION 'Invalid payment type: %', type;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM outgoing_pay_document
        WHERE payment_type = type
          AND date_trunc('month', payment_date) = date_trunc('month', pay_date)
    ) THEN
        RAISE EXCEPTION 'Платежки уже сформированы';
    END IF;


    FOR doc IN
        SELECT employee_number, account_no, bik, salary
        FROM employee
        WHERE fire_date IS NULL
    LOOP
        INSERT INTO outgoing_pay_document(
            payment_date,
            account_no,
            bik,
            employee_number,
            amount,
            payment_type,
            result
        ) VALUES (
            pay_date,
            doc.account_no,
            doc.bik,
            doc.employee_number,
            CASE WHEN type = 'ADVANCE' THEN 15000 ELSE  GREATEST(doc.salary - 15000, 0) END,
            type,
            NULL
        );

        total_amount := total_amount + CASE WHEN type = 'ADVANCE' THEN 15000 ELSE  GREATEST(doc.salary - 15000, 0) END;
    END LOOP;
    RAISE NOTICE '%: Всего к выплате: %', payment_month, total_amount;
END;
$$;