/*
Процедура 2. Оформление нового прайс-листа
Процедура предназначена для изменения цен в некотором прайс-листе. 
Процедура принимает id прайс-листа и процент надбавки или скидки. 
В результате формируется новый прайс-лист, который содержит услуги 
старого прайс-листа и измененную на % цену.
*/

CREATE OR REPLACE PROCEDURE fork_price_list(
    created_at      DATE,
    rate            FLOAT,
    new_date        DATE DEFAULT CURRENT_DATE 
)
LANGUAGE plpgsql
AS $$
DECLARE
    price_list_params RECORD;
    info              RECORD;
BEGIN
    SELECT nds, extra_charge, inflation_rate
    INTO price_list_params
    FROM price_list 
    WHERE creation_date = created_at;

    INSERT INTO price_list(
        creation_date,
        nds,
        extra_charge,
        inflation_rate
    ) VALUES (
        new_date,
        price_list_params.nds,
        price_list_params.extra_charge,
        price_list_params.inflation_rate 
    );

    FOR info IN
        SELECT id_service, price, client_type
        FROM price_list_service 
        WHERE creation_date = created_at
    LOOP
        INSERT INTO price_list_service(
            id_service,
            creation_date,
            price,
            client_type
        ) 
        VALUES (
            info.id_service,
            new_date,
            info.price * (1 - rate / 100),
            info.client_type
        );
    END LOOP;
END;
$$;

