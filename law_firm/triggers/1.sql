/*
Триггер 1:
При добавлении услуги по договору убедиться, 
что цена услуги выставляется по тому прайсу, 
который указан для данного клиента. 
Если иначе, то необходимо заменить цену на соответствующую.

Как формируется цена:
service.price - базовая цена услуги, которая хранится в ней самой
price_list.nds
price_list.extra_charge
price_list.inflation_rate
10% - наценка для ИП
30% - наценка для Юрлиц
Сначала учитывается коэффициент инфляции, потом НДС, потом наценка и наконец extra_charge
*/

CREATE OR REPLACE FUNCTION trg_check_price_for_client()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE 
    c_type TEXT;
    current_price BIGINT;
BEGIN
    SELECT client_type INTO c_type
    FROM client 
    WHERE id_client = NEW.id_client;

    SELECT pls.price INTO current_price 
    FROM price_list_service pls 
    JOIN price_list pl ON pls.creation_date = pl.creation_date
    WHERE pls.id_service = NEW.id_service AND 
        pls.client_type = c_type AND
        pl.creation_date = (
            SELECT creation_price_list_date
            FROM assignment_agreement
            WHERE assignment_agreement_no = NEW.assignment_agreement_no AND 
                  cooperation_agreement_no = NEW.cooperation_agreement_no AND 
                  id_client = NEW.id_client
        );

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No price found for client type % and service %', c_type, NEW.id_service;
    END IF;
    
    IF NEW.price IS DISTINCT FROM current_price THEN 
        NEW.price := current_price;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER check_price_for_client
BEFORE INSERT ON contract_service
FOR EACH ROW
EXECUTE FUNCTION trg_check_price_for_client();