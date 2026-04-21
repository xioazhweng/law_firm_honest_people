/*
Триггер2:
При добавлении новой платежки, сумма платежки раскидывается по документам 
исходя из даты их создания. В приоритете – старые документы. 
Суммы, разложенные по документам равны сумме платежки.
*/

CREATE OR REPLACE FUNCTION trg_check_amount()
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

CREATE TRIGGER check_amount
BEFORE INSERT ON contract_service
FOR EACH ROW
EXECUTE FUNCTION trg_check_amount();
