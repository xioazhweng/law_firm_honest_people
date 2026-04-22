/*
Триггер2:
При добавлении новой платежки, сумма платежки раскидывается по документам 
исходя из даты их создания. В приоритете – старые документы. 
Суммы, разложенные по документам равны сумме платежки.
*/

CREATE OR REPLACE FUNCTION distribute_payment_bundle()
RETURNS TRIGGER 
 LANGUAGE plpgsql
AS $$
DECLARE
    remaining_amount BIGINT;
    doc RECORD;
BEGIN
    remaining_amount := NEW.total_amount;
    FOR doc IN
        SELECT *
        FROM payment_document
        WHERE cooperation_agreement_no = NEW.parent_cooperation_agreement_no
          AND id_client = NEW.id_client
          AND amount > 0
        ORDER BY id_payment_document ASC
    LOOP
        IF remaining_amount <= 0 THEN
            EXIT;
        END IF;
        IF remaining_amount >= doc.amount THEN
            remaining_amount := remaining_amount - doc.amount;
            UPDATE payment_document
            SET amount = 0
            WHERE id_payment_document = doc.id_payment_document;
        ELSE
            UPDATE payment_document
            SET amount = amount - remaining_amount
            WHERE id_payment_document = doc.id_payment_document;
            remaining_amount := 0;
            EXIT;
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$;
CREATE TRIGGER trigger_distribute_payment_bundle
AFTER INSERT ON payment_bundle
FOR EACH ROW
EXECUTE FUNCTION distribute_payment_bundle();