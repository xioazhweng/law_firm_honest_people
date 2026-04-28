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
    RAISE NOTICE 'Starting distribution of payment bundle % for client %: total_amount=%', NEW.id_payment_bundle, NEW.id_client, NEW.total_amount;
    FOR doc IN
        SELECT *
        FROM payment_document
        WHERE cooperation_agreement_no = NEW.parent_cooperation_agreement_no
          AND id_client = NEW.id_client
           AND amount > 0
        ORDER BY id_payment_document ASC
    LOOP
        IF remaining_amount <= 0 THEN
            RAISE NOTICE 'No remaining amount, exiting loop.';
            EXIT;
        END IF;
        RAISE NOTICE 'Processing payment_document id=%: current amount=%, remaining_amount=%', doc.id_payment_document, doc.amount, remaining_amount;
        IF remaining_amount >= doc.amount THEN
            remaining_amount := remaining_amount - doc.amount;
            UPDATE payment_document
            SET amount = 0
            WHERE id_payment_document = doc.id_payment_document;
            RAISE NOTICE 'Fully paid document id=%: set amount to 0, remaining_amount now=%', doc.id_payment_document, remaining_amount;
        ELSE
            UPDATE payment_document
            SET amount = amount - remaining_amount
            WHERE id_payment_document = doc.id_payment_document;
            RAISE NOTICE 'Partially paid document id=%: subtracted %, remaining amount set to 0', doc.id_payment_document, remaining_amount;
            remaining_amount := 0;
            EXIT;
        END IF;
    END LOOP;
    RAISE NOTICE 'Distribution completed, remaining_amount=%', remaining_amount;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trigger_distribute_payment_bundle
AFTER INSERT ON payment_bundle
FOR EACH ROW
EXECUTE FUNCTION distribute_payment_bundle();