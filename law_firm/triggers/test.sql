DECLARE
    service_id TEXT;
    info RECORD;

SELECT assignment_agreement_no, cooperation_agreement_no, id_client INTO info
FROM assignment_agreement
LIMIT 1;

SELECT id_service INTO service_id
FROM service
LIMIT 1;

INSERT INTO contract_service (
    id_service,
    assignment_agreement_no,
    cooperation_agreement_no,
    id_client,
    price
)
VALUES (service_id, info.assignment_agreement_no, info.cooperation_agreement_no, info.id_client, 1000);

SELECT *
FROM contract_service cs
WHERE cs.assignment_agreement_no = info.assignment_agreement_no AND 
      cs.cooperation_agreement_no = info.cooperation_agreement_no;