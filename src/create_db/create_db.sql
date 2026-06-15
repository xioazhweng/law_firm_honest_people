CREATE TABLE client (
    id_client   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_type TEXT NOT NULL CHECK (client_type IN ('LEGAL', 'PERSON', 'ENTREPRENEUR')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE client_person (
    id_client BIGINT PRIMARY KEY REFERENCES client(id_client) ON DELETE CASCADE,
    fio 	  TEXT NOT NULL, 
	passport_data TEXT NOT NULL UNIQUE, 
	inn TEXT NOT NULL UNIQUE
);

CREATE TABLE client_entrepreneur (
    id_client BIGINT PRIMARY KEY REFERENCES client(id_client) ON DELETE CASCADE,
    fio TEXT NOT NULL, 
	inn TEXT NOT NULL UNIQUE, 
	ogrnip TEXT UNIQUE
    
);

CREATE TABLE client_legal (
    id_client BIGINT PRIMARY KEY REFERENCES client(id_client) ON DELETE CASCADE,
    company_name TEXT NOT NULL, 
	inn TEXT NOT NULL UNIQUE, 
    ogrn TEXT UNIQUE, 
	representative TEXT NOT NULL
);

-- 2. Банки
CREATE TABLE bank (
    bik VARCHAR(9) PRIMARY KEY CHECK (bik ~ '^04\d{7}$'), --только для РОССИИИ!!!!
    bank_name TEXT NOT NULL, 
	bank_legal_address TEXT, 
	bank_cor_account TEXT UNIQUE NOT NULL
);

CREATE TABLE bank_account (
    account_no TEXT NOT NULL, bik VARCHAR(9) NOT NULL,
    PRIMARY KEY(account_no, bik),
    FOREIGN KEY (bik) REFERENCES bank(bik) ON DELETE CASCADE
);

-- 3. работнички  
/*
"Lawer": 
"Manager":
"Administrator":
"Accountant"

*/
CREATE TABLE job_position (
    id_job_position SERIAL PRIMARY KEY, 
	job_name TEXT NOT NULL UNIQUE
);

CREATE TABLE employee (
    employee_number BIGINT PRIMARY KEY,
    fio TEXT NOT NULL,
    id_job_position INT REFERENCES job_position(id_job_position) ON DELETE SET NULL,
    account_no TEXT NOT NULL, 
	bik VARCHAR(9) NOT NULL,
    birth_date DATE NOT NULL,
	hire_date DATE NOT NULL,
	fire_date DATE,
	salary BIGINT NOT NULL CHECK (salary >= 0),
    gender CHAR(1) NOT NULL CHECK (gender IN ('M', 'F')),
    FOREIGN KEY (account_no, bik) REFERENCES bank_account(account_no, bik) ON DELETE SET NULL,
    CHECK (fire_date IS NULL OR fire_date >= hire_date)
);

-- 4. Услуги и прайсы
CREATE TABLE price_list (
    creation_date DATE PRIMARY KEY,
    nds FLOAT NOT NULL,
    extra_charge INT NOT NULL,
    inflation_rate FLOAT NOT NULL
);

CREATE TABLE service (
    id_service SERIAL PRIMARY KEY, 
    name_service TEXT NOT NULL UNIQUE, 
    service_description TEXT, 
    price BIGINT NOT NULL CHECK (price >= 0),
	required_documents TEXT
);

CREATE TABLE price_list_service (
    id_service INT NOT NULL, 
	creation_date DATE NOT NULL,
    price BIGINT NOT NULL CHECK (price >= 0), 
    client_type TEXT NOT NULL CHECK (client_type IN ('LEGAL', 'PERSON', 'ENTREPRENEUR')),
    PRIMARY KEY (id_service, creation_date, client_type),
    FOREIGN KEY (id_service) REFERENCES service ON DELETE CASCADE,
    FOREIGN KEY (creation_date) REFERENCES price_list ON DELETE CASCADE
);

-- 5. Договоры 
CREATE TABLE cooperation_agreement (
    id_client BIGINT NOT NULL, 
	cooperation_agreement_no BIGINT NOT NULL,
    start_date DATE NOT NULL,
	end_date DATE, 
	manager_number BIGINT, 
	lawyer_number BIGINT,
    PRIMARY KEY (id_client, cooperation_agreement_no),
    FOREIGN KEY (id_client) REFERENCES client,
    FOREIGN KEY (manager_number) REFERENCES employee,
    FOREIGN KEY (lawyer_number) REFERENCES employee,
    CHECK (end_date IS NULL OR end_date >= start_date)
);

CREATE TABLE assignment_agreement (
    assignment_agreement_no BIGINT NOT NULL,
    cooperation_agreement_no BIGINT NOT NULL,  
    id_client BIGINT NOT NULL,
    creation_price_list_date DATE, 
    created_at DATE NOT NULL,
	completion_date DATE, 
	deadline DATE NOT NULL,
	result BOOLEAN,
    PRIMARY KEY (assignment_agreement_no, cooperation_agreement_no, id_client),
    FOREIGN KEY (cooperation_agreement_no, id_client) REFERENCES cooperation_agreement(cooperation_agreement_no, id_client),
    FOREIGN KEY (creation_price_list_date) REFERENCES price_list(creation_date),
    CHECK (completion_date IS NULL OR completion_date <= deadline),
    CHECK (completion_date IS NULL or created_at <= completion_date)
);

CREATE TABLE contract_service ( 
    id BIGSERIAL PRIMARY KEY,
    id_service INT NOT NULL,
    assignment_agreement_no BIGINT NOT NULL,
    cooperation_agreement_no BIGINT NOT NULL,  
    id_client BIGINT NOT NULL, 
    price BIGINT NOT NULL,                
   -- PRIMARY KEY (id_service, assignment_agreement_no, cooperation_agreement_no, id_client),
    FOREIGN KEY (id_service) REFERENCES service ON DELETE CASCADE,
    FOREIGN KEY (assignment_agreement_no, cooperation_agreement_no, id_client) 
        REFERENCES assignment_agreement ON DELETE CASCADE
);

-- Платежи


CREATE TABLE payment_bundle (
    id_payment_bundle BIGSERIAL PRIMARY KEY,
    parent_cooperation_agreement_no BIGINT NOT NULL,
    id_client BIGINT NOT NULL,
    total_amount BIGINT NOT NULL CHECK (total_amount >= 0),
    created_at DATE NOT NULL,

    FOREIGN KEY (parent_cooperation_agreement_no, id_client)
        REFERENCES cooperation_agreement(cooperation_agreement_no, id_client)
);

CREATE TABLE payment_document (
    id_payment_document BIGSERIAL PRIMARY KEY,
    id_payment_bundle BIGINT,

    assignment_agreement_no BIGINT NOT NULL,
    cooperation_agreement_no BIGINT NOT NULL,
    id_client BIGINT NOT NULL,

    amount BIGINT NOT NULL CHECK (amount >= 0),

    FOREIGN KEY (id_payment_bundle)
        REFERENCES payment_bundle ON DELETE CASCADE,

    FOREIGN KEY (assignment_agreement_no, cooperation_agreement_no, id_client)
        REFERENCES assignment_agreement
);

CREATE TABLE income_pay_document (
    id_income_pay_document BIGSERIAL PRIMARY KEY,
    account_no TEXT NOT NULL, 
    bik VARCHAR(9) NOT NULL,
    amount BIGINT NOT NULL CHECK (amount >= 0), 
	payment_date DATE NOT NULL,
    FOREIGN KEY (account_no, bik) REFERENCES bank_account(account_no, bik)
);


CREATE TABLE income_payment_bundle (
    id_income_pay_document BIGINT PRIMARY KEY,
    id_payment_bundle BIGINT NOT NULL UNIQUE,

    FOREIGN KEY (id_income_pay_document)
        REFERENCES income_pay_document ON DELETE CASCADE,

    FOREIGN KEY (id_payment_bundle)
        REFERENCES payment_bundle ON DELETE CASCADE
);

CREATE TABLE outgoing_pay_document (
    payment_date DATE NOT NULL, 
	account_no TEXT NOT NULL, 
	bik VARCHAR(9) NOT NULL,
    employee_number BIGINT, 
	amount BIGINT NOT NULL CHECK (amount >= 0),
    payment_type TEXT NOT NULL CHECK (payment_type IN ('ADVANCE', 'PAYMENT')),
    result BOOLEAN,
    PRIMARY KEY(payment_date, account_no, bik),
    FOREIGN KEY (account_no, bik) REFERENCES bank_account(account_no, bik) ON DELETE CASCADE,
    FOREIGN KEY (employee_number) REFERENCES employee(employee_number) ON DELETE SET NULL
);
