from __future__ import annotations

import argparse
import logging
import os
import random
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values


BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = BASE_DIR / "db" / "create_db.sql"

fake = Faker("ru_RU")

DEFAULT_COUNTS = {
    "employees": 100,
    "client_person": 20,
    "client_entrepreneur": 12,
    "client_legal": 12,
    "banks": 5,
}

JOB_POSITIONS = ["Юрист", "Менеджер", "Администратор", "Бухгалтер"]

SALARY_RANGES = {
    "Юрист": (90_000, 180_000),
    "Менеджер": (70_000, 130_000),
    "Администратор": (50_000, 90_000),
    "Бухгалтер": (65_000, 110_000),
}

SERVICES = [
    (
        "Консультация",
        5_000,
        "Первичная юридическая консультация по вопросу клиента.",
        "Паспорт, краткое описание ситуации",
    ),
    (
        "Подготовка договора",
        15_000,
        "Составление или правовая экспертиза договора.",
        "Реквизиты сторон, проект договора, приложения",
    ),
    (
        "Судебное представительство",
        50_000,
        "Подготовка позиции и участие в судебном процессе.",
        "Иск, доказательства, доверенность",
    ),
    (
        "Регистрация ООО",
        25_000,
        "Сопровождение регистрации юридического лица.",
        "Паспорт учредителя, адрес, уставные документы",
    ),
    (
        "Сопровождение сделки",
        35_000,
        "Юридическая поддержка коммерческой сделки.",
        "Проект сделки, реквизиты сторон, переписка",
    ),
    (
        "Претензионная работа",
        18_000,
        "Подготовка претензий и ответов на претензии.",
        "Договор, акты, переписка, подтверждающие документы",
    ),
]

#порядок очиски таблиц
TRUNCATE_ORDER = [
    "payment_transaction",
    "income_pay_document",
    "outgoing_pay_document",
    "contract_service",
    "assignment_agreement",
    "cooperation_agreement",
    "price_list_service",
    "service",
    "price_list",
    "employee",
    "job_position",
    "client_person",
    "client_entrepreneur",
    "client_legal",
    "client",
    "bank_account",
    "bank",
]

def random_digits(length: int, prefix: str = "") -> str:
    if length < len(prefix):
        raise ValueError("Длина меньше длины префикса")
    tail = "".join(random.choices("0123456789", k=length - len(prefix)))
    return prefix + tail

def control_digit(number: str, divisor: int) -> str:
    return str(int(number) % divisor % 10)

def generate_ogrn() -> str:
    base = random_digits(12)
    return base + control_digit(base, 11)

def generate_ogrnip() -> str:
    base = random_digits(14)
    return base + control_digit(base, 13)

def generate_inn_person() -> str:
    return random_digits(12)

def generate_inn_company() -> str:
    return random_digits(10)

def generate_passport() -> str:
    return f"{random_digits(4)} {random_digits(6)}"

def generate_bik() -> str:
    return random_digits(9, prefix="04")

def generate_account() -> str:
    return random_digits(20)

def chunked_dates(count: int) -> list[date]:
    today = date.today()
    dates: list[date] = []
    for month_offset in range(count):
        month = today.month - month_offset
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        dates.append(date(year, month, 1))
    return sorted(set(dates))


class DBFiller:
    def __init__(self, db_config, counts):
        self.db_config = db_config
        self.counts = counts
        self.conn = None
        self.used_values: dict[str, set[str]] = {
            "passport": set(),
            "person_inn": set(),
            "company_inn": set(),
            "ogrn": set(),
            "ogrnip": set(),
            "bik": set(),
            "account": set(),
            "cor_account": set(),
        }

    def connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.conn.autocommit = False
        logging.info("Подключение к БД выполнено")

    def close(self):
        if self.conn is not None:
            self.conn.close()
            logging.info("Подключение к БД закрыто")

    def unique_value(self, key, factory):
        value = factory()
        while value in self.used_values[key]:
            value = factory()
        self.used_values[key].add(value)
        return value

    def apply_schema(self, cursor):
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        cursor.execute(sql)
        logging.info("Схема из %s применена", SCHEMA_PATH)

    def truncate_tables(self, cursor):
        cursor.execute(f"TRUNCATE TABLE {', '.join(TRUNCATE_ORDER)} RESTART IDENTITY CASCADE;")
        logging.info("Таблицы очищены")

    def insert_job_positions(self, cursor) -> dict[str, int]:
        result: dict[str, int] = {}
        for job_name in JOB_POSITIONS:
            cursor.execute(
                """
                INSERT INTO job_position (job_name)
                VALUES (%s)
                RETURNING id_job_position;
                """,
                (job_name,),
            )
            result[job_name] = cursor.fetchone()[0]
        return result

    def insert_banks_and_accounts(self, cursor, bank_count, account_count):
        banks: list[str] = []
        for _ in range(bank_count):
            bik = self.unique_value("bik", generate_bik)
            cor_account = self.unique_value("cor_account", generate_account)
            cursor.execute(
                """
                INSERT INTO bank (bik, bank_name, bank_legal_address, bank_cor_account)
                VALUES (%s, %s, %s, %s);
                """,
                (
                    bik,
                    f"{fake.company()} Банк",
                    fake.address(),
                    cor_account,
                ),
            )
            banks.append(bik)

        accounts: list[tuple[str, str]] = []
        for index in range(account_count):
            bik = banks[index % len(banks)]
            account_no = self.unique_value("account", generate_account)
            cursor.execute(
                """
                INSERT INTO bank_account (account_no, bik)
                VALUES (%s, %s);
                """,
                (account_no, bik),
            )
            accounts.append((account_no, bik))

        return accounts

    def insert_employees(self, cursor, position_ids, accounts, count):
        employees_by_position: dict[str, list[int]] = {name: [] for name in JOB_POSITIONS}
        weighted_positions = ["Юрист"] * 4 + ["Менеджер"] * 3 + ["Администратор"] * 2 + ["Бухгалтер"] * 2

        for index in range(count):
            job_name = weighted_positions[index] if index < len(weighted_positions) else random.choice(weighted_positions)
            employee_number = 100_000 + index + 1
            birth_date = fake.date_of_birth(minimum_age=22, maximum_age=65)
            min_hire_date = birth_date + timedelta(days=18 * 365)
            hire_date = fake.date_between(start_date=max(min_hire_date, date.today() - timedelta(days=365 * 15)), end_date=date.today())
            fire_date = None
            if random.random() < 0.12:
                fire_date = fake.date_between(start_date=hire_date, end_date=date.today())

            salary_min, salary_max = SALARY_RANGES[job_name]
            account_no, bik = accounts[index]

            cursor.execute(
                """
                INSERT INTO employee (
                    employee_number,
                    id_job_position,
                    account_no,
                    bik,
                    birth_date,
                    hire_date,
                    fire_date,
                    salary,
                    gender
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    employee_number,
                    position_ids[job_name],
                    account_no,
                    bik,
                    birth_date,
                    hire_date,
                    fire_date,
                    random.randint(salary_min, salary_max),
                    random.choice(["M", "F"]),
                ),
            )
            employees_by_position[job_name].append(employee_number)

        return employees_by_position

    def insert_price_lists(self, cursor, count = 6):
        creation_dates = chunked_dates(count)
        execute_values(
            cursor,
            "INSERT INTO price_list (creation_date) VALUES %s;",
            [(creation_date,) for creation_date in creation_dates],
        )
        return creation_dates

    def insert_services(self, cursor):
        prices_by_service_id: dict[int, int] = {}
        for name, price, description, documents in SERVICES:
            cursor.execute(
                """
                INSERT INTO service (name_service, price, service_description, required_documents)
                VALUES (%s, %s, %s, %s)
                RETURNING id_service;
                """,
                (name, price, description, documents),
            )
            service_id = cursor.fetchone()[0]
            prices_by_service_id[service_id] = price
        return prices_by_service_id

    def insert_price_list_service(self, cursor, service_ids, creation_dates, prices_by_service_id):
        rows = []
        for service_id in service_ids:
            base_price = prices_by_service_id[service_id]  
            for creation_date in creation_dates:
                for client_type in ["PERSON", "ENTREPRENEUR", "LEGAL"]:
                    if client_type == "PERSON":
                        price = base_price
                    elif client_type == "ENTREPRENEUR":
                        price = int(base_price * 1.1)  # ИП +10%
                    else:  # LEGAL
                        price = int(base_price * 1.3)  # Юрлицо +30%
                    rows.append((service_id, creation_date, client_type, price))

        execute_values(
            cursor,
            """
            INSERT INTO price_list_service 
            (id_service, creation_date, client_type, price) 
            VALUES %s;
            """,
            rows,
        )

    def create_client(self, cursor, client_type):
        cursor.execute(
            """
            INSERT INTO client (client_type)
            VALUES (%s)
            RETURNING id_client;
            """,
            (client_type,),
        )
        return cursor.fetchone()[0]

    def insert_clients(self, cursor):
        client_ids: list[int] = []

        for _ in range(self.counts["client_person"]):
            client_id = self.create_client(cursor, "PERSON")
            client_ids.append(client_id)
            cursor.execute(
                """
                INSERT INTO client_person (id_client, fio, passport_data, inn)
                VALUES (%s, %s, %s, %s);
                """,
                (
                    client_id,
                    fake.name(),
                    self.unique_value("passport", generate_passport),
                    self.unique_value("person_inn", generate_inn_person),
                ),
            )

        for _ in range(self.counts["client_entrepreneur"]):
            client_id = self.create_client(cursor, "ENTREPRENEUR")
            client_ids.append(client_id)
            cursor.execute(
                """
                INSERT INTO client_entrepreneur (id_client, fio, inn, ogrnip)
                VALUES (%s, %s, %s, %s);
                """,
                (
                    client_id,
                    fake.name(),
                    self.unique_value("person_inn", generate_inn_person),
                    self.unique_value("ogrnip", generate_ogrnip),
                ),
            )

        for _ in range(self.counts["client_legal"]):
            client_id = self.create_client(cursor, "LEGAL")
            client_ids.append(client_id)
            cursor.execute(
                """
                INSERT INTO client_legal (id_client, company_name, inn, ogrn, representative)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (
                    client_id,
                    fake.company(),
                    self.unique_value("company_inn", generate_inn_company),
                    self.unique_value("ogrn", generate_ogrn),
                    f"{fake.name()}, {fake.phone_number()}",
                ),
            )

        return client_ids

    def insert_agreements(self, cursor, client_ids, managers, lawyers, price_list_dates,
                          service_prices, firm_accounts, ):
        cooperation_number = 1
        assignment_number = 1
        payment_number = 1
        transaction_number = 1
        service_ids = list(service_prices.keys())

        for index, client_id in enumerate(client_ids, start=1):
            start_date = fake.date_between(start_date=date.today() - timedelta(days=365 * 3), end_date=date.today())
            manager_number = random.choice(managers)
            lawyer_number = random.choice(lawyers)

            cursor.execute(
                """
                INSERT INTO cooperation_agreement (
                    id_client,
                    cooperation_agreement_no,
                    start_date,
                    end_date,
                    manager_number,
                    lawyer_number
                )
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (client_id, cooperation_number, start_date, None, manager_number, lawyer_number),
            )

            assignment_count = random.randint(1, 3)
            for _ in range(assignment_count):
                creation_price_list_date = random.choice(price_list_dates)
                deadline = fake.date_between(start_date=start_date, end_date=date.today() + timedelta(days=120))
                completed = random.random() < 0.7
                completion_date = fake.date_between(start_date=start_date, end_date=deadline) if completed else None

                cursor.execute(
                    """
                    INSERT INTO assignment_agreement (
                        assignment_agreement_no,
                        cooperation_agreement_no,
                        id_client,
                        creation_price_list_date,
                        completion_date,
                        deadline,
                        result
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        assignment_number,
                        cooperation_number,
                        client_id,
                        creation_price_list_date,
                        completion_date,
                        deadline,
                        completed,
                    ),
                )

                selected_services = random.sample(service_ids, k=random.randint(1, min(3, len(service_ids))))
                assignment_total = 0
                for service_id in selected_services:
                    assignment_total += service_prices[service_id]
                    cursor.execute(
                        """
                        INSERT INTO contract_service (
                            id_service,
                            assignment_agreement_no,
                            cooperation_agreement_no,
                            id_client
                        )
                        VALUES (%s, %s, %s, %s);
                        """,
                        (service_id, assignment_number, cooperation_number, client_id),
                    )

                account_no, bik = firm_accounts[index % len(firm_accounts)]
                payment_date = completion_date or fake.date_between(start_date=start_date, end_date=date.today())

                cursor.execute(
                    """
                    INSERT INTO income_pay_document (payment_no, account_no, bik, amount, payment_date)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (payment_number, account_no, bik, assignment_total, payment_date),
                )
                cursor.execute(
                    """
                    INSERT INTO payment_transaction (
                        id_payment_transaction,
                        payment_no,
                        account_no,
                        bik,
                        assignment_agreement_no,
                        cooperation_agreement_no,
                        id_client
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        transaction_number,
                        payment_number,
                        account_no,
                        bik,
                        assignment_number,
                        cooperation_number,
                        client_id,
                    ),
                )

                assignment_number += 1
                payment_number += 1
                transaction_number += 1

            cooperation_number += 1

    def insert_outgoing_payments(self, cursor):
        cursor.execute(
            """
            SELECT employee_number, account_no, bik, salary
            FROM employee
            WHERE account_no IS NOT NULL AND bik IS NOT NULL;
            """
        )
        rows = cursor.fetchall()
        payment_rows = []
        for offset, (employee_number, account_no, bik, salary) in enumerate(rows, start=1):
            payment_date = date.today().replace(day=1) + timedelta(days=offset)
            payment_rows.append((payment_date, account_no, bik, employee_number, salary))

        execute_values(
            cursor,
            """
            INSERT INTO outgoing_pay_document (payment_date, account_no, bik, employee_number, amount)
            VALUES %s;
            """,
            payment_rows,
        )

    def run(self, apply_schema = False, truncate = True):
        self.connect()
        try:
            with self.conn.cursor() as cursor:
                if apply_schema:
                    self.apply_schema(cursor)
                if truncate:
                    self.truncate_tables(cursor)

                position_ids = self.insert_job_positions(cursor)
                accounts = self.insert_banks_and_accounts(
                    cursor,
                    bank_count=self.counts["banks"],
                    account_count=self.counts["employees"] + 20,
                )
                employees_by_position = self.insert_employees(
                    cursor,
                    position_ids=position_ids,
                    accounts=accounts,
                    count=self.counts["employees"],
                )
                price_list_dates = self.insert_price_lists(cursor)
                service_prices = self.insert_services(cursor)
                self.insert_price_list_service(cursor, list(service_prices.keys()), price_list_dates, service_prices)
                client_ids = self.insert_clients(cursor)
                self.insert_agreements(
                    cursor,
                    client_ids=client_ids,
                    managers=employees_by_position["Менеджер"],
                    lawyers=employees_by_position["Юрист"],
                    price_list_dates=price_list_dates,
                    service_prices=service_prices,
                    firm_accounts=accounts[-20:],
                )
                self.insert_outgoing_payments(cursor)

            self.conn.commit()
            logging.info("Генерация тестовых данных завершена")
        except Exception:
            if self.conn is not None:
                self.conn.rollback()
            logging.exception("Ошибка во время генерации данных")
            raise
        finally:
            self.close()


def build_parser():
    parser = argparse.ArgumentParser(description="Заполнение БД юридической фирмы тестовыми данными")
    parser.add_argument("--apply-schema", action="store_true", help="Сначала выполнить SQL из db/create_db.sql")
    parser.add_argument("--no-truncate", action="store_true", help="Не очищать таблицы перед заполнением")
    parser.add_argument("--employees", type=int, default=DEFAULT_COUNTS["employees"], help="Количество сотрудников")
    parser.add_argument("--persons", type=int, default=DEFAULT_COUNTS["client_person"], help="Количество клиентов-физлиц")
    parser.add_argument("--entrepreneurs", type=int, default=DEFAULT_COUNTS["client_entrepreneur"], help="Количество ИП")
    parser.add_argument("--legals", type=int, default=DEFAULT_COUNTS["client_legal"], help="Количество юрлиц")
    parser.add_argument("--banks", type=int, default=DEFAULT_COUNTS["banks"], help="Количество банков")
    return parser


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = build_parser().parse_args()

    db_config = {
        "host": os.getenv("PGHOST", "127.0.0.1"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "law_firm_db"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "1111"),
    }
    counts = {
        "employees": 10,
        "client_person": args.persons,
        "client_entrepreneur": args.entrepreneurs,
        "client_legal": args.legals,
        "banks": args.banks,
    }

    filler = DBFiller(db_config=db_config, counts=counts)
    filler.run(apply_schema=args.apply_schema, truncate=not args.no_truncate)


if __name__ == "__main__":
    main()
