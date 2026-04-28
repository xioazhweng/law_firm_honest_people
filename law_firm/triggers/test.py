from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import psycopg2
from collections import defaultdict
import argparse
import logging
from pathlib import Path
from data_generation.DBFiller import *


BASE_DIR = Path(__file__).resolve().parent.parent
TRIGGERS_PATH = BASE_DIR / "triggers" 

class DBFillerTestTriggers(DBFiller):
    def apply_schema(self, cursor):
        sql = self.schema_path.read_text(encoding="utf-8")
        cursor.execute(sql)
        sql = (TRIGGERS_PATH / "1.sql").read_text(encoding="utf-8")
        cursor.execute(sql)
        sql = (TRIGGERS_PATH / "2.sql").read_text(encoding="utf-8")
        cursor.execute(sql)
        logging.info("Схема из %s применена", self.schema_path)
    
    def insert_contract_service(self, cursor, service_prices):
        cursor.execute("""
            SELECT 
                aa.assignment_agreement_no, 
                aa.cooperation_agreement_no, 
                aa.id_client,
                c.client_type,
                aa.creation_price_list_date
            FROM assignment_agreement aa
            JOIN client c ON c.id_client = aa.id_client
        """)
        assignments = cursor.fetchall()
        service_ids = list(service_prices.keys())

        for assignment_number, cooperation_number, client_id, client_type, creation_price_list_date in assignments:
            selected_services = random.sample(service_ids, k=random.randint(1, min(3, len(service_ids))))
            for service_id in selected_services:
                cursor.execute("""
                    SELECT price
                    FROM price_list_service pls
                    WHERE pls.client_type = %s AND
                        pls.creation_date = %s AND
                        pls.id_service = %s
                """, (client_type, creation_price_list_date, service_id))
                
                result = cursor.fetchone()
                if result:
                    price_service = random.choice([result[0], 10000])
                else:
                    price_service = None  
                
                try:
                    cursor.execute("""
                        INSERT INTO contract_service (
                            id_service,
                            assignment_agreement_no,
                            cooperation_agreement_no,
                            id_client,
                            price
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING price;
                    """, (
                        service_id, 
                        assignment_number, 
                        cooperation_number, 
                        client_id, 
                        price_service
                    ))
                    self.print_notices()
                    inserted_price = cursor.fetchone()[0]
                   
                except psycopg2.errors.RaiseException as e:
                    logging.info(f"Trigger error for service {service_id}: {e}")

    
    def insert_payments(self, cursor):
        cursor.execute("""
            SELECT DISTINCT
                aa.id_client,
                aa.cooperation_agreement_no,
                aa.created_at
            FROM assignment_agreement aa
        """)
        agreements = cursor.fetchall()

        
        grouped = defaultdict(list)
        for client_id, coop_no, created_at in agreements:
            grouped[(client_id, created_at)].append(coop_no)

        for (client_id, created_at), coop_list in grouped.items():
            # Иногда вставляем неправильную сумму, чтобы триггер сработал
            wrong_total = random.choice([True, False])
            total_bundle_amount = 0 if wrong_total else 0  # Начальная сумма, триггер распределит правильно

            cursor.execute("""
                INSERT INTO payment_bundle (
                    parent_cooperation_agreement_no,
                    id_client,
                    total_amount,
                    created_at
                )
                VALUES (%s, %s, %s, %s)
                RETURNING id_payment_bundle;
            """, (coop_list[0], client_id, total_bundle_amount, created_at))
            self.print_notices()
            bundle_id = cursor.fetchone()[0]
            doc_id = []
            for coop_no in coop_list:
                cursor.execute("""
                    SELECT assignment_agreement_no
                    FROM assignment_agreement
                    WHERE cooperation_agreement_no = %s
                    AND id_client = %s
                    AND created_at = %s
                """, (coop_no, client_id, created_at))
                assignments = cursor.fetchall()

                for (assignment_no,) in assignments:
                    cursor.execute("""
                        SELECT COALESCE(SUM(price), 0)
                        FROM contract_service
                        WHERE assignment_agreement_no = %s
                        AND cooperation_agreement_no = %s
                        AND id_client = %s
                    """, (assignment_no, coop_no, client_id))
                    amount = cursor.fetchone()[0]

                    total_bundle_amount += amount
                    
        
                    cursor.execute("""
                        INSERT INTO payment_document (
                            assignment_agreement_no,
                            cooperation_agreement_no,
                            id_client,
                            amount
                        )
                        VALUES (%s, %s, %s, %s)
                        RETURNING id_payment_document;
                    """, (assignment_no, coop_no, client_id, amount))
                    doc_id = cursor.fetchall()
                    
            if wrong_total:
                total_bundle_amount = random.randint(0, 1504050)
            cursor.execute("""
                INSERT INTO payment_bundle (
                    parent_cooperation_agreement_no,
                    id_client,
                    total_amount,
                    created_at
                )
                VALUES (%s, %s, %s, %s)
                RETURNING id_payment_bundle;
            """, (coop_list[0], client_id, total_bundle_amount, created_at))
            self.print_notices()
            bundle_id = cursor.fetchone()[0]

            for doc in doc_id:
                cursor.execute("""
                    UPDATE payment_document
                    SET id_payment_bundle = %s
                    WHERE id_payment_document = %s
                """, (bundle_id, doc))

            cursor.execute("SELECT bik FROM bank ORDER BY RANDOM() LIMIT 1")
            bik = cursor.fetchone()[0]
            account_no = "".join(random.choices("0123456789", k=20))
            cursor.execute("""
                INSERT INTO bank_account (account_no, bik)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (account_no, bik))

            payment_date = created_at + timedelta(days=random.randint(10, 15))
            cursor.execute("""
                INSERT INTO income_pay_document (
                    account_no,
                    bik,
                    amount,
                    payment_date
                )
                VALUES (%s, %s, %s, %s)
                RETURNING id_income_pay_document;
            """, (account_no, bik, total_bundle_amount, payment_date))
            income_id = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO income_payment_bundle (
                    id_income_pay_document,
                    id_payment_bundle
                )
                VALUES (%s, %s);
            """, (income_id, bundle_id))

          
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
    base_dir = Path(__file__).resolve().parent.parent
    schema_path = base_dir / "db" / "create_db.sql"
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
    filler = DBFillerTestTriggers(db_config, counts, schema_path)
    filler.run(apply_schema=args.apply_schema, truncate=not args.no_truncate)
   


if __name__ == "__main__":
    main()
