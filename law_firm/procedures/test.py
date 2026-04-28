from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_generation.DBFiller import *
from collections import defaultdict
import argparse
import logging
from pathlib import Path
from tabulate import tabulate

BASE_DIR = Path(__file__).resolve().parent.parent
PROCEDURES_PATH = BASE_DIR / "procedures" 

class DBFillerTestProcedure(DBFiller):
    def apply_schema(self, cursor):
        sql = self.schema_path.read_text(encoding="utf-8")
        cursor.execute(sql)
        sql = (PROCEDURES_PATH / "1.sql").read_text(encoding="utf-8")
        cursor.execute(sql)
        sql = (PROCEDURES_PATH / "2.sql").read_text(encoding="utf-8")
        cursor.execute(sql)
        logging.info("Схема из %s применена", self.schema_path)
    def insert_outgoing_payments(self, cursor):
        cursor.execute(
            """
            SELECT employee_number, account_no, bik, salary, hire_date, fire_date
            FROM employee
            WHERE account_no IS NOT NULL AND bik IS NOT NULL;
            """
        )
        rows = cursor.fetchall()
        payment_rows = []
        base_end_date = date.today()  - timedelta(days=120)
        for employee_number, account_no, bik, salary, hire_date, fire_date in rows:
            current = date(hire_date.year, hire_date.month, 1)
            while current <= base_end_date and (fire_date is None or current <= fire_date):
                advance_date, salary_date = GenLib.get_payment_dates(current)
                advance = min(15000, salary)
                rest = max(0, salary - 15000)
                
                if base_end_date - current < timedelta(60):
                    answ1, answ2 = random.choice([True,  False, False]), random.choice([True,  False, False])
                else:
                    answ1 = answ2 = True
            
                payment_rows.append(
                    (advance_date, account_no, bik, employee_number, advance, 'ADVANCE', answ1)
                )

                if rest > 0:
                    payment_rows.append(
                        (salary_date, account_no, bik, employee_number, rest, 'PAYMENT', answ2)
                    )                    

                current = GenLib.add_month(current)
        execute_values(
            cursor,
            """
            INSERT INTO outgoing_pay_document 
            (payment_date, account_no, bik, employee_number, amount, payment_type, result)
            VALUES %s;
            """,
            payment_rows,
        )

    def test_pay_employees(self, cursor):
        d = date.today() - timedelta(days=200)
        while d < date.today():
            try:
                cursor.execute("CALL pay_employees('ADVANCE', %s);", (d,))
            except psycopg2.errors.RaiseException as e:
                print("RAISE EXCEPTION при ADVANCE для %s: %s", d, e)
                self.conn.rollback()  
            try:
                cursor.execute("CALL pay_employees('PAYMENT', %s);", (d,))
            except psycopg2.errors.RaiseException as e:
                print("RAISE EXCEPTION при PAYMENT для %s: %s", d, e)
                self.conn.rollback()
            d += timedelta(days=20)
        print("NOTICED ======")
        for notice in self.conn.notices:
            print(notice.rstrip('\n'))
        self.conn.notices.clear()
        
    def test_fork_price_list(self, cursor, rate):
        cursor.execute (
            """ 
                SELECT creation_date
                FROM price_list
            """
        )
        dates = cursor.fetchall()
        for d in dates:
            cursor.execute (
                """
                CALL fork_price_list(%s, %s, %s)
                """, (d[0], rate, d[0]+timedelta(days=1))
            )
        self.print_notices()
        cursor.execute (
            """ 
                SELECT creation_date, id_service, price, client_type
                FROM price_list_service
                ORDER BY client_type, id_service, creation_date;
            """
        )
        info = cursor.fetchall()
        headers = ["creation_date", "id_service", "price", "client_type"]
        print(f"{rate} % DISCOUNT")
        print(tabulate(info, headers=headers, tablefmt="grid"))


    def test_procedures(self):
        self.connect()
        try:
            with self.conn.cursor() as cursor:
                self.test_pay_employees(cursor)
                self.test_fork_price_list(cursor, 20)
            self.conn.commit()
        except Exception:
            if self.conn is not None:
                self.conn.rollback()
            logging.exception("Ошибка во время теста процедур")
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
    filler = DBFillerTestProcedure(db_config, counts, schema_path)
    filler.run(apply_schema=args.apply_schema, truncate=not args.no_truncate)
    filler.test_procedures()


if __name__ == "__main__":
    main()
