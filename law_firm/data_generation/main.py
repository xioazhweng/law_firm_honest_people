from __future__ import annotations
import argparse
import logging
from pathlib import Path
import os
from GenLib import *
from DBFiller import *


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
    filler = DBFiller(db_config, counts, schema_path)
    filler.run(apply_schema=args.apply_schema, truncate=not args.no_truncate)


if __name__ == "__main__":
    main()
