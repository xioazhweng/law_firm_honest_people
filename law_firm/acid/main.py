import psycopg2
import logging 
import threading
import os
import time
import random
from datetime import date

db_config = {
        "host": os.getenv("PGHOST", "127.0.0.1"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "law_firm_db"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "1111"),
    }

class BDConnection:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None 
    
    def connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.conn.autocommit = False
        #logging.info("Подключение к БД выполнено")
        return self.conn.cursor()
    
    def commit(self):
        self.conn.commit()
    def close(self):
        if self.conn is not None:
            self.conn.close()
            #logging.info("Подключение к БД закрыто")

def NonRepeatableReadA():
    A = BDConnection(db_config)
    cur = A.connect()
    logging.info("Начало транзакции NonRepeatableReadA")
    cur.execute("BEGIN;")
    cur.execute("SELECT name_service, price FROM service WHERE id_service = 1;")
    result = cur.fetchone()
    logging.info(f"A)первое чтение: {result}")
    time.sleep(5)
    cur.execute("SELECT name_service, price FROM service WHERE id_service = 1;")
    result = cur.fetchone()
    logging.info(f"B)второе чтение: {result}")
    cur.execute("COMMIT;")
    logging.info("Конец транзакции NonRepeatableReadA")
    A.close()

def NonRepeatableReadB():   
    B = BDConnection(db_config)
    cur = B.connect()
    time.sleep(1)
    logging.info("Начало транзакции NonRepeatableReadB")
    cur.execute("BEGIN;")
    price = random.randrange(1000, 100000)
    cur.execute("UPDATE service SET price = %s WHERE id_service = 1;", (price,))
    logging.info(f"Была назначена цена: {price}")
    cur.execute("COMMIT;")
    logging.info("Конец транзакции NonRepeatableReadB")
    B.close()

def PhantomReadA():
    A = BDConnection(db_config)
    cur = A.connect()
    logging.info("Начало транзации PhantomReadA")
    cur.execute("BEGIN;")
    threadhold = 30000
    cur.execute("SELECT COUNT(*) FROM service WHERE price < %s;", (threadhold,))
    result = cur.fetchone()
    logging.info(f"Всего: {result} услуг с ценой меньше {threadhold}")
    time.sleep(5)
    cur.execute("SELECT COUNT(*) FROM service WHERE price < %s;", (threadhold,))
    result = cur.fetchone()
    logging.info(f"Всего: {result} услуг с ценой меньше {threadhold}")
    cur.execute("COMMIT;")
    logging.info("конец транзации PhantomReadA")
    A.close()

def PhantomReadB():
    B = BDConnection(db_config)
    cur = B.connect()
    logging.info("Начало транзации PhantomReadB")
    cur.execute("BEGIN;")
    price = random.randrange(1000, 20000)
    cur.execute("UPDATE service SET price = %s WHERE id_service = 1;", (price,))
    logging.info(f"Была назначена цена: {price}")
    cur.execute("COMMIT;")
    logging.info("конец транзации PhantomReadB")
    B.close()

# Устанавливаем бизнес-правило:
# У клиента одновременно не может быть более 2 незавершенных assignment agreements

def insert_client():
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO client (client_type)
        VALUES ('PERSON')
        RETURNING id_client;
    """)

    id_client = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO client_person(id_client, fio, passport_data, inn)
        VALUES (%s, %s, %s, %s);
    """, (
        id_client,
        "Test Client",
        f"PASS{random.randint(10000,99999)}",
        f"INN{random.randint(10000,99999)}"
    ))

    cooperation_no = random.randint(1000, 9999)

    cur.execute("""
        INSERT INTO cooperation_agreement(
            id_client,
            cooperation_agreement_no,
            start_date
        )
        VALUES (%s, %s, CURRENT_DATE);
    """, (id_client, cooperation_no))

    cur.execute("""
        INSERT INTO assignment_agreement(
            assignment_agreement_no,
            cooperation_agreement_no,
            id_client,
            created_at,
            deadline
        )
        VALUES (%s, %s, %s, CURRENT_DATE, CURRENT_DATE + 10);
    """, (
        random.randint(10000, 99999),
        cooperation_no,
        id_client
    ))

    conn.close()

    return id_client, cooperation_no

def WriteSkew(name, id_client, cooperation_no):
    A = BDConnection(db_config)
    cur = A.connect()
    logging.info(f"Начало транзации WriteSkew{name}")
    cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; ")
    cur.execute(
    """
        SELECT COUNT(*)
        FROM assignment_agreement
        WHERE id_client = %s
        AND cooperation_agreement_no = %s
        AND completion_date IS NULL;
    """, (id_client, cooperation_no))
    count = cur.fetchone()[0]
    logging.info(f"{name}: договоров {count} штук")
    if count < 2:
        time.sleep(3)
        cur.execute(
            """
            INSERT INTO assignment_agreement(
                assignment_agreement_no,
                cooperation_agreement_no,
                id_client,
                created_at,
                deadline
            )
            VALUES (%s, %s, %s, CURRENT_DATE, CURRENT_DATE + 10);
            """, (
            random.randint(100000, 999999),
            cooperation_no,
            id_client
        ))
        logging.info(f"{name}: создан еще один договор")
    cur.execute("COMMIT;")
    logging.info(f"Конец транзации WriteSkew{name}")
    A.close()

def checkWriteSkew(id_client, cooperation_no):
    B = BDConnection(db_config)
    cur = B.connect()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM assignment_agreement
        WHERE id_client = %s
        AND cooperation_agreement_no = %s
        AND completion_date IS NULL;
        """, (id_client, cooperation_no))
    count = cur.fetchone()[0]
    logging.info(f"ИТОГОВОЕ КОЛИЧЕСТВО ACTIVE ASSIGNMENTS = {count}")
    B.close()

def run_transactions(targetA, targetB, title="", argsA = None, argsB = None):
    logging.info(f"\n === {title} ===\n")
    if argsA:
        tA = threading.Thread(target=targetA, args=argsA)
    else:
        tA = threading.Thread(target=targetA)
    if argsB:
        tB = threading.Thread(target=targetB, args=argsB)
    else:
        tB = threading.Thread(target=targetB)
    tA.start()
    tB.start()
    tA.join()
    tB.join()

def demoSavepoint():
    logging.info("\nТестируем SAVEPOINT\n")
    B = BDConnection(db_config)
    cur = B.connect()
    cur.execute("BEGIN;")
    cur.execute("""
        INSERT INTO service(name_service, price)
        VALUES ('TEST_1', 100);
    """)
    cur.execute("SAVEPOINT sp1;")
    cur.execute("""
        INSERT INTO service(name_service, price)
        VALUES ('TEST_2', 200);
    """)
    cur.execute("ROLLBACK TO sp1;")
    cur.execute("COMMIT;")

    cur.execute("SELECT * FROM service WHERE name_service LIKE 'TEST%'")
    logging.info(cur.fetchall())
    cur.execute("DELETE FROM service WHERE name_service = 'TEST_1'")
    B.commit()
    B.close()
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run_transactions(NonRepeatableReadA, NonRepeatableReadB, "Non-repeatable read")
    run_transactions(PhantomReadA, PhantomReadB, "Phantom read")
    id_client, cooperation_no = insert_client()
    run_transactions(WriteSkew, WriteSkew, "Phantom read", ("A", id_client, cooperation_no), ("B", id_client, cooperation_no))
    checkWriteSkew(id_client, cooperation_no)
    demoSavepoint()