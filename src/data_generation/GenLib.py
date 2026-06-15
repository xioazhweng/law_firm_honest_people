from __future__ import annotations
import random
from datetime import date, timedelta


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

def is_holiday(d):
    if d.weekday() >= 5:
        return True
    if d.month == 1 and 1 <= d.day <= 14:
        return True
    return False

def get_previous_workday(d):
    while(is_holiday(d)):
        d -= timedelta(days=1)
    return d

def get_payment_dates(today):
    advance_date = date(today.year, today.month, 25)
    if today.month == 12:
        salary_date = date(today.year + 1, 1, 10)
    else:
        salary_date = date(today.year, today.month + 1, 10)

    advance_date = get_previous_workday(advance_date)
    salary_date = get_previous_workday(salary_date)
    return advance_date, salary_date

def add_month(d):
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)