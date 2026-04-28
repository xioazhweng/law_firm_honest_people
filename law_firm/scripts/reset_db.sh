#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DB_HOST="${PGHOST:-127.0.0.1}"
DB_PORT="${PGPORT:-5432}"
DB_USER="${PGUSER:-postgres}"
DB_PASSWORD="${PGPASSWORD:-1111}"
DB_NAME="${PGDATABASE:-law_firm_db}"
MAINTENANCE_DB="${PGMAINTENANCE_DB:-postgres}"
PYTHON_BIN="${PYTHON_BIN:-python}"

export PGPASSWORD="$DB_PASSWORD"
export PGHOST="$DB_HOST"
export PGPORT="$DB_PORT"
export PGUSER="$DB_USER"
export PGDATABASE="$DB_NAME"

echo "[1/4] Завершаю активные подключения к базе '$DB_NAME'"
psql -d "$MAINTENANCE_DB" -v ON_ERROR_STOP=1 -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();" >/dev/null

echo "[2/4] Пересоздаю базу '$DB_NAME'"
dropdb --if-exists "$DB_NAME"
createdb "$DB_NAME"

echo "[3/4] Создаю таблицы и генерирую тестовые данные"
"$PYTHON_BIN" "$ROOT_DIR/data_generation/main.py" --apply-schema

echo "[4/4] База '$DB_NAME' успешно пересоздана и заполнена"
