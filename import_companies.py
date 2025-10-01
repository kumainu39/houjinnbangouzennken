#!/usr/bin/env python3
"""Import general corporations from a Shift-JIS CSV into PostgreSQL.

This script reads the legal entity CSV published by the Japanese government and
registers general corporations in the ``companyinfo`` database.

Usage example::

    python import_companies.py path/to/data.csv \
        --host localhost --port 5432 --user masaki --password 39masaki

"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
from decimal import Decimal, InvalidOperation
import logging
from pathlib import Path
from typing import Collection, Iterable, Optional

import psycopg2
from psycopg2 import DatabaseError, OperationalError
from psycopg2.extras import execute_batch


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)

DEFAULT_GENERAL_TYPE_CODES = ("301", "302", "303", "304", "305")
DEFAULT_ADDRESS_COLUMNS = (
    "国内所在地（都道府県）",
    "国内所在地（市区町村）",
    "国内所在地（丁目番地等）",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to the Shift-JIS encoded CSV file exported from the corporate number system.",
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5432, type=int)
    parser.add_argument("--user", default="masaki")
    parser.add_argument("--password", default="39masaki")
    parser.add_argument("--database", default="companyinfo")
    parser.add_argument("--table", default="companies")
    parser.add_argument(
        "--general-type-column",
        default="法人種別",
        help="Column name that contains the corporate type code used to filter general corporations.",
    )
    parser.add_argument(
        "--general-type-codes",
        nargs="*",
        default=list(DEFAULT_GENERAL_TYPE_CODES),
        help="Type codes that indicate a corporation should be treated as a general corporation.",
    )
    parser.add_argument(
        "--corporate-number-column",
        default="法人番号",
        help="Column name containing the corporate number.",
    )
    parser.add_argument(
        "--name-column",
        default="商号又は名称",
        help="Column name containing the company name.",
    )
    parser.add_argument(
        "--address-columns",
        nargs="*",
        default=list(DEFAULT_ADDRESS_COLUMNS),
        help="Columns to concatenate in order to build the full address.",
    )
    parser.add_argument(
        "--established-date-column",
        default="設立年月日",
        help="Column name containing the established date.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows to insert per batch when writing to the database.",
    )
    return parser.parse_args()


def ensure_table(conn, table: str) -> None:
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            corporate_number CHAR(13) PRIMARY KEY,
            company_name TEXT NOT NULL,
            address TEXT,
            established_on DATE
        )
    """
    with conn, conn.cursor() as cur:
        cur.execute(create_table_sql)
    LOGGER.info("Ensured table '%s' exists.", table)


def normalize_corporate_number(raw_value: str) -> str:
    value = raw_value.strip().replace("-", "")
    if not value:
        raise ValueError("Corporate number is empty")

    if "e" in value.lower():
        try:
            decimal_value = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid scientific notation corporate number: {value}") from exc
        normalized = format(decimal_value, "f").replace(".", "")
    else:
        normalized = "".join(ch for ch in value if ch.isdigit())

    normalized = normalized.zfill(13)
    if len(normalized) != 13:
        raise ValueError(f"Corporate number should be 13 digits, got '{normalized}' from '{raw_value}'")
    return normalized


DATE_PATTERNS = ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d")


def parse_date(value: str) -> Optional[dt.date]:
    value = value.strip()
    if not value:
        return None
    for pattern in DATE_PATTERNS:
        try:
            return dt.datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {value}")


def build_address(row: dict, address_columns: Iterable[str]) -> str:
    parts = []
    for column in address_columns:
        part = row.get(column, "").strip()
        if part:
            parts.append(part)
    return "".join(parts)


def row_is_general_corporation(row: dict, type_column: str, valid_codes: Collection[str]) -> bool:
    corp_type = row.get(type_column, "").strip()
    if not corp_type:
        return False

    digits = "".join(ch for ch in corp_type if ch.isdigit())
    if len(digits) >= 3:
        corp_code = digits[:3]
    else:
        corp_code = corp_type

    return corp_code in valid_codes


def read_rows(csv_path: Path) -> Iterable[dict]:
    with csv_path.open("r", encoding="shift_jis", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def prepare_records(
    rows: Iterable[dict],
    *,
    corporate_number_column: str,
    name_column: str,
    address_columns: Iterable[str],
    established_date_column: str,
    type_column: str,
    type_codes: Iterable[str],
) -> Iterable[tuple[str, str, str, Optional[dt.date]]]:
    allowed_codes = set(type_codes)
    for index, row in enumerate(rows, start=1):
        if not row_is_general_corporation(row, type_column, allowed_codes):
            continue
        try:
            raw_corporate_number = row[corporate_number_column]
        except KeyError as exc:
            raise KeyError(
                f"Missing corporate number column '{corporate_number_column}' in CSV row"
            ) from exc

        try:
            corporate_number = normalize_corporate_number(raw_corporate_number)
        except ValueError as exc:
            LOGGER.warning("Skipping row %d due to invalid corporate number: %s", index, exc)
            continue

        company_name = row.get(name_column, "").strip()
        address = build_address(row, address_columns)
        established_on_raw = row.get(established_date_column, "")
        if established_on_raw:
            try:
                established_on = parse_date(established_on_raw)
            except ValueError as exc:
                LOGGER.warning("Skipping row %d due to invalid established date: %s", index, exc)
                continue
        else:
            established_on = None

        yield (corporate_number, company_name, address, established_on)


def insert_records(conn, table: str, records: Iterable[tuple], batch_size: int) -> None:
    insert_sql = f"""
        INSERT INTO {table} (corporate_number, company_name, address, established_on)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (corporate_number)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            address = EXCLUDED.address,
            established_on = EXCLUDED.established_on
    """
    batch = []
    with conn:
        with conn.cursor() as cur:
            for record in records:
                batch.append(record)
                if len(batch) >= batch_size:
                    try:
                        execute_batch(cur, insert_sql, batch)
                    except DatabaseError as exc:
                        conn.rollback()
                        LOGGER.error("Database error during batch insert: %s", exc)
                        raise
                    LOGGER.info("Inserted %d records", len(batch))
                    batch.clear()
            if batch:
                try:
                    execute_batch(cur, insert_sql, batch)
                except DatabaseError as exc:
                    conn.rollback()
                    LOGGER.error("Database error during batch insert: %s", exc)
                    raise
                LOGGER.info("Inserted %d records", len(batch))


def main() -> None:
    args = parse_args()
    if not args.csv_path.exists():
        raise FileNotFoundError(f"CSV file '{args.csv_path}' does not exist")

    LOGGER.info(
        "Connecting to database %s@%s:%s/%s", args.user, args.host, args.port, args.database
    )
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            dbname=args.database,
        )
    except OperationalError as exc:
        LOGGER.error("Failed to connect to database: %s", exc)
        raise SystemExit(1) from exc

    try:
        ensure_table(conn, args.table)
        rows = read_rows(args.csv_path)
        records = prepare_records(
            rows,
            corporate_number_column=args.corporate_number_column,
            name_column=args.name_column,
            address_columns=args.address_columns,
            established_date_column=args.established_date_column,
            type_column=args.general_type_column,
            type_codes=args.general_type_codes,
        )
        insert_records(conn, args.table, records, args.batch_size)
    finally:
        conn.close()
        LOGGER.info("Database connection closed")


if __name__ == "__main__":
    main()
