#!/usr/bin/env python3
"""Import general corporations from a Shift-JIS CSV into PostgreSQL.

This script reads the corporate number CSV specified by the Cabinet Order /
Ministerial Ordinance (命令規則) for the Corporate Number system and registers
records for general corporations in PostgreSQL. All columns defined in the
specification are created (with comments) if they are missing, and every
processed row is written with an upsert.

Usage example::

    python import_companies.py path/to/data.csv         --host localhost --port 5432 --user masaki --password 39masaki

"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import logging
from pathlib import Path
from typing import Callable, Collection, Iterable, Iterator, Optional, Sequence

import psycopg2
from psycopg2 import DatabaseError, OperationalError, sql
from psycopg2.extras import execute_batch


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)

DEFAULT_GENERAL_TYPE_CODES = ("301", "302", "303", "304", "305")
PRIMARY_KEY_COLUMN = "corporate_number"
DATE_PATTERNS = ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d")


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    text = (value or "").strip()
    if not text:
        return None
    for pattern in DATE_PATTERNS:
        try:
            return dt.datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"unrecognized date format '{text}'")


def normalize_corporate_number(raw_value: Optional[str]) -> str:
    text = (raw_value or "").strip().replace("-", "")
    if not text:
        raise ValueError("corporate number is empty")
    if "e" in text.lower():
        try:
            decimal_value = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(f"invalid scientific notation corporate number: {text}") from exc
        normalized = format(decimal_value, "f").replace(".", "")
    else:
        normalized = "".join(ch for ch in text if ch.isdigit())
    normalized = normalized.zfill(13)
    if len(normalized) != 13:
        raise ValueError(f"corporate number should be 13 digits, got '{normalized}' from '{text}'")
    return normalized


def normalize_optional_corporate_number(raw_value: Optional[str]) -> Optional[str]:
    text = (raw_value or "").strip()
    if not text:
        return None
    return normalize_corporate_number(text)


def default_transform(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = value.strip()
    return text or None


def to_int(value: Optional[str]) -> Optional[int]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"expected integer, got '{value}'") from exc


def to_bool_flag(value: Optional[str]) -> Optional[bool]:
    text = (value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "t", "yes"}:
        return True
    if text in {"0", "false", "f", "no"}:
        return False
    raise ValueError(f"expected boolean flag, got '{value}'")


def normalize_postal_code(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or None


@dataclass(frozen=True)
class ColumnDefinition:
    csv_field: str
    db_column: str
    pg_type: str
    description: str
    aliases: tuple[str, ...] = ()
    transform: Callable[[Optional[str]], object] = default_transform


MEIREI_COLUMNS: Sequence[ColumnDefinition] = (
    ColumnDefinition(
        csv_field="sequenceNumber",
        db_column="sequence_number",
        pg_type="BIGINT",
        description="命令規則別表 項7: 順序番号",
        aliases=("順序番号",),
        transform=to_int,
    ),
    ColumnDefinition(
        csv_field="corporateNumber",
        db_column="corporate_number",
        pg_type="CHAR(13)",
        description="命令規則別表 項8: 法人番号",
        aliases=("法人番号",),
        transform=normalize_corporate_number,
    ),
    ColumnDefinition(
        csv_field="process",
        db_column="process",
        pg_type="TEXT",
        description="命令規則別表 項9: 処理区分",
        aliases=("処理区分",),
    ),
    ColumnDefinition(
        csv_field="correct",
        db_column="correct",
        pg_type="TEXT",
        description="命令規則別表 項10: 訂正区分",
        aliases=("訂正区分",),
    ),
    ColumnDefinition(
        csv_field="updateDate",
        db_column="update_date",
        pg_type="DATE",
        description="命令規則別表 項11: 更新年月日",
        aliases=("更新年月日",),
        transform=parse_date,
    ),
    ColumnDefinition(
        csv_field="changeDate",
        db_column="change_date",
        pg_type="DATE",
        description="命令規則別表 項12: 変更年月日",
        aliases=("変更年月日",),
        transform=parse_date,
    ),
    ColumnDefinition(
        csv_field="name",
        db_column="name",
        pg_type="TEXT",
        description="命令規則別表 項13: 商号又は名称",
        aliases=("商号又は名称",),
    ),
    ColumnDefinition(
        csv_field="nameImageId",
        db_column="name_image_id",
        pg_type="TEXT",
        description="命令規則別表 項14: 商号又は名称イメージID",
        aliases=("商号又は名称イメージID",),
    ),
    ColumnDefinition(
        csv_field="kind",
        db_column="kind",
        pg_type="TEXT",
        description="命令規則別表 項15: 法人種別",
        aliases=("法人種別", "法人種別コード"),
    ),
    ColumnDefinition(
        csv_field="prefectureName",
        db_column="prefecture_name",
        pg_type="TEXT",
        description="命令規則別表 項16: 国内所在地（都道府県）",
        aliases=("都道府県名", "国内所在地（都道府県）"),
    ),
    ColumnDefinition(
        csv_field="cityName",
        db_column="city_name",
        pg_type="TEXT",
        description="命令規則別表 項17: 国内所在地（市区町村）",
        aliases=("市区町村名", "国内所在地（市区町村）"),
    ),
    ColumnDefinition(
        csv_field="streetNumber",
        db_column="street_number",
        pg_type="TEXT",
        description="命令規則別表 項18: 国内所在地（丁目番地等）",
        aliases=("丁目番地等", "国内所在地（丁目番地等）"),
    ),
    ColumnDefinition(
        csv_field="addressImageId",
        db_column="address_image_id",
        pg_type="TEXT",
        description="命令規則別表 項19: 所在地イメージID",
        aliases=("所在地イメージID",),
    ),
    ColumnDefinition(
        csv_field="prefectureCode",
        db_column="prefecture_code",
        pg_type="TEXT",
        description="命令規則別表 項20: 都道府県コード",
        aliases=("都道府県コード",),
    ),
    ColumnDefinition(
        csv_field="cityCode",
        db_column="city_code",
        pg_type="TEXT",
        description="命令規則別表 項21: 市区町村コード",
        aliases=("市区町村コード",),
    ),
    ColumnDefinition(
        csv_field="postCode",
        db_column="post_code",
        pg_type="TEXT",
        description="命令規則別表 項22: 郵便番号",
        aliases=("郵便番号",),
        transform=normalize_postal_code,
    ),
    ColumnDefinition(
        csv_field="addressOutside",
        db_column="address_outside",
        pg_type="TEXT",
        description="命令規則別表 項23: 国外所在地",
        aliases=("国外所在地",),
    ),
    ColumnDefinition(
        csv_field="addressOutsideImageId",
        db_column="address_outside_image_id",
        pg_type="TEXT",
        description="命令規則別表 項24: 国外所在地イメージID",
        aliases=("国外所在地イメージID",),
    ),
    ColumnDefinition(
        csv_field="closeDate",
        db_column="close_date",
        pg_type="DATE",
        description="命令規則別表 項25: 登記記録の閉鎖等年月日",
        aliases=("登記記録の閉鎖等年月日",),
        transform=parse_date,
    ),
    ColumnDefinition(
        csv_field="closeCause",
        db_column="close_cause",
        pg_type="TEXT",
        description="命令規則別表 項26: 登記記録の閉鎖等の事由",
        aliases=("登記記録の閉鎖等の事由",),
    ),
    ColumnDefinition(
        csv_field="successorCorporateNumber",
        db_column="successor_corporate_number",
        pg_type="CHAR(13)",
        description="命令規則別表 項27: 承継先法人番号",
        aliases=("承継先法人番号",),
        transform=normalize_optional_corporate_number,
    ),
    ColumnDefinition(
        csv_field="changeCause",
        db_column="change_cause",
        pg_type="TEXT",
        description="命令規則別表 項28: 変更事由",
        aliases=("変更事由",),
    ),
    ColumnDefinition(
        csv_field="assignmentDate",
        db_column="assignment_date",
        pg_type="DATE",
        description="命令規則別表 項29: 法人番号指定年月日",
        aliases=("法人番号指定年月日",),
        transform=parse_date,
    ),
    ColumnDefinition(
        csv_field="latest",
        db_column="latest",
        pg_type="BOOLEAN",
        description="命令規則別表 項30: 最新履歴等",
        aliases=("最新履歴等",),
        transform=to_bool_flag,
    ),
    ColumnDefinition(
        csv_field="enName",
        db_column="en_name",
        pg_type="TEXT",
        description="命令規則別表 項31: 商号又は名称（英語表記）",
        aliases=("商号又は名称（英語表記）",),
    ),
    ColumnDefinition(
        csv_field="enPrefectureName",
        db_column="en_prefecture_name",
        pg_type="TEXT",
        description="命令規則別表 項32: 都道府県名（英語表記）",
        aliases=("都道府県名（英語表記）",),
    ),
    ColumnDefinition(
        csv_field="enCityName",
        db_column="en_city_name",
        pg_type="TEXT",
        description="命令規則別表 項33: 市区町村名（英語表記）",
        aliases=("市区町村名（英語表記）",),
    ),
    ColumnDefinition(
        csv_field="enAddressOutside",
        db_column="en_address_outside",
        pg_type="TEXT",
        description="命令規則別表 項34: 国外所在地（英語表記）",
        aliases=("国外所在地（英語表記）",),
    ),
    ColumnDefinition(
        csv_field="furigana",
        db_column="furigana",
        pg_type="TEXT",
        description="命令規則別表 項35: フリガナ",
        aliases=("フリガナ",),
    ),
    ColumnDefinition(
        csv_field="hihyoji",
        db_column="hihyoji",
        pg_type="TEXT",
        description="命令規則別表 項36: 非表示理由",
        aliases=("非表示理由", "備考"),
    ),
)

COLUMN_NAME_LOOKUP: dict[str, str] = {}
for column in MEIREI_COLUMNS:
    for alias in (column.csv_field, *column.aliases):
        COLUMN_NAME_LOOKUP[alias] = column.csv_field
        COLUMN_NAME_LOOKUP[alias.lower()] = column.csv_field

ROW_FIELDNAMES = [column.csv_field for column in MEIREI_COLUMNS]


def resolve_column_key(column_name: str) -> str:
    key = column_name.strip()
    if not key:
        raise KeyError("column name is empty")
    mapped = COLUMN_NAME_LOOKUP.get(key)
    if mapped:
        return mapped
    mapped = COLUMN_NAME_LOOKUP.get(key.lower())
    if mapped:
        return mapped
    raise KeyError(f"unknown column '{column_name}' for命令規則データ")


def row_looks_like_header(first_row: Sequence[str]) -> bool:
    if not first_row:
        return False
    first_cell = first_row[0].lstrip("﻿").strip().strip('"')
    if not first_cell:
        return False
    return not first_cell.isdigit()


def normalize_row(raw_row: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
    normalized = {field: None for field in ROW_FIELDNAMES}
    for raw_key, raw_value in raw_row.items():
        if raw_key is None:
            continue
        cleaned_key = raw_key.lstrip("﻿")
        mapped = COLUMN_NAME_LOOKUP.get(cleaned_key) or COLUMN_NAME_LOOKUP.get(cleaned_key.lower())
        if mapped:
            normalized[mapped] = raw_value
    return normalized


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
        default="kind",
        help="Column to read the corporation type code (e.g. 'kind' or '法人種別').",
    )
    parser.add_argument(
        "--general-type-codes",
        nargs="*",
        default=list(DEFAULT_GENERAL_TYPE_CODES),
        help="Type codes that indicate a corporation should be treated as a general corporation.",
    )
    parser.add_argument(
        "--corporate-number-column",
        default="corporateNumber",
        help="Column containing the corporate number (e.g. 'corporateNumber' or '法人番号').",
    )
    parser.add_argument(
        "--name-column",
        default="name",
        help="Column containing the company name (e.g. 'name' or '商号又は名称').",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows to insert per batch when writing to the database.",
    )
    return parser.parse_args()


def row_is_general_corporation(
    row: dict[str, Optional[str]],
    type_key: str,
    valid_codes: Collection[str],
) -> bool:
    corp_type = (row.get(type_key) or "").strip()
    if not corp_type:
        return False
    digits = "".join(ch for ch in corp_type if ch.isdigit())
    corp_code = digits[:3] if len(digits) >= 3 else corp_type
    return corp_code in valid_codes


def read_rows(csv_path: Path) -> Iterator[dict[str, Optional[str]]]:
    with csv_path.open("r", encoding="shift_jis", newline="") as handle:
        reader = csv.reader(handle)
        try:
            first_row = next(reader)
        except StopIteration:
            return
        handle.seek(0)
        if row_looks_like_header(first_row):
            dict_reader = csv.DictReader(handle)
        else:
            dict_reader = csv.DictReader(handle, fieldnames=ROW_FIELDNAMES)
        for raw_row in dict_reader:
            yield normalize_row(raw_row)


def prepare_records(
    rows: Iterable[dict[str, Optional[str]]],
    column_definitions: Sequence[ColumnDefinition],
    *,
    type_column: str,
    type_codes: Iterable[str],
    corporate_number_column: str,
    name_column: str,
) -> Iterator[tuple[object, ...]]:
    allowed_codes = {code.strip() for code in type_codes if code.strip()}

    try:
        type_key = resolve_column_key(type_column)
    except KeyError as exc:
        raise KeyError(
            f"general-type-column '{type_column}' is not defined in 命令規則項目"
        ) from exc

    try:
        corporate_key = resolve_column_key(corporate_number_column)
    except KeyError as exc:
        raise KeyError(
            f"corporate-number-column '{corporate_number_column}' is not defined in 命令規則項目"
        ) from exc

    try:
        name_key = resolve_column_key(name_column)
    except KeyError as exc:
        raise KeyError(
            f"name-column '{name_column}' is not defined in 命令規則項目"
        ) from exc

    for index, row in enumerate(rows, start=1):
        if allowed_codes and not row_is_general_corporation(row, type_key, allowed_codes):
            continue

        corporate_id_for_log = (row.get(corporate_key) or "").strip()
        name_for_log = (row.get(name_key) or "").strip()

        record: list[object] = []
        skip = False
        for column in column_definitions:
            raw_value = row.get(column.csv_field)
            try:
                value = column.transform(raw_value)
            except ValueError as exc:
                LOGGER.warning(
                    "Skipping row %d (corporate %s, name %s) due to invalid %s: %s",
                    index,
                    corporate_id_for_log or "unknown",
                    name_for_log or "unknown",
                    column.csv_field,
                    exc,
                )
                skip = True
                break
            record.append(value)
        if skip:
            continue
        yield tuple(record)



def build_insert_sql(conn, table: str, columns: Sequence[str]) -> str:
    column_identifiers = [sql.Identifier(col) for col in columns]
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
    updates = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
        for col in columns
        if col != PRIMARY_KEY_COLUMN
    )
    statement = sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({values}) "
        "ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    ).format(
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(column_identifiers),
        values=placeholders,
        pk=sql.Identifier(PRIMARY_KEY_COLUMN),
        updates=updates,
    )
    return statement.as_string(conn)


def insert_records(
    conn,
    table: str,
    column_definitions: Sequence[ColumnDefinition],
    records: Iterable[tuple[object, ...]],
    batch_size: int,
) -> int:
    columns = [column.db_column for column in column_definitions]
    insert_sql = build_insert_sql(conn, table, columns)
    batch: list[tuple[object, ...]] = []
    total_inserted = 0
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
                    total_inserted += len(batch)
                    LOGGER.info("Inserted %d records (total %d)", len(batch), total_inserted)
                    batch.clear()
            if batch:
                try:
                    execute_batch(cur, insert_sql, batch)
                except DatabaseError as exc:
                    conn.rollback()
                    LOGGER.error("Database error during batch insert: %s", exc)
                    raise
                total_inserted += len(batch)
                LOGGER.info("Inserted %d records (total %d)", len(batch), total_inserted)
    return total_inserted


def ensure_table(conn, table: str, column_definitions: Sequence[ColumnDefinition]) -> None:
    column_sql = [
        sql.SQL("{name} {type}").format(
            name=sql.Identifier(column.db_column),
            type=sql.SQL(column.pg_type),
        )
        for column in column_definitions
    ]
    create_sql = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {table} ({columns}, PRIMARY KEY ({pk}))"
    ).format(
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(column_sql),
        pk=sql.Identifier(PRIMARY_KEY_COLUMN),
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = %s",
                (table,),
            )
            existing_columns = {row[0] for row in cur.fetchall()}
            for column in column_definitions:
                if column.db_column not in existing_columns:
                    cur.execute(
                        sql.SQL("ALTER TABLE {table} ADD COLUMN {column} {type}").format(
                            table=sql.Identifier(table),
                            column=sql.Identifier(column.db_column),
                            type=sql.SQL(column.pg_type),
                        )
                    )
            cur.execute(
                "SELECT 1 FROM information_schema.table_constraints "
                "WHERE table_schema = current_schema() AND table_name = %s "
                "AND constraint_type = 'PRIMARY KEY'",
                (table,),
            )
            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("ALTER TABLE {table} ADD PRIMARY KEY ({pk})").format(
                        table=sql.Identifier(table),
                        pk=sql.Identifier(PRIMARY_KEY_COLUMN),
                    )
                )
            for column in column_definitions:
                if column.description:
                    cur.execute(
                        sql.SQL("COMMENT ON COLUMN {table}.{column} IS %s").format(
                            table=sql.Identifier(table),
                            column=sql.Identifier(column.db_column),
                        ),
                        (column.description,),
                    )
    LOGGER.info("Ensured table '%s' contains命令規則の %d 項目.", table, len(column_definitions))


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
        ensure_table(conn, args.table, MEIREI_COLUMNS)
        rows = read_rows(args.csv_path)
        records = prepare_records(
            rows,
            MEIREI_COLUMNS,
            type_column=args.general_type_column,
            type_codes=args.general_type_codes,
            corporate_number_column=args.corporate_number_column,
            name_column=args.name_column,
        )
        total = insert_records(conn, args.table, MEIREI_COLUMNS, records, args.batch_size)
        LOGGER.info("Upserted %d general corporation records.", total)
    finally:
        conn.close()
        LOGGER.info("Database connection closed")


if __name__ == "__main__":
    main()
