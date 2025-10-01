# Corporate Number Import Script

This repository contains a Python utility that imports Japan Corporate Number
(法人番号) CSV data into PostgreSQL. The script follows the column layout
specified by the Cabinet Order / Ministerial Ordinance (命令規則) for the
corporate number publication data. When the target table is missing the
required columns, they are created automatically (including column comments that
note the corresponding 命令規則 item).

## Prerequisites

- Python 3.10+
- PostgreSQL database `companyinfo` with user `masaki` and password `39masaki`
- `psycopg2-binary` (install with `pip install -r requirements.txt`)

## Usage

```bash
pip install -r requirements.txt
python import_companies.py /path/to/00_zenkoku_all_YYYYMMDD.csv     --host localhost --port 5432 --user masaki --password 39masaki
```

### Key options

| Option | Description | Default |
| --- | --- | --- |
| `--table` | Destination table | `companies` |
| `--general-type-column` | Column containing the corporation type code (`kind`, `法人種別` etc.) | `kind` |
| `--general-type-codes` | Codes treated as 'general corporations' | `301 302 303 304 305` |
| `--corporate-number-column` | Column containing the corporate number | `corporateNumber` |
| `--name-column` | Column containing the legal name | `name` |
| `--batch-size` | Batch size for inserts/updates | `500` |

The CSV published by the National Tax Agency may not include a header row. The
script detects this automatically and applies the 命令規則 column order. If you
download a ZIP archive, unzip it first and point the script at the CSV file.

## Behaviour

- Reads the CSV in Shift-JIS encoding and normalises every column defined in the
  命令規則別表 (sequence number, corporate number, type code, address fields,
  closure information, English names, furigana, 非表示理由, etc.).
- General-corporation filtering is controlled by `--general-type-codes`. Rows
  with different type codes are skipped.
- Inserts are performed with `ON CONFLICT (corporate_number) DO UPDATE`, making
  the import idempotent.
- All 命令規則 columns are created in the destination table with appropriate
  PostgreSQL data types and explanatory comments.
- Date columns accept `YYYY-MM-DD`, `YYYY/MM/DD`, `YYYY.MM.DD`, or `YYYYMMDD`.
- Columns that are empty in the source become `NULL`. Postal codes and corporate
  numbers are normalised to digits only (13 digits for 法人番号, 7 digits for 郵便
  番号 where present).

## Notes

- The script leaves additional project-specific columns (such as `company_name`
  or `address`) untouched if they already exist in the table; only the 命令規則
  columns are guaranteed.
- Depending on hardware, importing the nationwide dataset (約 5.6M rows) can take
  several minutes. Increase `--batch-size` if you have sufficient memory to
  improve throughput.
- The script logs each batch insert and reports skipped rows with the corporate
  number when invalid data is encountered.
