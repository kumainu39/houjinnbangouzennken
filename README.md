# 会社情報取り込みスクリプト

本リポジトリには、国税庁が公開している法人番号データ（Shift-JIS 形式の CSV）から、一般法人のみを抽出して PostgreSQL の `companyinfo` データベースへ登録するスクリプトを含みます。

## 前提条件

- Python 3.10 以上
- PostgreSQL（`companyinfo` データベース、ユーザー `masaki`、パスワード `39masaki`）
- `psycopg2-binary` ライブラリ（`pip install -r requirements.txt` でインストール）

## 使い方

1. 依存関係をインストールします。
   ```bash
   pip install -r requirements.txt
   ```
2. スクリプトを実行します。
   ```bash
   python import_companies.py /path/to/法人番号データ.csv \
       --host localhost --port 5432 --user masaki --password 39masaki
   ```

   必要に応じて以下のオプションを指定できます。

   | オプション | 説明 | 既定値 |
   | --- | --- | --- |
   | `--table` | 登録先テーブル名 | `companies` |
   | `--general-type-column` | 一般法人かどうか判定する法人種別コードの列名 | `法人種別` |
   | `--general-type-codes` | 一般法人を示す法人種別コード（複数可） | `301 302 303 304 305` |
   | `--corporate-number-column` | 法人番号の列名 | `法人番号` |
   | `--name-column` | 会社名の列名 | `商号又は名称` |
   | `--address-columns` | 住所を構成する列名（複数指定可） | `国内所在地（都道府県）` `国内所在地（市区町村）` `国内所在地（丁目番地等）` |
   | `--established-date-column` | 設立日の列名 | `設立年月日` |
   | `--batch-size` | バッチ挿入サイズ | `500` |

## 動作概要

- CSV は Shift-JIS で読み込み、列値はすべて文字列として扱います。
- 法人番号に科学表記（例: `1234567890123E+0`）が含まれている場合でも `Decimal` を用いて 13 桁の文字列へ変換し、下桁が欠落しないようにします。
- 法人種別コードが `301` ～ `305` の一般法人のみを登録します。
- `ON CONFLICT` によって既存の法人番号は更新されます。
- 法人番号や設立日が不正な行はスキップされ、警告ログに記録されます。データベース接続に失敗した場合はエラーメッセージを表示し終了します。

## テーブル定義

スクリプトは必要に応じて以下のテーブルを自動で作成します。

```sql
CREATE TABLE IF NOT EXISTS companies (
    corporate_number CHAR(13) PRIMARY KEY,
    company_name TEXT NOT NULL,
    address TEXT,
    established_on DATE
);
```

## 注意事項

- 設立日は `YYYY-MM-DD` / `YYYY/MM/DD` / `YYYY.MM.DD` / `YYYYMMDD` のいずれかの形式に対応しています。
- 住所の列構成が異なる場合は `--address-columns` オプションで列名を指定してください。
- テーブル定義やフィルタ条件を変更したい場合は必要に応じてスクリプトを調整してください。
