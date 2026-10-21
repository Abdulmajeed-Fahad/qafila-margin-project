"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"

APP_TABLES = ["orders", "order_items", "products", "stores"]

# The three tables we publish: iceberg name -> (dbt model schema.table, columns to sort by)
PUBLISHED = {
    "silver.product_cost_history": ("silver.product_cost_history", ["sku", "valid_from"]),
    "gold.weekly_margin": ("gold.weekly_margin", ["iso_week", "city", "category"]),
    "quality.price_quarantine": ("quality.price_quarantine", ["source_file", "sku", "valid_from", "raw_cost", "reason"]),
}


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Running this twice must not duplicate anything.
    """
    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")

        # 1) App database: read the SQLite file with Python's sqlite3 (no DuckDB extension to download),
        #    then replace the raw table. "Replace" (not "append") is what makes a re-run safe.
        with closing(sqlite3.connect(f"file:{(data_dir / 'qafila_oltp.sqlite').resolve()}?mode=ro", uri=True)) as src:
            for table in APP_TABLES:
                df = pd.read_sql_query(f"SELECT * FROM {table}", src, dtype_backend="pyarrow")  # noqa: F841
                con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM df")

        # 2) Supplier files: every column stays text (all_varchar), nothing is parsed yet.
        #    We add the file name and the row's position in its file. UNION BY NAME copes with the
        #    renamed column (unit_cost -> cost_sar): each file simply leaves the other one empty.
        files = sorted((data_dir / "suppliers").glob("prices_*.csv"))
        if not files:
            raise SystemExit(f"no supplier files found in {data_dir / 'suppliers'}")
        selects = []
        for f in files:
            path = f.resolve().as_posix().replace("'", "''")
            name = f.name.replace("'", "''")
            selects.append(f"SELECT '{name}' AS source_file, row_number() OVER () AS file_row, * "
                           f"FROM read_csv('{path}', all_varchar = true, header = true)")
        con.sql("CREATE OR REPLACE TABLE raw.supplier_prices AS " + " UNION ALL BY NAME ".join(selects))


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg (see the brief for the exact columns).

        silver.product_cost_history   gold.weekly_margin   quality.price_quarantine

    Update existing tables in place, never drop and recreate them: the table's snapshots are
    how we see what a number was before the supplier's correction arrived.
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db), read_only=True) as con:
        for iceberg_name, (model, sort_cols) in PUBLISHED.items():
            new = con.sql(f"SELECT * FROM {model}").arrow()
            new = new.read_all() if hasattr(new, "read_all") else new
            _write_iceberg(catalog, iceberg_name, new.sort_by([(c, "ascending") for c in sort_cols]))


def _write_iceberg(catalog: SqlCatalog, name: str, new: pa.Table) -> None:
    """Create the table the first time; afterwards change it in place, only when the data changed.

    - Same data as last time: do nothing, so a re-run adds no snapshot and changes nothing.
    - New data: ONE overwrite snapshot (old files out, new files in). The table is never dropped, so
      every older snapshot is still there and can be read with time travel.
    """
    namespace = name.split(".")[0]
    catalog.create_namespace_if_not_exists(namespace)

    if not catalog.table_exists(name):
        table = catalog.create_table(name, schema=new.schema)
        table.append(new.cast(table.schema().as_arrow()))
        print(f"  {name}: created, {new.num_rows} rows")
        return

    table = catalog.load_table(name)
    target = table.schema().as_arrow()
    new = new.cast(target)
    sort_by = [(c, "ascending") for c in target.names]
    current = table.scan().to_arrow().cast(target)
    if current.sort_by(sort_by).equals(new.sort_by(sort_by)):
        print(f"  {name}: unchanged, {new.num_rows} rows (no new snapshot)")
        return

    old_files = [task.file for task in table.scan().plan_files()]
    with table.transaction() as tx:
        with tx.update_snapshot().overwrite() as overwrite:
            for data_file in old_files:
                overwrite.delete_data_file(data_file)
            # write the new rows as new Parquet files, then add them in the same snapshot
            from pyiceberg.io.pyarrow import _dataframe_to_data_files
            for data_file in _dataframe_to_data_files(table_metadata=tx.table_metadata, write_uuid=overwrite.commit_uuid,
                                                      df=new, io=table.io):
                overwrite.append_data_file(data_file)
    print(f"  {name}: updated in place, {new.num_rows} rows (was {current.num_rows})")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
