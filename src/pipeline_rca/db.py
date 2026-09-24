from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
import pandas as pd

from pipeline_rca import config

SCHEMAS = (config.RAW, config.STAGING, config.FINAL, config.META)


@contextmanager
def connect(db_path: Path | str | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    # always close the connection - duckdb only allows one writer per file,
    # which will matter once airflow runs each stage as a separate task
    path = Path(db_path or config.DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        for schema in SCHEMAS:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        yield con
    finally:
        con.close()


def write_table(con: duckdb.DuckDBPyConnection, schema: str, table: str, df: pd.DataFrame) -> int:
    con.register("_incoming", df)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM _incoming")
    finally:
        con.unregister("_incoming")
    return len(df)


def read_table(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> pd.DataFrame:
    return con.execute(f"SELECT * FROM {schema}.{table}").df()


def row_count(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
