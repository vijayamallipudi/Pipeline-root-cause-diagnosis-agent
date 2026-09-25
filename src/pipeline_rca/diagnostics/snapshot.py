"""Profile every table in every layer: row count, types, nulls, distincts, min/max/sum.

One snapshot per table per run, saved to meta.layer_snapshots so runs can be
compared later.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

import duckdb

from pipeline_rca import config
from pipeline_rca.diagnostics.contracts import CONTRACTS

NUMERIC_TYPES = {"DOUBLE", "BIGINT", "INTEGER", "FLOAT", "DECIMAL", "HUGEINT", "SMALLINT"}

SNAPSHOTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.META}.layer_snapshots (
    run_id      VARCHAR,
    layer       VARCHAR,
    table_name  VARCHAR,
    row_count   BIGINT,
    profile     JSON
)
"""


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_count: int
    null_pct: float
    distinct_count: int
    min: float | str | None = None
    max: float | str | None = None
    sum: float | None = None


@dataclass
class TableSnapshot:
    layer: str
    table: str
    row_count: int
    primary_key: str
    duplicate_keys: int
    columns: dict[str, ColumnProfile] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return f"{self.layer}.{self.table}"

    def to_dict(self) -> dict:
        return asdict(self)


def _column_types(con: duckdb.DuckDBPyConnection, layer: str, table: str) -> dict[str, str]:
    rows = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [layer, table],
    ).fetchall()
    return dict(rows)


def profile_table(con: duckdb.DuckDBPyConnection, layer: str, table: str, primary_key: str) -> TableSnapshot:
    types = _column_types(con, layer, table)

    # build one query that profiles every column in a single scan
    exprs = ["COUNT(*) AS n", f"COUNT(*) - COUNT(DISTINCT {primary_key}) AS dup_keys"]
    for i, (col, dtype) in enumerate(types.items()):
        exprs += [f'COUNT("{col}") AS nn_{i}', f'COUNT(DISTINCT "{col}") AS d_{i}']
        if dtype in NUMERIC_TYPES:
            exprs += [f'MIN("{col}") AS mn_{i}', f'MAX("{col}") AS mx_{i}', f'SUM("{col}") AS s_{i}']
        elif dtype.startswith(("DATE", "TIMESTAMP")):
            exprs += [f'CAST(MIN("{col}") AS VARCHAR) AS mn_{i}', f'CAST(MAX("{col}") AS VARCHAR) AS mx_{i}']

    row = con.execute(f"SELECT {', '.join(exprs)} FROM {layer}.{table}").df().iloc[0].to_dict()
    n = int(row["n"])

    snap = TableSnapshot(layer, table, n, primary_key, int(row["dup_keys"]))
    for i, (col, dtype) in enumerate(types.items()):
        nulls = n - int(row[f"nn_{i}"])
        mn, mx, s = row.get(f"mn_{i}"), row.get(f"mx_{i}"), row.get(f"s_{i}")
        snap.columns[col] = ColumnProfile(
            name=col,
            dtype=dtype,
            null_count=nulls,
            null_pct=round(100.0 * nulls / n, 2) if n else 0.0,
            distinct_count=int(row[f"d_{i}"]),
            min=None if mn is None or mn != mn else (float(mn) if dtype in NUMERIC_TYPES else mn),
            max=None if mx is None or mx != mx else (float(mx) if dtype in NUMERIC_TYPES else mx),
            sum=None if s is None or s != s else round(float(s), 2),
        )
    return snap


def take_snapshots(con: duckdb.DuckDBPyConnection) -> dict[str, TableSnapshot]:
    return {c.name: profile_table(con, c.layer, c.table, c.primary_key) for c in CONTRACTS}


def save_snapshots(con: duckdb.DuckDBPyConnection, run_id: str, snapshots: dict[str, TableSnapshot]) -> None:
    con.execute(SNAPSHOTS_DDL)
    con.execute(f"DELETE FROM {config.META}.layer_snapshots WHERE run_id = ?", [run_id])
    for snap in snapshots.values():
        con.execute(
            f"INSERT INTO {config.META}.layer_snapshots VALUES (?, ?, ?, ?, ?)",
            [run_id, snap.layer, snap.table, snap.row_count, json.dumps(snap.to_dict(), default=str)],
        )
