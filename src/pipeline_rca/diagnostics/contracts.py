"""What each layer is supposed to look like.

These are the expectations the diagnostics compare against: column types,
primary keys, max null % per column, and how rows are expected to move
between layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# pipeline order - used to figure out where a problem first showed up
TRANSITIONS = ["source->raw", "raw->staging", "staging->final"]
LAYER_OF_TRANSITION = {"source->raw": "raw", "raw->staging": "staging", "staging->final": "final"}

_CUSTOMER_COLS = {
    "customer_id": "VARCHAR",
    "first_name": "VARCHAR",
    "last_name": "VARCHAR",
    "email": "VARCHAR",
    "state": "VARCHAR",
    "date_of_birth": "TIMESTAMP_S",
    "credit_score": "BIGINT",
    "annual_income": "DOUBLE",
    "customer_since": "TIMESTAMP_S",
}

_LOAN_COLS = {
    "loan_id": "VARCHAR",
    "customer_id": "VARCHAR",
    "loan_type": "VARCHAR",
    "source_system": "VARCHAR",
    "principal": "DOUBLE",
    "balance": "DOUBLE",
    "interest_rate": "DOUBLE",
    "term_months": "BIGINT",
    "status": "VARCHAR",
    "origination_date": "TIMESTAMP_S",
}

_PORTFOLIO_COLS = {
    "loan_id": "VARCHAR",
    "customer_id": "VARCHAR",
    "state": "VARCHAR",
    "credit_score": "BIGINT",
    "risk_band": "VARCHAR",
    "loan_type": "VARCHAR",
    "source_system": "VARCHAR",
    "principal": "DOUBLE",
    "balance": "DOUBLE",
    "interest_rate": "DOUBLE",
    "term_months": "BIGINT",
    "status": "VARCHAR",
    "is_delinquent": "BOOLEAN",
    "origination_date": "DATE",
}


@dataclass(frozen=True)
class TableContract:
    layer: str
    table: str
    columns: dict[str, str]
    primary_key: str
    max_null_pct: float = 1.0  # default for every column
    null_overrides: dict[str, float] = field(default_factory=dict)
    # which transition writes this table
    written_by: str = ""

    @property
    def name(self) -> str:
        return f"{self.layer}.{self.table}"

    def null_limit(self, column: str) -> float:
        return self.null_overrides.get(column, self.max_null_pct)


CONTRACTS = [
    TableContract("raw", "customers", _CUSTOMER_COLS, "customer_id", written_by="source->raw"),
    TableContract("raw", "loans", _LOAN_COLS, "loan_id", written_by="source->raw"),
    TableContract("staging", "customers", _CUSTOMER_COLS, "customer_id", written_by="raw->staging"),
    TableContract("staging", "loans", _LOAN_COLS, "loan_id", written_by="raw->staging"),
    TableContract("final", "loan_portfolio", _PORTFOLIO_COLS, "loan_id", written_by="staging->final"),
]

# risk_band = UNKNOWN only happens when credit_score is missing, so it should be rare
MAX_UNKNOWN_RISK_PCT = 1.0

# how far a total can drift between layers before we flag it
MEASURE_TOLERANCE_PCT = 0.5
