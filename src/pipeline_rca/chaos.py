"""Chaos injector - breaks the pipeline on purpose so we have something to diagnose.

The pipeline calls injector.apply(stage, table, df) at two points:
  extract -> before writing to raw
  staging -> before writing to staging

It only does something if the active scenario targets that stage/table.
When it fires it saves ground_truth (what it broke, how many rows) so we can
check later whether the diagnosis found the right thing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Callable

import numpy as np
import pandas as pd

from pipeline_rca import config


class Scenario(str, Enum):
    NONE = "none"
    TYPE_CHANGE = "type_change"
    NULL_SPIKE = "null_spike"
    DROPPED_JOIN_KEY = "dropped_join_key"
    DUPLICATE_ROWS = "duplicate_rows"


@dataclass(frozen=True)
class ChaosSpec:
    scenario: Scenario
    title: str
    stage: str  # extract or staging
    table: str
    description: str


def _type_change(df: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, dict]:
    # CORE_BANKING starts sending balance as "$12,345.67", so the column ends up varchar
    df = df.copy()
    affected = df["source_system"] == "CORE_BANKING"
    df["balance"] = [
        f"${v:,.2f}" if hit else f"{v:.2f}" for v, hit in zip(df["balance"], affected)
    ]
    return df, {
        "column": "balance",
        "old_type": "DOUBLE",
        "new_type": "VARCHAR",
        "affected_segment": "source_system = 'CORE_BANKING'",
        "rows_affected": int(affected.sum()),
    }


def _null_spike(df: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, dict]:
    # bureau feed partly failed, ~35% of customers lose their credit score
    df = df.copy()
    mask = rng.random(len(df)) < 0.35
    df.loc[mask, "credit_score"] = pd.NA
    return df, {"column": "credit_score", "rows_affected": int(mask.sum()), "null_rate": 0.35}


def _dropped_join_key(df: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, dict]:
    # casting to int strips the leading zeros: '00001234' -> '1234'
    # these no longer match staging.customers so the inner join drops them
    df = df.copy()
    affected = df["source_system"] == "CARD_PLATFORM"
    df.loc[affected, "customer_id"] = df.loc[affected, "customer_id"].astype(int).astype(str)
    return df, {
        "column": "customer_id",
        "affected_segment": "source_system = 'CARD_PLATFORM'",
        "rows_affected": int(affected.sum()),
        "example": "'00001234' -> '1234'",
    }


def _duplicate_rows(df: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, dict]:
    # load got retried and appended the last 12 months of loans a second time
    cutoff = pd.Timestamp(config.AS_OF_DATE - timedelta(days=365))
    batch = df[df["origination_date"] >= cutoff]
    out = pd.concat([df, batch], ignore_index=True)
    return out, {
        "key": "loan_id",
        "rows_affected": len(batch),
        "affected_segment": f"origination_date >= '{cutoff.date()}'",
    }


SCENARIOS: dict[Scenario, tuple[ChaosSpec, Callable]] = {
    Scenario.TYPE_CHANGE: (
        ChaosSpec(
            Scenario.TYPE_CHANGE,
            "Column type change",
            "extract",
            "loans",
            "CORE_BANKING started sending balance as a currency string ('$12,345.67'). "
            "The numeric cast in staging turns those into NULL without failing.",
        ),
        _type_change,
    ),
    Scenario.NULL_SPIKE: (
        ChaosSpec(
            Scenario.NULL_SPIKE,
            "Null spike",
            "extract",
            "customers",
            "Credit bureau feed partly failed, ~35% of customers have no credit_score. "
            "Those loans end up in the UNKNOWN risk band.",
        ),
        _null_spike,
    ),
    Scenario.DROPPED_JOIN_KEY: (
        ChaosSpec(
            Scenario.DROPPED_JOIN_KEY,
            "Dropped join key",
            "staging",
            "loans",
            "Staging cast CARD_PLATFORM customer_id to int and lost the leading zeros. "
            "Those loans don't join to customers anymore and drop out of final.",
        ),
        _dropped_join_key,
    ),
    Scenario.DUPLICATE_ROWS: (
        ChaosSpec(
            Scenario.DUPLICATE_ROWS,
            "Duplicate rows",
            "staging",
            "loans",
            "A retried load appended the latest batch of loans again. "
            "Rows are duplicated and final balances are inflated.",
        ),
        _duplicate_rows,
    ),
}


@dataclass
class ChaosInjector:
    scenario: Scenario = Scenario.NONE
    seed: int = config.DEFAULT_SEED
    ground_truth: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.scenario = Scenario(self.scenario)

    @property
    def spec(self) -> ChaosSpec | None:
        entry = SCENARIOS.get(self.scenario)
        return entry[0] if entry else None

    def apply(self, stage: str, table: str, df: pd.DataFrame) -> pd.DataFrame:
        spec = self.spec
        if spec is None or (spec.stage, spec.table) != (stage, table):
            return df
        _, fn = SCENARIOS[self.scenario]
        rng = np.random.default_rng(self.seed + 99)
        out, details = fn(df, rng)
        self.ground_truth = {
            "scenario": self.scenario.value,
            "title": spec.title,
            "injected_at": f"{stage}:{table}",
            "description": spec.description,
            **details,
        }
        return out


def list_scenarios() -> list[dict]:
    rows = [{"scenario": Scenario.NONE.value, "title": "Healthy run", "stage": "-", "table": "-",
             "description": "Nothing injected, everything should pass."}]
    for spec, _ in SCENARIOS.values():
        rows.append({"scenario": spec.scenario.value, "title": spec.title, "stage": spec.stage,
                     "table": spec.table, "description": spec.description})
    return rows
