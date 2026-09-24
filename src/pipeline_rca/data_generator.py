"""Fake customer + loan data using Faker.

Two tables, like what upstream would send us:
  customers - from the CRM
  loans     - from 3 source systems (core banking, cards, mortgage)

Some values are intentionally messy (lowercase codes, trailing spaces) so
staging has something to clean.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd
from faker import Faker

from pipeline_rca import config

# loan_type: (source_system, principal range, terms in months, rate range)
LOAN_PRODUCTS: dict[str, tuple[str, tuple[int, int], list[int], tuple[float, float]]] = {
    "MORTGAGE": ("MORTGAGE_SVC", (120_000, 750_000), [180, 360], (0.055, 0.078)),
    "AUTO": ("CORE_BANKING", (8_000, 65_000), [36, 48, 60, 72], (0.045, 0.110)),
    "PERSONAL": ("CORE_BANKING", (2_000, 40_000), [12, 24, 36, 60], (0.080, 0.190)),
    "STUDENT": ("CORE_BANKING", (5_000, 90_000), [120, 180], (0.040, 0.085)),
    "CREDIT_CARD": ("CARD_PLATFORM", (500, 25_000), [0], (0.180, 0.290)),
}
LOAN_TYPE_WEIGHTS = [0.15, 0.25, 0.20, 0.10, 0.30]

STATUSES = ["CURRENT", "DELINQUENT_30", "DELINQUENT_90", "DEFAULT", "PAID_OFF", "WITHDRAWN"]
STATUS_WEIGHTS = [0.78, 0.07, 0.03, 0.02, 0.07, 0.03]


def _messy_case(values: pd.Series, rng: np.random.Generator, rate: float = 0.1) -> pd.Series:
    # lowercase some values and add trailing spaces to a few
    out = values.copy()
    mask = rng.random(len(out)) < rate
    out[mask] = out[mask].str.lower()
    pad = rng.random(len(out)) < rate / 2
    out[pad] = out[pad] + " "
    return out


def generate_customers(n: int = config.N_CUSTOMERS, seed: int = config.DEFAULT_SEED) -> pd.DataFrame:
    fake = Faker("en_US")
    Faker.seed(seed)
    rng = np.random.default_rng(seed)
    as_of = config.AS_OF_DATE

    credit = np.clip(rng.normal(690, 70, n).round(), 300, 850).astype(int)
    income = np.round(rng.lognormal(mean=11.1, sigma=0.5, size=n), -2)

    df = pd.DataFrame(
        {
            # zero padded ids, same as most core banking systems
            "customer_id": [f"{i:08d}" for i in range(1, n + 1)],
            "first_name": [fake.first_name() for _ in range(n)],
            "last_name": [fake.last_name() for _ in range(n)],
            "email": [fake.unique.email() for _ in range(n)],
            "state": [fake.state_abbr(include_territories=False) for _ in range(n)],
            "date_of_birth": [
                fake.date_between(as_of - timedelta(days=80 * 365), as_of - timedelta(days=21 * 365))
                for _ in range(n)
            ],
            "credit_score": pd.array(credit, dtype="Int64"),
            "annual_income": income,
            "customer_since": [
                fake.date_between(as_of - timedelta(days=20 * 365), as_of) for _ in range(n)
            ],
        }
    )
    df["state"] = _messy_case(df["state"], rng)
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"])
    df["customer_since"] = pd.to_datetime(df["customer_since"])
    return df


def generate_loans(
    customers: pd.DataFrame, n: int = config.N_LOANS, seed: int = config.DEFAULT_SEED
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 1)
    as_of = pd.Timestamp(config.AS_OF_DATE)

    loan_types = rng.choice(list(LOAN_PRODUCTS), size=n, p=LOAN_TYPE_WEIGHTS)
    statuses = rng.choice(STATUSES, size=n, p=STATUS_WEIGHTS)

    source_system, principal, term, rate = [], [], [], []
    for lt in loan_types:
        src, (lo, hi), terms, (rlo, rhi) = LOAN_PRODUCTS[lt]
        source_system.append(src)
        principal.append(round(float(rng.uniform(lo, hi)), 2))
        term.append(int(rng.choice(terms)))
        rate.append(round(float(rng.uniform(rlo, rhi)), 4))

    principal_arr = np.array(principal)
    balance = np.round(principal_arr * rng.uniform(0.05, 1.0, n), 2)
    # paid off / withdrawn loans have nothing outstanding
    balance[np.isin(statuses, ["PAID_OFF", "WITHDRAWN"])] = 0.0

    days_back = rng.integers(0, 8 * 365, n)
    origination = as_of - pd.to_timedelta(days_back, unit="D")

    df = pd.DataFrame(
        {
            "loan_id": [f"L{i:09d}" for i in range(1, n + 1)],
            "customer_id": rng.choice(customers["customer_id"].to_numpy(), size=n),
            "loan_type": loan_types,
            "source_system": source_system,
            "principal": principal_arr,
            "balance": balance,
            "interest_rate": rate,
            "term_months": term,
            "status": statuses,
            "origination_date": origination,
        }
    )
    df["status"] = _messy_case(df["status"].astype(str), rng)
    return df


def generate_source_data(seed: int = config.DEFAULT_SEED) -> dict[str, pd.DataFrame]:
    customers = generate_customers(seed=seed)
    loans = generate_loans(customers, seed=seed)
    return {"customers": customers, "loans": loans}
