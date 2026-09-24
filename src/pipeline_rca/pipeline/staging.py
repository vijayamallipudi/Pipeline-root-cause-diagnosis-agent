"""Staging layer (pandas).

- trim / uppercase codes (state, status)
- cast numbers and dates
- drop WITHDRAWN loans (never funded)
- dedupe on primary key
"""

from __future__ import annotations

import pandas as pd

from pipeline_rca import config
from pipeline_rca.chaos import ChaosInjector
from pipeline_rca.db import connect, read_table, write_table

EXCLUDED_STATUSES = {"WITHDRAWN"}


def clean_customers(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["first_name"] = df["first_name"].str.strip()
    df["last_name"] = df["last_name"].str.strip()
    df["email"] = df["email"].str.strip().str.lower()
    df["state"] = df["state"].str.strip().str.upper()
    df["credit_score"] = pd.to_numeric(df["credit_score"], errors="coerce").astype("Int64")
    df["annual_income"] = pd.to_numeric(df["annual_income"], errors="coerce")
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"])
    df["customer_since"] = pd.to_datetime(df["customer_since"])
    return df.drop_duplicates(subset="customer_id", keep="first").reset_index(drop=True)


def clean_loans(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.upper()
    # errors="coerce" never fails the job, anything it can't parse just becomes NULL.
    # common pattern, and exactly how bad data slips through silently
    for col in ("principal", "balance", "interest_rate"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["term_months"] = pd.to_numeric(df["term_months"], errors="coerce").astype("Int64")
    df["origination_date"] = pd.to_datetime(df["origination_date"])
    df = df[~df["status"].isin(EXCLUDED_STATUSES)]
    return df.drop_duplicates(subset="loan_id", keep="first").reset_index(drop=True)


def transform_staging(injector: ChaosInjector, db_path=None) -> dict[str, int]:
    counts = {}
    with connect(db_path) as con:
        for table, cleaner in (("customers", clean_customers), ("loans", clean_loans)):
            df = cleaner(read_table(con, config.RAW, table))
            df = injector.apply("staging", table, df)
            counts[table] = write_table(con, config.STAGING, table, df)
    return counts
