import os
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.getenv("RCA_DATA_DIR", PROJECT_ROOT / "data"))
DB_PATH = Path(os.getenv("RCA_DB_PATH", DATA_DIR / "pipeline.duckdb"))

# fixed seed + fixed as-of date so every run generates the same data
DEFAULT_SEED = int(os.getenv("RCA_SEED", "42"))
AS_OF_DATE = date(2026, 6, 30)

N_CUSTOMERS = int(os.getenv("RCA_N_CUSTOMERS", "2000"))
N_LOANS = int(os.getenv("RCA_N_LOANS", "5000"))

# schemas in the duckdb file
RAW = "raw"
STAGING = "staging"
FINAL = "final"
META = "meta"
