"""Raw layer - land the source data as-is, no cleaning."""

from __future__ import annotations

from pipeline_rca import config
from pipeline_rca.chaos import ChaosInjector
from pipeline_rca.data_generator import generate_source_data
from pipeline_rca.db import connect, write_table


def extract(injector: ChaosInjector, db_path=None, seed: int = config.DEFAULT_SEED) -> dict[str, int]:
    sources = generate_source_data(seed=seed)
    counts = {}
    with connect(db_path) as con:
        for table, df in sources.items():
            df = injector.apply("extract", table, df)
            counts[table] = write_table(con, config.RAW, table, df)
    return counts
