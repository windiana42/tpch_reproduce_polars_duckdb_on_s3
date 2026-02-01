#!/usr/bin/env python3
"""Copy + project benchmark: read parquet, select 3 columns, write to S3.

No filtering - just column projection and write.
"""

import time

from config import (
    OUTPUT_PREFIX,
    TPCH_PATHS,
    cleanup_s3_path,
    duckdb_setup_s3,
    polars_storage_options,
)

DUCKDB_OUTPUT = f"{OUTPUT_PREFIX}/duckdb_copy"
POLARS_OUTPUT = f"{OUTPUT_PREFIX}/polars_copy.parquet"


# ============================================================
# DuckDB
# ============================================================
def run_duckdb():
    import duckdb

    con = duckdb.connect()
    duckdb_setup_s3(con)

    # Build UNION ALL query for multiple input files (no filter)
    subqueries = " UNION ALL ".join(
        [
            f"SELECT l_shipdate, l_discount, l_quantity FROM read_parquet('{p}')"
            for p in TPCH_PATHS
        ]
    )

    cleanup_s3_path(DUCKDB_OUTPUT)

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    con.execute(f"""
        COPY ({subqueries})
        TO '{DUCKDB_OUTPUT}'
        (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 300000);
    """)
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    con.close()
    return elapsed


# ============================================================
# Polars
# ============================================================
def run_polars():
    import polars as pl

    storage_opts = polars_storage_options()

    # Build lazy query (no filter)
    lf = pl.scan_parquet(TPCH_PATHS, storage_options=storage_opts).select(
        ["l_shipdate", "l_discount", "l_quantity"]
    )

    cleanup_s3_path(POLARS_OUTPUT)

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    lf.sink_parquet(POLARS_OUTPUT, storage_options=storage_opts, row_group_size=300_000)
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    return elapsed


if __name__ == "__main__":
    print(f"Input: {len(TPCH_PATHS)} files")
    print(f"DuckDB: {run_duckdb():.3f}s")
    print(f"Polars: {run_polars():.3f}s")
