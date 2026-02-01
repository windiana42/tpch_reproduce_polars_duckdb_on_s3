#!/usr/bin/env python3
"""Filter + write benchmark: read parquet, filter rows, write to S3.

TPC-H Q6-style filter on lineitem table:
- l_shipdate between 1994-01-01 and 1995-01-01
- l_discount between 0.05 and 0.07
- l_quantity < 24
"""

import time

from config import (
    OUTPUT_PREFIX,
    TPCH_PATHS,
    cleanup_s3_path,
    duckdb_setup_s3,
    polars_storage_options,
)

DUCKDB_OUTPUT = f"{OUTPUT_PREFIX}/duckdb_filter"
POLARS_OUTPUT = f"{OUTPUT_PREFIX}/polars_filter.parquet"


# ============================================================
# DuckDB
# ============================================================
def run_duckdb():
    import duckdb

    con = duckdb.connect()
    duckdb_setup_s3(con)

    # Build UNION ALL query for multiple input files
    subqueries = " UNION ALL ".join(
        [
            f"""SELECT l_shipdate, l_discount, l_quantity FROM read_parquet('{p}')
            WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"""
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
    from datetime import date

    import polars as pl

    storage_opts = polars_storage_options()

    # Build lazy query
    lf = (
        pl.scan_parquet(TPCH_PATHS, storage_options=storage_opts)
        .select(["l_shipdate", "l_discount", "l_quantity"])
        .filter(
            (pl.col("l_shipdate") >= date(1994, 1, 1))
            & (pl.col("l_shipdate") < date(1995, 1, 1))
            & (pl.col("l_discount") >= 0.05)
            & (pl.col("l_discount") <= 0.07)
            & (pl.col("l_quantity") < 24)
        )
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
