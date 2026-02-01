import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from s3_tpch_duckdb_polars_bench import _duckdb_cli_exec, _duckdb_cli_script  # noqa: E402


def main() -> None:
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    script = _duckdb_cli_script(
        sql_body="SELECT 1",
        region=os.environ.get("BENCH_AWS_REGION", "us-east-1"),
        endpoint_url=endpoint,
        threads=None,
        load_tpch=False,
    )
    time_s, mem_mb, out = _duckdb_cli_exec(
        sql_script=script, capture_stdout=True, csv_no_header=True
    )
    print({"time_s": time_s, "mem_mb": mem_mb, "out": out.strip()})


if __name__ == "__main__":
    main()
