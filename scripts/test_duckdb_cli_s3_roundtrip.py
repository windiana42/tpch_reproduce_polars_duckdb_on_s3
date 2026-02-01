import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from s3_tpch_duckdb_polars_bench import _duckdb_cli_exec, _duckdb_cli_script  # noqa: E402


def main() -> None:
    bucket = os.environ.get("BENCH_S3_BUCKET", "benchmark")
    prefix = os.environ.get("BENCH_S3_PREFIX", "tpch")
    region = os.environ.get("BENCH_AWS_REGION", "us-east-1")
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")

    out = f"s3://{bucket}/{prefix}/cli_smoke/{uuid.uuid4()}"
    file_path = f"{out}/one.parquet"

    write_script = _duckdb_cli_script(
        sql_body=f"COPY (SELECT 1 AS x) TO '{file_path}' (FORMAT PARQUET)",
        region=region,
        endpoint_url=endpoint_url,
        threads=None,
        load_tpch=False,
    )
    time_s, mem_mb, _ = _duckdb_cli_exec(
        sql_script=write_script, capture_stdout=False, csv_no_header=False
    )
    print({"write_time_s": time_s, "write_mem_mb": mem_mb, "path": file_path})

    read_script = _duckdb_cli_script(
        sql_body=f"SELECT COUNT(*) FROM read_parquet('{file_path}')",
        region=region,
        endpoint_url=endpoint_url,
        threads=None,
        load_tpch=False,
    )
    time_s, mem_mb, out_text = _duckdb_cli_exec(
        sql_script=read_script, capture_stdout=True, csv_no_header=True
    )
    last = out_text.strip().splitlines()[-1] if out_text.strip() else ""
    print({"read_time_s": time_s, "read_mem_mb": mem_mb, "count": last})


if __name__ == "__main__":
    main()
