# DuckDB vs Polars TPC-H (S3/MinIO) Benchmark

This repo benchmarks **DuckDB** (CLI backend) vs **Polars** on **TPC-H `lineitem`** Parquet data stored on **S3-compatible storage**.

It is designed to run fully offline (extensions are installed from conda packages) and works well with a local **MinIO** started via `docker-compose`.

## Prerequisites

- **Docker Engine** installed and running (to start MinIO)
- **Pixi** installed (`pixi` on `PATH`)

Optional:

- **docker-compose on PATH**
  - If you *don’t* have it, this repo provides a Pixi environment that installs a `docker-compose` executable (see below).

## One-time setup (local MinIO)

Start the S3-compatible backend (MinIO):

- **Using system `docker-compose`**:

```bash
docker-compose up -d
```

- **Using Pixi-provided `docker-compose`**:

```bash
pixi run -e docker -- docker-compose up -d
```

MinIO will listen on:

- **S3 API**: `http://localhost:9000`
- **Console**: `http://localhost:9001` (default credentials are `minioadmin` / `minioadmin`)

## Configure the benchmark (`bench.env`)

The Pixi tasks source `scripts/activate.sh`, which loads configuration from `bench.env`.

This repo already includes a `bench.env` suitable for local MinIO. If you want to reset to defaults:

```bash
cp bench.env.example bench.env
```

Key settings for “DuckDB CLI + extensions” mode:

- `BENCH_DUCKDB_BACKEND=cli`
- `BENCH_DUCKDB_S3_METHOD=httpfs`
- `AWS_ENDPOINT_URL=http://localhost:9000`

## Run everything

This runs:

- dataset generation to S3 (DuckDB `tpch` extension + `dbgen`)
- read-only benchmarks (`count`)
- write benchmarks (`filter_write`, `copy_project`)
- string-table benchmarks (`join`, `groupby`)
- plots

```bash
pixi run all
```

Outputs are written to local directories (defaults from `bench.env`):

- `results_s3`
- `results_s3_filter_write`
- `results_s3_copy_project`
- `results_string`

## Clean up

Stop MinIO:

- **System `docker-compose`**:

```bash
docker-compose down -v
```

- **Pixi-provided `docker-compose`**:

```bash
pixi run -e docker -- docker-compose down -v
```

## Notes / Troubleshooting

- **Bucket creation**: when `AWS_ENDPOINT_URL` is set (e.g. MinIO), the benchmark script will automatically create the target bucket (e.g. `benchmark`) if missing.
- **DuckDB extensions**: `httpfs` and `tpch` are provided via conda packages (`duckdb-extension-httpfs`, `duckdb-extension-tpch`). The benchmark loads them by absolute path, so it does not rely on `INSTALL ...` downloads.
- **Docker-compose via Pixi**: the Pixi `docker` environment only provides the `docker-compose` *client*. You still need Docker installed and running on the host.

