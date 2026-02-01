#!/bin/bash
# Demonstrator: DuckDB CLI + httpfs extension from conda-forge
#
# This script proves that duckdb-cli and duckdb-extension-httpfs packages
# from conda-forge are compatible and can read/write to S3 (MinIO).
#
# Usage:
#   pixi run bash scripts/demo_duckdb_cli_httpfs.sh
#
# Or with custom MinIO settings:
#   AWS_ENDPOINT_URL=http://localhost:9000 \
#   AWS_ACCESS_KEY_ID=minioadmin \
#   AWS_SECRET_ACCESS_KEY=minioadmin \
#   pixi run bash scripts/demo_duckdb_cli_httpfs.sh

set -e

echo "=== DuckDB CLI + httpfs Extension Compatibility Demo ==="
echo

# --- Configuration ---
ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:9000}"
ACCESS_KEY="${AWS_ACCESS_KEY_ID:-minioadmin}"
SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
REGION="${BENCH_AWS_REGION:-us-east-1}"
BUCKET="${BENCH_S3_BUCKET:-benchmark}"
PREFIX="${BENCH_S3_PREFIX:-tpch}"

# Strip protocol for DuckDB secret
ENDPOINT="${ENDPOINT_URL#http://}"
ENDPOINT="${ENDPOINT#https://}"

# Detect USE_SSL
if [[ "$ENDPOINT_URL" == https://* ]]; then
    USE_SSL="true"
else
    USE_SSL="false"
fi

# --- Find DuckDB CLI ---
DUCKDB_CLI="${CONDA_PREFIX}/bin/duckdb"
if [[ ! -x "$DUCKDB_CLI" ]]; then
    DUCKDB_CLI="$(command -v duckdb)"
fi
echo "DuckDB CLI: $DUCKDB_CLI"
echo "DuckDB version: $("$DUCKDB_CLI" -version)"
echo

# --- Find httpfs extension from conda package ---
DUCKDB_VERSION="$("$DUCKDB_CLI" -version | head -1 | awk '{print $1}' | sed 's/^v//')"

# Detect platform
UNAME_S="$(uname -s)"
UNAME_M="$(uname -m)"
if [[ "$UNAME_S" == "Darwin" ]]; then
    PLATFORM="osx_${UNAME_M}"
elif [[ "$UNAME_S" == "Linux" ]]; then
    if [[ "$UNAME_M" == "x86_64" ]]; then
        PLATFORM="linux_amd64"
    else
        PLATFORM="linux_${UNAME_M}"
    fi
else
    PLATFORM="${UNAME_S}_${UNAME_M}"
fi

HTTPFS_EXT="${CONDA_PREFIX}/duckdb/extensions/v${DUCKDB_VERSION}/${PLATFORM}/httpfs.duckdb_extension"
echo "Looking for httpfs extension at:"
echo "  $HTTPFS_EXT"

if [[ ! -f "$HTTPFS_EXT" ]]; then
    echo "ERROR: httpfs extension not found!"
    echo "Make sure duckdb-extension-httpfs is installed via conda/pixi."
    exit 1
fi
echo "Found httpfs extension ($(du -h "$HTTPFS_EXT" | cut -f1))"
echo

# --- Test output path ---
TEST_ID="$(date +%s)-$$"
TEST_PATH="s3://${BUCKET}/${PREFIX}/cli_demo/${TEST_ID}"
echo "Test S3 path: $TEST_PATH"
echo

# --- Build SQL script ---
SQL_SCRIPT=$(mktemp /tmp/duckdb_demo_XXXXXX.sql)
cat > "$SQL_SCRIPT" << EOF
-- Load httpfs extension from conda package
LOAD '${HTTPFS_EXT}';

-- Create S3 secret for MinIO
CREATE SECRET (
    TYPE s3,
    KEY_ID '${ACCESS_KEY}',
    SECRET '${SECRET_KEY}',
    ENDPOINT '${ENDPOINT}',
    USE_SSL ${USE_SSL},
    URL_STYLE path,
    REGION '${REGION}'
);

-- Generate test data and write to S3
COPY (
    SELECT
        i AS id,
        'row_' || i AS name,
        random() AS value
    FROM generate_series(1, 1000) AS t(i)
) TO '${TEST_PATH}/test_data.parquet' (FORMAT PARQUET);

-- Read back and verify
SELECT 'Rows written and read back:' AS status, COUNT(*) AS row_count
FROM read_parquet('${TEST_PATH}/test_data.parquet');

-- Show some sample data
SELECT * FROM read_parquet('${TEST_PATH}/test_data.parquet') LIMIT 5;
EOF

echo "=== SQL Script ==="
cat "$SQL_SCRIPT"
echo
echo "=== Executing ==="

# Run DuckDB CLI with unsigned extensions enabled
"$DUCKDB_CLI" -unsigned -no-stdin :memory: < "$SQL_SCRIPT"

echo
echo "=== SUCCESS ==="
echo "DuckDB CLI ($DUCKDB_VERSION) + httpfs extension from conda-forge"
echo "successfully wrote and read Parquet data to/from S3!"
echo
echo "Test path: $TEST_PATH"

# Cleanup
rm -f "$SQL_SCRIPT"
