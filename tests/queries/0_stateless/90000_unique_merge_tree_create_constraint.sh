#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test 1: UniqueMergeTree without unique projection should fail
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_no_proj"
echo "$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="CREATE TABLE test_no_proj (x Int32, y UInt64) ENGINE = UniqueMergeTree() ORDER BY x" 2>&1)" \
  | grep -c 'DB::Exception:.*requires a projection.*with TYPE unique'
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_no_proj"

# Test 2: UniqueMergeTree with a normal (non-unique) projection should fail
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_wrong_proj"
echo "$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="CREATE TABLE test_wrong_proj (x Int32, y UInt64, PROJECTION __unique_index (SELECT count() GROUP BY x)) ENGINE = UniqueMergeTree() ORDER BY x" 2>&1)" \
  | grep -c 'DB::Exception:.*has index but it is not of TYPE unique\|DB::Exception:.*exists but has no index'
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_wrong_proj"

# Test 3: UniqueMergeTree projection name parameter must be a string literal
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_version_param"
echo "$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="CREATE TABLE test_version_param (x Int32, y UInt64, PROJECTION __unique_index INDEX x TYPE unique) ENGINE = UniqueMergeTree(123) ORDER BY x" 2>&1)" \
  | grep -c 'DB::Exception:.*must be a string literal'
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_version_param"

# Test 4: Unique projection index key must be simple identifiers, not expressions
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_expr_key"
echo "$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="CREATE TABLE test_expr_key (x Int32, y UInt64, PROJECTION __unique_index INDEX x + y TYPE unique) ENGINE = UniqueMergeTree() ORDER BY x" 2>&1)" \
  | grep -c 'DB::Exception:.*must be simple identifiers'
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_expr_key"

# Test 5: Successful creation with single-column unique projection
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_ok_single"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_ok_single (x Int32, y UInt64, PROJECTION __unique_index INDEX x TYPE unique) ENGINE = UniqueMergeTree() ORDER BY x"
echo "$(${CLICKHOUSE_CLIENT} --query="SELECT engine FROM system.tables WHERE name = 'test_ok_single' AND database = currentDatabase()")"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_ok_single"

# Test 6: Successful creation with multi-column unique projection
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_ok_multi"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_ok_multi (x Int32, y UInt64, z String, PROJECTION __unique_index INDEX x, y TYPE unique) ENGINE = UniqueMergeTree() ORDER BY x"
echo "$(${CLICKHOUSE_CLIENT} --query="SELECT engine FROM system.tables WHERE name = 'test_ok_multi' AND database = currentDatabase()")"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_ok_multi"

# Test 7: Successful creation with version column specified in TYPE unique('ver')
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_ok_version"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_ok_version (x Int32, y UInt64, ver UInt64, PROJECTION __unique_index INDEX x TYPE unique('ver')) ENGINE = UniqueMergeTree() ORDER BY x"
echo "$(${CLICKHOUSE_CLIENT} --query="SELECT engine FROM system.tables WHERE name = 'test_ok_version' AND database = currentDatabase()")"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_ok_version"
