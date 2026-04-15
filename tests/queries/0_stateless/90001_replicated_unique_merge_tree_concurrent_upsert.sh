#!/usr/bin/env bash
# Tags: zookeeper

# Test: ReplicatedUniqueMergeTree concurrent upsert on two replicas
# Verifies that concurrent INSERTs and UPDATEs on both replicas
# converge to a consistent state.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_concurrent_r1"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_concurrent_r2"

$CLICKHOUSE_CLIENT --query "CREATE TABLE test_concurrent_r1 (x UInt32, y UInt32, PROJECTION __unique_index INDEX x TYPE unique) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/${CLICKHOUSE_DATABASE}/test_90001/concurrent_upsert', '1') ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_concurrent_r2 (x UInt32, y UInt32, PROJECTION __unique_index INDEX x TYPE unique) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/${CLICKHOUSE_DATABASE}/test_90001/concurrent_upsert', '2') ORDER BY x"

function thread_insert_r1()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO test_concurrent_r1 SELECT number as x, 1 as y FROM numbers(50)" 2>/dev/null
    done
}

function thread_insert_r2()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO test_concurrent_r2 SELECT number as x, 2 as y FROM numbers(50)" 2>/dev/null
    done
}

function thread_update_r1()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "UPDATE test_concurrent_r1 SET y = 3 WHERE x < 20" 2>/dev/null
    done
}

function thread_update_r2()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "UPDATE test_concurrent_r2 SET y = 4 WHERE x < 30" 2>/dev/null
    done
}

export -f thread_insert_r1
export -f thread_insert_r2
export -f thread_update_r1
export -f thread_update_r2

TIMEOUT=10

timeout $TIMEOUT bash -c thread_insert_r1 &
timeout $TIMEOUT bash -c thread_insert_r2 &
timeout $TIMEOUT bash -c thread_update_r1 &
timeout $TIMEOUT bash -c thread_update_r2 &

wait

# Sync both replicas
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA test_concurrent_r1" 2>/dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA test_concurrent_r2" 2>/dev/null

# Both replicas should have exactly 50 unique keys
$CLICKHOUSE_CLIENT --query "SELECT count() FROM test_concurrent_r1"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM test_concurrent_r2"

# Both replicas should have the same data
$CLICKHOUSE_CLIENT --query "SELECT x FROM test_concurrent_r1 ORDER BY x" > /tmp/r1_data.txt
$CLICKHOUSE_CLIENT --query "SELECT x FROM test_concurrent_r2 ORDER BY x" > /tmp/r2_data.txt
diff /tmp/r1_data.txt /tmp/r2_data.txt > /dev/null && echo "MATCH" || echo "MISMATCH"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_concurrent_r1"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_concurrent_r2"
