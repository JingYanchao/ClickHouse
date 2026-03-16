#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_concurrent";
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_concurrent (x UInt32, y UInt32, PROJECTION __unique_index INDEX x TYPE unique) ENGINE = UniqueMergeTree ORDER BY x";

function thread1()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO test_concurrent SELECT number as x, 0 as y FROM numbers(50)";
    done
}

function thread2()
{
   while true; do
           $CLICKHOUSE_CLIENT --query "INSERT INTO test_concurrent SELECT number as x, 0 as y FROM numbers(50)";
       done
}

function thread3()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "UPDATE test_concurrent set y = 0 where x < 20";
    done
}

function thread4()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "UPDATE test_concurrent set y = 0 where x < 50";
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &

wait
$CLICKHOUSE_CLIENT -q "select x from test_concurrent order by x"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_concurrent"
