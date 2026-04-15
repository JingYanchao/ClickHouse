"""
Integration tests for ReplicatedUniqueMergeTree.

Tests out-of-order fetch scenarios, node restart convergence,
and multi-node dedup correctness using a 2-node cluster.
"""

import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[],
    with_zookeeper=True,
    macros={"shard": "1", "replica": "1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[],
    with_zookeeper=True,
    macros={"shard": "1", "replica": "2"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table_name, zk_path, replica_id, extra_settings=""):
    node.query(
        f"""
        CREATE TABLE {table_name}
        (
            id UInt32,
            value UInt32,
            PROJECTION __unique_index INDEX id TYPE unique
        )
        ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/test_rumt/{zk_path}', '{replica_id}')
        ORDER BY id
        {extra_settings}
    """
    )


def drop_table(nodes, table_name):
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def get_data(node, table_name):
    """Return sorted (id, value) pairs as a list of tuples."""
    result = node.query(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = []
    for line in result.strip().split("\n"):
        if line:
            parts = line.split("\t")
            rows.append((int(parts[0]), int(parts[1])))
    return rows


def test_out_of_order_fetch_basic(started_cluster):
    """
    Test: Replica 2 stops fetches, replica 1 inserts + merges,
    then replica 2 resumes fetches and gets the merged part
    before all source INSERT parts arrive.

    This exercises the dedupForFetch fallback path where
    areAllBlockNumbersCovered returns false.
    """
    table = "test_ooo_fetch_basic"
    drop_table([node1, node2], table)

    try:
        create_table(node1, table, "ooo_fetch_basic", "1")
        create_table(node2, table, "ooo_fetch_basic", "2")

        # Insert on node1
        node1.query(f"INSERT INTO {table} SELECT number, number FROM numbers(10)")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        # Stop fetches on node2
        node2.query(f"SYSTEM STOP FETCHES {table}")

        # Insert more overlapping data on node1
        node1.query(
            f"INSERT INTO {table} SELECT number, number + 100 FROM numbers(5)"
        )
        node1.query(
            f"INSERT INTO {table} SELECT number + 5, number + 200 FROM numbers(5)"
        )

        # Merge on node1
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        # Resume fetches on node2 — it may get the merged part
        # before the individual INSERT parts
        node2.query(f"SYSTEM START FETCHES {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        # Both nodes should converge
        data1 = get_data(node1, table)
        data2 = get_data(node2, table)

        assert data1 == data2, f"Data mismatch:\nnode1: {data1}\nnode2: {data2}"
        assert len(data1) == 10

        # Verify specific values
        expected = [(i, i + 100) for i in range(5)] + [
            (i, i + 200) for i in range(5, 10)
        ]
        assert data1 == expected, f"Unexpected data: {data1}"

    finally:
        drop_table([node1, node2], table)


def test_out_of_order_fetch_with_local_inserts(started_cluster):
    """
    Test: Replica 2 has local INSERT parts when it receives a merged part
    from replica 1 via out-of-order fetch.

    The reverse dedup in dedupForFetch must correctly handle the
    interaction between local parts and the fetched merged part.
    """
    table = "test_ooo_fetch_local"
    drop_table([node1, node2], table)

    try:
        create_table(node1, table, "ooo_fetch_local", "1")
        create_table(node2, table, "ooo_fetch_local", "2")

        # Insert on node1 and sync
        node1.query(f"INSERT INTO {table} SELECT number, number FROM numbers(10)")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        # Stop fetches on node2
        node2.query(f"SYSTEM STOP FETCHES {table}")

        # Insert on node2 (local insert with overlapping keys)
        node2.query(
            f"INSERT INTO {table} SELECT number, number + 500 FROM numbers(5)"
        )

        # Insert and merge on node1
        node1.query(
            f"INSERT INTO {table} SELECT number, number + 1000 FROM numbers(5)"
        )
        # Wait for node1 to sync node2's insert
        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        # Resume fetches on node2
        node2.query(f"SYSTEM START FETCHES {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        # Both nodes should converge
        data1 = get_data(node1, table)
        data2 = get_data(node2, table)

        assert data1 == data2, f"Data mismatch:\nnode1: {data1}\nnode2: {data2}"
        assert len(data1) == 10

    finally:
        drop_table([node1, node2], table)


def test_node_restart_convergence(started_cluster):
    """
    Test: After node2 restarts, it rebuilds delete bitmaps via
    buildAllDeleteBitmapsOnStartup and converges with node1.
    """
    table = "test_restart_conv"
    drop_table([node1, node2], table)

    try:
        create_table(node1, table, "restart_conv", "1")
        create_table(node2, table, "restart_conv", "2")

        # Insert and upsert on node1
        node1.query(f"INSERT INTO {table} SELECT number, number FROM numbers(20)")
        node1.query(
            f"INSERT INTO {table} SELECT number, number + 100 FROM numbers(10)"
        )
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        data_before = get_data(node2, table)

        # Restart node2
        node2.restart_clickhouse()

        # After restart, node2 should rebuild delete bitmaps
        data_after = get_data(node2, table)

        assert (
            data_before == data_after
        ), f"Data changed after restart:\nbefore: {data_before}\nafter: {data_after}"

        # Verify convergence with node1
        data1 = get_data(node1, table)
        assert data1 == data_after, f"Divergence after restart:\nnode1: {data1}\nnode2: {data_after}"

    finally:
        drop_table([node1, node2], table)


def test_insert_during_fetch(started_cluster):
    """
    Test: Node2 receives inserts while also fetching parts from node1.
    Both nodes should converge to the same final state.
    """
    table = "test_insert_during_fetch"
    drop_table([node1, node2], table)

    try:
        create_table(node1, table, "insert_during_fetch", "1")
        create_table(node2, table, "insert_during_fetch", "2")

        # Interleaved inserts on both nodes
        node1.query(f"INSERT INTO {table} SELECT number, 1 FROM numbers(10)")
        node2.query(f"INSERT INTO {table} SELECT number, 2 FROM numbers(10)")
        node1.query(f"INSERT INTO {table} SELECT number, 3 FROM numbers(10)")
        node2.query(f"INSERT INTO {table} SELECT number, 4 FROM numbers(10)")

        # Sync both replicas
        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        # Both should converge
        data1 = get_data(node1, table)
        data2 = get_data(node2, table)

        assert data1 == data2, f"Data mismatch:\nnode1: {data1}\nnode2: {data2}"
        assert len(data1) == 10

        # All values should be 4 (last write wins by block number)
        for id_val, value in data1:
            assert value == 4, f"Key {id_val} has value {value}, expected 4"

    finally:
        drop_table([node1, node2], table)


def test_merge_on_both_replicas(started_cluster):
    """
    Test: Both replicas independently merge parts. After sync,
    they should have identical data.
    """
    table = "test_merge_both"
    drop_table([node1, node2], table)

    try:
        create_table(node1, table, "merge_both", "1")
        create_table(node2, table, "merge_both", "2")

        # Insert multiple parts
        for i in range(5):
            node1.query(f"INSERT INTO {table} SELECT number, {i} FROM numbers(10)")

        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        # Merge on node1
        node1.query(f"OPTIMIZE TABLE {table} FINAL")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        data1 = get_data(node1, table)
        data2 = get_data(node2, table)

        assert data1 == data2, f"Data mismatch:\nnode1: {data1}\nnode2: {data2}"
        assert len(data1) == 10

        # All values should be 4 (last insert)
        for id_val, value in data1:
            assert value == 4, f"Key {id_val} has value {value}, expected 4"

    finally:
        drop_table([node1, node2], table)


def test_out_of_order_fetch_with_version(started_cluster):
    """
    Test: Out-of-order fetch with version column.
    Version-based dedup must produce correct results even when
    the fetch order differs from the insert order.
    """
    table = "test_ooo_version"
    drop_table([node1, node2], table)

    try:
        for node, replica_id in [(node1, "1"), (node2, "2")]:
            node.query(
                f"""
                CREATE TABLE {table}
                (
                    id UInt32,
                    value UInt32,
                    version UInt64,
                    PROJECTION __unique_index INDEX id TYPE unique('version')
                )
                ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/test_rumt/ooo_version', '{replica_id}')
                ORDER BY id
            """
            )

        # Stop fetches on node2
        node2.query(f"SYSTEM STOP FETCHES {table}")

        # Insert with version=1
        node1.query(
            f"INSERT INTO {table} SELECT number, number, 1 FROM numbers(10)"
        )
        # Insert with version=5 (should win)
        node1.query(
            f"INSERT INTO {table} SELECT number, number + 100, 5 FROM numbers(5)"
        )
        # Insert with version=2 (should lose to version=5)
        node1.query(
            f"INSERT INTO {table} SELECT number, number + 999, 2 FROM numbers(5)"
        )

        # Merge on node1
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        # Resume fetches on node2
        node2.query(f"SYSTEM START FETCHES {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")

        data1 = get_data(node1, table)
        data2 = get_data(node2, table)

        assert data1 == data2, f"Data mismatch:\nnode1: {data1}\nnode2: {data2}"

        # Keys 0-4 should have value = number + 100 (version 5 wins)
        for i in range(5):
            assert data1[i] == (
                i,
                i + 100,
            ), f"Key {i}: expected ({i}, {i+100}), got {data1[i]}"

    finally:
        drop_table([node1, node2], table)
