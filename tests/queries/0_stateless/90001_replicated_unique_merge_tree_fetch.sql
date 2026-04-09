-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree two-replica fetch dedup
-- Verifies that when replica 2 fetches parts from replica 1,
-- the dedup state (delete marks) is correctly rebuilt on replica 2.

SELECT '--- two replica insert and fetch ---';

DROP TABLE IF EXISTS replicated_umt_fetch_r1;
DROP TABLE IF EXISTS replicated_umt_fetch_r2;

CREATE TABLE replicated_umt_fetch_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_umt_fetch', '1')
ORDER BY id;

CREATE TABLE replicated_umt_fetch_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_umt_fetch', '2')
ORDER BY id;

-- Insert into replica 1: 10 rows
INSERT INTO replicated_umt_fetch_r1 SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA replicated_umt_fetch_r2;

-- Both replicas should see the same 10 rows
SELECT count() FROM replicated_umt_fetch_r1;
SELECT count() FROM replicated_umt_fetch_r2;

-- Upsert on replica 1: update keys 0..4
INSERT INTO replicated_umt_fetch_r1 SELECT number, number + 100 FROM numbers(5);
SYSTEM SYNC REPLICA replicated_umt_fetch_r2;

-- Replica 1: verify dedup
SELECT '--- replica 1 after upsert ---';
SELECT * FROM replicated_umt_fetch_r1 ORDER BY id;

-- Replica 2: should have identical results after fetch + dedup
SELECT '--- replica 2 after fetch ---';
SELECT * FROM replicated_umt_fetch_r2 ORDER BY id;

-- Verify counts match
SELECT count() FROM replicated_umt_fetch_r1;
SELECT count() FROM replicated_umt_fetch_r2;

DROP TABLE replicated_umt_fetch_r1;
DROP TABLE replicated_umt_fetch_r2;

SELECT '--- two replica insert on different replicas ---';

DROP TABLE IF EXISTS replicated_umt_cross_insert_r1;
DROP TABLE IF EXISTS replicated_umt_cross_insert_r2;

CREATE TABLE replicated_umt_cross_insert_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_umt_cross_insert', '1')
ORDER BY id;

CREATE TABLE replicated_umt_cross_insert_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_umt_cross_insert', '2')
ORDER BY id;

-- Insert different data on each replica with overlapping keys
INSERT INTO replicated_umt_cross_insert_r1 SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA replicated_umt_cross_insert_r2;

-- Now insert overlapping keys on replica 2
INSERT INTO replicated_umt_cross_insert_r2 SELECT number, number + 200 FROM numbers(5);
SYSTEM SYNC REPLICA replicated_umt_cross_insert_r1;

-- Both replicas should converge to the same result
SELECT '--- replica 1 ---';
SELECT * FROM replicated_umt_cross_insert_r1 ORDER BY id;
SELECT '--- replica 2 ---';
SELECT * FROM replicated_umt_cross_insert_r2 ORDER BY id;

DROP TABLE replicated_umt_cross_insert_r1;
DROP TABLE replicated_umt_cross_insert_r2;
