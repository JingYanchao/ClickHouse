-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree fetch dedup and merge_tree fetch
--
-- Covers two-replica fetch dedup, cross-replica insert, and merge_tree fetch.

-- ===================================================================
-- Two-replica insert and fetch
-- ===================================================================

SELECT '--- two replica insert and fetch ---';

DROP TABLE IF EXISTS replicated_unique_fetch_r1;
DROP TABLE IF EXISTS replicated_unique_fetch_r2;

CREATE TABLE replicated_unique_fetch_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_fetch', '1')
ORDER BY id;

CREATE TABLE replicated_unique_fetch_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_fetch', '2')
ORDER BY id;

-- Insert into replica 1: 10 rows
INSERT INTO replicated_unique_fetch_r1 SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_fetch_r2;

-- Both replicas should see the same 10 rows
SELECT count() FROM replicated_unique_fetch_r1;
SELECT count() FROM replicated_unique_fetch_r2;

-- Upsert on replica 1: update keys 0..4
INSERT INTO replicated_unique_fetch_r1 SELECT number, number + 100 FROM numbers(5);
SYSTEM SYNC REPLICA replicated_unique_fetch_r2;

-- Replica 1: verify dedup
SELECT '--- replica 1 after upsert ---';
SELECT * FROM replicated_unique_fetch_r1 ORDER BY id;

-- Replica 2: should have identical results after fetch + dedup
SELECT '--- replica 2 after fetch ---';
SELECT * FROM replicated_unique_fetch_r2 ORDER BY id;

-- Verify counts match
SELECT count() FROM replicated_unique_fetch_r1;
SELECT count() FROM replicated_unique_fetch_r2;

DROP TABLE replicated_unique_fetch_r1;
DROP TABLE replicated_unique_fetch_r2;

-- ===================================================================
-- Two-replica insert on different replicas
-- ===================================================================

SELECT '--- two replica insert on different replicas ---';

DROP TABLE IF EXISTS replicated_unique_cross_insert_r1;
DROP TABLE IF EXISTS replicated_unique_cross_insert_r2;

CREATE TABLE replicated_unique_cross_insert_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_cross_insert', '1')
ORDER BY id;

CREATE TABLE replicated_unique_cross_insert_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_cross_insert', '2')
ORDER BY id;

-- Insert different data on each replica with overlapping keys
INSERT INTO replicated_unique_cross_insert_r1 SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_cross_insert_r2;

-- Now insert overlapping keys on replica 2
INSERT INTO replicated_unique_cross_insert_r2 SELECT number, number + 200 FROM numbers(5);
SYSTEM SYNC REPLICA replicated_unique_cross_insert_r1;

-- Both replicas should converge to the same result
SELECT '--- replica 1 ---';
SELECT * FROM replicated_unique_cross_insert_r1 ORDER BY id;
SELECT '--- replica 2 ---';
SELECT * FROM replicated_unique_cross_insert_r2 ORDER BY id;

DROP TABLE replicated_unique_cross_insert_r1;
DROP TABLE replicated_unique_cross_insert_r2;

-- ===================================================================
-- merge_tree fetch: UPDATE on replica 1, fetch on replica 2
-- ===================================================================
SELECT '--- merge_tree fetch: basic ---';

DROP TABLE IF EXISTS r1_merge_tree_fetch;
DROP TABLE IF EXISTS r2_merge_tree_fetch;

CREATE TABLE r1_merge_tree_fetch
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_merge_tree_fetch', '1')
ORDER BY id;

CREATE TABLE r2_merge_tree_fetch
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_merge_tree_fetch', '2')
ORDER BY id;

-- Insert and upsert on replica 1
INSERT INTO r1_merge_tree_fetch SELECT number, number FROM numbers(10);
INSERT INTO r1_merge_tree_fetch SELECT number, number + 100 FROM numbers(5);
SYSTEM SYNC REPLICA r2_merge_tree_fetch;

-- UPDATE on replica 1
UPDATE r1_merge_tree_fetch SET value = 999 WHERE id = 0;
SYSTEM SYNC REPLICA r2_merge_tree_fetch;

-- Both replicas should have the same result
SELECT '--- r1 after merge_tree ---';
SELECT * FROM r1_merge_tree_fetch ORDER BY id;
SELECT '--- r2 after merge_tree ---';
SELECT * FROM r2_merge_tree_fetch ORDER BY id;

DROP TABLE r1_merge_tree_fetch;
DROP TABLE r2_merge_tree_fetch;

-- ===================================================================
-- merge_tree fetch: UPDATE + INSERT + merge on replica 1, fetch on replica 2
-- ===================================================================
SELECT '--- merge_tree fetch: update then insert ---';

DROP TABLE IF EXISTS r1_merge_tree_insert_fetch;
DROP TABLE IF EXISTS r2_merge_tree_insert_fetch;

CREATE TABLE r1_merge_tree_insert_fetch
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_merge_tree_insert_fetch', '1')
ORDER BY id;

CREATE TABLE r2_merge_tree_insert_fetch
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_merge_tree_insert_fetch', '2')
ORDER BY id;

INSERT INTO r1_merge_tree_insert_fetch SELECT number, number FROM numbers(10);
UPDATE r1_merge_tree_insert_fetch SET value = 500 WHERE id < 3;

-- Insert overlapping keys after merge_tree
INSERT INTO r1_merge_tree_insert_fetch SELECT number, number + 1000 FROM numbers(5);

OPTIMIZE TABLE r1_merge_tree_insert_fetch FINAL;
SYSTEM SYNC REPLICA r2_merge_tree_insert_fetch;

SELECT '--- r1 final ---';
SELECT * FROM r1_merge_tree_insert_fetch ORDER BY id;
SELECT '--- r2 final ---';
SELECT * FROM r2_merge_tree_insert_fetch ORDER BY id;

DROP TABLE r1_merge_tree_insert_fetch;
DROP TABLE r2_merge_tree_insert_fetch;

-- ===================================================================
-- merge_tree fetch: UPDATE on replica 2, verify on replica 1
-- ===================================================================
SELECT '--- merge_tree fetch: update on r2 ---';

DROP TABLE IF EXISTS r1_merge_tree_on_r2;
DROP TABLE IF EXISTS r2_merge_tree_on_r2;

CREATE TABLE r1_merge_tree_on_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_merge_tree_on_r2', '1')
ORDER BY id;

CREATE TABLE r2_merge_tree_on_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_merge_tree_on_r2', '2')
ORDER BY id;

INSERT INTO r1_merge_tree_on_r2 SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA r2_merge_tree_on_r2;

-- UPDATE on replica 2
UPDATE r2_merge_tree_on_r2 SET value = 777 WHERE id >= 7;
SYSTEM SYNC REPLICA r1_merge_tree_on_r2;

SELECT '--- r1 after r2 merge_tree ---';
SELECT * FROM r1_merge_tree_on_r2 ORDER BY id;
SELECT '--- r2 after r2 merge_tree ---';
SELECT * FROM r2_merge_tree_on_r2 ORDER BY id;

DROP TABLE r1_merge_tree_on_r2;
DROP TABLE r2_merge_tree_on_r2;
