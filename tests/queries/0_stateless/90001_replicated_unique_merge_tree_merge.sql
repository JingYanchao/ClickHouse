-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree merge and mutation dedup
-- Verifies that OPTIMIZE FINAL (merge) and UPDATE (mutation) work correctly
-- with dedup on a replicated table.

SELECT '--- merge dedup ---';

DROP TABLE IF EXISTS replicated_unique_merge_dedup;

CREATE TABLE replicated_unique_merge_dedup
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_merge_dedup', '1')
ORDER BY id;

-- Insert 10 rows, then upsert 10 rows with same keys
INSERT INTO replicated_unique_merge_dedup SELECT number, number, number FROM numbers(10);
INSERT INTO replicated_unique_merge_dedup SELECT number, number, number FROM numbers(10);

-- Before merge: multiple parts with cross-part delete marks
SELECT count() FROM replicated_unique_merge_dedup;
SELECT * FROM replicated_unique_merge_dedup ORDER BY id;

-- Merge all parts
OPTIMIZE TABLE replicated_unique_merge_dedup FINAL;

-- After merge: single part, dedup state preserved
SELECT count() FROM replicated_unique_merge_dedup;
SELECT * FROM replicated_unique_merge_dedup ORDER BY id;

DROP TABLE replicated_unique_merge_dedup;

SELECT '--- mutation dedup ---';

DROP TABLE IF EXISTS replicated_unique_mutation_dedup;

CREATE TABLE replicated_unique_mutation_dedup
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_mutation_dedup', '1')
ORDER BY id;

-- Insert and upsert to create cross-part delete marks
INSERT INTO replicated_unique_mutation_dedup SELECT number, number, number FROM numbers(10);
INSERT INTO replicated_unique_mutation_dedup SELECT number, number, number FROM numbers(10);

-- UPDATE (mutation) should preserve dedup state
UPDATE replicated_unique_mutation_dedup SET value1 = 100 WHERE id = 1;
SELECT * FROM replicated_unique_mutation_dedup ORDER BY id;

-- UPDATE + OPTIMIZE
UPDATE replicated_unique_mutation_dedup SET value1 = 200, value2 = 200 WHERE id = 2;
OPTIMIZE TABLE replicated_unique_mutation_dedup FINAL;
SELECT * FROM replicated_unique_mutation_dedup ORDER BY id;

DROP TABLE replicated_unique_mutation_dedup;

SELECT '--- two replica merge ---';

DROP TABLE IF EXISTS replicated_unique_two_replica_merge_r1;
DROP TABLE IF EXISTS replicated_unique_two_replica_merge_r2;

CREATE TABLE replicated_unique_two_replica_merge_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_two_replica_merge', '1')
ORDER BY id;

CREATE TABLE replicated_unique_two_replica_merge_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_two_replica_merge', '2')
ORDER BY id;

-- Insert and upsert on replica 1
INSERT INTO replicated_unique_two_replica_merge_r1 SELECT number, number FROM numbers(10);
INSERT INTO replicated_unique_two_replica_merge_r1 SELECT number, number + 100 FROM numbers(5);
SYSTEM SYNC REPLICA replicated_unique_two_replica_merge_r2;

-- Merge on replica 1 (only source_replica executes merge, replica 2 fetches)
OPTIMIZE TABLE replicated_unique_two_replica_merge_r1 FINAL;
SYSTEM SYNC REPLICA replicated_unique_two_replica_merge_r2;

-- Both replicas should have identical results
SELECT '--- replica 1 after merge ---';
SELECT * FROM replicated_unique_two_replica_merge_r1 ORDER BY id;
SELECT '--- replica 2 after merge ---';
SELECT * FROM replicated_unique_two_replica_merge_r2 ORDER BY id;

DROP TABLE replicated_unique_two_replica_merge_r1;
DROP TABLE replicated_unique_two_replica_merge_r2;

-- ===================================================================
-- Projection Direct Merge: delete bitmap filtering + offset translation
-- on replicated table. Verifies projection entry count and offset mapping.
-- ===================================================================
SELECT '--- projection direct merge: delete bitmap ---';

DROP TABLE IF EXISTS repl_proj_dm_r1;
DROP TABLE IF EXISTS repl_proj_dm_r2;

CREATE TABLE repl_proj_dm_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/repl_proj_dm', '1')
ORDER BY id;

CREATE TABLE repl_proj_dm_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/repl_proj_dm', '2')
ORDER BY id;

INSERT INTO repl_proj_dm_r1 SELECT number, number FROM numbers(10);
INSERT INTO repl_proj_dm_r1 SELECT number + 5, number + 100 FROM numbers(10);
INSERT INTO repl_proj_dm_r1 SELECT number, number + 200 FROM numbers(3);

OPTIMIZE TABLE repl_proj_dm_r1 FINAL;
SYSTEM SYNC REPLICA repl_proj_dm_r2;

-- Projection count == data count on both replicas
SELECT '--- r1 counts ---';
SELECT count() FROM repl_proj_dm_r1;
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_r1', '__unique_index');

SELECT '--- r2 counts ---';
SELECT count() FROM repl_proj_dm_r2;
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_r2', '__unique_index');

-- Verify offset mapping on r1: all offsets are distinct and within valid range
SELECT '--- r1 offset check ---';
SELECT count() = count(DISTINCT tupleElement(_unique_kv, 2)) AS all_offsets_distinct
FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_r1', '__unique_index');

DROP TABLE repl_proj_dm_r1;
DROP TABLE repl_proj_dm_r2;

-- ===================================================================
-- Projection Direct Merge: unique key != ORDER BY (different ordering)
-- ===================================================================
SELECT '--- projection direct merge: different order ---';

DROP TABLE IF EXISTS repl_proj_dm_difforder_r1;
DROP TABLE IF EXISTS repl_proj_dm_difforder_r2;

CREATE TABLE repl_proj_dm_difforder_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/repl_proj_dm_difforder', '1')
ORDER BY (value, id);

CREATE TABLE repl_proj_dm_difforder_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/repl_proj_dm_difforder', '2')
ORDER BY (value, id);

-- Descending values so parent order differs from id order
INSERT INTO repl_proj_dm_difforder_r1 SELECT number, 10 - number FROM numbers(10);
-- Update ids 3..7
INSERT INTO repl_proj_dm_difforder_r1 SELECT number + 3, number + 1000 FROM numbers(5);

OPTIMIZE TABLE repl_proj_dm_difforder_r1 FINAL;
SYSTEM SYNC REPLICA repl_proj_dm_difforder_r2;

SELECT '--- r1 ---';
SELECT count() FROM repl_proj_dm_difforder_r1;
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_difforder_r1', '__unique_index');

SELECT count() = count(DISTINCT tupleElement(_unique_kv, 2)) AS all_offsets_distinct
FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_difforder_r1', '__unique_index');

SELECT '--- r2 ---';
SELECT count() FROM repl_proj_dm_difforder_r2;
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_difforder_r2', '__unique_index');

DROP TABLE repl_proj_dm_difforder_r1;
DROP TABLE repl_proj_dm_difforder_r2;

-- ===================================================================
-- Projection Direct Merge: versioned unique index with delete bitmaps
-- ===================================================================
SELECT '--- projection direct merge: versioned ---';

DROP TABLE IF EXISTS repl_proj_dm_ver_r1;
DROP TABLE IF EXISTS repl_proj_dm_ver_r2;

CREATE TABLE repl_proj_dm_ver_r1
(
    id UInt32,
    value UInt32,
    ver UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('ver')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/repl_proj_dm_ver', '1')
ORDER BY id;

CREATE TABLE repl_proj_dm_ver_r2
(
    id UInt32,
    value UInt32,
    ver UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('ver')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/repl_proj_dm_ver', '2')
ORDER BY id;

INSERT INTO repl_proj_dm_ver_r1 SELECT number, number, 1 FROM numbers(10);
INSERT INTO repl_proj_dm_ver_r1 SELECT number, number + 100, 5 FROM numbers(5);
INSERT INTO repl_proj_dm_ver_r1 SELECT number + 5, number + 200, 3 FROM numbers(5);

OPTIMIZE TABLE repl_proj_dm_ver_r1 FINAL;
SYSTEM SYNC REPLICA repl_proj_dm_ver_r2;

SELECT '--- r1 ---';
SELECT count() FROM repl_proj_dm_ver_r1;
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_ver_r1', '__unique_index');

-- Verify projection versions: ids 0..4 ver=5, ids 5..9 ver=3
SELECT
    countIf(tupleElement(tupleElement(_unique_kv, 2), 1) = 5) AS ver5,
    countIf(tupleElement(tupleElement(_unique_kv, 2), 1) = 3) AS ver3
FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_ver_r1', '__unique_index');

-- Verify versioned offset mapping: all offsets are distinct
SELECT count() = count(DISTINCT tupleElement(tupleElement(_unique_kv, 2), 2)) AS all_offsets_distinct
FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_ver_r1', '__unique_index');

SELECT '--- r2 ---';
SELECT count() FROM repl_proj_dm_ver_r2;
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'repl_proj_dm_ver_r2', '__unique_index');

DROP TABLE repl_proj_dm_ver_r1;
DROP TABLE repl_proj_dm_ver_r2;
