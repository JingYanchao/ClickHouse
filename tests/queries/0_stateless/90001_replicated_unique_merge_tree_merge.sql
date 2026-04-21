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
