-- Test: UniqueMergeTree dedup correctness after table restart (DETACH/ATTACH)
-- and lazy materialization with deleted rows.
--
-- Scenario 1: buildAllDeleteMarksOnStartup
-- After DETACH + ATTACH, in-memory delete marks are lost and must be rebuilt
-- by buildAllDeleteMarksOnStartup. Verify that dedup results are identical
-- before and after restart.
--
-- Scenario 2: Lazy materialization with apply_deleted_mask
-- When a query uses LIMIT, the optimizer may split reading into a main stream
-- (which filters deleted rows via _row_exists) and a lazy stream (which reads
-- by global row indices). The lazy stream must NOT apply _row_exists filter
-- again, otherwise row count mismatch occurs.

-- ===================================================================
-- Scenario 1: Restart dedup (buildAllDeleteMarksOnStartup)
-- ===================================================================
select '--- restart dedup: basic ---';

DROP TABLE IF EXISTS test_restart_dedup;

CREATE TABLE test_restart_dedup
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert 10 rows, then upsert 5 of them with new values.
-- This creates cross-part delete marks for the first 5 rows.
INSERT INTO test_restart_dedup SELECT number, number FROM numbers(10);
INSERT INTO test_restart_dedup SELECT number, number + 100 FROM numbers(5);

-- Verify before restart.
SELECT count() FROM test_restart_dedup;
SELECT sum(value) FROM test_restart_dedup;

-- DETACH + ATTACH to trigger buildAllDeleteMarksOnStartup.
DETACH TABLE test_restart_dedup;
ATTACH TABLE test_restart_dedup;

-- Verify after restart: results must be identical.
SELECT count() FROM test_restart_dedup;
SELECT sum(value) FROM test_restart_dedup;

DROP TABLE test_restart_dedup;

-- ===================================================================
-- Scenario 1b: Restart dedup with intra-part duplicates
-- ===================================================================
select '--- restart dedup: intra-part ---';

DROP TABLE IF EXISTS test_restart_dedup_intra;

CREATE TABLE test_restart_dedup_intra
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert rows with duplicate keys in a single batch.
-- The SST builder keeps the last-write-wins entry; buildIntraPartDeleteMark
-- detects the losers.
INSERT INTO test_restart_dedup_intra VALUES (1, 10), (2, 20), (1, 100), (3, 30), (2, 200);

SELECT count() FROM test_restart_dedup_intra;
SELECT * FROM test_restart_dedup_intra ORDER BY id;

-- Restart to rebuild delete marks from scratch.
DETACH TABLE test_restart_dedup_intra;
ATTACH TABLE test_restart_dedup_intra;

SELECT count() FROM test_restart_dedup_intra;
SELECT * FROM test_restart_dedup_intra ORDER BY id;

DROP TABLE test_restart_dedup_intra;

-- ===================================================================
-- Scenario 1c: Restart dedup with multiple partitions
-- ===================================================================
select '--- restart dedup: multi-partition ---';

DROP TABLE IF EXISTS test_restart_dedup_part;

CREATE TABLE test_restart_dedup_part
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

-- Insert into two partitions, then upsert overlapping keys.
INSERT INTO test_restart_dedup_part VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO test_restart_dedup_part VALUES ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
-- Upsert within each partition.
INSERT INTO test_restart_dedup_part VALUES ('2024-01-01', 1, 100);
INSERT INTO test_restart_dedup_part VALUES ('2024-01-02', 2, 200);

SELECT * FROM test_restart_dedup_part ORDER BY dt, id;

DETACH TABLE test_restart_dedup_part;
ATTACH TABLE test_restart_dedup_part;

SELECT * FROM test_restart_dedup_part ORDER BY dt, id;

DROP TABLE test_restart_dedup_part;

-- ===================================================================
-- Scenario 2: Lazy materialization with deleted rows
-- ===================================================================
select '--- lazy materialization with dedup ---';

DROP TABLE IF EXISTS test_lazy_dedup;

CREATE TABLE test_lazy_dedup
(
    id UInt32,
    value1 String,
    value2 String,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert 100 rows, then upsert 50 of them.
-- After dedup, rows 0..49 have updated values, rows 50..99 are unchanged.
INSERT INTO test_lazy_dedup SELECT number, toString(number), toString(number) FROM numbers(100);
INSERT INTO test_lazy_dedup SELECT number, 'updated_' || toString(number), 'updated_' || toString(number) FROM numbers(50);

-- Query with LIMIT to trigger lazy materialization optimization.
-- The main stream reads only the columns needed for ORDER BY (id),
-- then the lazy stream reads value1, value2 for the top-N rows.
-- If apply_deleted_mask is incorrectly applied in the lazy stream,
-- it would cause row count mismatch.
SELECT id, value1 FROM test_lazy_dedup ORDER BY id LIMIT 10;

-- Also test with a larger limit that spans both updated and original rows.
SELECT id, value1 FROM test_lazy_dedup ORDER BY id LIMIT 10 OFFSET 45;

-- Verify total count is correct.
SELECT count() FROM test_lazy_dedup;

DROP TABLE test_lazy_dedup;

-- ===================================================================
-- Scenario 3: Restart dedup with version column
-- ===================================================================
select '--- restart dedup: version ---';

DROP TABLE IF EXISTS unique_version_restart;

CREATE TABLE unique_version_restart
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO unique_version_restart SELECT number, 500, 5 FROM numbers(10);
INSERT INTO unique_version_restart SELECT number, 100, 1 FROM numbers(10);

-- Before restart
SELECT id, value FROM unique_version_restart ORDER BY id;

DETACH TABLE unique_version_restart;
ATTACH TABLE unique_version_restart;

-- After restart: version 5 should still win
SELECT id, value FROM unique_version_restart ORDER BY id;

-- Insert with higher version after restart
INSERT INTO unique_version_restart SELECT number, 999, 10 FROM numbers(3);
SELECT id, value FROM unique_version_restart ORDER BY id;

DROP TABLE unique_version_restart;
