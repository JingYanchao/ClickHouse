-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree out-of-order fetch dedup
--
-- When a replica fetches a merged part but does not yet have all the
-- source INSERT parts locally, the block range coverage check
-- (areAllBlockNumbersCovered) fails. In this case, dedupForFetch must
-- fall back to dedupForInsert (full cross-part dedup) and discard the
-- delete mark received from the source replica.
--
-- This test simulates the scenario by:
-- 1. Inserting multiple parts on replica 1
-- 2. Merging on replica 1
-- 3. Stopping fetches on replica 2, then inserting new overlapping data
-- 4. Resuming fetches so replica 2 gets the merged part before all source parts
-- 5. Verifying both replicas converge to the same result

-- ===================================================================
-- Scenario 1: Out-of-order fetch of merged part
-- ===================================================================
SELECT '--- out-of-order fetch: basic ---';

DROP TABLE IF EXISTS r1_ooo_fetch;
DROP TABLE IF EXISTS r2_ooo_fetch;

CREATE TABLE r1_ooo_fetch
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_fetch', '1')
ORDER BY id;

CREATE TABLE r2_ooo_fetch
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_fetch', '2')
ORDER BY id;

-- Insert 3 batches on replica 1 with overlapping keys
INSERT INTO r1_ooo_fetch SELECT number, number FROM numbers(10);
INSERT INTO r1_ooo_fetch SELECT number, number + 100 FROM numbers(5);
INSERT INTO r1_ooo_fetch SELECT number + 5, number + 200 FROM numbers(5);

-- Sync replica 2 so it has all 3 INSERT parts
SYSTEM SYNC REPLICA r2_ooo_fetch;

-- Verify both replicas have the same data before merge
SELECT '--- r1 before merge ---';
SELECT * FROM r1_ooo_fetch ORDER BY id;
SELECT '--- r2 before merge ---';
SELECT * FROM r2_ooo_fetch ORDER BY id;

-- Merge on replica 1
OPTIMIZE TABLE r1_ooo_fetch FINAL;

-- Sync replica 2 (it will fetch the merged part)
SYSTEM SYNC REPLICA r2_ooo_fetch;

-- Both replicas should converge
SELECT '--- r1 after merge ---';
SELECT * FROM r1_ooo_fetch ORDER BY id;
SELECT '--- r2 after merge ---';
SELECT * FROM r2_ooo_fetch ORDER BY id;

DROP TABLE r1_ooo_fetch;
DROP TABLE r2_ooo_fetch;

-- ===================================================================
-- Scenario 2: Out-of-order fetch with concurrent INSERT on replica 2
-- Replica 2 has local INSERT parts that overlap with the fetched merged part.
-- The reverse dedup must correctly handle these local parts.
-- ===================================================================
SELECT '--- out-of-order fetch: concurrent insert ---';

DROP TABLE IF EXISTS r1_ooo_concurrent;
DROP TABLE IF EXISTS r2_ooo_concurrent;

CREATE TABLE r1_ooo_concurrent
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_concurrent', '1')
ORDER BY id;

CREATE TABLE r2_ooo_concurrent
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_concurrent', '2')
ORDER BY id;

-- Insert on replica 1
INSERT INTO r1_ooo_concurrent SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA r2_ooo_concurrent;

-- Insert overlapping keys on replica 2 (these will be local INSERT parts)
INSERT INTO r2_ooo_concurrent SELECT number, number + 500 FROM numbers(5);
SYSTEM SYNC REPLICA r1_ooo_concurrent;

-- Now insert more on replica 1 and merge
INSERT INTO r1_ooo_concurrent SELECT number, number + 1000 FROM numbers(5);
OPTIMIZE TABLE r1_ooo_concurrent FINAL;

-- Sync replica 2 — it fetches the merged part
SYSTEM SYNC REPLICA r2_ooo_concurrent;

-- Both replicas should converge: keys 0..4 should have value = number + 1000
-- (the latest insert), keys 5..9 should have value = number + 500 or number
-- depending on which was later
SELECT '--- r1 final ---';
SELECT * FROM r1_ooo_concurrent ORDER BY id;
SELECT '--- r2 final ---';
SELECT * FROM r2_ooo_concurrent ORDER BY id;

DROP TABLE r1_ooo_concurrent;
DROP TABLE r2_ooo_concurrent;

-- ===================================================================
-- Scenario 3: Out-of-order fetch with version column
-- Version-based dedup must work correctly even when fetch is out of order.
-- ===================================================================
SELECT '--- out-of-order fetch: version column ---';

DROP TABLE IF EXISTS r1_ooo_version;
DROP TABLE IF EXISTS r2_ooo_version;

CREATE TABLE r1_ooo_version
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_version', '1')
ORDER BY id;

CREATE TABLE r2_ooo_version
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_version', '2')
ORDER BY id;

-- Insert with version=1 on replica 1
INSERT INTO r1_ooo_version SELECT number, number, 1 FROM numbers(10);
SYSTEM SYNC REPLICA r2_ooo_version;

-- Upsert with higher version on replica 1
INSERT INTO r1_ooo_version SELECT number, number + 100, 5 FROM numbers(5);
-- Upsert with lower version (should NOT win)
INSERT INTO r1_ooo_version SELECT number, number + 999, 2 FROM numbers(5);

-- Merge on replica 1
OPTIMIZE TABLE r1_ooo_version FINAL;
SYSTEM SYNC REPLICA r2_ooo_version;

-- Both replicas: keys 0..4 should have value = number + 100 (version 5 wins)
SELECT '--- r1 version result ---';
SELECT id, value FROM r1_ooo_version ORDER BY id;
SELECT '--- r2 version result ---';
SELECT id, value FROM r2_ooo_version ORDER BY id;

DROP TABLE r1_ooo_version;
DROP TABLE r2_ooo_version;

-- ===================================================================
-- Scenario 4: Fetch of merged part after multiple merges
-- Tests that multi-level merge results are correctly deduped on fetch.
-- ===================================================================
SELECT '--- out-of-order fetch: multi-level merge ---';

DROP TABLE IF EXISTS r1_ooo_multi;
DROP TABLE IF EXISTS r2_ooo_multi;

CREATE TABLE r1_ooo_multi
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_multi', '1')
ORDER BY id;

CREATE TABLE r2_ooo_multi
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ooo_multi', '2')
ORDER BY id;

-- Insert 4 batches with overlapping keys
INSERT INTO r1_ooo_multi SELECT number, 1 FROM numbers(10);
INSERT INTO r1_ooo_multi SELECT number, 2 FROM numbers(10);
INSERT INTO r1_ooo_multi SELECT number, 3 FROM numbers(10);
INSERT INTO r1_ooo_multi SELECT number, 4 FROM numbers(10);

-- Merge all into one part
OPTIMIZE TABLE r1_ooo_multi FINAL;
SYSTEM SYNC REPLICA r2_ooo_multi;

-- Both replicas should see value=4 for all keys (last write wins)
SELECT '--- r1 multi-level ---';
SELECT * FROM r1_ooo_multi ORDER BY id;
SELECT '--- r2 multi-level ---';
SELECT * FROM r2_ooo_multi ORDER BY id;

-- Verify counts
SELECT count() FROM r1_ooo_multi;
SELECT count() FROM r2_ooo_multi;

DROP TABLE r1_ooo_multi;
DROP TABLE r2_ooo_multi;
