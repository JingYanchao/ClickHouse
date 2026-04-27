-- Tags: no-random-merge-tree-settings
-- Test unique projection merge with overlapping keys across parts.
-- When the same key exists in multiple parts, merge should keep only one
-- entry per unique key (using max(part_offset) or max(version, part_offset)).

-- ==================================================================
-- Test 1: Non-versioned mode — overlapping keys across 3 parts.
-- Before merge: 300 entries (100 per part, all same keys 0..99).
-- After merge: only 100 entries (one per unique key).
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_overlap;

CREATE TABLE test_unique_proj_overlap
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- 3 parts with the SAME keys (0..99)
INSERT INTO test_unique_proj_overlap SELECT number, 100 FROM numbers(100);
INSERT INTO test_unique_proj_overlap SELECT number, 200 FROM numbers(100);
INSERT INTO test_unique_proj_overlap SELECT number, 300 FROM numbers(100);

-- Before merge: 300 total entries
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_overlap', '__unique_index');

OPTIMIZE TABLE test_unique_proj_overlap FINAL;

-- After merge: only 100 entries (deduped by key)
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_overlap', '__unique_index');

DROP TABLE test_unique_proj_overlap;

-- ==================================================================
-- Test 2: Partially overlapping keys across parts.
-- Part 1: keys 0..49, Part 2: keys 25..74, Part 3: keys 50..99.
-- Unique keys: 0..99 = 100 keys. Total input: 150 entries.
-- After merge: 100 entries.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_partial_overlap;

CREATE TABLE test_unique_proj_partial_overlap
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO test_unique_proj_partial_overlap SELECT number, 1 FROM numbers(50);
INSERT INTO test_unique_proj_partial_overlap SELECT number + 25, 2 FROM numbers(50);
INSERT INTO test_unique_proj_partial_overlap SELECT number + 50, 3 FROM numbers(50);

-- Before merge
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_partial_overlap', '__unique_index');

OPTIMIZE TABLE test_unique_proj_partial_overlap FINAL;

-- After merge: 100 unique keys
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_partial_overlap', '__unique_index');

DROP TABLE test_unique_proj_partial_overlap;

-- ==================================================================
-- Test 3: Versioned mode — overlapping keys, highest version wins.
-- Part 1: keys 0..49 with version=1
-- Part 2: keys 0..49 with version=3
-- Part 3: keys 0..49 with version=2
-- After merge: 50 entries, all with version=3.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_overlap_ver;

CREATE TABLE test_unique_proj_overlap_ver
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO test_unique_proj_overlap_ver SELECT number, 100, 1 FROM numbers(50);
INSERT INTO test_unique_proj_overlap_ver SELECT number, 200, 3 FROM numbers(50);
INSERT INTO test_unique_proj_overlap_ver SELECT number, 300, 2 FROM numbers(50);

-- Before merge: 150 entries
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_overlap_ver', '__unique_index');

OPTIMIZE TABLE test_unique_proj_overlap_ver FINAL;

-- After merge: 50 entries (deduped by key, max version wins)
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_overlap_ver', '__unique_index');

-- All surviving entries should have version=3
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) = 3) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_overlap_ver', '__unique_index');
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) != 3) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_overlap_ver', '__unique_index');

DROP TABLE test_unique_proj_overlap_ver;

-- ==================================================================
-- Test 4: Version tiebreaker — same version, different part_offset.
-- When two entries for the same key have the same version, the one
-- with the higher part_offset should win.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_ver_tie;

CREATE TABLE test_unique_proj_ver_tie
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- Two parts with same keys and same version
INSERT INTO test_unique_proj_ver_tie SELECT number, 100, 5 FROM numbers(30);
INSERT INTO test_unique_proj_ver_tie SELECT number, 200, 5 FROM numbers(30);

-- Before merge: 60 entries
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_ver_tie', '__unique_index');

OPTIMIZE TABLE test_unique_proj_ver_tie FINAL;

-- After merge: 30 entries (deduped, same version → higher offset wins)
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_ver_tie', '__unique_index');

-- All should have version=5
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) = 5) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_ver_tie', '__unique_index');

DROP TABLE test_unique_proj_ver_tie;
