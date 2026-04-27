-- Test: verify that during merge, the versioned SortedStringKV value column is
-- aggregated using max(version, part_offset) rule.
-- When the same unique key exists in multiple parts with different versions,
-- after merge the entry with the highest version should win.

DROP TABLE IF EXISTS test_unique_proj_merge_ver;

CREATE TABLE test_unique_proj_merge_ver
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192,
    merge_max_block_size = 8192;

-- Part 1: insert ids 0..99 with version=1
INSERT INTO test_unique_proj_merge_ver SELECT number, 100, 1 FROM numbers(100);
-- Part 2: insert the SAME ids 0..99 but with version=5 (higher version should win)
INSERT INTO test_unique_proj_merge_ver SELECT number, 200, 5 FROM numbers(100);
-- Part 3: insert the SAME ids 0..99 but with version=3 (lower than part2, should lose)
INSERT INTO test_unique_proj_merge_ver SELECT number, 300, 3 FROM numbers(100);

-- Before merge: 3 parts, 300 total entries in the projection (100 per part).
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver', '__unique_index');

OPTIMIZE TABLE test_unique_proj_merge_ver FINAL;

-- After merge: the max(version, part_offset) rule should keep only the entry with
-- version=5 for each key. So we expect exactly 100 entries (one per unique id).
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver', '__unique_index');

-- Verify that the surviving version is 5 for all entries.
-- tupleElement(_unique_kv, 2) is Tuple(version, part_offset).
-- tupleElement(tupleElement(_unique_kv, 2), 1) extracts the version.
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) = 5) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver', '__unique_index');
-- None should have version != 5
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) != 5) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver', '__unique_index');

DROP TABLE test_unique_proj_merge_ver;

-- Same test with different version ordering: ensure part insertion order doesn't matter.
DROP TABLE IF EXISTS test_unique_proj_merge_ver2;

CREATE TABLE test_unique_proj_merge_ver2
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192,
    merge_max_block_size = 8192;

-- Insert with highest version first, then lower versions
-- Part 1: version=10 (highest)
INSERT INTO test_unique_proj_merge_ver2 SELECT number, 100, 10 FROM numbers(50);
-- Part 2: version=1 (lowest)
INSERT INTO test_unique_proj_merge_ver2 SELECT number, 200, 1 FROM numbers(50);
-- Part 3: version=5 (middle)
INSERT INTO test_unique_proj_merge_ver2 SELECT number, 300, 5 FROM numbers(50);

SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver2', '__unique_index');

OPTIMIZE TABLE test_unique_proj_merge_ver2 FINAL;

-- After merge: should keep entries with version=10
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver2', '__unique_index');
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) = 10) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver2', '__unique_index');
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) != 10) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver2', '__unique_index');

DROP TABLE test_unique_proj_merge_ver2;

-- Test with vertical merge algorithm
DROP TABLE IF EXISTS test_unique_proj_merge_ver_vertical;

CREATE TABLE test_unique_proj_merge_ver_vertical
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192,
    merge_max_block_size = 8192,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 0,
    min_bytes_for_wide_part = 0;

-- Part 1: version=1
INSERT INTO test_unique_proj_merge_ver_vertical SELECT number, 100, 1 FROM numbers(100);
-- Part 2: version=5 (should win)
INSERT INTO test_unique_proj_merge_ver_vertical SELECT number, 200, 5 FROM numbers(100);
-- Part 3: version=3 (should lose)
INSERT INTO test_unique_proj_merge_ver_vertical SELECT number, 300, 3 FROM numbers(100);

SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver_vertical', '__unique_index');

OPTIMIZE TABLE test_unique_proj_merge_ver_vertical FINAL;

-- After merge: only version=5 entries should remain
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver_vertical', '__unique_index');
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) = 5) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver_vertical', '__unique_index');
SELECT countIf(tupleElement(tupleElement(_unique_kv, 2), 1) != 5) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver_vertical', '__unique_index');

DROP TABLE test_unique_proj_merge_ver_vertical;

-- ===================================================================
-- Test 4: Offset translation with versioned mode (non-overlapping keys).
-- This exercises the MergeTreeSequentialSource offset remapping logic
-- for VersionedSortedStringKV, where value is Tuple(version, part_offset)
-- and only the part_offset sub-column needs translation.
-- ===================================================================
DROP TABLE IF EXISTS test_unique_proj_merge_ver_offset;

CREATE TABLE test_unique_proj_merge_ver_offset
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192,
    merge_max_block_size = 8192;

-- Insert 3 parts with NON-OVERLAPPING contiguous id ranges so merge does NOT
-- reduce rows (takes the merge path, not rebuild path).
-- Use different row counts per part (100, 200, 300) so that untranslated
-- offsets would collide and fail the DISTINCT count check.
INSERT INTO test_unique_proj_merge_ver_offset SELECT number, rand(), 1 FROM numbers(100);
INSERT INTO test_unique_proj_merge_ver_offset SELECT number + 100, rand(), 2 FROM numbers(200);
INSERT INTO test_unique_proj_merge_ver_offset SELECT number + 300, rand(), 3 FROM numbers(300);

-- Before merge: 600 entries total.
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver_offset', '__unique_index');

-- Merge all parts: offset translation must happen for (version, part_offset) tuples.
OPTIMIZE TABLE test_unique_proj_merge_ver_offset FINAL;

-- After merge: all 600 entries preserved (no key overlap), all offsets are distinct.
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver_offset', '__unique_index');
-- If offsets were NOT translated, multiple entries from different source parts would
-- carry the same old per-part offsets, yielding <600 distinct part_offset values.
-- tupleElement(tupleElement(_unique_kv, 2), 2) extracts the part_offset from Tuple(version, part_offset).
SELECT count(DISTINCT tupleElement(tupleElement(_unique_kv, 2), 2)) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_ver_offset', '__unique_index');

DROP TABLE test_unique_proj_merge_ver_offset;
