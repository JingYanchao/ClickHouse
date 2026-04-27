DROP TABLE IF EXISTS test_unique_proj_merge;

CREATE TABLE test_unique_proj_merge
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192,
    merge_max_block_size = 8192;

-- Insert 3 parts with non-overlapping contiguous id ranges so merge does NOT
-- reduce rows and takes the merge path (not rebuild path).
-- IMPORTANT: Use different row counts per part (100, 200, 300) so that the
-- per-part offset ranges differ. If all parts had the same size, untranslated
-- offsets could accidentally pass a naive count() check.
INSERT INTO test_unique_proj_merge SELECT number, rand() FROM numbers(100);
INSERT INTO test_unique_proj_merge SELECT number + 100, rand() FROM numbers(200);
INSERT INTO test_unique_proj_merge SELECT number + 300, rand() FROM numbers(300);

-- Before merge: verify total entries and that all offsets are distinct within each part.
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge', '__unique_index');
SELECT count(DISTINCT (tupleElement(_unique_kv, 2), _part)) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge', '__unique_index');

-- Merge all parts into one.
OPTIMIZE TABLE test_unique_proj_merge FINAL;

-- After merge: verify offset translation correctness.
-- If offsets were NOT translated, multiple projection entries from different source
-- parts would still carry their old per-part offsets (0..99, 0..199, 0..299),
-- resulting in only 300 distinct offset values instead of 600.
-- With correct translation, each entry has a unique offset in the merged part.
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge', '__unique_index');
SELECT count(DISTINCT tupleElement(_unique_kv, 2)) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge', '__unique_index');

DROP TABLE test_unique_proj_merge;

-- Same test but with vertical merge algorithm to cover that code path as well.
DROP TABLE IF EXISTS test_unique_proj_merge_vertical;

CREATE TABLE test_unique_proj_merge_vertical
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192,
    merge_max_block_size = 8192,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 0,
    min_bytes_for_wide_part = 0;

INSERT INTO test_unique_proj_merge_vertical SELECT number, rand() FROM numbers(100);
INSERT INTO test_unique_proj_merge_vertical SELECT number + 100, rand() FROM numbers(200);
INSERT INTO test_unique_proj_merge_vertical SELECT number + 300, rand() FROM numbers(300);

-- Before merge
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_vertical', '__unique_index');
SELECT count(DISTINCT (tupleElement(_unique_kv, 2), _part)) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_vertical', '__unique_index');

OPTIMIZE TABLE test_unique_proj_merge_vertical FINAL;

-- After merge
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_vertical', '__unique_index');
SELECT count(DISTINCT tupleElement(_unique_kv, 2)) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_merge_vertical', '__unique_index');

DROP TABLE test_unique_proj_merge_vertical;
