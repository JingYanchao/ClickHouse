-- Tags: no-random-merge-tree-settings
-- Test basic unique projection creation, in-block deduplication, and introspection.

-- ==================================================================
-- Test 1: Basic unique projection with non-versioned mode.
-- Verify in-block dedup: duplicate keys within a single INSERT should
-- be deduplicated, keeping only one entry per unique key.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_basic;

CREATE TABLE test_unique_proj_basic
(
    id UInt32,
    value String,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- Insert 100 rows with unique keys
INSERT INTO test_unique_proj_basic SELECT number, toString(number) FROM numbers(100);

-- Projection should have 100 entries (one per unique key)
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_basic', '__unique_index');

-- All keys should be distinct
SELECT count(DISTINCT tupleElement(_unique_kv, 1)) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_basic', '__unique_index');

-- All offsets should be distinct
SELECT count(DISTINCT tupleElement(_unique_kv, 2)) FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_basic', '__unique_index');

DROP TABLE test_unique_proj_basic;

-- ==================================================================
-- Test 2: In-block dedup — duplicate keys within a single INSERT.
-- With non-versioned mode, the entry with the higher part_offset wins.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_inblock_dedup;

CREATE TABLE test_unique_proj_inblock_dedup
(
    id UInt32,
    value String,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- Insert 200 rows where id = number % 50, so each key appears 4 times
INSERT INTO test_unique_proj_inblock_dedup SELECT number % 50, toString(number) FROM numbers(200);

-- Should have exactly 50 unique entries (one per distinct key)
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_inblock_dedup', '__unique_index');

DROP TABLE test_unique_proj_inblock_dedup;

-- ==================================================================
-- Test 3: Versioned unique projection with in-block dedup.
-- The entry with the highest version should win; ties broken by offset.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_ver_inblock;

CREATE TABLE test_unique_proj_ver_inblock
(
    id UInt32,
    value String,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- Insert rows with same key (id=1) but different versions
INSERT INTO test_unique_proj_ver_inblock VALUES (1, 'v1', 1), (1, 'v5', 5), (1, 'v3', 3), (2, 'v2', 2), (2, 'v4', 4);

-- Should have 2 entries (one for id=1, one for id=2)
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_ver_inblock', '__unique_index');

-- For id=1, version=5 should win; for id=2, version=4 should win
SELECT tupleElement(tupleElement(_unique_kv, 2), 1) AS version
FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_ver_inblock', '__unique_index')
ORDER BY version;

DROP TABLE test_unique_proj_ver_inblock;

-- ==================================================================
-- Test 4: Multi-column unique key.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_multikey;

CREATE TABLE test_unique_proj_multikey
(
    a UInt32,
    b UInt32,
    value String,
    PROJECTION __unique_index INDEX a, b TYPE unique
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 8192;

INSERT INTO test_unique_proj_multikey VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z');

-- 3 distinct (a, b) combinations → 3 entries
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_multikey', '__unique_index');

-- Insert duplicate (a=1, b=1) — should still be 3 entries after merge
INSERT INTO test_unique_proj_multikey VALUES (1, 1, 'x2');
OPTIMIZE TABLE test_unique_proj_multikey FINAL;
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_multikey', '__unique_index');

DROP TABLE test_unique_proj_multikey;

-- ==================================================================
-- Test 5: Unique projection with expression key.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_expr;

CREATE TABLE test_unique_proj_expr
(
    id UInt32,
    value String,
    PROJECTION __unique_index INDEX sipHash64(id) TYPE unique
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO test_unique_proj_expr SELECT number, toString(number) FROM numbers(50);

-- 50 distinct keys (sipHash64 of each unique id)
SELECT count() FROM mergeTreeProjection(currentDatabase(), 'test_unique_proj_expr', '__unique_index');

DROP TABLE test_unique_proj_expr;

-- ==================================================================
-- Test 6: Error handling — version column must be UInt64.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_bad_version;

CREATE TABLE test_unique_proj_bad_version
(
    id UInt32,
    value String,
    version UInt32,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- ==================================================================
-- Test 7: Error handling — version column must exist in table.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_missing_ver;

CREATE TABLE test_unique_proj_missing_ver
(
    id UInt32,
    value String,
    PROJECTION __unique_index INDEX id TYPE unique('nonexistent')
)
ENGINE = MergeTree
ORDER BY id; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- ==================================================================
-- Test 8: Error handling — cannot DROP a column referenced by unique
-- projection key (uk is NOT in ORDER BY, so this tests the projection
-- check specifically).
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_drop_col;

CREATE TABLE test_unique_proj_drop_col
(
    id UInt32,
    uk UInt32,
    value String,
    PROJECTION __unique_index INDEX uk TYPE unique
)
ENGINE = MergeTree
ORDER BY id;

ALTER TABLE test_unique_proj_drop_col DROP COLUMN uk; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

DROP TABLE test_unique_proj_drop_col;

-- ==================================================================
-- Test 9: Error handling — cannot DROP version column referenced by
-- unique projection.
-- ==================================================================
DROP TABLE IF EXISTS test_unique_proj_drop_ver;

CREATE TABLE test_unique_proj_drop_ver
(
    id UInt32,
    value String,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = MergeTree
ORDER BY id;

ALTER TABLE test_unique_proj_drop_ver DROP COLUMN version; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

DROP TABLE test_unique_proj_drop_ver;
