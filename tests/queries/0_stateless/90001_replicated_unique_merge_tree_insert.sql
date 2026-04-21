-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree basic INSERT dedup (with and without version)
-- Mirrors 90000_unique_merge_tree_dml INSERT tests but uses ReplicatedUniqueMergeTree.

-- ===================================================================
-- INSERT: basic dedup (single replica)
-- ===================================================================

DROP TABLE IF EXISTS replicated_unique_basic_insert;

CREATE TABLE replicated_unique_basic_insert
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_basic_insert', '1')
ORDER BY id;

-- Basic insert: 10 rows
INSERT INTO replicated_unique_basic_insert SELECT number AS id, number AS value1, number AS value2 FROM numbers(10);
SELECT * FROM replicated_unique_basic_insert ORDER BY id;

-- Re-insert same keys: should be deduped (same values)
INSERT INTO replicated_unique_basic_insert VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
SELECT * FROM replicated_unique_basic_insert ORDER BY id;

-- Upsert: key 1 gets new values
INSERT INTO replicated_unique_basic_insert VALUES (1, 100, 100);
SELECT * FROM replicated_unique_basic_insert ORDER BY id;

-- Multiple inserts with intra-block duplicates
INSERT INTO replicated_unique_basic_insert VALUES (10, 10, 10);
INSERT INTO replicated_unique_basic_insert VALUES (20, 10, 10), (20, 11, 11);
INSERT INTO replicated_unique_basic_insert VALUES (20, 10, 10), (20, 11, 11);
INSERT INTO replicated_unique_basic_insert VALUES (21, 11, 11), (10, 10, 10), (21, 12, 12);
INSERT INTO replicated_unique_basic_insert VALUES (21, 11, 11), (10, 10, 10), (21, 12, 12);
SELECT * FROM replicated_unique_basic_insert ORDER BY id;

DROP TABLE replicated_unique_basic_insert;

-- ===================================================================
-- INSERT with version column (single replica)
-- ===================================================================

DROP TABLE IF EXISTS replicated_unique_version_insert;

CREATE TABLE replicated_unique_version_insert
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_version_insert', '1')
ORDER BY id;

INSERT INTO replicated_unique_version_insert SELECT number AS id, number AS value1, number AS value2, 1 AS version FROM numbers(10);
SELECT id, value1, value2 FROM replicated_unique_version_insert ORDER BY id;

INSERT INTO replicated_unique_version_insert VALUES (1, 1, 1, 2), (2, 2, 2, 2), (3, 3, 3, 2);
SELECT id, value1, value2 FROM replicated_unique_version_insert ORDER BY id;

INSERT INTO replicated_unique_version_insert VALUES (1, 100, 100, 3);
SELECT id, value1, value2 FROM replicated_unique_version_insert ORDER BY id;

INSERT INTO replicated_unique_version_insert VALUES (10, 10, 10, 4);
INSERT INTO replicated_unique_version_insert VALUES (20, 10, 10, 5), (20, 11, 11, 5);
INSERT INTO replicated_unique_version_insert VALUES (20, 10, 10, 6), (20, 11, 11, 6);
INSERT INTO replicated_unique_version_insert VALUES (21, 11, 11, 7), (10, 10, 10, 7), (21, 12, 12, 7);
INSERT INTO replicated_unique_version_insert VALUES (21, 11, 11, 8), (10, 10, 10, 8), (21, 12, 12, 8);
SELECT id, value1, value2 FROM replicated_unique_version_insert ORDER BY id;

DROP TABLE replicated_unique_version_insert;
