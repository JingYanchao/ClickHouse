-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree basic INSERT dedup (single replica)
-- Mirrors 90000_unique_merge_tree_insert but uses ReplicatedUniqueMergeTree.

DROP TABLE IF EXISTS replicated_umt_basic_insert;

CREATE TABLE replicated_umt_basic_insert
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_umt_basic_insert', '1')
ORDER BY id;

-- Basic insert: 10 rows
INSERT INTO replicated_umt_basic_insert SELECT number AS id, number AS value1, number AS value2 FROM numbers(10);
SELECT * FROM replicated_umt_basic_insert ORDER BY id;

-- Re-insert same keys: should be deduped (same values)
INSERT INTO replicated_umt_basic_insert VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
SELECT * FROM replicated_umt_basic_insert ORDER BY id;

-- Upsert: key 1 gets new values
INSERT INTO replicated_umt_basic_insert VALUES (1, 100, 100);
SELECT * FROM replicated_umt_basic_insert ORDER BY id;

-- Multiple inserts with intra-block duplicates
INSERT INTO replicated_umt_basic_insert VALUES (10, 10, 10);
INSERT INTO replicated_umt_basic_insert VALUES (20, 10, 10), (20, 11, 11);
INSERT INTO replicated_umt_basic_insert VALUES (20, 10, 10), (20, 11, 11);
INSERT INTO replicated_umt_basic_insert VALUES (21, 11, 11), (10, 10, 10), (21, 12, 12);
INSERT INTO replicated_umt_basic_insert VALUES (21, 11, 11), (10, 10, 10), (21, 12, 12);
SELECT * FROM replicated_umt_basic_insert ORDER BY id;

DROP TABLE replicated_umt_basic_insert;
