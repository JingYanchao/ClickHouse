-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree with row-level version column
-- Mirrors 90000_unique_merge_tree_insert_version but uses ReplicatedUniqueMergeTree.

DROP TABLE IF EXISTS replicated_umt_version_insert;

CREATE TABLE replicated_umt_version_insert
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_umt_version_insert', '1')
ORDER BY id;

INSERT INTO replicated_umt_version_insert SELECT number AS id, number AS value1, number AS value2, 1 AS version FROM numbers(10);
SELECT id, value1, value2 FROM replicated_umt_version_insert ORDER BY id;

INSERT INTO replicated_umt_version_insert VALUES (1, 1, 1, 2), (2, 2, 2, 2), (3, 3, 3, 2);
SELECT id, value1, value2 FROM replicated_umt_version_insert ORDER BY id;

INSERT INTO replicated_umt_version_insert VALUES (1, 100, 100, 3);
SELECT id, value1, value2 FROM replicated_umt_version_insert ORDER BY id;

INSERT INTO replicated_umt_version_insert VALUES (10, 10, 10, 4);
INSERT INTO replicated_umt_version_insert VALUES (20, 10, 10, 5), (20, 11, 11, 5);
INSERT INTO replicated_umt_version_insert VALUES (20, 10, 10, 6), (20, 11, 11, 6);
INSERT INTO replicated_umt_version_insert VALUES (21, 11, 11, 7), (10, 10, 10, 7), (21, 12, 12, 7);
INSERT INTO replicated_umt_version_insert VALUES (21, 11, 11, 8), (10, 10, 10, 8), (21, 12, 12, 8);
SELECT id, value1, value2 FROM replicated_umt_version_insert ORDER BY id;

DROP TABLE replicated_umt_version_insert;
