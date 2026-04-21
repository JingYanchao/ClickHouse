-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree unsupported operations
-- Verifies that REPLACE PARTITION and MOVE PARTITION TO TABLE are correctly rejected.

DROP TABLE IF EXISTS replicated_unique_unsupported_ops;
DROP TABLE IF EXISTS replicated_unique_unsupported_ops_src;
DROP TABLE IF EXISTS replicated_unique_unsupported_ops_dst;

CREATE TABLE replicated_unique_unsupported_ops
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_unsupported_ops', '1')
ORDER BY id;

CREATE TABLE replicated_unique_unsupported_ops_src
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_unsupported_ops_src', '1')
ORDER BY id;

CREATE TABLE replicated_unique_unsupported_ops_dst
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/replicated_unique_unsupported_ops_dst', '1')
ORDER BY id;

INSERT INTO replicated_unique_unsupported_ops SELECT number, number FROM numbers(10);
INSERT INTO replicated_unique_unsupported_ops_src SELECT number, number FROM numbers(10);

-- REPLACE PARTITION should fail
ALTER TABLE replicated_unique_unsupported_ops REPLACE PARTITION id '0' FROM replicated_unique_unsupported_ops_src; -- { serverError NOT_IMPLEMENTED }

-- MOVE PARTITION TO TABLE should fail
ALTER TABLE replicated_unique_unsupported_ops MOVE PARTITION id '0' TO TABLE replicated_unique_unsupported_ops_dst; -- { serverError NOT_IMPLEMENTED }

-- DROP PARTITION should work (no dedup bypass issue)
SELECT '--- drop partition works ---';
SELECT count() FROM replicated_unique_unsupported_ops;

-- TRUNCATE should work
TRUNCATE TABLE replicated_unique_unsupported_ops;
SELECT count() FROM replicated_unique_unsupported_ops;

-- Re-insert after truncate should work correctly
INSERT INTO replicated_unique_unsupported_ops SELECT number, number FROM numbers(5);
SELECT * FROM replicated_unique_unsupported_ops ORDER BY id;

DROP TABLE replicated_unique_unsupported_ops;
DROP TABLE replicated_unique_unsupported_ops_src;
DROP TABLE replicated_unique_unsupported_ops_dst;
