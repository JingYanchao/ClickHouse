-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree mutation operations
--
-- Covers ALTER UPDATE, ALTER DELETE, MODIFY COLUMN, ADD COLUMN,
-- ADD/MATERIALIZE/CLEAR INDEX and their interaction with dedup state
-- on the replicated variant.

-- ===================================================================
-- ALTER UPDATE: basic
-- ===================================================================
SELECT '--- alter update: basic ---';

DROP TABLE IF EXISTS rep_unique_mutation_update;

CREATE TABLE rep_unique_mutation_update
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_update', '1')
ORDER BY id;

INSERT INTO rep_unique_mutation_update SELECT number, number, number FROM numbers(10);

ALTER TABLE rep_unique_mutation_update UPDATE value1 = 999 WHERE id = 5 SETTINGS mutations_sync = 2;
SELECT * FROM rep_unique_mutation_update ORDER BY id;

-- UPDATE on multiple rows
ALTER TABLE rep_unique_mutation_update UPDATE value2 = value2 + 1000 WHERE id < 3 SETTINGS mutations_sync = 2;
SELECT * FROM rep_unique_mutation_update ORDER BY id;

-- After UPDATE, upsert should still dedup correctly
INSERT INTO rep_unique_mutation_update VALUES (5, 555, 555);
SELECT * FROM rep_unique_mutation_update WHERE id = 5;

DROP TABLE rep_unique_mutation_update;

-- ===================================================================
-- ALTER UPDATE: with version column
-- ===================================================================
SELECT '--- alter update: with version ---';

DROP TABLE IF EXISTS rep_unique_mutation_update_ver;

CREATE TABLE rep_unique_mutation_update_ver
(
    id UInt32,
    value1 UInt32,
    ver UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('ver')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_update_ver', '1')
ORDER BY id;

INSERT INTO rep_unique_mutation_update_ver SELECT number, number, 1 FROM numbers(5);

-- UPDATE that also bumps version
ALTER TABLE rep_unique_mutation_update_ver UPDATE value1 = 999, ver = 2 WHERE id = 3 SETTINGS mutations_sync = 2;
SELECT id, value1, ver FROM rep_unique_mutation_update_ver ORDER BY id;

-- After mutation, upsert with lower version should not overwrite
INSERT INTO rep_unique_mutation_update_ver VALUES (3, 100, 1);
SELECT id, value1, ver FROM rep_unique_mutation_update_ver WHERE id = 3;

-- Upsert with higher version should overwrite
INSERT INTO rep_unique_mutation_update_ver VALUES (3, 200, 3);
SELECT id, value1, ver FROM rep_unique_mutation_update_ver WHERE id = 3;

DROP TABLE rep_unique_mutation_update_ver;

-- ===================================================================
-- ALTER DELETE: not supported for ReplicatedUniqueMergeTree
-- ===================================================================
SELECT '--- alter delete: not supported ---';

DROP TABLE IF EXISTS rep_unique_mutation_delete;

CREATE TABLE rep_unique_mutation_delete
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_delete', '1')
ORDER BY id;

INSERT INTO rep_unique_mutation_delete SELECT number, number FROM numbers(10);

ALTER TABLE rep_unique_mutation_delete DELETE WHERE id >= 8 SETTINGS mutations_sync = 2; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM rep_unique_mutation_delete ORDER BY id;

DROP TABLE rep_unique_mutation_delete;

-- ===================================================================
-- ALTER DELETE: with partition also not supported
-- ===================================================================
SELECT '--- alter delete: partition not supported ---';

DROP TABLE IF EXISTS rep_unique_mutation_delete_part;

CREATE TABLE rep_unique_mutation_delete_part
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_delete_part', '1')
PARTITION BY dt
ORDER BY id;

INSERT INTO rep_unique_mutation_delete_part VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO rep_unique_mutation_delete_part VALUES ('2024-01-02', 3, 30), ('2024-01-02', 4, 40);

ALTER TABLE rep_unique_mutation_delete_part DELETE WHERE dt = '2024-01-01' SETTINGS mutations_sync = 2; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM rep_unique_mutation_delete_part ORDER BY dt, id;

DROP TABLE rep_unique_mutation_delete_part;

-- ===================================================================
-- MODIFY COLUMN: type change on non-key column preserves dedup
-- ===================================================================
SELECT '--- modify column: non-key ---';

DROP TABLE IF EXISTS rep_unique_mutation_modify;

CREATE TABLE rep_unique_mutation_modify
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_modify', '1')
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO rep_unique_mutation_modify SELECT number, number FROM numbers(5);

ALTER TABLE rep_unique_mutation_modify MODIFY COLUMN value UInt64 SETTINGS mutations_sync = 2;
SELECT id, value, toTypeName(value) FROM rep_unique_mutation_modify ORDER BY id;

-- After mutation, dedup should still work
INSERT INTO rep_unique_mutation_modify VALUES (3, 999);
SELECT id, value FROM rep_unique_mutation_modify WHERE id = 3;

DROP TABLE rep_unique_mutation_modify;

-- ===================================================================
-- MODIFY COLUMN: type change on key column is forbidden
-- ===================================================================
SELECT '--- modify column: key column forbidden ---';

DROP TABLE IF EXISTS rep_unique_mutation_modify_key;

CREATE TABLE rep_unique_mutation_modify_key
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_modify_key', '1')
ORDER BY id;

ALTER TABLE rep_unique_mutation_modify_key MODIFY COLUMN id UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE rep_unique_mutation_modify_key;

-- ===================================================================
-- ADD COLUMN: preserves dedup
-- ===================================================================
SELECT '--- add column ---';

DROP TABLE IF EXISTS rep_unique_mutation_add_col;

CREATE TABLE rep_unique_mutation_add_col
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_add_col', '1')
ORDER BY id;

INSERT INTO rep_unique_mutation_add_col SELECT number, number FROM numbers(5);

ALTER TABLE rep_unique_mutation_add_col ADD COLUMN extra String DEFAULT 'hello' SETTINGS mutations_sync = 2;
SELECT id, value, extra FROM rep_unique_mutation_add_col ORDER BY id;

-- Dedup should still work after ADD COLUMN
INSERT INTO rep_unique_mutation_add_col VALUES (3, 999, 'world');
SELECT id, value, extra FROM rep_unique_mutation_add_col WHERE id = 3;

DROP TABLE rep_unique_mutation_add_col;

-- ===================================================================
-- Mutation + merge interaction
-- ===================================================================
SELECT '--- mutation + merge ---';

DROP TABLE IF EXISTS rep_unique_mutation_merge;

CREATE TABLE rep_unique_mutation_merge
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_merge', '1')
ORDER BY id;

INSERT INTO rep_unique_mutation_merge SELECT number, number FROM numbers(5);
INSERT INTO rep_unique_mutation_merge SELECT number + 5, number + 5 FROM numbers(5);
INSERT INTO rep_unique_mutation_merge VALUES (0, 100);

-- Before merge: id=0 should have value=100 (dedup)
SELECT * FROM rep_unique_mutation_merge ORDER BY id;

-- Mutate while parts are separate
ALTER TABLE rep_unique_mutation_merge UPDATE value = value + 1000 WHERE id < 3 SETTINGS mutations_sync = 2;

-- Merge after mutation
OPTIMIZE TABLE rep_unique_mutation_merge FINAL;
SELECT * FROM rep_unique_mutation_merge ORDER BY id;

-- Dedup should still work after merge
INSERT INTO rep_unique_mutation_merge VALUES (0, 200);
SELECT * FROM rep_unique_mutation_merge WHERE id = 0;

DROP TABLE rep_unique_mutation_merge;

-- ===================================================================
-- Mutation + version column + merge
-- ===================================================================
SELECT '--- mutation + version + merge ---';

DROP TABLE IF EXISTS rep_unique_mutation_ver_merge;

CREATE TABLE rep_unique_mutation_ver_merge
(
    id UInt32,
    value UInt32,
    ver UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('ver')
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_ver_merge', '1')
ORDER BY id;

INSERT INTO rep_unique_mutation_ver_merge SELECT number, number, 1 FROM numbers(5);
INSERT INTO rep_unique_mutation_ver_merge SELECT number + 5, number + 5, 1 FROM numbers(5);

-- Mutation that bumps version
ALTER TABLE rep_unique_mutation_ver_merge UPDATE value = 999, ver = 10 WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT id, value, ver FROM rep_unique_mutation_ver_merge WHERE id = 2;

OPTIMIZE TABLE rep_unique_mutation_ver_merge FINAL;

-- After merge, upsert with lower version should not overwrite
INSERT INTO rep_unique_mutation_ver_merge VALUES (2, 100, 5);
SELECT id, value, ver FROM rep_unique_mutation_ver_merge WHERE id = 2;

-- Upsert with higher version should overwrite
INSERT INTO rep_unique_mutation_ver_merge VALUES (2, 200, 20);
SELECT id, value, ver FROM rep_unique_mutation_ver_merge WHERE id = 2;

DROP TABLE rep_unique_mutation_ver_merge;

-- ===================================================================
-- ADD INDEX + MATERIALIZE INDEX + CLEAR INDEX
-- ===================================================================
SELECT '--- add/materialize/clear index ---';

DROP TABLE IF EXISTS rep_unique_mutation_index;

CREATE TABLE rep_unique_mutation_index
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_index', '1')
PARTITION BY id % 2
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO rep_unique_mutation_index VALUES (0, 2), (1, 1), (2, 1), (3, 1), (4, 2);

ALTER TABLE rep_unique_mutation_index ADD INDEX idx (value) TYPE minmax GRANULARITY 1;
ALTER TABLE rep_unique_mutation_index MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT id, value FROM rep_unique_mutation_index ORDER BY id;

-- Clear the index
ALTER TABLE rep_unique_mutation_index CLEAR INDEX idx SETTINGS mutations_sync = 2;
SELECT id, value FROM rep_unique_mutation_index ORDER BY id;

-- Dedup should still work after index operations
INSERT INTO rep_unique_mutation_index VALUES (0, 200);
SELECT * FROM rep_unique_mutation_index WHERE id = 0;

DROP TABLE rep_unique_mutation_index;

-- ===================================================================
-- Multiple mutations in sequence
-- ===================================================================
SELECT '--- multiple sequential mutations ---';

DROP TABLE IF EXISTS rep_unique_mutation_seq;

CREATE TABLE rep_unique_mutation_seq
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_seq', '1')
ORDER BY id;

INSERT INTO rep_unique_mutation_seq SELECT number, number FROM numbers(10);

ALTER TABLE rep_unique_mutation_seq UPDATE value = 100 WHERE id % 2 = 0 SETTINGS mutations_sync = 2;
ALTER TABLE rep_unique_mutation_seq DELETE WHERE id >= 7 SETTINGS mutations_sync = 2; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE rep_unique_mutation_seq ADD COLUMN extra UInt32 DEFAULT 0 SETTINGS mutations_sync = 2;

SELECT * FROM rep_unique_mutation_seq ORDER BY id;

-- Upsert after multiple mutations
INSERT INTO rep_unique_mutation_seq VALUES (8, 888, 1);
SELECT * FROM rep_unique_mutation_seq ORDER BY id;

DROP TABLE rep_unique_mutation_seq;

-- ===================================================================
-- Replicated: mutation on replica 1, verify dedup on replica 2
-- ===================================================================
SELECT '--- mutation replication to r2 ---';

DROP TABLE IF EXISTS rep_unique_mutation_r1;
DROP TABLE IF EXISTS rep_unique_mutation_r2;

CREATE TABLE rep_unique_mutation_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_r', '1')
ORDER BY id;

CREATE TABLE rep_unique_mutation_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/rep_unique_mutation_r', '2')
ORDER BY id;

INSERT INTO rep_unique_mutation_r1 SELECT number, number FROM numbers(5);

-- Mutate on r1
ALTER TABLE rep_unique_mutation_r1 UPDATE value = 999 WHERE id = 2 SETTINGS mutations_sync = 2;

-- Wait for r2 to catch up
SYSTEM SYNC REPLICA rep_unique_mutation_r2;

SELECT * FROM rep_unique_mutation_r2 ORDER BY id;

-- Dedup on r2 should be consistent after replication
INSERT INTO rep_unique_mutation_r2 VALUES (2, 111);
SELECT * FROM rep_unique_mutation_r2 WHERE id = 2;

DROP TABLE rep_unique_mutation_r1;
DROP TABLE rep_unique_mutation_r2;
