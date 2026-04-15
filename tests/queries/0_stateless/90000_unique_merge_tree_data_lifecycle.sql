-- Test: UniqueMergeTree data lifecycle operations
--
-- Covers DELETE FROM, DROP PARTITION, TRUNCATE, and partition isolation
-- to verify dedup state is correctly maintained across data lifecycle ops.

-- ===================================================================
-- DELETE FROM: basic
-- ===================================================================
SELECT '--- delete: basic ---';

DROP TABLE IF EXISTS umt_delete_basic;

CREATE TABLE umt_delete_basic
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_delete_basic SELECT number, number FROM numbers(10);
SELECT count() FROM umt_delete_basic;

DELETE FROM umt_delete_basic WHERE id < 3;
SELECT count() FROM umt_delete_basic;
SELECT * FROM umt_delete_basic ORDER BY id;

DROP TABLE umt_delete_basic;

-- ===================================================================
-- DELETE FROM: INSERT after DELETE with same key
-- ===================================================================
SELECT '--- delete: insert after delete ---';

DROP TABLE IF EXISTS umt_delete_reinsert;

CREATE TABLE umt_delete_reinsert
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_delete_reinsert SELECT number, number FROM numbers(10);
DELETE FROM umt_delete_reinsert WHERE id < 5;
INSERT INTO umt_delete_reinsert SELECT number, number + 100 FROM numbers(5);

SELECT count() FROM umt_delete_reinsert;
SELECT * FROM umt_delete_reinsert ORDER BY id;

DROP TABLE umt_delete_reinsert;

-- ===================================================================
-- DELETE FROM: merge after delete
-- ===================================================================
SELECT '--- delete: merge after delete ---';

DROP TABLE IF EXISTS umt_delete_merge;

CREATE TABLE umt_delete_merge
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_delete_merge SELECT number, number FROM numbers(10);
INSERT INTO umt_delete_merge SELECT number, number + 100 FROM numbers(5);

DELETE FROM umt_delete_merge WHERE id >= 8;
OPTIMIZE TABLE umt_delete_merge FINAL;

SELECT count() FROM umt_delete_merge;
SELECT * FROM umt_delete_merge ORDER BY id;

DROP TABLE umt_delete_merge;

-- ===================================================================
-- DELETE FROM: multiple cycles
-- ===================================================================
SELECT '--- delete: multiple cycles ---';

DROP TABLE IF EXISTS umt_delete_cycles;

CREATE TABLE umt_delete_cycles
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_delete_cycles SELECT number, 1 FROM numbers(10);
DELETE FROM umt_delete_cycles WHERE id % 2 = 0;
INSERT INTO umt_delete_cycles SELECT number * 2, 2 FROM numbers(5);
DELETE FROM umt_delete_cycles WHERE id = 1;
INSERT INTO umt_delete_cycles VALUES (1, 3);

SELECT count() FROM umt_delete_cycles;
SELECT * FROM umt_delete_cycles ORDER BY id;

DROP TABLE umt_delete_cycles;

-- ===================================================================
-- DROP PARTITION: basic + re-insert
-- ===================================================================
SELECT '--- drop partition: basic ---';

DROP TABLE IF EXISTS umt_drop_part;

CREATE TABLE umt_drop_part
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO umt_drop_part VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO umt_drop_part VALUES ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
INSERT INTO umt_drop_part VALUES ('2024-01-01', 1, 100);

SELECT * FROM umt_drop_part ORDER BY dt, id;

ALTER TABLE umt_drop_part DROP PARTITION '2024-01-01';
SELECT * FROM umt_drop_part ORDER BY dt, id;

INSERT INTO umt_drop_part VALUES ('2024-01-01', 1, 999), ('2024-01-01', 2, 888);
SELECT * FROM umt_drop_part ORDER BY dt, id;

DROP TABLE umt_drop_part;

-- ===================================================================
-- TRUNCATE: basic + re-insert
-- ===================================================================
SELECT '--- truncate: basic ---';

DROP TABLE IF EXISTS umt_truncate;

CREATE TABLE umt_truncate
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_truncate SELECT number, number FROM numbers(10);
INSERT INTO umt_truncate SELECT number, number + 100 FROM numbers(5);
SELECT count() FROM umt_truncate;

TRUNCATE TABLE umt_truncate;
SELECT count() FROM umt_truncate;

INSERT INTO umt_truncate SELECT number, number + 200 FROM numbers(5);
SELECT * FROM umt_truncate ORDER BY id;

DROP TABLE umt_truncate;

-- ===================================================================
-- Partition isolation: same key in different partitions should coexist
-- ===================================================================
SELECT '--- partition isolation: basic ---';

DROP TABLE IF EXISTS umt_partition_isolation;

CREATE TABLE umt_partition_isolation
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO umt_partition_isolation VALUES ('2024-01-01', 1, 10);
INSERT INTO umt_partition_isolation VALUES ('2024-01-02', 1, 20);
INSERT INTO umt_partition_isolation VALUES ('2024-01-03', 1, 30);

SELECT count() FROM umt_partition_isolation;
SELECT * FROM umt_partition_isolation ORDER BY dt;

DROP TABLE umt_partition_isolation;

-- ===================================================================
-- Partition isolation: upsert within partition does not affect others
-- ===================================================================
SELECT '--- partition isolation: upsert ---';

DROP TABLE IF EXISTS umt_partition_upsert;

CREATE TABLE umt_partition_upsert
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO umt_partition_upsert VALUES
    ('2024-01-01', 1, 10), ('2024-01-01', 2, 20),
    ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);

INSERT INTO umt_partition_upsert VALUES ('2024-01-01', 1, 100);

SELECT * FROM umt_partition_upsert ORDER BY dt, id;

DROP TABLE umt_partition_upsert;

-- ===================================================================
-- Partition isolation: merge preserves isolation
-- ===================================================================
SELECT '--- partition isolation: merge ---';

DROP TABLE IF EXISTS umt_partition_merge;

CREATE TABLE umt_partition_merge
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO umt_partition_merge VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO umt_partition_merge VALUES ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
INSERT INTO umt_partition_merge VALUES ('2024-01-01', 1, 100);
INSERT INTO umt_partition_merge VALUES ('2024-01-02', 1, 300);

OPTIMIZE TABLE umt_partition_merge FINAL;

SELECT * FROM umt_partition_merge ORDER BY dt, id;

DROP TABLE umt_partition_merge;

-- ===================================================================
-- DROP PARTITION isolation: does not affect other partitions
-- ===================================================================
SELECT '--- drop partition: isolation ---';

DROP TABLE IF EXISTS umt_drop_isolation;

CREATE TABLE umt_drop_isolation
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO umt_drop_isolation VALUES
    ('2024-01-01', 1, 10), ('2024-01-01', 2, 20),
    ('2024-01-02', 1, 30), ('2024-01-02', 2, 40),
    ('2024-01-03', 1, 50), ('2024-01-03', 2, 60);

INSERT INTO umt_drop_isolation VALUES
    ('2024-01-01', 1, 100),
    ('2024-01-02', 1, 300),
    ('2024-01-03', 1, 500);

ALTER TABLE umt_drop_isolation DROP PARTITION '2024-01-02';

SELECT * FROM umt_drop_isolation ORDER BY dt, id;

DROP TABLE umt_drop_isolation;
