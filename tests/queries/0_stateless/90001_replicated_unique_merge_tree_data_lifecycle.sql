-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree data lifecycle operations
--
-- Covers DELETE FROM, DROP PARTITION, and TRUNCATE on replicated tables
-- to verify dedup state is correctly maintained and replicated.

-- ===================================================================
-- DELETE FROM: basic, on replica 1, verify on replica 2
-- ===================================================================
SELECT '--- replicated delete: basic ---';

DROP TABLE IF EXISTS r1_delete;
DROP TABLE IF EXISTS r2_delete;

CREATE TABLE r1_delete
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_delete', '1')
ORDER BY id;

CREATE TABLE r2_delete
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_delete', '2')
ORDER BY id;

INSERT INTO r1_delete SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA r2_delete;

DELETE FROM r1_delete WHERE id < 3;
SYSTEM SYNC REPLICA r2_delete;

SELECT '--- r1 after delete ---';
SELECT count() FROM r1_delete;
SELECT * FROM r1_delete ORDER BY id;
SELECT '--- r2 after delete ---';
SELECT count() FROM r2_delete;
SELECT * FROM r2_delete ORDER BY id;

DROP TABLE r1_delete;
DROP TABLE r2_delete;

-- ===================================================================
-- DELETE FROM: DELETE then INSERT same keys
-- ===================================================================
SELECT '--- replicated delete: reinsert ---';

DROP TABLE IF EXISTS r1_delete_reinsert;
DROP TABLE IF EXISTS r2_delete_reinsert;

CREATE TABLE r1_delete_reinsert
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_delete_reinsert', '1')
ORDER BY id;

CREATE TABLE r2_delete_reinsert
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_delete_reinsert', '2')
ORDER BY id;

INSERT INTO r1_delete_reinsert SELECT number, number FROM numbers(10);
DELETE FROM r1_delete_reinsert WHERE id < 5;
INSERT INTO r1_delete_reinsert SELECT number, number + 100 FROM numbers(5);
SYSTEM SYNC REPLICA r2_delete_reinsert;

SELECT '--- r1 after delete + reinsert ---';
SELECT count() FROM r1_delete_reinsert;
SELECT * FROM r1_delete_reinsert ORDER BY id;
SELECT '--- r2 after delete + reinsert ---';
SELECT count() FROM r2_delete_reinsert;
SELECT * FROM r2_delete_reinsert ORDER BY id;

DROP TABLE r1_delete_reinsert;
DROP TABLE r2_delete_reinsert;

-- ===================================================================
-- DELETE FROM: cross-replica DELETE + INSERT
-- ===================================================================
SELECT '--- replicated delete: cross replica ---';

DROP TABLE IF EXISTS r1_delete_cross;
DROP TABLE IF EXISTS r2_delete_cross;

CREATE TABLE r1_delete_cross
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_delete_cross', '1')
ORDER BY id;

CREATE TABLE r2_delete_cross
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_delete_cross', '2')
ORDER BY id;

INSERT INTO r1_delete_cross SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA r2_delete_cross;

DELETE FROM r2_delete_cross WHERE id >= 8;
SYSTEM SYNC REPLICA r1_delete_cross;

INSERT INTO r1_delete_cross SELECT number + 7, number + 700 FROM numbers(5);
SYSTEM SYNC REPLICA r2_delete_cross;

SELECT '--- r1 final ---';
SELECT * FROM r1_delete_cross ORDER BY id;
SELECT '--- r2 final ---';
SELECT * FROM r2_delete_cross ORDER BY id;

DROP TABLE r1_delete_cross;
DROP TABLE r2_delete_cross;

-- ===================================================================
-- DROP PARTITION: on replica 1, verify on replica 2
-- ===================================================================
SELECT '--- replicated drop partition ---';

DROP TABLE IF EXISTS r1_drop_part;
DROP TABLE IF EXISTS r2_drop_part;

CREATE TABLE r1_drop_part
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_drop_part', '1')
PARTITION BY dt
ORDER BY id;

CREATE TABLE r2_drop_part
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_drop_part', '2')
PARTITION BY dt
ORDER BY id;

INSERT INTO r1_drop_part VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO r1_drop_part VALUES ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
INSERT INTO r1_drop_part VALUES ('2024-01-01', 1, 100);
SYSTEM SYNC REPLICA r2_drop_part;

ALTER TABLE r1_drop_part DROP PARTITION '2024-01-01';
SYSTEM SYNC REPLICA r2_drop_part;

SELECT '--- r1 after drop ---';
SELECT * FROM r1_drop_part ORDER BY dt, id;
SELECT '--- r2 after drop ---';
SELECT * FROM r2_drop_part ORDER BY dt, id;

INSERT INTO r1_drop_part VALUES ('2024-01-01', 1, 999);
SYSTEM SYNC REPLICA r2_drop_part;

SELECT '--- r1 after re-insert ---';
SELECT * FROM r1_drop_part ORDER BY dt, id;
SELECT '--- r2 after re-insert ---';
SELECT * FROM r2_drop_part ORDER BY dt, id;

DROP TABLE r1_drop_part;
DROP TABLE r2_drop_part;

-- ===================================================================
-- TRUNCATE: on replica 1, verify on replica 2
-- ===================================================================
SELECT '--- replicated truncate ---';

DROP TABLE IF EXISTS r1_truncate;
DROP TABLE IF EXISTS r2_truncate;

CREATE TABLE r1_truncate
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_truncate', '1')
ORDER BY id;

CREATE TABLE r2_truncate
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_truncate', '2')
ORDER BY id;

INSERT INTO r1_truncate SELECT number, number FROM numbers(10);
INSERT INTO r1_truncate SELECT number, number + 100 FROM numbers(5);
SYSTEM SYNC REPLICA r2_truncate;

SELECT count() FROM r1_truncate;
SELECT count() FROM r2_truncate;

TRUNCATE TABLE r1_truncate;
SYSTEM SYNC REPLICA r2_truncate;

SELECT count() FROM r1_truncate;
SELECT count() FROM r2_truncate;

INSERT INTO r1_truncate SELECT number, number + 200 FROM numbers(5);
SYSTEM SYNC REPLICA r2_truncate;

SELECT '--- r1 after truncate + insert ---';
SELECT * FROM r1_truncate ORDER BY id;
SELECT '--- r2 after truncate + insert ---';
SELECT * FROM r2_truncate ORDER BY id;

DROP TABLE r1_truncate;
DROP TABLE r2_truncate;
