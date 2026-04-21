-- Test: UniqueMergeTree data lifecycle operations
--
-- Covers DROP PARTITION, TRUNCATE, and partition isolation
-- to verify dedup state is correctly maintained across data lifecycle ops.

-- ===================================================================
-- DROP PARTITION: basic + re-insert
-- ===================================================================
SELECT '--- drop partition: basic ---';

DROP TABLE IF EXISTS unique_drop_part;

CREATE TABLE unique_drop_part
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_drop_part VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO unique_drop_part VALUES ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
INSERT INTO unique_drop_part VALUES ('2024-01-01', 1, 100);

SELECT * FROM unique_drop_part ORDER BY dt, id;

ALTER TABLE unique_drop_part DROP PARTITION '2024-01-01';
SELECT * FROM unique_drop_part ORDER BY dt, id;

INSERT INTO unique_drop_part VALUES ('2024-01-01', 1, 999), ('2024-01-01', 2, 888);
SELECT * FROM unique_drop_part ORDER BY dt, id;

DROP TABLE unique_drop_part;

-- ===================================================================
-- TRUNCATE: basic + re-insert
-- ===================================================================
SELECT '--- truncate: basic ---';

DROP TABLE IF EXISTS unique_truncate;

CREATE TABLE unique_truncate
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO unique_truncate SELECT number, number FROM numbers(10);
INSERT INTO unique_truncate SELECT number, number + 100 FROM numbers(5);
SELECT count() FROM unique_truncate;

TRUNCATE TABLE unique_truncate;
SELECT count() FROM unique_truncate;

INSERT INTO unique_truncate SELECT number, number + 200 FROM numbers(5);
SELECT * FROM unique_truncate ORDER BY id;

DROP TABLE unique_truncate;

-- ===================================================================
-- Partition isolation: same key in different partitions should coexist
-- ===================================================================
SELECT '--- partition isolation: basic ---';

DROP TABLE IF EXISTS unique_partition_isolation;

CREATE TABLE unique_partition_isolation
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_partition_isolation VALUES ('2024-01-01', 1, 10);
INSERT INTO unique_partition_isolation VALUES ('2024-01-02', 1, 20);
INSERT INTO unique_partition_isolation VALUES ('2024-01-03', 1, 30);

SELECT count() FROM unique_partition_isolation;
SELECT * FROM unique_partition_isolation ORDER BY dt;

DROP TABLE unique_partition_isolation;

-- ===================================================================
-- Partition isolation: upsert within partition does not affect others
-- ===================================================================
SELECT '--- partition isolation: upsert ---';

DROP TABLE IF EXISTS unique_partition_upsert;

CREATE TABLE unique_partition_upsert
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_partition_upsert VALUES
    ('2024-01-01', 1, 10), ('2024-01-01', 2, 20),
    ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);

INSERT INTO unique_partition_upsert VALUES ('2024-01-01', 1, 100);

SELECT * FROM unique_partition_upsert ORDER BY dt, id;

DROP TABLE unique_partition_upsert;

-- ===================================================================
-- Partition isolation: merge preserves isolation
-- ===================================================================
SELECT '--- partition isolation: merge ---';

DROP TABLE IF EXISTS unique_partition_merge;

CREATE TABLE unique_partition_merge
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_partition_merge VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO unique_partition_merge VALUES ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
INSERT INTO unique_partition_merge VALUES ('2024-01-01', 1, 100);
INSERT INTO unique_partition_merge VALUES ('2024-01-02', 1, 300);

OPTIMIZE TABLE unique_partition_merge FINAL;

SELECT * FROM unique_partition_merge ORDER BY dt, id;

DROP TABLE unique_partition_merge;

-- ===================================================================
-- DROP PARTITION isolation: does not affect other partitions
-- ===================================================================
SELECT '--- drop partition: isolation ---';

DROP TABLE IF EXISTS unique_drop_isolation;

CREATE TABLE unique_drop_isolation
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_drop_isolation VALUES
    ('2024-01-01', 1, 10), ('2024-01-01', 2, 20),
    ('2024-01-02', 1, 30), ('2024-01-02', 2, 40),
    ('2024-01-03', 1, 50), ('2024-01-03', 2, 60);

INSERT INTO unique_drop_isolation VALUES
    ('2024-01-01', 1, 100),
    ('2024-01-02', 1, 300),
    ('2024-01-03', 1, 500);

ALTER TABLE unique_drop_isolation DROP PARTITION '2024-01-02';

SELECT * FROM unique_drop_isolation ORDER BY dt, id;

DROP TABLE unique_drop_isolation;
