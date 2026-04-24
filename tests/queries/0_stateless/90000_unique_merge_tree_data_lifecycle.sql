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

-- ===================================================================
-- DROP PARTITION: dedup still works after re-insert into dropped partition
-- ===================================================================
SELECT '--- drop partition: dedup after re-insert ---';

DROP TABLE IF EXISTS unique_drop_dedup;

CREATE TABLE unique_drop_dedup
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_drop_dedup VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO unique_drop_dedup VALUES ('2024-01-01', 1, 100);

ALTER TABLE unique_drop_dedup DROP PARTITION '2024-01-01';
SELECT count() FROM unique_drop_dedup;

-- Re-insert and verify dedup works on the fresh partition
INSERT INTO unique_drop_dedup VALUES ('2024-01-01', 1, 200), ('2024-01-01', 2, 300);
INSERT INTO unique_drop_dedup VALUES ('2024-01-01', 1, 999);
SELECT * FROM unique_drop_dedup ORDER BY id;

OPTIMIZE TABLE unique_drop_dedup FINAL;
SELECT * FROM unique_drop_dedup ORDER BY id;

DROP TABLE unique_drop_dedup;

-- ===================================================================
-- TRUNCATE: dedup still works after re-insert
-- ===================================================================
SELECT '--- truncate: dedup after re-insert ---';

DROP TABLE IF EXISTS unique_truncate_dedup;

CREATE TABLE unique_truncate_dedup
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_truncate_dedup VALUES
    ('2024-01-01', 1, 10), ('2024-01-01', 2, 20),
    ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);

TRUNCATE TABLE unique_truncate_dedup;
SELECT count() FROM unique_truncate_dedup;

-- Re-insert same keys and verify dedup works
INSERT INTO unique_truncate_dedup VALUES ('2024-01-01', 1, 100), ('2024-01-01', 2, 200);
INSERT INTO unique_truncate_dedup VALUES ('2024-01-01', 1, 999);
SELECT * FROM unique_truncate_dedup ORDER BY dt, id;

-- Also verify cross-partition dedup isolation after truncate
INSERT INTO unique_truncate_dedup VALUES ('2024-01-02', 1, 500);
SELECT * FROM unique_truncate_dedup ORDER BY dt, id;

OPTIMIZE TABLE unique_truncate_dedup FINAL;
SELECT * FROM unique_truncate_dedup ORDER BY dt, id;

DROP TABLE unique_truncate_dedup;

-- ===================================================================
-- DROP PARTITION ALL: equivalent to truncate via ALTER TABLE
-- ===================================================================
SELECT '--- drop partition all ---';

DROP TABLE IF EXISTS unique_drop_all;

CREATE TABLE unique_drop_all
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_drop_all VALUES
    ('2024-01-01', 1, 10), ('2024-01-02', 2, 20), ('2024-01-03', 3, 30);

ALTER TABLE unique_drop_all DROP PARTITION ALL;
SELECT count() FROM unique_drop_all;

INSERT INTO unique_drop_all VALUES ('2024-01-01', 1, 100);
INSERT INTO unique_drop_all VALUES ('2024-01-01', 1, 200);
SELECT * FROM unique_drop_all ORDER BY id;

DROP TABLE unique_drop_all;

-- ===================================================================
-- ATTACH PART: detach then attach with dedup
-- ===================================================================
SELECT '--- attach part: basic dedup ---';

DROP TABLE IF EXISTS unique_attach;

CREATE TABLE unique_attach
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id
SETTINGS max_parts_to_merge_at_once = 1; -- prevent background merges

INSERT INTO unique_attach VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
SELECT * FROM unique_attach ORDER BY id;

-- Detach the partition
ALTER TABLE unique_attach DETACH PARTITION '2024-01-01';
SELECT count() FROM unique_attach;

-- Insert new data with overlapping keys while partition is detached
INSERT INTO unique_attach VALUES ('2024-01-01', 1, 100), ('2024-01-01', 3, 300);
SELECT * FROM unique_attach ORDER BY id;

-- Attach the detached partition back; dedup should resolve conflicts
ALTER TABLE unique_attach ATTACH PARTITION '2024-01-01';
SELECT * FROM unique_attach ORDER BY id;

DROP TABLE unique_attach;

-- ===================================================================
-- ATTACH PART: attach into empty table
-- ===================================================================
SELECT '--- attach part: into empty table ---';

DROP TABLE IF EXISTS unique_attach_empty;

CREATE TABLE unique_attach_empty
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY dt
ORDER BY id;

INSERT INTO unique_attach_empty VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);

-- Detach, then verify table is empty
ALTER TABLE unique_attach_empty DETACH PARTITION '2024-01-01';
SELECT count() FROM unique_attach_empty;

-- Attach back into empty table
ALTER TABLE unique_attach_empty ATTACH PARTITION '2024-01-01';
SELECT * FROM unique_attach_empty ORDER BY id;

-- Verify dedup still works after attach
INSERT INTO unique_attach_empty VALUES ('2024-01-01', 1, 999);
SELECT * FROM unique_attach_empty ORDER BY id;

DROP TABLE unique_attach_empty;
