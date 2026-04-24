-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree data lifecycle operations
--
-- Covers DROP PARTITION and TRUNCATE on replicated tables
-- to verify dedup state is correctly maintained and replicated.

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

-- ===================================================================
-- DROP PARTITION: dedup still works after re-insert
-- ===================================================================
SELECT '--- replicated drop partition: dedup after re-insert ---';

DROP TABLE IF EXISTS r1_drop_dedup;
DROP TABLE IF EXISTS r2_drop_dedup;

CREATE TABLE r1_drop_dedup
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_drop_dedup', '1')
PARTITION BY dt
ORDER BY id;

CREATE TABLE r2_drop_dedup
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_drop_dedup', '2')
PARTITION BY dt
ORDER BY id;

INSERT INTO r1_drop_dedup VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO r1_drop_dedup VALUES ('2024-01-01', 1, 100);
SYSTEM SYNC REPLICA r2_drop_dedup;

ALTER TABLE r1_drop_dedup DROP PARTITION '2024-01-01';
SYSTEM SYNC REPLICA r2_drop_dedup;

SELECT count() FROM r1_drop_dedup;
SELECT count() FROM r2_drop_dedup;

INSERT INTO r1_drop_dedup VALUES ('2024-01-01', 1, 200), ('2024-01-01', 2, 300);
INSERT INTO r1_drop_dedup VALUES ('2024-01-01', 1, 999);
SYSTEM SYNC REPLICA r2_drop_dedup;

SELECT '--- r1 dedup after re-insert ---';
SELECT * FROM r1_drop_dedup ORDER BY id;
SELECT '--- r2 dedup after re-insert ---';
SELECT * FROM r2_drop_dedup ORDER BY id;

DROP TABLE r1_drop_dedup;
DROP TABLE r2_drop_dedup;

-- ===================================================================
-- TRUNCATE: dedup still works after re-insert
-- ===================================================================
SELECT '--- replicated truncate: dedup after re-insert ---';

DROP TABLE IF EXISTS r1_trunc_dedup;
DROP TABLE IF EXISTS r2_trunc_dedup;

CREATE TABLE r1_trunc_dedup
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_trunc_dedup', '1')
PARTITION BY dt
ORDER BY id;

CREATE TABLE r2_trunc_dedup
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_trunc_dedup', '2')
PARTITION BY dt
ORDER BY id;

INSERT INTO r1_trunc_dedup VALUES
    ('2024-01-01', 1, 10), ('2024-01-01', 2, 20),
    ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
SYSTEM SYNC REPLICA r2_trunc_dedup;

TRUNCATE TABLE r1_trunc_dedup;
SYSTEM SYNC REPLICA r2_trunc_dedup;

SELECT count() FROM r1_trunc_dedup;
SELECT count() FROM r2_trunc_dedup;

INSERT INTO r1_trunc_dedup VALUES ('2024-01-01', 1, 100), ('2024-01-01', 2, 200);
INSERT INTO r1_trunc_dedup VALUES ('2024-01-01', 1, 999);
SYSTEM SYNC REPLICA r2_trunc_dedup;

SELECT '--- r1 dedup after truncate ---';
SELECT * FROM r1_trunc_dedup ORDER BY dt, id;
SELECT '--- r2 dedup after truncate ---';
SELECT * FROM r2_trunc_dedup ORDER BY dt, id;

DROP TABLE r1_trunc_dedup;
DROP TABLE r2_trunc_dedup;

-- ===================================================================
-- DROP PARTITION ALL: equivalent to truncate via ALTER TABLE
-- ===================================================================
SELECT '--- replicated drop partition all ---';

DROP TABLE IF EXISTS r1_drop_all;
DROP TABLE IF EXISTS r2_drop_all;

CREATE TABLE r1_drop_all
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_drop_all', '1')
PARTITION BY dt
ORDER BY id;

CREATE TABLE r2_drop_all
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_drop_all', '2')
PARTITION BY dt
ORDER BY id;

INSERT INTO r1_drop_all VALUES
    ('2024-01-01', 1, 10), ('2024-01-02', 2, 20), ('2024-01-03', 3, 30);
SYSTEM SYNC REPLICA r2_drop_all;

ALTER TABLE r1_drop_all DROP PARTITION ALL;
SYSTEM SYNC REPLICA r2_drop_all;

SELECT count() FROM r1_drop_all;
SELECT count() FROM r2_drop_all;

INSERT INTO r1_drop_all VALUES ('2024-01-01', 1, 100);
INSERT INTO r1_drop_all VALUES ('2024-01-01', 1, 200);
SYSTEM SYNC REPLICA r2_drop_all;

SELECT '--- r1 after drop all ---';
SELECT * FROM r1_drop_all ORDER BY id;
SELECT '--- r2 after drop all ---';
SELECT * FROM r2_drop_all ORDER BY id;

DROP TABLE r1_drop_all;
DROP TABLE r2_drop_all;

-- ===================================================================
-- ATTACH PART: detach then attach with dedup on replicated table
-- ===================================================================
SELECT '--- replicated attach part: basic dedup ---';

DROP TABLE IF EXISTS r1_attach;
DROP TABLE IF EXISTS r2_attach;

CREATE TABLE r1_attach
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_attach', '1')
PARTITION BY dt
ORDER BY id;

CREATE TABLE r2_attach
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_attach', '2')
PARTITION BY dt
ORDER BY id;

INSERT INTO r1_attach VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
SYSTEM SYNC REPLICA r2_attach;

-- Detach on r1
ALTER TABLE r1_attach DETACH PARTITION '2024-01-01';
SYSTEM SYNC REPLICA r2_attach;
SELECT count() FROM r1_attach;
SELECT count() FROM r2_attach;

-- Insert new data with overlapping keys while partition is detached
INSERT INTO r1_attach VALUES ('2024-01-01', 1, 100), ('2024-01-01', 3, 300);
SYSTEM SYNC REPLICA r2_attach;

-- Attach the detached partition back; dedup should resolve conflicts
ALTER TABLE r1_attach ATTACH PARTITION '2024-01-01';
SYSTEM SYNC REPLICA r2_attach;

SELECT '--- r1 after attach ---';
SELECT * FROM r1_attach ORDER BY id;
SELECT '--- r2 after attach ---';
SELECT * FROM r2_attach ORDER BY id;

DROP TABLE r1_attach;
DROP TABLE r2_attach;

-- ===================================================================
-- ATTACH PART: attach into empty replicated table
-- ===================================================================
SELECT '--- replicated attach part: into empty table ---';

DROP TABLE IF EXISTS r1_attach_empty;
DROP TABLE IF EXISTS r2_attach_empty;

CREATE TABLE r1_attach_empty
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_attach_empty', '1')
PARTITION BY dt
ORDER BY id;

CREATE TABLE r2_attach_empty
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_90001/r_attach_empty', '2')
PARTITION BY dt
ORDER BY id;

INSERT INTO r1_attach_empty VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
SYSTEM SYNC REPLICA r2_attach_empty;

ALTER TABLE r1_attach_empty DETACH PARTITION '2024-01-01';
SYSTEM SYNC REPLICA r2_attach_empty;
SELECT count() FROM r1_attach_empty;

ALTER TABLE r1_attach_empty ATTACH PARTITION '2024-01-01';
SYSTEM SYNC REPLICA r2_attach_empty;

SELECT '--- r1 after re-attach ---';
SELECT * FROM r1_attach_empty ORDER BY id;
SELECT '--- r2 after re-attach ---';
SELECT * FROM r2_attach_empty ORDER BY id;

-- Verify dedup still works after attach
INSERT INTO r1_attach_empty VALUES ('2024-01-01', 1, 999);
SYSTEM SYNC REPLICA r2_attach_empty;

SELECT '--- r1 dedup after attach ---';
SELECT * FROM r1_attach_empty ORDER BY id;
SELECT '--- r2 dedup after attach ---';
SELECT * FROM r2_attach_empty ORDER BY id;

DROP TABLE r1_attach_empty;
DROP TABLE r2_attach_empty;
