-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree TTL behavior
--
-- `UniqueMergeTree` (replicated or not) forces `ttl_only_drop_parts = true`
-- so that TTL removes whole parts rather than individual rows. This test
-- mirrors 04006_unique_merge_tree_ttl.sql on a replicated pair and
-- verifies the TTL result replicates correctly.

-- ===================================================================
-- Whole-part TTL drop, visible on both replicas
-- ===================================================================
SELECT '--- whole-part TTL drop ---';

DROP TABLE IF EXISTS r1_ttl;
DROP TABLE IF EXISTS r2_ttl;

CREATE TABLE r1_ttl
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl', '1')
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

CREATE TABLE r2_ttl
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl', '2')
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

-- Prevent background TTL merge from dropping expired parts before we observe them.
SYSTEM STOP TTL MERGES r1_ttl;
SYSTEM STOP TTL MERGES r2_ttl;

-- Fully-expired part and fully-fresh part on r1; replicate to r2.
INSERT INTO r1_ttl SELECT number, number, now() - INTERVAL 10 DAY FROM numbers(10);
INSERT INTO r1_ttl SELECT number + 100, number + 100, now() + INTERVAL 10 DAY FROM numbers(5);
SYSTEM SYNC REPLICA r2_ttl;

SELECT 'r1 before TTL:', count() FROM r1_ttl;
SELECT 'r2 before TTL:', count() FROM r2_ttl;

SYSTEM START TTL MERGES r1_ttl;
SYSTEM START TTL MERGES r2_ttl;
ALTER TABLE r1_ttl MATERIALIZE TTL SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA r2_ttl;

-- Expired part dropped wholesale; fresh part kept as-is.
SELECT 'r1 after TTL:', count() FROM r1_ttl;
SELECT 'r2 after TTL:', count() FROM r2_ttl;
SELECT 'r1 rows:';
SELECT id, value FROM r1_ttl ORDER BY id;
SELECT 'r2 rows:';
SELECT id, value FROM r2_ttl ORDER BY id;

DROP TABLE r1_ttl;
DROP TABLE r2_ttl;

-- ===================================================================
-- Mixed part: with `ttl_only_drop_parts = true` the part is not
-- eligible for drop and is not rewritten either, so all rows remain.
-- Verify on both replicas.
-- ===================================================================
SELECT '--- mixed part: expired rows preserved ---';

DROP TABLE IF EXISTS r1_ttl_mixed;
DROP TABLE IF EXISTS r2_ttl_mixed;

CREATE TABLE r1_ttl_mixed
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl_mixed', '1')
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

CREATE TABLE r2_ttl_mixed
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl_mixed', '2')
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

-- Prevent background TTL merge from dropping parts before we observe them.
SYSTEM STOP TTL MERGES r1_ttl_mixed;
SYSTEM STOP TTL MERGES r2_ttl_mixed;

-- Single part with a mix of expired and fresh rows.
INSERT INTO r1_ttl_mixed SELECT
    number,
    number,
    if(number % 2 = 0, now() - INTERVAL 10 DAY, now() + INTERVAL 10 DAY)
FROM numbers(10);
SYSTEM SYNC REPLICA r2_ttl_mixed;

SELECT 'r1 before TTL:', count() FROM r1_ttl_mixed;
SELECT 'r2 before TTL:', count() FROM r2_ttl_mixed;

SYSTEM START TTL MERGES r1_ttl_mixed;
SYSTEM START TTL MERGES r2_ttl_mixed;
ALTER TABLE r1_ttl_mixed MATERIALIZE TTL SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA r2_ttl_mixed;

-- All rows remain on both replicas because the part is not fully expired.
SELECT 'r1 after TTL:', count() FROM r1_ttl_mixed;
SELECT 'r2 after TTL:', count() FROM r2_ttl_mixed;
SELECT id, value FROM r1_ttl_mixed ORDER BY id;
SELECT id, value FROM r2_ttl_mixed ORDER BY id;

DROP TABLE r1_ttl_mixed;
DROP TABLE r2_ttl_mixed;

-- ===================================================================
-- Insert still works after TTL, and dedup still works afterwards
-- ===================================================================
SELECT '--- insert after TTL ---';

DROP TABLE IF EXISTS r1_ttl_insert;
DROP TABLE IF EXISTS r2_ttl_insert;

CREATE TABLE r1_ttl_insert
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl_insert', '1')
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

CREATE TABLE r2_ttl_insert
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl_insert', '2')
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

-- Prevent background TTL merge from dropping expired parts before we observe them.
SYSTEM STOP TTL MERGES r1_ttl_insert;
SYSTEM STOP TTL MERGES r2_ttl_insert;

INSERT INTO r1_ttl_insert SELECT number, number, now() - INTERVAL 10 DAY FROM numbers(10);
SYSTEM SYNC REPLICA r2_ttl_insert;

SYSTEM START TTL MERGES r1_ttl_insert;
SYSTEM START TTL MERGES r2_ttl_insert;
ALTER TABLE r1_ttl_insert MATERIALIZE TTL SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA r2_ttl_insert;

SELECT 'r1 after TTL drop:', count() FROM r1_ttl_insert;
SELECT 'r2 after TTL drop:', count() FROM r2_ttl_insert;

-- Fresh inserts after TTL, on r2 to exercise replication both ways.
INSERT INTO r2_ttl_insert VALUES (1, 10, now() + INTERVAL 10 DAY);
INSERT INTO r2_ttl_insert VALUES (2, 20, now() + INTERVAL 10 DAY);
SYSTEM SYNC REPLICA r1_ttl_insert;

SELECT 'r1 after fresh insert:', count() FROM r1_ttl_insert;
SELECT 'r2 after fresh insert:', count() FROM r2_ttl_insert;

-- Upsert the same id on r1 to verify dedup replicates.
INSERT INTO r1_ttl_insert VALUES (1, 999, now() + INTERVAL 10 DAY);
SYSTEM SYNC REPLICA r2_ttl_insert;
OPTIMIZE TABLE r1_ttl_insert FINAL;
SYSTEM SYNC REPLICA r2_ttl_insert;

SELECT 'r1 after upsert:', count() FROM r1_ttl_insert;
SELECT id, value FROM r1_ttl_insert ORDER BY id;
SELECT 'r2 after upsert:', count() FROM r2_ttl_insert;
SELECT id, value FROM r2_ttl_insert ORDER BY id;

DROP TABLE r1_ttl_insert;
DROP TABLE r2_ttl_insert;

-- ===================================================================
-- Partitioned TTL: expired partitions dropped on both replicas
-- ===================================================================
SELECT '--- partitioned TTL ---';

DROP TABLE IF EXISTS r1_ttl_part;
DROP TABLE IF EXISTS r2_ttl_part;

CREATE TABLE r1_ttl_part
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl_part', '1')
PARTITION BY toYYYYMM(event_time)
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

CREATE TABLE r2_ttl_part
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/r_ttl_part', '2')
PARTITION BY toYYYYMM(event_time)
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

-- Prevent background TTL merge from dropping expired parts before we observe them.
SYSTEM STOP TTL MERGES r1_ttl_part;
SYSTEM STOP TTL MERGES r2_ttl_part;

-- Old partition entirely expired.
INSERT INTO r1_ttl_part SELECT number, number, toDateTime('2000-01-15 00:00:00') FROM numbers(5);
-- Fresh partition far in the future.
INSERT INTO r1_ttl_part SELECT number + 100, number + 100, now() + INTERVAL 10 DAY FROM numbers(5);
SYSTEM SYNC REPLICA r2_ttl_part;

SELECT 'r1 before TTL:', count() FROM r1_ttl_part;
SELECT 'r2 before TTL:', count() FROM r2_ttl_part;

SYSTEM START TTL MERGES r1_ttl_part;
SYSTEM START TTL MERGES r2_ttl_part;
ALTER TABLE r1_ttl_part MATERIALIZE TTL SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA r2_ttl_part;

SELECT 'r1 after TTL:', count() FROM r1_ttl_part;
SELECT 'r2 after TTL:', count() FROM r2_ttl_part;
SELECT id, value FROM r1_ttl_part ORDER BY id;
SELECT id, value FROM r2_ttl_part ORDER BY id;

-- Upsert on r2 to verify dedup + replication after TTL drop.
INSERT INTO r2_ttl_part VALUES (100, 9999, now() + INTERVAL 10 DAY);
SYSTEM SYNC REPLICA r1_ttl_part;
OPTIMIZE TABLE r2_ttl_part FINAL;
SYSTEM SYNC REPLICA r1_ttl_part;

SELECT 'r1 after upsert:', count() FROM r1_ttl_part;
SELECT id, value FROM r1_ttl_part ORDER BY id;
SELECT 'r2 after upsert:', count() FROM r2_ttl_part;
SELECT id, value FROM r2_ttl_part ORDER BY id;

DROP TABLE r1_ttl_part;
DROP TABLE r2_ttl_part;
