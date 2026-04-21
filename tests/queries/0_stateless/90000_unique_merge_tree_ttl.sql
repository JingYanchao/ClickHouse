-- Test: UniqueMergeTree TTL behavior
--
-- `UniqueMergeTree` forces `ttl_only_drop_parts = true` so that TTL
-- removes whole parts rather than individual rows (row-level delete
-- merges would rewrite rows at new positions and invalidate the
-- per-part dedup delete bitmap). This test verifies:
--   1. TTL drops whole parts (expired parts disappear, non-expired
--      parts keep all their rows intact).
--   2. A part that mixes expired and fresh rows is NOT row-rewritten;
--      all rows remain, which is the proof that TTL operates at part
--      granularity only.
--   3. New inserts after TTL still work and dedup still works.

-- ===================================================================
-- Whole-part TTL drop: an entirely-expired part is removed,
-- a fully-fresh part is kept as-is
-- ===================================================================
SELECT '--- whole-part TTL drop ---';

DROP TABLE IF EXISTS umt_ttl;

CREATE TABLE umt_ttl
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

-- Fully-expired part.
INSERT INTO umt_ttl SELECT number, number, now() - INTERVAL 10 DAY FROM numbers(10);
-- Fully-fresh part.
INSERT INTO umt_ttl SELECT number + 100, number + 100, now() + INTERVAL 10 DAY FROM numbers(5);

SELECT 'rows before TTL:', count() FROM umt_ttl;

ALTER TABLE umt_ttl MATERIALIZE TTL SETTINGS mutations_sync = 2;

-- Expired part dropped wholesale; fresh part kept as-is.
SELECT 'rows after TTL:', count() FROM umt_ttl;
SELECT id, value FROM umt_ttl ORDER BY id;

DROP TABLE umt_ttl;

-- ===================================================================
-- Key test: a part that mixes expired and fresh rows.
-- With `ttl_only_drop_parts = true`, the part has live rows so it is
-- NOT eligible for TTL drop; and the TTL mutation does not rewrite
-- the part to remove individual expired rows. All rows therefore
-- remain after MATERIALIZE TTL.
-- ===================================================================
SELECT '--- mixed part: expired rows preserved ---';

DROP TABLE IF EXISTS umt_ttl_mixed;

CREATE TABLE umt_ttl_mixed
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

-- Single part with a mix of expired (even ids) and fresh (odd ids) rows.
INSERT INTO umt_ttl_mixed SELECT
    number,
    number,
    if(number % 2 = 0, now() - INTERVAL 10 DAY, now() + INTERVAL 10 DAY)
FROM numbers(10);

SELECT 'rows before TTL:', count() FROM umt_ttl_mixed;

ALTER TABLE umt_ttl_mixed MATERIALIZE TTL SETTINGS mutations_sync = 2;

-- All 10 rows are preserved because the part is not fully expired.
SELECT 'rows after TTL:', count() FROM umt_ttl_mixed;
SELECT id, value FROM umt_ttl_mixed ORDER BY id;

DROP TABLE umt_ttl_mixed;

-- ===================================================================
-- Insert still works after TTL, and dedup still works afterwards
-- ===================================================================
SELECT '--- insert after TTL ---';

DROP TABLE IF EXISTS umt_ttl_insert;

CREATE TABLE umt_ttl_insert
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

INSERT INTO umt_ttl_insert SELECT number, number, now() - INTERVAL 10 DAY FROM numbers(10);
ALTER TABLE umt_ttl_insert MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT 'after TTL drop:', count() FROM umt_ttl_insert;

-- Fresh inserts after TTL.
INSERT INTO umt_ttl_insert VALUES (1, 10, now() + INTERVAL 10 DAY);
INSERT INTO umt_ttl_insert VALUES (2, 20, now() + INTERVAL 10 DAY);
SELECT 'after fresh insert:', count() FROM umt_ttl_insert;
SELECT id, value FROM umt_ttl_insert ORDER BY id;

-- Upsert the same id: dedup must still work.
INSERT INTO umt_ttl_insert VALUES (1, 999, now() + INTERVAL 10 DAY);
OPTIMIZE TABLE umt_ttl_insert FINAL SETTINGS mutations_sync = 1;
SELECT 'after upsert:', count() FROM umt_ttl_insert;
SELECT id, value FROM umt_ttl_insert ORDER BY id;

DROP TABLE umt_ttl_insert;

-- ===================================================================
-- Partitioned TTL: expired partitions dropped, non-expired kept;
-- dedup within surviving partitions still holds
-- ===================================================================
SELECT '--- partitioned TTL ---';

DROP TABLE IF EXISTS umt_ttl_part;

CREATE TABLE umt_ttl_part
(
    id UInt32,
    value UInt32,
    event_time DateTime,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY id
TTL event_time + INTERVAL 1 DAY;

-- Old partition entirely expired.
INSERT INTO umt_ttl_part SELECT number, number, toDateTime('2000-01-15 00:00:00') FROM numbers(5);
-- Fresh partition far in the future.
INSERT INTO umt_ttl_part SELECT number + 100, number + 100, now() + INTERVAL 10 DAY FROM numbers(5);

SELECT 'before TTL:', count() FROM umt_ttl_part;

ALTER TABLE umt_ttl_part MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT 'after TTL:', count() FROM umt_ttl_part;
SELECT id, value FROM umt_ttl_part ORDER BY id;

-- Upsert one of the surviving ids to verify dedup still holds.
INSERT INTO umt_ttl_part VALUES (100, 9999, now() + INTERVAL 10 DAY);
OPTIMIZE TABLE umt_ttl_part FINAL SETTINGS mutations_sync = 1;

SELECT 'after upsert:', count() FROM umt_ttl_part;
SELECT id, value FROM umt_ttl_part ORDER BY id;

DROP TABLE umt_ttl_part;
