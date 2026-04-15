-- Test: UniqueMergeTree miscellaneous tests
--
-- Covers version column semantics and multi-column type support.

-- ===================================================================
-- Version: lower version should NOT overwrite higher version
-- ===================================================================
SELECT '--- version: lower does not overwrite ---';

DROP TABLE IF EXISTS umt_version_order;

CREATE TABLE umt_version_order
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert with version=5
INSERT INTO umt_version_order SELECT number, 500, 5 FROM numbers(10);
-- Try to overwrite with version=1 (should fail — lower version)
INSERT INTO umt_version_order SELECT number, 100, 1 FROM numbers(10);
-- Try to overwrite with version=3 (should fail — still lower)
INSERT INTO umt_version_order SELECT number, 300, 3 FROM numbers(10);

SELECT count() FROM umt_version_order;
SELECT id, value FROM umt_version_order ORDER BY id;

-- Now overwrite with version=10 (should succeed — higher version)
INSERT INTO umt_version_order SELECT number, 1000, 10 FROM numbers(5);

SELECT id, value FROM umt_version_order ORDER BY id;

DROP TABLE umt_version_order;

-- ===================================================================
-- Version: across merge
-- ===================================================================
SELECT '--- version: across merge ---';

DROP TABLE IF EXISTS umt_version_merge;

CREATE TABLE umt_version_merge
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert with mixed versions across parts
INSERT INTO umt_version_merge SELECT number, 100, 1 FROM numbers(10);
INSERT INTO umt_version_merge SELECT number, 500, 5 FROM numbers(10);
INSERT INTO umt_version_merge SELECT number, 300, 3 FROM numbers(10);

-- Before merge: version 5 should win
SELECT id, value FROM umt_version_merge ORDER BY id;

OPTIMIZE TABLE umt_version_merge FINAL;

-- After merge: same result
SELECT id, value FROM umt_version_merge ORDER BY id;

-- Insert with version=10 after merge
INSERT INTO umt_version_merge SELECT number, 1000, 10 FROM numbers(5);
SELECT id, value FROM umt_version_merge ORDER BY id;

DROP TABLE umt_version_merge;

-- ===================================================================
-- Version: with restart (DETACH/ATTACH)
-- ===================================================================
SELECT '--- version: restart ---';

DROP TABLE IF EXISTS umt_version_restart;

CREATE TABLE umt_version_restart
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_version_restart SELECT number, 500, 5 FROM numbers(10);
INSERT INTO umt_version_restart SELECT number, 100, 1 FROM numbers(10);

-- Before restart
SELECT id, value FROM umt_version_restart ORDER BY id;

DETACH TABLE umt_version_restart;
ATTACH TABLE umt_version_restart;

-- After restart: version 5 should still win
SELECT id, value FROM umt_version_restart ORDER BY id;

-- Insert with higher version after restart
INSERT INTO umt_version_restart SELECT number, 999, 10 FROM numbers(3);
SELECT id, value FROM umt_version_restart ORDER BY id;

DROP TABLE umt_version_restart;

-- ===================================================================
-- Version: equal version tiebreak (later insert wins)
-- ===================================================================
SELECT '--- version: equal version tiebreak ---';

DROP TABLE IF EXISTS umt_version_tiebreak;

CREATE TABLE umt_version_tiebreak
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Two inserts with the same version — later insert (higher max_block) should win
INSERT INTO umt_version_tiebreak SELECT number, 100, 5 FROM numbers(10);
INSERT INTO umt_version_tiebreak SELECT number, 200, 5 FROM numbers(10);

SELECT id, value FROM umt_version_tiebreak ORDER BY id;

DROP TABLE umt_version_tiebreak;

-- ===================================================================
-- Multi-column type: IPv4
-- ===================================================================

drop table if exists test_ipv4;
drop table if exists test_ipv4_upsert;
CREATE TABLE test_ipv4
(
    `x` IPv4,
    `y` Int32
)ENGINE = MergeTree
ORDER BY (x,y);

CREATE TABLE test_ipv4_upsert
(
    `x` IPv4,
    `y` Int32,
    PROJECTION __unique_index INDEX x, y TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY (x,y);
INSERT INTO test_ipv4 SELECT * FROM generateRandom('x IPv4, y Int32', 1, 10, 2) LIMIT 99999;
INSERT INTO test_ipv4_upsert SELECT * FROM test_ipv4;
select count(*) from test_ipv4_upsert;
INSERT INTO test_ipv4_upsert SELECT * FROM test_ipv4;
select count(*) from test_ipv4_upsert;

-- ===================================================================
-- Multi-column type: UUID
-- ===================================================================

drop table if exists test_uuid;
drop table if exists test_uuid_upsert;
CREATE TABLE test_uuid
(
    `x` UUID,
    `y` Int32
)ENGINE = MergeTree
ORDER BY (x,y);

CREATE TABLE test_uuid_upsert
(
    `x` UUID,
    `y` Int32,
    PROJECTION __unique_index INDEX x, y TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY (x,y);

INSERT INTO test_uuid SELECT * FROM generateRandom('x UUID, y Int32', 1, 10, 2) LIMIT 100002;
INSERT INTO test_uuid_upsert SELECT * FROM test_uuid;
select count(*) from test_uuid_upsert;
INSERT INTO test_uuid_upsert SELECT * FROM test_uuid;
select count(*) from test_uuid_upsert;
