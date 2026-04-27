-- Test: UniqueMergeTree basic DML operations
--
-- Covers INSERT (with and without version), UPDATE (with and without version).
-- INSERT: basic dedup
drop table if exists unique_merge_tree_insert;

CREATE TABLE IF NOT EXISTS unique_merge_tree_insert
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

insert into unique_merge_tree_insert (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
select * from unique_merge_tree_insert order by id;

insert into unique_merge_tree_insert values(1, 1, 1) (2, 2, 2) (3, 3, 3);
select * from unique_merge_tree_insert order by id;

insert into unique_merge_tree_insert values(1, 100, 100);
select * from unique_merge_tree_insert order by id;

insert into unique_merge_tree_insert values(10, 10, 10);
insert into unique_merge_tree_insert values(20, 10, 10),(20, 11, 11);
insert into unique_merge_tree_insert values(20, 10, 10),(20, 11, 11);
insert into unique_merge_tree_insert values(21, 11, 11),(10, 10, 10),(21, 12, 12);
insert into unique_merge_tree_insert values(21, 11, 11),(10, 10, 10),(21, 12, 12);
select * from unique_merge_tree_insert order by id;

drop table if exists unique_merge_tree_insert;
-- INSERT with version column
drop table if exists unique_merge_tree_insert_version;

CREATE TABLE IF NOT EXISTS unique_merge_tree_insert_version
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

insert into unique_merge_tree_insert_version select number as id, number as value1, number as value2, 1 as version from numbers(10);
select `id`, `value1`, `value2` from unique_merge_tree_insert_version order by id;

insert into unique_merge_tree_insert_version values(1, 1, 1, 2) (2, 2, 2, 2) (3, 3, 3, 2);
select `id`, `value1`, `value2` from unique_merge_tree_insert_version order by id;

insert into unique_merge_tree_insert_version values(1, 100, 100, 3);
select `id`, `value1`, `value2` from unique_merge_tree_insert_version order by id;

insert into unique_merge_tree_insert_version values(10, 10, 10, 4);
insert into unique_merge_tree_insert_version values(20, 10, 10, 5),(20, 11, 11, 5);
insert into unique_merge_tree_insert_version values(20, 10, 10, 6),(20, 11, 11, 6);
insert into unique_merge_tree_insert_version values(21, 11, 11, 7),(10, 10, 10, 7),(21, 12, 12, 7);
insert into unique_merge_tree_insert_version values(21, 11, 11, 8),(10, 10, 10, 8),(21, 12, 12, 8);
select `id`, `value1`, `value2` from unique_merge_tree_insert_version order by id;

drop table if exists unique_merge_tree_insert_version;
-- INSERT with version: lower version should NOT overwrite higher version
drop table if exists unique_version_order;

CREATE TABLE unique_version_order
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert with version=5
INSERT INTO unique_version_order SELECT number, 500, 5 FROM numbers(10);
-- Try to overwrite with version=1 (should fail — lower version)
INSERT INTO unique_version_order SELECT number, 100, 1 FROM numbers(10);
-- Try to overwrite with version=3 (should fail — still lower)
INSERT INTO unique_version_order SELECT number, 300, 3 FROM numbers(10);

SELECT count() FROM unique_version_order;
SELECT id, value FROM unique_version_order ORDER BY id;

-- Now overwrite with version=10 (should succeed — higher version)
INSERT INTO unique_version_order SELECT number, 1000, 10 FROM numbers(5);

SELECT id, value FROM unique_version_order ORDER BY id;

DROP TABLE unique_version_order;
-- UPDATE: basic
drop table if exists unique_merge_tree_update;

CREATE TABLE IF NOT EXISTS unique_merge_tree_update
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

insert into unique_merge_tree_update (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
select * from unique_merge_tree_update order by id;

update unique_merge_tree_update set value1=100 where id=1;
select * from unique_merge_tree_update order by id;

update unique_merge_tree_update set value1=100, value2=200 where id=1;
select * from unique_merge_tree_update order by id;

update unique_merge_tree_update set value1=100 where id=100;
select * from unique_merge_tree_update order by id;

update unique_merge_tree_update set value1=105 where value1 = 5;
select * from unique_merge_tree_update order by id;

update unique_merge_tree_update set value1=value1+6 where value1 = 6;
select * from unique_merge_tree_update order by id;

update unique_merge_tree_update set value1=100, value2=200, id=100 where id=1; -- { serverError 36 }

drop table if exists unique_merge_tree_update;
-- UPDATE with version column
drop table if exists unique_merge_tree_update_version;

CREATE TABLE IF NOT EXISTS unique_merge_tree_update_version
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

insert into unique_merge_tree_update_version select number as id, number as value1, number as value2, 1 as version from numbers(10);
select `id`, `value1`, `value2` from unique_merge_tree_update_version order by id;

update unique_merge_tree_update_version set value1=100, version = 2 where id=1;
select `id`, `value1`, `value2` from unique_merge_tree_update_version order by id;

update unique_merge_tree_update_version set value1=100, value2=200, version = 3 where id=1;
select`id`, `value1`, `value2` from unique_merge_tree_update_version order by id;

update unique_merge_tree_update_version set value1=100, version = 4 where id=100;
select `id`, `value1`, `value2` from unique_merge_tree_update_version order by id;

update unique_merge_tree_update_version set value1=500, version = 1 where id=100;
select `id`, `value1`, `value2` from unique_merge_tree_update_version order by id;

update unique_merge_tree_update_version set value1=100, value2=200, id=100 where id=1; -- { serverError 36 }

drop table if exists unique_merge_tree_update_version;
-- INSERT with version: equal version tiebreak (later insert wins)
drop table if exists unique_version_tiebreak;

CREATE TABLE unique_version_tiebreak
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Two inserts with the same version — later insert (higher max_block) should win
INSERT INTO unique_version_tiebreak SELECT number, 100, 5 FROM numbers(10);
INSERT INTO unique_version_tiebreak SELECT number, 200, 5 FROM numbers(10);

SELECT id, value FROM unique_version_tiebreak ORDER BY id;

DROP TABLE unique_version_tiebreak;
-- INSERT: IPv4 unique key type
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

drop table test_ipv4;
drop table test_ipv4_upsert;
-- INSERT: UUID unique key type
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

drop table test_uuid;
drop table test_uuid_upsert;
-- INSERT: expression unique key

drop table if exists unique_expr_key_dml;

CREATE TABLE unique_expr_key_dml
(
    a UInt32,
    b UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX a * 100 + b TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY a;

INSERT INTO unique_expr_key_dml VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30);
SELECT a, b, value FROM unique_expr_key_dml ORDER BY a, b;

-- Upsert: a*100+b = 101 again, should overwrite
INSERT INTO unique_expr_key_dml VALUES (1, 1, 100);
SELECT a, b, value FROM unique_expr_key_dml ORDER BY a, b;

-- UPDATE on expression-key table
UPDATE unique_expr_key_dml SET value = 999 WHERE a = 2 AND b = 1;
SELECT a, b, value FROM unique_expr_key_dml ORDER BY a, b;

DROP TABLE unique_expr_key_dml;

-- TRUNCATE: write → truncate → re-insert should succeed without error

drop table if exists unique_truncate_dml;

CREATE TABLE unique_truncate_dml
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert initial data
INSERT INTO unique_truncate_dml SELECT number, number * 10, number * 100 FROM numbers(10);
SELECT count() FROM unique_truncate_dml;
SELECT * FROM unique_truncate_dml ORDER BY id;

-- Truncate the table
TRUNCATE TABLE unique_truncate_dml;
SELECT count() FROM unique_truncate_dml;

-- Re-insert same keys after truncate, should succeed
INSERT INTO unique_truncate_dml SELECT number, number * 20, number * 200 FROM numbers(5);
SELECT count() FROM unique_truncate_dml;
SELECT * FROM unique_truncate_dml ORDER BY id;

-- Upsert to verify dedup still works
INSERT INTO unique_truncate_dml VALUES (0, 999, 999);
SELECT * FROM unique_truncate_dml ORDER BY id;

DROP TABLE unique_truncate_dml;

-- TRUNCATE with version column: write → truncate → re-insert

drop table if exists unique_truncate_version_dml;

CREATE TABLE unique_truncate_version_dml
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert initial data with version=1
INSERT INTO unique_truncate_version_dml SELECT number, number * 10, number * 100, 1 FROM numbers(10);
SELECT count() FROM unique_truncate_version_dml;

-- Truncate the table
TRUNCATE TABLE unique_truncate_version_dml;
SELECT count() FROM unique_truncate_version_dml;

-- Re-insert same keys after truncate (version=1), should succeed
INSERT INTO unique_truncate_version_dml SELECT number, number * 20, number * 200, 1 FROM numbers(5);
SELECT count() FROM unique_truncate_version_dml;
SELECT id, value1, value2 FROM unique_truncate_version_dml ORDER BY id;

-- Overwrite with higher version
INSERT INTO unique_truncate_version_dml VALUES (0, 999, 999, 2);
SELECT id, value1, value2 FROM unique_truncate_version_dml ORDER BY id;

DROP TABLE unique_truncate_version_dml;
