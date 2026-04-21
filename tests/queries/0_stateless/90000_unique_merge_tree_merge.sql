-- Test: UniqueMergeTree merge dedup and delete mark propagation
--
-- Covers horizontal/vertical merge, write-version correctness,
-- multi-part merge propagation, sequential merges, and merge with version column.

-- ===================================================================
-- Horizontal merge
-- ===================================================================

select '--- test horizontal ---';

drop table if exists horizontal_upsert_table;
CREATE TABLE horizontal_upsert_table
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)ENGINE = UniqueMergeTree()
ORDER BY id;

insert into horizontal_upsert_table (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
insert into horizontal_upsert_table (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
update horizontal_upsert_table set value1=100 where id=1;
OPTIMIZE table horizontal_upsert_table final;
select * from horizontal_upsert_table order by id;
drop table if exists horizontal_upsert_table;

-- ===================================================================
-- Vertical merge
-- ===================================================================

select '--- test vertical ---';

drop table if exists vertical_upsert_table;
CREATE TABLE vertical_upsert_table
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)ENGINE = UniqueMergeTree()
ORDER BY id
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 0;

insert into vertical_upsert_table (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
insert into vertical_upsert_table (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);

update vertical_upsert_table set value1=100 where id=1;
OPTIMIZE table vertical_upsert_table final;
select * from vertical_upsert_table order by id;
drop table if exists vertical_upsert_table;

-- ===================================================================
-- Horizontal merge: write-version correctness
-- ===================================================================

select '--- test horizontal version ---';

drop table if exists horizontal_upsert_table_write_version;
CREATE TABLE horizontal_upsert_table_write_version
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)ENGINE = UniqueMergeTree()
ORDER BY id;

insert into horizontal_upsert_table_write_version (id, value1, value2) values (1,1,1);
insert into horizontal_upsert_table_write_version (id, value1, value2) values (2,2,2);
insert into horizontal_upsert_table_write_version (id, value1, value2) values (3,3,3);
-- Before merge: 3 SST entries across 3 parts
select count() from mergeTreeProjection(currentDatabase(), 'horizontal_upsert_table_write_version', '__unique_index');
optimize table horizontal_upsert_table_write_version final;
-- After merge: still 3 entries with distinct offsets
select count() from mergeTreeProjection(currentDatabase(), 'horizontal_upsert_table_write_version', '__unique_index');
select count(DISTINCT tupleElement(_unique_kv, 2)) from mergeTreeProjection(currentDatabase(), 'horizontal_upsert_table_write_version', '__unique_index');
drop table if exists horizontal_upsert_table_write_version;

-- ===================================================================
-- Vertical merge: write-version correctness
-- ===================================================================

select '--- test vertical version ---';

drop table if exists vertical_upsert_table_write_version;
CREATE TABLE vertical_upsert_table_write_version
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)ENGINE = UniqueMergeTree()
ORDER BY id
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 0;

insert into vertical_upsert_table_write_version (id, value1, value2) values (1,1,1);
insert into vertical_upsert_table_write_version (id, value1, value2) values (2,2,2);
insert into vertical_upsert_table_write_version (id, value1, value2) values (3,3,3);
-- Before merge: 3 SST entries across 3 parts
select count() from mergeTreeProjection(currentDatabase(), 'vertical_upsert_table_write_version', '__unique_index');
optimize table vertical_upsert_table_write_version final;
-- After merge: still 3 entries with distinct offsets
select count() from mergeTreeProjection(currentDatabase(), 'vertical_upsert_table_write_version', '__unique_index');
select count(DISTINCT tupleElement(_unique_kv, 2)) from mergeTreeProjection(currentDatabase(), 'vertical_upsert_table_write_version', '__unique_index');
drop table if exists vertical_upsert_table_write_version;

-- ===================================================================
-- Vertical merge: simple unique key
-- ===================================================================

select '--- test vertical (simple unique key) ---';

drop table if exists vertical_upsert_table_expr;
CREATE TABLE vertical_upsert_table_expr
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)ENGINE = UniqueMergeTree()
ORDER BY id
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 0;

insert into vertical_upsert_table_expr (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
insert into vertical_upsert_table_expr (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);

update vertical_upsert_table_expr set value1=100 where id=1;
OPTIMIZE table vertical_upsert_table_expr final;
select * from vertical_upsert_table_expr order by id;
drop table if exists vertical_upsert_table_expr;

-- ===================================================================
-- Merge propagation: multi-part insert then merge
-- ===================================================================
SELECT '--- merge propagation: basic ---';

DROP TABLE IF EXISTS unique_merge_propagation;

CREATE TABLE unique_merge_propagation
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Create multiple parts
INSERT INTO unique_merge_propagation SELECT number, 1 FROM numbers(10);
INSERT INTO unique_merge_propagation SELECT number, 2 FROM numbers(10);
INSERT INTO unique_merge_propagation SELECT number, 3 FROM numbers(10);

-- Before merge: all keys should have value=3 (last write wins)
SELECT count() FROM unique_merge_propagation;
SELECT * FROM unique_merge_propagation ORDER BY id;

-- Merge
OPTIMIZE TABLE unique_merge_propagation FINAL;

-- After merge: same result
SELECT count() FROM unique_merge_propagation;
SELECT * FROM unique_merge_propagation ORDER BY id;

-- Insert after merge: should correctly dedup against merged part
INSERT INTO unique_merge_propagation SELECT number, 4 FROM numbers(5);
SELECT count() FROM unique_merge_propagation;
SELECT * FROM unique_merge_propagation ORDER BY id;

DROP TABLE unique_merge_propagation;

-- ===================================================================
-- Merge propagation: sequential merges
-- ===================================================================
SELECT '--- merge propagation: sequential merges ---';

DROP TABLE IF EXISTS unique_merge_sequential;

CREATE TABLE unique_merge_sequential
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Round 1: insert and merge
INSERT INTO unique_merge_sequential SELECT number, 1 FROM numbers(10);
INSERT INTO unique_merge_sequential SELECT number, 2 FROM numbers(10);
OPTIMIZE TABLE unique_merge_sequential FINAL;
SELECT count() FROM unique_merge_sequential;

-- Round 2: insert more and merge again
INSERT INTO unique_merge_sequential SELECT number, 3 FROM numbers(10);
INSERT INTO unique_merge_sequential SELECT number + 10, 3 FROM numbers(10);
OPTIMIZE TABLE unique_merge_sequential FINAL;
SELECT count() FROM unique_merge_sequential;
SELECT * FROM unique_merge_sequential ORDER BY id;

DROP TABLE unique_merge_sequential;

-- ===================================================================
-- Merge propagation: with version column
-- ===================================================================
SELECT '--- merge propagation: version ---';

DROP TABLE IF EXISTS unique_merge_version;

CREATE TABLE unique_merge_version
(
    id UInt32,
    value UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique('version')
)
ENGINE = UniqueMergeTree()
ORDER BY id;

-- Insert with increasing versions
INSERT INTO unique_merge_version SELECT number, 1, 1 FROM numbers(10);
INSERT INTO unique_merge_version SELECT number, 2, 5 FROM numbers(10);
-- Insert with LOWER version (should NOT win)
INSERT INTO unique_merge_version SELECT number, 999, 2 FROM numbers(10);

SELECT count() FROM unique_merge_version;
SELECT id, value FROM unique_merge_version ORDER BY id;

OPTIMIZE TABLE unique_merge_version FINAL;

-- After merge: version 5 should still win
SELECT count() FROM unique_merge_version;
SELECT id, value FROM unique_merge_version ORDER BY id;

-- Insert with even higher version
INSERT INTO unique_merge_version SELECT number, 100, 10 FROM numbers(5);
SELECT id, value FROM unique_merge_version ORDER BY id;

DROP TABLE unique_merge_version;

-- ===================================================================
-- Merge propagation: with expression key
-- ===================================================================
SELECT '--- merge propagation: expression key ---';

DROP TABLE IF EXISTS unique_merge_expr;

CREATE TABLE unique_merge_expr
(
    a UInt32,
    b UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX a * 100 + b TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY a;

-- Insert rows with distinct expression keys
INSERT INTO unique_merge_expr VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30);
-- Upsert: a*100+b = 101 again, should overwrite
INSERT INTO unique_merge_expr VALUES (1, 1, 100);

SELECT count() FROM unique_merge_expr;
SELECT a, b, value FROM unique_merge_expr ORDER BY a, b;

-- Merge
OPTIMIZE TABLE unique_merge_expr FINAL;

SELECT count() FROM unique_merge_expr;
SELECT a, b, value FROM unique_merge_expr ORDER BY a, b;

-- Insert after merge
INSERT INTO unique_merge_expr VALUES (2, 1, 200), (3, 0, 300);
SELECT count() FROM unique_merge_expr;
SELECT a, b, value FROM unique_merge_expr ORDER BY a, b;

DROP TABLE unique_merge_expr;