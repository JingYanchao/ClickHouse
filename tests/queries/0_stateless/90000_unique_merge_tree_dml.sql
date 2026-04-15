-- Test: UniqueMergeTree basic DML operations
--
-- Covers INSERT (with and without version), UPDATE (with and without version).

-- ===================================================================
-- INSERT: basic dedup
-- ===================================================================

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

-- ===================================================================
-- INSERT with version column
-- ===================================================================

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

-- ===================================================================
-- UPDATE: basic
-- ===================================================================

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

-- ===================================================================
-- UPDATE with version column
-- ===================================================================

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
