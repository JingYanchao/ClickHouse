drop table if exists horizontal_upsert_table;
CREATE TABLE horizontal_upsert_table
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)ENGINE = UniqueMergeTree()
ORDER BY id;

select '--- test horizontal ---';
insert into horizontal_upsert_table (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
insert into horizontal_upsert_table (id, value1, value2) select number as id, number as value1, number as value2 from numbers(10);
update horizontal_upsert_table set value1=100 where id=1;
OPTIMIZE table horizontal_upsert_table final;
select * from horizontal_upsert_table order by id;
drop table if exists horizontal_upsert_table;

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
select _row_version from horizontal_upsert_table_write_version order by _row_version;
optimize table horizontal_upsert_table_write_version final;
select _row_version from horizontal_upsert_table_write_version order by _row_version;
drop table if exists horizontal_upsert_table_write_version;

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
select _row_version from vertical_upsert_table_write_version order by _row_version;
optimize table vertical_upsert_table_write_version final;
select _row_version from vertical_upsert_table_write_version order by _row_version;
drop table if exists vertical_upsert_table_write_version;

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
