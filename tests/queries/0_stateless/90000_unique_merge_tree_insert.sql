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
