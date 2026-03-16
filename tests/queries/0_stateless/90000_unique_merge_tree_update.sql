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
