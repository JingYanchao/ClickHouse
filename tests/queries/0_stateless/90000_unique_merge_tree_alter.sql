drop table if exists unique_merge_tree_alter;

CREATE TABLE IF NOT EXISTS unique_merge_tree_alter
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

insert into unique_merge_tree_alter (id, value1, value2) select number as id, number as value1, number as value2 from numbers(5);
insert into unique_merge_tree_alter (id, value1, value2) select number + 5 as id, number as value1, number as value2 from numbers(5);
insert into unique_merge_tree_alter (id, value1, value2) select number + 10 as id, number as value1, number as value2 from numbers(5);
optimize table unique_merge_tree_alter final;
select _row_version from unique_merge_tree_alter order by id;
alter table unique_merge_tree_alter modify column value1 UInt64 settings mutations_sync = 2;
select _row_version from unique_merge_tree_alter order by id;

insert into unique_merge_tree_alter (id, value1, value2) select number as id, number as value1, number as value2 from numbers(5);
insert into unique_merge_tree_alter (id, value1, value2) select number + 5 as id, number as value1, number as value2 from numbers(5);
insert into unique_merge_tree_alter (id, value1, value2) select number + 10 as id, number as value1, number as value2 from numbers(5);
alter table unique_merge_tree_alter modify column value1 UInt64 settings mutations_sync = 2;
select id, value1, value2, _row_version from unique_merge_tree_alter order by id;
optimize table unique_merge_tree_alter final;
alter table unique_merge_tree_alter modify column value1 UInt128 settings mutations_sync = 2;
select id, value1, value2, _row_version from unique_merge_tree_alter order by id;
drop table if exists unique_merge_tree_alter;
