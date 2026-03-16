drop table if exists unique_merge_tree_insert_version;

CREATE TABLE IF NOT EXISTS unique_merge_tree_insert_version
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree('version')
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
