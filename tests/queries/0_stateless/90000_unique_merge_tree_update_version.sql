drop table if exists unique_merge_tree_update_version;

CREATE TABLE IF NOT EXISTS unique_merge_tree_update_version
(
    id UInt32,
    value1 UInt32,
    value2 UInt32,
    version UInt64,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree('version')
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
