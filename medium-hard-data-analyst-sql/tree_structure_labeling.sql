-- Presto
with tree as (
  select 1 as node, 2 as parent union all
  select 2, 5 union all
  select 3, 5 union all
  select 4, 3 union all
  select 5, null union all
  select 6, 4 union all
  select 7, 6
)
select *
from tree;
