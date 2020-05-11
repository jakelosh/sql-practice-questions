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
,tree_child as (
  select t1.node, t1.parent, count(t2.node) as child_ct
  from tree t1
  left join tree t2
  on t1.node = t2.parent
  group by t1.node, t1.parent
)
select node, 
       case
         when parent is null
           then 'root'
         when child_ct = 0
           then 'leaf'
         else
           'inner'
       end as label
from tree_child
order by node;
