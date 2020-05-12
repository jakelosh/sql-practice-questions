-- Presto
-- Part I
with distinct_monthly_users as (
  select 1 as user_id, date('2020-01-01') as month union all
  select 234, date('2020-01-01') union all
  select 3 , date('2020-01-01')  union all
  select 234 , date('2020-02-01')  union all
  select 4 , date('2020-02-01')  union all
  select 5 , date('2020-02-01')  union all
  select 6 , date('2020-03-01')  union all
  select 4 , date('2020-03-01')  union all
  select 5 , date('2020-03-01')  
)
,joined_monthly_users as (
  select mu_1.month, mu_1.user_id, mu_2.month as prev_month, mu_2.user_id as prev_user_id
  from   distinct_monthly_users mu_1
  inner join distinct_monthly_users mu_2
  on mu_1.user_id = mu_2.user_id
  and mu_1.month = mu_2.month + interval '1' month
)
select month, count(*) as retained_user_ct
from joined_monthly_users
group by month
order by month;


-- Part II
with logins as (
  select 1 as user_id, date('2020-01-01') as date union all
  select 234, date('2020-01-01') union all
  select 3 , date('2020-01-01')  union all
  select 234 , date('2020-02-01')  union all
  select 4 , date('2020-02-01')  union all
  select 5 , date('2020-02-01')  union all
  select 6 , date('2020-03-01')  union all
  select 4 , date('2020-03-01')  union all
  select 5 , date('2020-03-01')  
)
,churned_users as (
  select date_trunc('month', a.date) as month, a.user_id, date_trunc('month', b.date) as prev_month, b.user_id as prev_user_id
  from   logins a
  right join logins b
  on a.user_id = b.user_id
  and date_trunc('month', a.date) = date_trunc('month', b.date) + interval '1' month
)
select prev_month, count(prev_user_id) as churned_user_ct
from churned_users
where month is null
group by prev_month
order by prev_month;


-- Part III
with logins as (
  select 1 as user_id, date('2020-01-01') as date union all
  select 234, date('2020-01-01') union all
  select 3 , date('2020-01-01')  union all
  select 3 , date('2020-01-01')  union all
  select 234 , date('2020-02-01')  union all
  select 4 , date('2020-02-01')  union all
  select 5 , date('2020-02-01')  union all
  select 6 , date('2020-03-01')  union all
  select 6 , date('2020-03-01')  union all
  select 4 , date('2020-03-01')  union all
  select 5 , date('2020-03-01')  union all
  select 1 , date('2020-03-01')  union all
  select 3 , date('2020-04-01')  union all
  select 1 , date('2020-04-01')
)
,churned_users as (
  select date_trunc('month', a.date) month,
         a.user_id,
         max(date_trunc('month', b.date)) most_recent_active_previously
  from logins a
  inner join logins b on a.user_id = b.user_id 
        and  date_trunc('month', a.date) > date_trunc('month', b.date)
   group by date_trunc('month', a.date), a.user_id
)
select month, count(distinct user_id) reactivated_users
from churned_users
where month > most_recent_active_previously + interval '1' month
group by month
order by month;



