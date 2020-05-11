-- Presto
with monthly_users as (
  select date_format(date, '%Y%m') as month, count(distinct user_id) as active_ct
  from logins
  where start_d >= date '2020-01-01'
  group by date_format(start_d, '%Y%m')
  order by date_format(start_d, '%Y%m')
)
,window as (
  select mu_1.month, mu_1.active_ct, mu_2.active_ct as active_ct_lag
  from   monthly_users mu_1
  left join monthly_users mu_2
  on mu_1.month = cast(cast(mu_2.month as bigint)+1 as varchar)
)
select month, active_ct - active_ct_lag as mom_chg_mau, 1.00*active_ct/active_ct_lag - 1 as mom_pct_chg_mau
from window
order by month;
