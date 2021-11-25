select referring_site, sum(visitors) as visitors
from spectrum_usage_referrer
where date between '2021-09-01' and '2021-10-31'
group by referring_site
order by sum(visitors) desc;

select browser_type, sum(visitors) as visitors
from spectrum_usage_browser
where date between '2021-09-01' and '2021-10-31'
group by browser_type
order by sum(visitors) desc;

select os_type, sum(visitors) as visitors
from spectrum_usage_os
where date between '2021-09-01' and '2021-10-31'
group by os_type
order by visitors desc;

select hour, sum(visitors) as visitors
from spectrum_usage_hour
where date between '2021-09-01' and '2021-10-31'
group by hour
order by hour;

select country, sum(visitors) as visitors
from spectrum_usage_geo
where date between '2021-09-01' and '2021-10-31'
group by country
order by visitors desc;

select x.region, sum(x.visitors) as visotors
from
  (
    select x.country, x.visitors, c.cov_spectrum_region as region
    from
      (
        select country, sum(visitors) as visitors
        from spectrum_usage_geo
        where date between '2021-09-01' and '2021-10-31'
        group by country
        order by visitors desc
      ) x
      left join spectrum_country_mapping c on x.country = c.cov_spectrum_country
  ) x
group by x.region;

-- The number of unique visitors per day
-- Using the OS table returns the same unique visitors number as goaccess would do.
select
  count(*) number_day,
  avg(visitors) avg_visitors,
  percentile_cont(0.5) within group (order by visitors) as median_visitors
from
  (
    select
      date,
      sum(visitors) as visitors,
      sum(hits) as hits
    from spectrum_usage_os
    where date between '2021-09-01' and '2021-10-31'
    group by date
    order by date desc
  ) x;
