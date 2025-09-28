-- Q1 Generate a report showing the top 3 months (2019–2024) where 
-- any city recorded the sharpest month-over-month decline in net_circulation.
-- @ TESTED


WITH a AS (
SELECT 
City_ID,
Month_year,
Net_Circulation,
LAG(Net_Circulation, 1)    	-- using Lag function to get previous year
OVER(PARTITION BY City_ID 		
ORDER BY Month_year)
 AS prev_Circulation
FROM fact_print_sales
)

-- finding the Percentage decline in net_circulation 
SELECT 
c.City as city_name,DATE_FORMAT(Month_year, '%Y-%m') 
AS Month,
a.Net_Circulation,
a.prev_Circulation,
(a.Net_Circulation - a.prev_Circulation) AS drop_in_copies,
CASE WHEN a.prev_Circulation > 0 
THEN ROUND(100.0 * (a.Net_Circulation - a.prev_Circulation) / a.prev_Circulation, 2)
ELSE NULL
END AS pct_change
FROM a
JOIN dim_city c ON a.City_ID = c.City_ID
WHERE a.prev_Circulation IS NOT NULL
ORDER BY pct_change ASC   
LIMIT 3;


-- Q2 Identify ad categories that contributed > 50% of total yearly ad revenue.
-- @TESTED ( here quarter is in text format hence using substring)
 
 -- //finding yearly ad revenue according to ad category
 
 with a as
(select ad_category,substring(quarter,1,4) as year,
sum(ad_revenue_inr) as yearly_ad_revenue 
from fact_ad_revenue 
group by substring(quarter,1,4),ad_category
order by ad_category,substring(quarter,1,4)),


b as(	-- // finding total yearly ad revenue 
Select substring(quarter,1,4) as year,
sum(ad_revenue_inr) as yearly_revenue
from fact_ad_revenue 
group by substring(quarter,1,4)
order by substring(quarter,1,4)),

-- // finding yearly revenue concentration according to category
c as 
(
select a.year,ad.standard_ad_category as category_name,a.yearly_ad_revenue 
as category_revenue
,b.yearly_revenue as total_revenue_year
,round(yearly_ad_revenue/yearly_revenue*100,2)
as pct_of_year_total from a join b 
on a.year=b.year
join dim_ad_category ad
on ad.ad_category_id=a.ad_category
)

-- // final result
select * from c 
where pct_of_year_total>50

-- Q3 For 2024, rank cities by print efficiency = net_circulation / copies_printed. Return top 5.
-- @ verified 2
-- // finding efficiency ratio and top 5 cities according to print efficiency for 2024

SELECT *,
DENSE_RANK() OVER
(ORDER BY efficiency_ratio_2024 DESC) AS ranks
FROM 
(
SELECT 
c.City as city_name,
SUM(s.Copies_Sold + s.Copies_Returned) 
AS copies_printed_2024,		-- // copies printed=copies sold + copies returned
SUM(s.net_circulation) 
AS net_circulation_2024,
ROUND(SUM(s.net_circulation) / SUM(s.Copies_Sold + s.Copies_Returned),4) 
AS efficiency_ratio_2024	-- print efficiency = net_circulation / copies_printed
FROM fact_print_sales s
JOIN dim_city c 
ON s.City_Id = c.City_Id
WHERE YEAR(s.month_year) = 2024
GROUP BY c.City_id, c.City
) AS a
LIMIT 5;


-- Q4 For each city, compute the change in internet penetration from Q1-2021 to Q4-2021 and
-- identify the city with the highest improvement.
-- TESTED (#VERIFIED)
-- //finding delta internet rate= internet_rate_q4_2021 − internet_rate_q1_2021
with a as 
(
select city_id,internet_penetration -- // internet_penetration_q4_2021
as internet_rate_q4_2021 
from fact_city_readiness 
where quarter in ("2021-Q4")),

b as (
select city_id,internet_penetration 
as internet_rate_q1_2021 	-- // internet_penetration_q1_2021
from fact_city_readiness 
where quarter in ("2021-Q1"))

-- // delta internet rate
select c.city as city_name,internet_rate_q1_2021,
internet_rate_q4_2021,
round((internet_rate_q4_2021-internet_rate_q1_2021),2)
 as delta_internet_rate 
 from a 
 join b
 on a.city_id=b.city_id 
 join dim_city c
 on a.city_id=c.city_id
order by delta_internet_rate desc


-- Q5 Find cities where both net_circulation and ad_revenue decreased every year from 2019 through 2024 
-- tested (not verified)
with a as ( -- circulation_data
select
t.city,
t.year,
t.yearly_net_circulation,
lag(t.yearly_net_circulation, 1) over (partition by t.city order by t.year) as prev_year_circulation,
case
when t.yearly_net_circulation < lag(t.yearly_net_circulation, 1) over (partition by t.city order by t.year) then 'Yes' else 'No'
end as is_declining_print
from (
select
c.city,
year(s.month_year) as year,
sum(s.net_circulation) as yearly_net_circulation
from fact_print_sales s
join dim_city c on s.city_id = c.city_id
group by c.city, year(s.month_year)
) t
),

b as ( -- edition_yr_city
select distinct
s.edition_id,
year(s.month_year) as year,
s.city_id
from fact_print_sales s
),

e as ( -- finding yearly ad revenue (edition-level)
select
ad.edition_id,
cast(substring(ad.quarter, 1, 4) as unsigned) as year,
round(sum(ad.ad_revenue_inr), 2) as revenue
from fact_ad_revenue ad
group by ad.edition_id, cast(substring(ad.quarter, 1, 4) as unsigned)
),

d as ( -- revenue_data
select
t.city,
t.year,
t.yearly_revenue,
lag(t.yearly_revenue, 1) 
over (partition by t.city order by t.year) 
as prev_year_revenue,
case
when t.yearly_revenue < lag(t.yearly_revenue, 1) 
over (partition by t.city order by t.year) 
then 'Yes' else 'No'
end as is_declining_revenue
from 
(
select
c.city,
e.year,
sum(e.revenue) as yearly_revenue
from e
join b on e.edition_id = b.edition_id and e.year = b.year
join dim_city c on c.city_id = b.city_id
group by c.city, e.year
) t
),

f as ( -- consistent_decline
select
a.city,
count(case when a.is_declining_print = 'Yes' then 1 end) as print_decline_count,
count(case when d.is_declining_revenue = 'Yes' then 1 end) as revenue_decline_count
from a
join d on a.city = d.city and a.year = d.year
where a.year between 2020 and 2024
group by a.city
)

select
f.city
from f
where f.print_decline_count = 5
and f.revenue_decline_count = 5
order by f.city;


 
 
 
 
 -- Q6 In 2021, identify the city with the highest digital readiness score but among the bottom 3 in digital pilot engagement.
-- readiness_score = AVG(smartphone_rate, internet_rate, literacy_rate)
-- Bottom 3 engagement uses the chosen engagement metric provided ( engagement_rate)
 -- Tested 
 
with a as -- finding readiness_score_2021
(
select 
city_id,
round(avg((literacy_rate + smartphone_penetration
 + internet_penetration) / 3), 2) 
as readiness_score_2021
from fact_city_readiness
where substring(quarter,1,4) = '2021'
group by city_id)
 

,b as -- finding engagement_metric_2021 (taking engagement rate)
(
select city_id, SUBSTRING(launch_month,1,4) as Year,
 round(100-avg_bounce_rate,2)  -- //engagement rate= 100-avg bounce rate
 as engagement_metric_2021 
from fact_digital_pilot 
where year(launch_month) = 2021
group by city_id, SUBSTRING(launch_month,1,4)
),

d as -- //ranking based on readiness score and engagement metric
(
select c.city,a.readiness_score_2021,
b.engagement_metric_2021,
DENSE_RANK() OVER(order by a.readiness_score_2021 desc) 
as readiness_rnk_desc,
DENSE_RANK() OVER(order by b.engagement_metric_2021)
 as engagement_rnk
from a  JOIN b 
on a.city_id = b.city_id
INNER JOIN dim_city c on b.city_id = c.city_id
)
-- // finding outliers
select * , 
case when readiness_rnk_desc in (1,2,3) and 
engagement_rnk in (1,2,3) 
then 'Yes' else 'No' end as is_outlier
from d

 