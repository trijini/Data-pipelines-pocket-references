create table if not exists order_summary_daily (
	order_date date,
	order_country varchar(10),
	total_revenue numeric,
	order_count int);

insert into order_summary_daily (order_date, order_country, total_revenue, order_count)
select
  o.OrderDate as order_date,
  c.CustomerCountry as order_country,
  sum(o.OrderTotal) as total_revenue,
  count(o.OrderId) as order_count
from Orders o
inner join Customers c on c.CustomerId = o.CustomerId
group by o.OrderDate, c.CustomerCountry;