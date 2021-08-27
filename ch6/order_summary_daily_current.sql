create table order_summary_daily_pit(
	order_date date,
	order_country varchar(10),
	total_revenue decimal,
	order_count int
);

insert into order_summary_daily_pit (order_date, order_country, total_revenue, order_count)
with customer_pit as (
	select cs.CustomerId, o.OrderId, max(cs.LastUpdated) as max_update_date
	from Orders o
	inner join Customers_staging cs on cs.CustomerId = o.CustomerId and cs.LastUpdated <= o.OrderDate
	group by cs.CustomerId, o.OrderId
	)
select 
	o.OrderDate as order_date, 
	cs.CustomerCountry as order_country, 
	sum(o.OrderTotal) as total_revenue, 
	count(o.OrderID) as order_count
from Orders o
inner join customer_pit cp on cp.CustomerId = o.CustomerId and cp.OrderId = o.OrderId
inner join Customers_staging cs on cs.CustomerId = cp.CustomerId and cs.LastUpdated = cp.max_update_date
group by o.OrderDate, cs.CustomerCountry;