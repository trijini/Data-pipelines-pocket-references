create table Orders (
	OrderId int,
	OrderStatus varchar(30),
	OrderDate timestamp,
	CustomerId int,
	OrderTotal decimal(5, 2)
);

insert into Orders values(1, 'Shipped', '2020-06-09', 100, 50.05);
insert into Orders values(2, 'Shipped', '2020-07-11', 101, 57.45);
insert into Orders values(3, 'Shipped', '2020-07-12', 102, 135.99);
insert into Orders values(4, 'Shipped', '2020-07-12', 100, 43.00);