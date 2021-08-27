create table Customers_scd(
  CustomerId int,
  CustomerName varchar(20),
  CustomerCountry varchar(10),
  ValidFrom timestamp,
  Expired timestamp);

insert into Customers_scd values(100,'Jane','USA','2019-05-01 7:01:10','2020-06-20 8:15:34');
insert into Customers_scd values(100,'Jane','UK','2020-06-20 8:15:34','2030-12-31 23:59:59');