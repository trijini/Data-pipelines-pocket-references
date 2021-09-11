CREATE TABLE Orders_Full (
  OrderId INT,
  OrderStatus VARCHAR(30),
  OrderDate TIMESTAMP,
  CustomerId INT,
  OrderTotal NUMERIC
);

INSERT INTO Orders_Full VALUES(1, 'Shipped', '2020-06-09', 100, 50.05);
INSERT INTO Orders_Full VALUES(2, 'Shipped', '2020-07-11', 101, 57.45);
INSERT INTO Orders_Full VALUES(3, 'Shipped', '2020-07-12', 102, 135.99);
INSERT INTO Orders_Full VALUES(4, 'Shipped', '2020-07-12', 100, 43.00);
