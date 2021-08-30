-- The result will be in TIME format (hh:mm:ss) unlike the book
CREATE TABLE orders_time_to_ship (
	OrderId int,
	backordered_days time -- originally, the author used interval data type in PostgreSQL
);

INSERT INTO orders_time_to_ship (OrderId, backordered_days)
WITH o_backordered AS (
	SELECT OrderId, MIN(LastUpdated) AS first_backordered
	FROM Orders_cdc
	WHERE OrderStatus = 'Backordered'
	GROUP BY OrderId
), o_shipped AS (
	SELECT OrderId, MIN(LastUpdated ) AS first_shipped
	FROM Orders_cdc
	WHERE OrderStatus = 'Shipped'
	GROUP BY OrderId
)
SELECT 
	b.OrderId,
	TIMEDIFF(first_shipped, first_backordered) AS backordered_days
FROM o_backordered b
INNER JOIN o_shipped s ON s.OrderId = b.OrderId;

SELECT * FROM orders_time_to_ship;

-- average time it took for OrderStatus backordered to be shipped
SELECT SEC_TO_TIME(AVG(TIME_TO_SEC(backordered_days))) FROM orders_time_to_ship;