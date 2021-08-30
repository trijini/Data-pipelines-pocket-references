CREATE TABLE orders_current (
	order_status varchar(20),
	order_count int
);

INSERT INTO orders_current (order_status, order_count)
WITH o_latest AS (
	SELECT OrderId, MAX(LastUpdated) AS max_updated
	FROM Orders_cdc
	GROUP BY orderid
)
SELECT o.OrderStatus, COUNT(*) AS order_count
FROM Orders_cdc o
INNER JOIN o_latest ON o_latest.OrderId = o.OrderId
	AND o_latest.max_updated = o.LastUpdated
GROUP BY o.OrderStatus;

-- ignore counting eventType with delete
TRUNCATE TABLE orders_current;

INSERT INTO orders_current (order_status, order_count)
WITH o_latest AS (
	SELECT OrderId, MAX(LastUpdated) AS max_updated
	FROM Orders_cdc
	GROUP BY orderid
)
SELECT o.OrderStatus, COUNT(*) AS order_count
FROM Orders_cdc o
INNER JOIN o_latest ON o_latest.OrderId = o.OrderId
	AND o_latest.max_updated = o.LastUpdated
WHERE o.EventType <> 'delete' -- the only difference with the previous query
GROUP BY o.OrderStatus;