INSERT INTO PageViews VALUES(102,'2020-06-02 12:03:42', '/home', 'NULL');
INSERT INTO PageViews VALUES(101,'2020-06-03 12:25:01', '/product/567','social');

CREATE TABLE pageviews_daily_2 AS
SELECT * FROM pageviews_daily;

-- showing incorrect approach to implement incremental refresh
INSERT INTO pageviews_daily_2 (view_date, url_path, customer_country, view_count)
SELECT
  CAST(p.ViewTime as Date) AS view_date,
  p.UrlPath AS url_path,
  c.CustomerCountry AS customer_country,
  COUNT(*) AS view_count
FROM PageViews p
LEFT JOIN Customers c ON c.CustomerId = p.CustomerId
WHERE
  p.ViewTime > (SELECT MAX(view_date) FROM pageviews_daily_2)
GROUP BY
  CAST(p.ViewTime as Date),
  p.UrlPath,
  c.CustomerCountry;

/*
 * problem occurs as ViewTime in PageViews table is in timestamp data type whereas
 * view_date in pageviews_daily table is in date data type.
 */
SELECT *
FROM pageviews_daily_2
ORDER BY view_date, url_path, customer_country;

SELECT view_date, SUM(view_count) AS daily_views
FROM pageviews_daily_2
GROUP BY view_date
ORDER BY view_date;

