CREATE TABLE pageviews_daily_3 AS
SELECT * FROM pageviews_daily;
-- another incorrect approach implementing incremental refresh
INSERT INTO pageviews_daily_3 (view_date, url_path, customer_country, view_count)
SELECT
	CAST(p.ViewTime as Date) AS view_date,
	p.UrlPath AS  url_path,
	c.CustomerCountry AS customer_country,
	COUNT(*) AS view_count
FROM PageViews p
LEFT JOIN Customers c ON c.CustomerId = p.CustomerId
WHERE p.ViewTime > '2020-06-02 12:00:00'
GROUP BY
	CAST(p.ViewTime as Date),
	p.UrlPath,
	c.CustomerCountry;

/* cannot recognize records with 2020-06-02 its because
 * the condition is checking what's only after 2020-06-02 12:00:00
 */
SELECT * FROM pageviews_daily_3
ORDER BY view_date, url_path, customer_country;