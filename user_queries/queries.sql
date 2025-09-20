-- latest snapshot by lane
WITH latest AS (
	SELECT MAX(valid_day) AS max_day FROM final.daily_lane_equipment_prices
)
SELECT
	CONCAT(origin_port_name, ' to ', destination_port_name) AS lane,
	median_price_usd,
	avg_price_usd
FROM final.daily_lane_equipment_prices, latest
WHERE valid_day = latest.max_day
ORDER BY median_price_usd DESC;

-- latest snapshot by region
WITH latest AS (
	SELECT MAX(valid_day) AS max_day FROM final.daily_lane_equipment_prices
)
SELECT
	origin_region_name,
	destination_region_name,
	COUNT(*) AS lane_count,
	AVG(avg_price_usd) AS avg_of_avg_price,
	AVG(median_price_usd) AS avg_of_median_price
FROM final.daily_lane_equipment_prices, latest
WHERE valid_day = latest.max_day
GROUP BY 1,2
ORDER BY avg_of_median_price DESC;

-- Daily average by lane (for a selected lane):
SELECT
	valid_day,
	AVG(avg_price_usd) AS daily_avg_price
FROM final.daily_lane_equipment_prices
WHERE origin_port_name = 'Shanghai'
	AND destination_port_name = 'Long Beach, CA'
GROUP BY valid_day
ORDER BY valid_day;

-- Weekly average trend by region pair:
SELECT
	DATE_TRUNC('week', valid_day) AS week_start,
	origin_region_name,
	destination_region_name,
	AVG(avg_price_usd) AS weekly_avg_price
FROM final.daily_lane_equipment_prices
GROUP BY 1,2,3
ORDER BY week_start, weekly_avg_price desc, origin_region_name, destination_region_name;

-- Month-over-month change by lane:
WITH monthly AS (
SELECT
	DATE_TRUNC('month', valid_day) AS month,
	origin_port_name,
	destination_port_name,
	AVG(avg_price_usd) AS monthly_avg_price
FROM final.daily_lane_equipment_prices
GROUP BY 1,2,3
),
lagged AS (
SELECT
	month,
	origin_port_name,
	destination_port_name,
	monthly_avg_price,
	LAG(monthly_avg_price) OVER (PARTITION BY origin_port_name, destination_port_name ORDER BY month) AS prev_month_price
FROM monthly
)
SELECT
	month,
	CONCAT(origin_port_name, ' to ', destination_port_name) AS lane,
	monthly_avg_price,
	prev_month_price,
	CASE
	WHEN prev_month_price IS NULL THEN NULL
	WHEN prev_month_price = 0 THEN NULL
	ELSE (monthly_avg_price - prev_month_price) / prev_month_price
	END AS mom_change_pct
FROM lagged
ORDER BY month, lane;

-- Lane-level monthly price table:
SELECT
	DATE_TRUNC('month', valid_day) AS month,
	CONCAT(origin_port_name, ' to ', destination_port_name) AS lane,
	AVG(avg_price_usd) AS avg_price,
	AVG(median_price_usd) AS median_price
FROM final.daily_lane_equipment_prices
GROUP BY 1,2
ORDER BY month, lane;

-- Latest-day lane leaderboard:
WITH latest AS (SELECT MAX(valid_day) AS max_day FROM final.daily_lane_equipment_prices)
SELECT
	CONCAT(origin_port_name, ' to ', destination_port_name) AS lane,
	avg_price_usd,
	median_price_usd
FROM final.daily_lane_equipment_prices, latest
WHERE valid_day = latest.max_day
ORDER BY median_price_usd DESC;

-- Monthly region pair averages:
SELECT
	DATE_TRUNC('month', valid_day) AS month,
	origin_region_name,
	destination_region_name,
	AVG(avg_price_usd) AS avg_price
FROM final.daily_lane_equipment_prices
GROUP BY 1,2,3
ORDER BY month, origin_region_name, destination_region_name;

-- Top origin regions into a specific destination region over last 90 days:
WITH recent AS (
	SELECT MAX(valid_day) AS max_day FROM final.daily_lane_equipment_prices
),
win AS (
	SELECT DATE_ADD(max_day, -90) AS start_day, max_day FROM recent
)
SELECT
	origin_region_name,
	AVG(avg_price_usd) AS avg_price
FROM final.daily_lane_equipment_prices, win
WHERE valid_day BETWEEN win.start_day AND win.max_day
	AND destination_region_name like 'Los Angeles Ports'
GROUP BY origin_region_name;

-- Equipment view
-- Price differences by equipment across lanes:
SELECT
	EQUIPMENT_ID,
	AVG(avg_price_usd) AS avg_price
FROM final.daily_lane_equipment_prices
GROUP BY EQUIPMENT_ID
ORDER BY avg_price DESC;

-- Lane x equipment pivot (aggregated):
SELECT
	origin_port_name,
	destination_port_name,
	EQUIPMENT_ID,
AVG(avg_price_usd) AS avg_price
FROM final.daily_lane_equipment_prices
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Top 10 lanes by average price over the full period:
SELECT
	CONCAT(origin_port_name, ' to ', destination_port_name) AS lane,
	AVG(avg_price_usd) AS avg_price
FROM final.daily_lane_equipment_prices
GROUP BY lane
ORDER BY avg_price DESC
LIMIT 10;

-- Most volatile lanes (highest standard deviation of daily avg):
SELECT
	CONCAT(origin_port_name, ' to ', destination_port_name) AS lane,
	STDDEV_POP(avg_price_usd) AS price_volatility
FROM final.daily_lane_equipment_prices
GROUP BY lane
ORDER BY price_volatility DESC
LIMIT 10;

-- Data quality views
-- Share of dq_ok = false by lane:
SELECT
	CONCAT(origin_port_name, ' to ', destination_port_name) AS lane,
	AVG(CASE WHEN dq_ok THEN 0 ELSE 1 END) AS bad_data_rate
FROM final.daily_lane_equipment_prices
GROUP BY lane
ORDER BY bad_data_rate DESC;

-- Rows with missing key price fields:
SELECT *
FROM final.daily_lane_equipment_prices
WHERE avg_price_usd IS NULL
	OR median_price_usd IS NULL
LIMIT 100;
