DROP TABLE IF EXISTS dm.salary_per_hour_temp;
CREATE TABLE dm.salary_per_hour_temp AS (
	-- employees table deduplication
	WITH unique_employee AS (
		SELECT
			e.*
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY employe_id ORDER BY salary DESC) AS num
			FROM 
				di.employees
		) e
		WHERE num = 1 -- get the biggest salary for each employe_id
	)
	-- historical joined data contains all required columns for aggregate calculation
	, hist_joined_data AS (
		SELECT
			t.*
			, CASE 
				WHEN t.checkout - t.checkin < '00:00:00' THEN NULL -- negative working hours is considered null
				ELSE t.checkout - t.checkin
			END AS total_hours
			, EXTRACT(YEAR FROM t.date) AS "year"
			, EXTRACT(MONTH FROM t.date) AS "month"	
			, e.branch_id 
			, e.salary
			, e.join_date
			, e.resign_date
		FROM
			di.timesheets t
		LEFT JOIN
			unique_employee e ON t.employee_id = e.employe_id
		WHERE
			(t."date" BETWEEN e.join_date AND e.resign_date) OR (t."date" >= e.join_date AND e.resign_date IS NULL) -- only include employee timesheet after join_date and before resign_date
	)
	-- total hours calculation and aggregation
	, total_hours AS (
		SELECT 
			"year"
			, "month"
			, branch_id
			, SUM(EXTRACT(EPOCH FROM total_hours)) / 3600 AS total_hours_float
		FROM
			hist_joined_data
		GROUP BY
			"year"
			, "month"
			, branch_id
	)
	-- total salary calculation and aggregation
	, total_salary AS (
		SELECT 
			"year"
			, "month"
			, branch_id
			, SUM(salary) AS total_salary
		FROM (
			SELECT
				DISTINCT 
				"year"
				, "month"
				, branch_id
				, employee_id
				, salary
			FROM
				hist_joined_data		
			) cjd
		GROUP BY
			"year"
			, "month"
			, branch_id
	)
	-- combine aggregation CTE for salary per hour calculation and output column 
	SELECT
		th."year"
		, th."month"
		, th.branch_id
		, ROUND(ts.total_salary / th.total_hours_float) AS salary_per_hour
		, CURRENT_TIMESTAMP AS ingesttime
	FROM
		total_hours th
	LEFT JOIN
		total_salary ts USING("year", "month", branch_id)
	ORDER BY
		th."year"
		, th."month"
		, th.branch_id
);
DROP TABLE IF EXISTS dm.salary_per_hour; 
ALTER TABLE dm.salary_per_hour_temp RENAME TO salary_per_hour;
	
