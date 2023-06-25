1.) PostgreSQL is used to write the SQL
2.) Because there is some duplicate data in employees table (based on employe_id) due to implementation of SCD 2, 
the data will be deduplicate by choosing the biggest salary for each employe_id
3.) Each timesheet row has checkin and checkout in the same day, 
Therefore, if checkin > checkout it will be considered as invalid or null,
and if checkin = checkout it will be considered as 0 working hour
4.) There is some data in timesheet table that has date before employee joining or after employee resigning (invalid date),
meaning that the employee dont even work in the company and the company dont have to pay salary,
Therefore, only include timesheet data with valid date, after join_date and before resign_date (only if employee resign)
5.) Only employee that has minimum 1 timesheet event with valid date (point 4) for each month & branch (invalid/zero working hour still counted)
will be counted for total salary calculation
6.) salary_per_hour is rounded into integer
7.) There is 2 schmea in database:
	di (data integration): to store raw data from csv
	dm (data mart): to store aggregate calculation data
8.) Source table is: di.timesheets and di.employees
9.) Target table is: dm.salary_per_hour
10.) Temporary table is used to update data to prevent table loss if the transformation is error 
because of fully overwritten update