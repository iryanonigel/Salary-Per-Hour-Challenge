CREATE TABLE di.timesheets ( -- correspond with destination table (schema and table name)
	timesheet_id int,
	employee_id int,
	date date,
	checkin time,
	checkout time,
	ingesttime timestamp,
	ingest_from text
);