1.) PostgreSQL is used for database
2.) ELT approach is used to process the data. Therefore, aggregate transformation is done in database.
Raw table stored in database could be use for further aggregate transformation if needed
3.) There is 2 main process:
- Upload all csv data to postgres incrementally
- Transform data with aggregation query in postgres
4.) Employees table contains some duplicate based on employe_id due to implementation of SCD 2.
Therefore, it is better to read full data to do deduplication based on the latest ingest file and the biggest salary
5.) Overwrite method is used to update data for aggregate transformation
6.) There is 2 schmea in database:
	di (data integration): to store raw data from csv
	dm (data mart): to store aggregate calculation data
7.) CSV file has consistent name format, so it can be ordered from old to latest file name
example:
- employees_20230530.csv (oldest)
- employees_20230531.csv
- employees_20230601.csv
- employees_20230602.csv
- employees_20230603.csv (latest)
8.) CSV file also has the same structure to the destination table, so it can be ingest to destination table
9.) If the code fail to process the data (csv) today because server down, 
it will process the data (csv) again tomorrow by checking the latest ingest csv in destination table
10.) If there is no new data in folder, then the code will not process csv file