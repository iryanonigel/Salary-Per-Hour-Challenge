1.) PostgreSQL is used for database
2.) ELT approach is used to process the data. Therefore, aggregate transformation is done in database.
Raw table stored in database could be use for further aggregate transformation if needed
3.) There is 2 main process:
- Upload all csv data to postgres incrementally
- Transform data with aggregation query in postgres
4.) Employees table contains some duplicate based on employe_id due to implementation of SCD 2.
Therefore, it is better to read full data to do deduplication based on the latest ingest file and the biggest salary
5.) Overwrite method is used to update data for aggregate transformation