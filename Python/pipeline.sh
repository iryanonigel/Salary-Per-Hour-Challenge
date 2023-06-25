# Extract & load employees csv to pg (incrementally)
echo '--- Extract & load employees csv to pg (incrementally) ---'
python csv_to_pg.py --config employees.json

# Extract & load timesheets csv to pg (incrementally)
echo '--- Extract & load timesheets csv to pg (incrementally) ---'
python csv_to_pg.py --config timesheets.json

# Transform data into salary_per_hour table in pg (overwrite)
echo '--- Transform data into salary_per_hour table in pg (overwrite) ---'
python pg_to_pg.py --config salary_per_hour.json

read -p "Press Enter to exit..."