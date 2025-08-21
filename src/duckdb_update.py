import duckdb

conn = duckdb.connect('warehouse/condor.duckdb')
with open('sql/05_params.sql', 'r') as file:
    sql_commands = file.read()

# Execute the SQL commands
conn.execute(sql_commands)
