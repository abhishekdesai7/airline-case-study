import duckdb

# Connect to the DuckDB database
conn = duckdb.connect('warehouse/condor.duckdb')

# Define your SQL queries
queries = [
    "SELECT * FROM cfg.params;",
    "SELECT * FROM kpi.pacs_leg ORDER BY pacs_per_seat_leg DESC LIMIT 5;"
]

# Execute each query and print the results
for query in queries:
    result = conn.execute(query).fetchall()
    print(f"Results for query: {query}")
    for row in result:
        print(row)
    print("\n")  # Add a newline for better readability

# Close the connection
conn.close()
