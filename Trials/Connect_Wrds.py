import wrds

# Connect to WRDS
db = wrds.Connection(wrds_username='rvt2018', prompt=False)

# List all tables in the EDGAR library
print("Available tables in EDGAR library:")
print(db.list_tables(library='edgar'))

# Close connection
db.close()