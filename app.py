from postgres import initialize_db, get_db, sample_data, empty_database

empty_database()
initialize_db()
sample_data()

output = get_db("select * from person")
print(output)