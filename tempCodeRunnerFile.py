def load_old_data():
     df.read_sql_query('''SELECT * FROM weather''', conn)
     first_date = df['date_time'][0]
     old_date = first_date - 3600 * 950
     return old_date