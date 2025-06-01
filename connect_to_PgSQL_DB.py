import psycopg2
from psycopg2 import sql
 
# Database connection parameters
db_params = {
    "host": "localhost",        # Database host
    "database": "postgres",   # Replace with your database name
    "user": "postgres",    # Replace with your database username
    "password": "1234"  # Replace with your database password
}
 
def connect_and_insert():
    try:
        # Establish the connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
 
        # Create table if it doesn't exist
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS auxo.tbl_Created_via_Python (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT
        );
        '''
        cursor.execute(create_table_query)
 
        # Insert data into the table
        insert_query = '''
        INSERT INTO auxo.tbl_Created_via_Python (name, age) VALUES (%s, %s);
        '''
        data_to_insert = ('John Doe', 30)  # Example data
        cursor.execute(insert_query, data_to_insert)
 
        # Commit the changes
        conn.commit()
 
        print("Data inserted successfully.")
 
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
 
connect_and_insert()