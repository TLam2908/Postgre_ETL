import psycopg2
from queries_sql import create_table_queries, drop_table_queries
hostname = 'localhost'
database = 'sparkifydb'
username = 'postgres'
pwd = 'admin123'
port_id = 5432

def create_database():
    try:
        # Connect to default database
        conn = psycopg2.connect(f"host=127.0.0.1 dbname=postgres user={username} password={pwd}")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    
        cur.execute(f"DROP DATABASE IF EXISTS {database}")
        cur.execute(f"CREATE DATABASE {database} WITH ENCODING 'utf8' TEMPLATE template0")
    
        conn.close()
    
        # Connect to sparkify database
        conn = psycopg2.connect(f"host={hostname} dbname={database} user={username} password={pwd} port={port_id}")
        cur = conn.cursor()
        print("Connection to database successful")
        return conn, cur
    except Exception as error:
        print(f"Error: Could not connect to the database. {error}")
    

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        print("Tables dropped successfully")
    except Exception as error:
        print(f"Error: Could not drop tables. {error}")
        conn.close()
    

def create_tables(cur, conn):
    """
    Create each table using the queries in `create_table_queries` list.
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        print("Tables created successfully")    
    except Exception as error:
        print(f"Error: Could not create tables. {error}")
        conn.close()

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    conn, cur = create_database() # type: ignore
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    
if __name__ == "__main__":
    main()