import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


#Function to execute COPY Queries
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

#Function to execute INSERT INTO Queries
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""
    - Connects to the Redshift Cluster
    - Returns the connection and cursor to Redshift Cluster DB
    - Executes the COPY and INSERT Queries
"""
def main():
    # Importing CONFIG Data
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to DB and return the connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Functon Call
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()