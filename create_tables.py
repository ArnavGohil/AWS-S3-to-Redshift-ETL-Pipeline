import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#Function to execute DROP TABLE Queries
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

#Function to execute CREATE TABLE Queries
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


"""
    - Connects to the Redshift Cluster
    - Returns the connection and cursor to Redshift Cluster DB
    - Executes the DROP and CREATE Queries
"""
def main()
    # Importing CONFIG Data
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to DB and return the connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Functon Call
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()