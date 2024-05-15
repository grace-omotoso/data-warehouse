import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function takes two parameters as input and loads the staging tables in the specified query
 
    Parameters:
    cur (cursor): A temporary memory that keeps query results
    conn(database connection): An instance of a database connection
 
    Returns:
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function takes two parameters as input and populates the analytics tables in the specified query 
 
    Parameters:
    cur (cursor): A temporary memory that keeps query results
    conn(database connection): An instance of a database connection
 
    Returns:
    None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()