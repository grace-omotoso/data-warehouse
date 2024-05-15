import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function takes two parameters as input and drops the tables in the specified query
 
    Parameters:
    cur (cursor): A temporary memory that keeps query results
    conn(database connection): An instance of a database connection
 
    Returns:
    None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function takes two parameters as input and creates the tables in the specified query
 
    Parameters:
    cur (cursor): A temporary memory that keeps query results
    conn(database connection): An instance of a database connection
 
    Returns:
    None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connect to database instance
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()