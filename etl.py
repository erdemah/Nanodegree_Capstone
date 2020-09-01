import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from sql_queries import create_table_queries, drop_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from s3 to Redshift as staging tables.
    :param cur: cursor
    :param conn: dwh connection object
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables to fact and dimension tables.
    :param cur: cursor
    :param conn: Redshift connection object
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """
    Drops tables one by one in the dwh within a for loop
    :param cur: cursor
    :param conn: dwh connection object
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tables in the dwh for given queries in a list
    :param cur: cursor
    :param conn: dwh connection object
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connection to dwh is realised using the credentials in dwh.cfg file.
    Loads data from s3 to dwh as staging tables and
    inserts data from staging tables to fact and dimension tables.
    :return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('DWH', 'host'), config.get("DWH", "dwh_db"), config.get("DWH", "dwh_db_user"), config.get("DWH", "dwh_db_password"), config.get("DWH", "dwh_port")))
    cur = conn.cursor()
    print("Dropping Tables")
    drop_tables(cur, conn)
    print("Creating Tables")
    create_tables(cur, conn)

    print("Loading Tables")
    load_staging_tables(cur, conn)
    print("Inserting Tables")
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
