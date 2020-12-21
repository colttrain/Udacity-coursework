import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Loads staging tables into Redshift using queries in sql_queries
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Inserts data from staging tables into below tables:
    factSongplays, dimTime, dimSongs, dimArtists, dimUsers
    - Done with query found in sql_queries
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Connects to Redshift cluster and runs insert and loading functions:
    insert_tables, load_staging_tables
    - Redshift info found in dwh.cfg
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()