# -*- coding: utf-8 -*-
"""
Created on Wed May 10 15:53:30 2023

@author: ZHU
"""

import argparse
import psycopg2


def create_table(table_name):
    # Establish the database connection
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="password"
    )

    schema = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            主订单ID VARCHAR(50) PRIMARY KEY,
            淘宝ID VARCHAR(50),
            OpenID VARCHAR(50),
            BuyerID VARCHAR(50),
            姓名 VARCHAR(50)[],
            地址 TEXT[],
            手机 VARCHAR(50)[]
        )
    """.format(table_name=table_name)

    # Execute the schema creation query
    with conn.cursor() as cur:
        cur.execute(schema)
        conn.commit()

    # Close the database connection
    conn.close()


def main():
    # Create the argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table_name", help="Name of the table")
    args = parser.parse_args()

    # Check if the table name is provided
    if not args.table_name:
        parser.error("Please provide the name of the table using the -t/--table_name argument.")

    # Retrieve the table name from the command-line argument
    table_name = args.table_name

    # Call the create_table function
    create_table(table_name)

if __name__ == "__main__":
    main()
