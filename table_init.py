# -*- coding: utf-8 -*-
"""
Created on Wed May 10 17:24:47 2023

@author: ZHU
"""

import argparse
import psycopg2
import pandas as pd
from psycopg2 import extras
import concurrent.futures
from tqdm import tqdm


def establish_connection():
    """
    Establishes a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: The database connection object.
    """
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="19840124"
    )
    return conn


def insert_chunk(chunk, table_name, conn):
    """
    Inserts a chunk of data into a PostgreSQL table.

    Args:
        chunk (pandas.DataFrame): The chunk of data to insert.
        table_name (str): The name of the table to insert the data into.
        conn (psycopg2.extensions.connection): The database connection object.
    """
    cur = conn.cursor()

    try:
        # Convert NaN values in the chunk to None
        chunk = chunk.where(pd.notnull(chunk), None)
        values = [tuple(x) for x in chunk.values]

        # Get the column names in the same order as the chunk DataFrame
        columns = chunk.columns.tolist()

        # Use parameterized query with the table name and column order
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        # Execute the parameterized query with the values
        extras.execute_values(cur, query, values)

        # Commit the transaction to persist the changes
        conn.commit()

    except psycopg2.Error as error:
        print("Error occurred while inserting data:", error)

        # Rollback the transaction in case of an error
        conn.rollback()

    finally:
        # Close the cursor
        cur.close()


def insert_data(data, table_name):
    """
    Inserts data into a PostgreSQL table in chunks using multiple threads for parallel processing.

    Args:
        data (pandas.DataFrame): The data to insert.
        table_name (str): The name of the table to insert the data into.
    """
    # Define the size of each chunk
    chunksize = 10000

    # Create a list of chunks
    chunks = [data[i:i + chunksize] for i in range(0, len(data), chunksize)]

    # Establish a database connection
    conn = establish_connection()

    # Create a ThreadPoolExecutor to run the insert_chunk function in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        futures = [executor.submit(insert_chunk, chunk, table_name, conn) for chunk in chunks]

        # Use tqdm to track the progress of the insertions
        with tqdm(total=len(chunks)) as pbar:
            # Wait for the futures to complete and catch any exceptions
            for future in concurrent.futures.as_completed(futures):
                pbar.update(1)
                try:
                    # Get the result of the future (this will raise an exception if one occurred)
                    result = future.result()
                    # Process the result if needed
                except Exception as e:
                    # Handle the exception here
                    print("Exception occurred:", e)

    # Close the database connection
    conn.close()


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--table_name', help='Table name', required=True)
    parser.add_argument('-d', '--data', help='File path of the DataFrame', required=True)
    args = parser.parse_args()

    # Get the table name and data file path from the command-line arguments
    table_name = args.table_name
    data_file = args.data

    # Read the DataFrame from the specified data file path
    df = pd.read_pickle(data_file)

    # Call the insert_data function to insert the data into the specified table
    insert_data(df, table_name)


if __name__ == '__main__':
    main()
