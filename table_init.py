# -*- coding: utf-8 -*-
"""
Created on Wed May 10 17:24:47 2023

@author: ZHU
"""

import psycopg2
import pandas as pd
from psycopg2 import extras
import concurrent.futures
from tqdm import tqdm


def insert_chunk(chunk, table_name):
    """
    Inserts a chunk of data into a PostgreSQL table.

    Args:
        chunk (pandas.DataFrame): The chunk of data to insert.
        table_name (str): The name of the table to insert the data into.
    """
    # Establish the database connection using a context manager
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="password"
    )
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
        # Close the cursor and connection
        cur.close()
        conn.close()


# Example usage
df = pd.read_pickle(r'D:\淘宝ID数据更新\main_order_privacy_data_202305041417.pkl')
table_name = "tb_privacy_data"

# define the size of each chunk
chunksize = 10000

# create a list of chunks
chunks = [df[i:i+chunksize] for i in range(0, len(df), chunksize)]
    
# create a ThreadPoolExecutor to run the insert_chunk function in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
    futures = [executor.submit(insert_chunk, chunk, table_name) for chunk in chunks]

    # use tqdm to track the progress of the insertions
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
