# -*- coding: utf-8 -*-
"""
Created on Sat May  6 19:05:37 2023

@author: ZHU
"""

import psycopg2
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from tqdm import tqdm
from collections import defaultdict


def process_chunk(chunk, progress, queue, table_name, id_col, value_col):
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="password"
    )

    cur = conn.cursor()
    logs = defaultdict(list)

    # update_queries = []
    # insert_queries = []

    for index, row in chunk.iterrows():
        progress.update(1)
        order_id = index
        value = row[value_col]
        value = value.tolist() if isinstance(value, np.ndarray) else value
        assert isinstance(value, list)

        cur.execute(f"SELECT {value_col} FROM {table_name} WHERE {id_col} = %s", (order_id,))
        result = cur.fetchone()

        if result is not None:
            existing = [] if result[0] is None else result[0]
            new = list(set(value) - set(existing))
            if not new:
                # new is empty list
                continue
            updated = existing + new
            sql_query = f"UPDATE {table_name} SET {value_col} = %s WHERE {id_col} = %s"
            params = (updated, order_id)
            # update_queries.append(params)
        else:
            sql_query = f"INSERT INTO {table_name} ({id_col}, {value_col}) VALUES (%s, %s)"
            params = (order_id, value)
            # insert_queries.append(params)

        logs['order_id'].append(order_id)
        logs['existing'].append(existing if result is not None else np.nan)
        logs['updated'].append(updated if result is not None else value)
        logs['type'].append('UPDATE' if result is not None else 'INSERT')
        logs['sql_query'].append(cur.mogrify(sql_query, params).decode('utf-8'))

    # if update_queries:
    #     cur.executemany(f"UPDATE {table_name} SET {value_col} = %s WHERE {id_col} = %s", update_queries)
    # if insert_queries:
    #     cur.executemany(f"INSERT INTO {table_name} ({id_col}, {value_col}) VALUES (%s, %s)", insert_queries)

    # conn.commit()
    cur.close()
    conn.close()
    queue.put(logs)

    return progress


# Split the data into chunks of size 10000
data = pd.read_pickle('update_tel.pkl')
table_name = 'tb_privacy_data'
id_col = '主订单id'
value_col = '手机'

chunk_size = 10000
chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

# Create a queue to store the results
result_queue = Queue()

# Process the chunks concurrently using a thread pool
with ThreadPoolExecutor(max_workers=7) as executor:
    futures = []
    progress_bars = []  # Create a list to store the progress bars
    for chunk_index, chunk in enumerate(chunks):
        progress = tqdm(total=len(chunk), desc=f'Processing Chunk {chunk_index+1}/{len(chunks)}')
        progress_bars.append(progress) # Add the progress bar to the list
        future = executor.submit(process_chunk, chunk, progress, result_queue, table_name, id_col, value_col)
        futures.append(future)
    for future in futures:
        future.result()
    
    # Wait for all progress bars to be properly cleaned up
    for progress in progress_bars:
        progress.close()

# Retrieve the results from the queue
logs = {'order_id': [], 'existing': [], 'updated': [], 'type': [], 'sql_query': []}
while not result_queue.empty():
    chunk_logs = result_queue.get()
    logs['order_id'].extend(chunk_logs['order_id'])
    logs['existing'].extend(chunk_logs['existing'])
    logs['updated'].extend(chunk_logs['updated'])
    logs['type'].extend(chunk_logs['type'])
    logs['sql_query'].extend(chunk_logs['sql_query'])

log_df = pd.DataFrame(logs)
log_df.to_pickle('logs.pkl')
