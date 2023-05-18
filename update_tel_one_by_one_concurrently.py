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


def update_value(existing_value, new_value):
    if isinstance(new_value, (np.ndarray, list)):
        # 如果 new_value 是 NumPy 数组或列表
        new_value = new_value.tolist() if isinstance(new_value, np.ndarray) else new_value

        if existing_value is None:
            # 如果 existing_value 为空，则返回 new_value 并设置 to_update_flag 为 True
            return new_value, True

        existing = existing_value[0] if existing_value[0] is not None else []
        assert isinstance(existing, list)

        # 找到 new_value 中不存在于 existing 中的新元素
        new_elements = list(set(new_value) - set(existing))
        if not new_elements:
            # 如果没有新元素，则无需更新，返回 None 和 False
            return None, False

        updated_value = existing + new_elements
        
    elif isinstance(new_value, str):
        # 如果 new_value 是字符串
        if existing_value is None:
            # 如果 existing_value 为空，则返回 new_value 并设置 to_update_flag 为 True
            return new_value, True

        existing = existing_value[0] if existing_value[0] is not None else ""
        assert isinstance(existing, str)

        if existing == new_value:
            # 如果 existing 和 new_value 相等，则无需更新，返回 None 和 False
            return None, False

        updated_value = new_value
        
    else:
        # 如果 new_value 既不是数组/列表，也不是字符串，则无需更新，返回 None 和 False
        return None, False

    # 返回更新后的值和 to_update_flag 设置为 True
    return updated_value, True


def process_chunk(chunk, progress, queue, table_name, id_col, value_col):
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="password"
    )

    cur = conn.cursor()

    try:
        query_log = defaultdict(list)

        # update_queries = []
        # insert_queries = []
        
        for index, row in chunk.iterrows():
            progress.update(1)
            order_id = index
            new_value = row[value_col]
            
            cur.execute(f"SELECT {value_col} FROM {table_name} WHERE {id_col} = %s", (order_id,))
            existing_value = cur.fetchone()
            
            updated_value, to_update = update_value(existing_value, new_value)
            if not to_update:
                continue
        
            if existing_value is not None:
                sql_query = f"UPDATE {table_name} SET {value_col} = %s WHERE {id_col} = %s"
                params = (updated_value, order_id)
                # update_queries.append(params)
            else:
                sql_query = f"INSERT INTO {table_name} ({id_col}, {value_col}) VALUES (%s, %s)"
                params = (order_id, updated_value)
                # insert_queries.append(params)
        
            query_log['order_id'].append(order_id)
            query_log['existing'].append(existing_value[0] if existing_value is not None else np.nan)
            query_log['updated'].append(updated_value)
            query_log['type'].append('UPDATE' if existing_value is not None else 'INSERT')
            query_log['sql_query'].append(cur.mogrify(sql_query, params).decode('utf-8'))
        
        queue.put(query_log)
        
        # if update_queries:
        #     cur.executemany(f"UPDATE {table_name} SET {value_col} = %s WHERE {id_col} = %s", update_queries)
            
        # if insert_queries:
        #     cur.executemany(f"INSERT INTO {table_name} ({id_col}, {value_col}) VALUES (%s, %s)", insert_queries)
            
        # conn.commit()
    
    finally:
        cur.close()
        conn.close()
        return progress


# Split the data into chunks of size 10000
data = pd.read_pickle('update_手机.pkl')
table_name = 'tb_privacy_data'
id_col = '主订单ID'
value_col = '手机'

chunk_size = 10000
chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

# Create a queue to store the results
result_queue = Queue()

# Process the chunks concurrently using a thread pool
with ThreadPoolExecutor(max_workers=6) as executor:
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
logs = defaultdict(list)
while not result_queue.empty():
    chunk_logs = result_queue.get()
    logs['order_id'].extend(chunk_logs['order_id'])
    logs['existing'].extend(chunk_logs['existing'])
    logs['updated'].extend(chunk_logs['updated'])
    logs['type'].extend(chunk_logs['type'])
    logs['sql_query'].extend(chunk_logs['sql_query'])

log_df = pd.DataFrame(logs)
log_df.to_pickle('logs.pkl')
