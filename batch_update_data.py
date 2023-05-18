# -*- coding: utf-8 -*-
"""
Created on Sat May  6 19:05:37 2023

@author: ZHU
"""

import psycopg2
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from tqdm import tqdm


def update_value(row):
    new_value = row['value']
    existing_value = row['existing']
    
    if existing_value != existing_value: 
        assert pd.isnull(existing_value)
        
        if isinstance(new_value, (str, list)):
            return new_value, 'INSERT', True
        
        elif isinstance(new_value, np.ndarray):
            return new_value.tolist(), 'INSERT', True
        
        else:
            return None, 'INSERT', False
    
    if isinstance(new_value, (np.ndarray, list)):
        # 如果 new_value 是 NumPy 数组或列表
        new_value = new_value.tolist() if isinstance(new_value, np.ndarray) else new_value

        existing = existing_value if existing_value is not None else []
        assert isinstance(existing, list)

        # 找到 new_value 中不存在于 existing 中的新元素
        new_elements = list(set(new_value) - set(existing))
        if not new_elements:
            # 如果没有新元素，则无需更新，返回 None 和 False
            return None, 'UPDATE', False

        updated_value = existing + new_elements
        
    elif isinstance(new_value, str):
        assert (isinstance(existing_value, str) or existing_value is None)
        if existing_value == new_value:
            # 如果 existing_value 和 new_value 相等，则无需更新，返回 None 和 False
            return None, 'UPDATE', False

        updated_value = new_value
        
    else:
        # 如果 new_value 既不是数组/列表，也不是字符串，则无需更新，返回 None 和 False
        return None, 'UPDATE', False

    # 返回更新后的值和 to_update_flag 设置为 True
    return updated_value, 'UPDATE', True


def make_query(row):
    order_id = row.name
    updated_value = row['updated']
    query_type = row['type']
    to_update = row['flag']
    
    if not to_update:
        return None, None
        
    if query_type == 'UPDATE':
        sql_query = f"UPDATE {table_name} SET {value_col} = %s WHERE {id_col} = %s"
        params = (updated_value, order_id)
        
    elif query_type == 'INSERT':
        sql_query = f"INSERT INTO {table_name} ({id_col}, {value_col}) VALUES (%s, %s)"
        params = (order_id, updated_value)
    
    else:
        return None, None
    return sql_query, params


def process_chunk(chunk, queue, table_name, id_col, value_col):
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="password"
    )

    cur = conn.cursor()
    
    # Fetch all existing values for the chunk at once
    order_ids = chunk.index.tolist()
    cur.execute(f"SELECT {id_col}, {value_col} FROM {table_name} WHERE {id_col} IN %s", (tuple(order_ids),))
    
    results = pd.DataFrame(cur.fetchall(), columns=[id_col, "existing"])
    results.set_index(id_col, inplace=True)
    
    df = chunk.rename(columns={value_col: "value"}).join(results, how="left")
    df[['updated', 'type', 'flag']] = df.apply(update_value, axis=1, result_type='expand')
    df[['query_base', 'params']] = df.apply(make_query, axis=1, result_type="expand")
    queue.put(df)
    cur.close()
    conn.close()
    

# Split the data into chunks of size 10000
data = pd.read_pickle('update_手机.pkl')
table_name = 'tb_privacy_data'
id_col = '主订单ID'
value_col = '手机'

chunk_size = 10000
chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

# Create a queue to store the results
result_queue = Queue()

# create a ThreadPoolExecutor to run the insert_chunk function in parallel
with ThreadPoolExecutor(max_workers=1) as executor:
    futures = [executor.submit(process_chunk, chunk, result_queue, table_name, id_col, value_col) for chunk in chunks]

    # use tqdm to track the progress of the insertions
    with tqdm(total=len(chunks)) as pbar:
        # Wait for the futures to complete and catch any exceptions
        for future in as_completed(futures):
            pbar.update(1)
            try:
                # Get the result of the future (this will raise an exception if one occurred)
                result = future.result()
                # Process the result if needed
            except Exception as e:
                # Handle the exception here
                print("Exception occurred:", e)

# Retrieve the results from the queue
li = []
while not result_queue.empty():
    chunk_logs = result_queue.get()
    li.append(chunk_logs)
    
df = pd.concat(li)