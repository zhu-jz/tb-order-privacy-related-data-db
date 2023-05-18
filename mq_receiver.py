"""
Created on Mon May 15 19:25:34 2023

@author: ZHU
"""

import pika
import pickle
import pandas as pd
import numpy as np
import psycopg2
from queue import Queue
import os
import time
import random
import string
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
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


# Define a function to process a chunk of the DataFrame
def process_chunk(chunk, result_queue):
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="password"
    )

    cur = conn.cursor()
    
    #
    try:
        table_name = 'tb_privacy_data'
        id_col = chunk.index.name
        value_col = chunk.columns[0]
        
        # Fetch all existing values for the chunk at once
        order_ids = chunk.index.tolist()
        cur.execute(f"SELECT {id_col}, {value_col} FROM {table_name} WHERE {id_col} IN %s", (tuple(order_ids),))
        
        results = pd.DataFrame(cur.fetchall(), columns=[id_col, "existing"])
        results.set_index(id_col, inplace=True)
        
        df = chunk.rename(columns={value_col: "value"}).join(results, how="left")
        df[['updated', 'type', 'flag']] = df.apply(update_value, axis=1, result_type='expand')
        # df[['query_base', 'params']] = df.apply(lambda row: make_query(row, table_name, id_col, value_col), axis=1, result_type="expand")
    
        result_queue.put(df[df['flag']].copy())
        
    except Exception as e:
        print(e)
        
    finally:
        cur.close()
        conn.close()


# Define a callback function to handle incoming messages
def process_message(ch, method, properties, body):
    # Deserialize the DataFrame
    dataframe = pickle.loads(body)
    
    # Create a queue to store the results
    result_queue = Queue()

    # Split the DataFrame into chunks
    num_chunks = 4  # Specify the number of chunks to split into
    chunks = np.array_split(dataframe, num_chunks)
    
    # create a ThreadPoolExecutor to run the insert_chunk function in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_chunk, chunk, result_queue) for chunk in chunks]

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

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
    try:
        # Retrieve the results from the queue
        li = []
        while not result_queue.empty():
            chunk_logs = result_queue.get()
            li.append(chunk_logs)
            
        df = pd.concat(li)
        
        # 如果目录不存在，创建目录
        parent_dir = os.path.abspath(os.path.join('.', 'results')) # 父目录
        sub_dir = f'job_{time.strftime("%Y%m%d")}' # 子目录
        directory = os.path.join(parent_dir, sub_dir)
        
        if not os.path.isdir(directory):
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        # 随机文件名
        s = string.ascii_letters + string.digits
        random_str = ''.join(random.choices(s, k=16))
        fn = f'{random_str}.pkl'
        file = os.path.join(directory, fn)
        assert not os.path.isfile(file) # 需要确保文件名不重复
        
        # 保存
        df.to_pickle(file)
        
    except Exception as e:
        print(e)


def process_control_message(ch, method, properties, body):
    message = body.decode()
    if message == "stop":
        print("Received stop signal. Closing receiver.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()
        

# Connect to RabbitMQ server
credentials = pika.PlainCredentials('ZHU', 'password')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='dataframe_queue')
channel.queue_declare(queue='control_queue')

# Set the prefetch count to control the number of unacknowledged messages
channel.basic_qos(prefetch_count=1)

# Set the callback function to handle incoming messages
channel.basic_consume(queue='dataframe_queue', on_message_callback=process_message)
channel.basic_consume(queue='control_queue', on_message_callback=process_control_message)

# Start consuming messages
try:
    print("Receiver started. Waiting for messages...")
    channel.start_consuming()
except KeyboardInterrupt:
    print("Receiver stopped by user.")
finally:
    connection.close()
