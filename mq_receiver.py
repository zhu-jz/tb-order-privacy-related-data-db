"""
Created on Mon May 15 19:25:34 2023

@author: ZHU
"""

import argparse
import concurrent.futures
import logging
import os
import pickle
import random
import string
import time
from pathlib import Path
from queue import Queue

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.pool
import pika
from tqdm import tqdm


# Initialize logging
logging.basicConfig(level=logging.INFO)

# Database connection pool
db_pool = None


def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--maxconn', type=int, default=4, help='Maximum number of database connections')
    parser.add_argument('--num_chunks', type=int, default=4, help='Number of chunks to split the dataframe into')
    parser.add_argument('--max_workers', type=int, default=4, help='Maximum number of worker threads')
    return parser.parse_args()


def initialize_db_pool(args):
    global db_pool
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1,  # minconn
        args.maxconn,  # maxconn
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="password"
    )


def update_value(row):
    new_value = row['value']
    existing_value = row['existing']
    
    if existing_value != existing_value: # None == None is True, np.nan == np.nan is False
        assert pd.isnull(existing_value)

        if isinstance(new_value, (str, list, np.ndarray)):
            return new_value.tolist() if isinstance(new_value, np.ndarray) else new_value, 'INSERT', True
        else:
            return None, 'INSERT', False
    
    if isinstance(new_value, (np.ndarray, list)):
        new_value = new_value.tolist() if isinstance(new_value, np.ndarray) else new_value
        existing = existing_value if existing_value is not None else []
        new_elements = list(set(new_value) - set(existing))
        if not new_elements:
            return None, 'UPDATE', False
        return existing + new_elements, 'UPDATE', True

    elif isinstance(new_value, str):
        if existing_value == new_value:
            return None, 'UPDATE', False
        return new_value, 'UPDATE', True

    else:
        return None, 'UPDATE', False


def process_chunk(chunk, result_queue):
    conn = db_pool.getconn()
    cur = conn.cursor()
    
    try:
        table_name = 'tb_privacy_data'
        id_col = chunk.index.name
        value_col = chunk.columns[0]
        
        order_ids = chunk.index.tolist()
        cur.execute(f"SELECT {id_col}, {value_col} FROM {table_name} WHERE {id_col} IN %s", (tuple(order_ids),))
        
        results = pd.DataFrame(cur.fetchall(), columns=[id_col, "existing"])
        results.set_index(id_col, inplace=True)
        
        df = chunk.rename(columns={value_col: "value"}).join(results, how="left")
        df[['updated', 'type', 'flag']] = df.apply(update_value, axis=1, result_type='expand')
    
        result_queue.put(df[df['flag']].copy())
        
    except Exception as e:
        logging.exception("Exception occurred while processing chunk")
        
    finally:
        cur.close()
        db_pool.putconn(conn)


def process_message(ch, method, properties, body, args):
    dataframe = pickle.loads(body)
    result_queue = Queue()
    chunks = np.array_split(dataframe, args.num_chunks)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [executor.submit(process_chunk, chunk, result_queue) for chunk in chunks]

        with tqdm(total=len(chunks)) as pbar:
            for future in concurrent.futures.as_completed(futures):
                pbar.update(1)
                try:
                    future.result()
                except Exception as e:
                    logging.exception("Exception occurred while processing future")

    try:
        li = []
        while not result_queue.empty():
            chunk_logs = result_queue.get()
            li.append(chunk_logs)
            
        df = pd.concat(li)
        
        parent_dir = os.path.abspath(os.path.join('.', 'results'))
        sub_dir = f'job_{time.strftime("%Y%m%d")}'
        directory = os.path.join(parent_dir, sub_dir)
        
        if not os.path.isdir(directory):
            Path(directory).mkdir(parents=True, exist_ok=True)

        s = string.ascii_letters + string.digits
        random_str = ''.join(random.choices(s, k=16))
        fn = f'{random_str}.pkl'
        file = os.path.join(directory, fn)
        assert not os.path.isfile(file)
        
        df.to_pickle(file)
        
        # Acknowledge the message after successfully saving the results
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logging.exception("Exception occurred while saving results")


def process_control_message(ch, method, properties, body):
    message = body.decode()
    if message == "stop":
        logging.info("Received stop signal. Closing receiver.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()


def start_receiver(args):
    credentials = pika.PlainCredentials('ZHU', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='dataframe_queue')
    channel.queue_declare(queue='control_queue')

    channel.basic_qos(prefetch_count=1)

    # Pass num_chunks and max_workers to process_message
    on_message_callback = lambda ch, method, properties, body: process_message(ch, method, properties, body, args)
    channel.basic_consume(queue='dataframe_queue', on_message_callback=on_message_callback)
    channel.basic_consume(queue='control_queue', on_message_callback=process_control_message)

    try:
        logging.info("Receiver started. Waiting for messages...")
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Receiver stopped by user.")
    finally:
        connection.close()
        db_pool.closeall()


if __name__ == "__main__":
    args = parse_arguments()
    initialize_db_pool(args)
    start_receiver(args)
