# -*- coding: utf-8 -*-
"""
Created on Mon May 15 19:40:33 2023

@author: ZHU
"""

import pika
import pickle
import pandas as pd
import numpy as np


def call_script_with_dataframe(dataframe):
    # Connect to RabbitMQ server
    credentials = pika.PlainCredentials('ZHU', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue='dataframe_queue')

    # Convert DataFrame to bytes
    serialized_dataframe = pickle.dumps(dataframe)

    # Publish the DataFrame to the queue
    channel.basic_publish(exchange='',
                          routing_key='dataframe_queue',
                          body=serialized_dataframe)

    # Close the connection
    connection.close()

# Example usage
data = pd.read_pickle('update_手机.pkl')

# Split the DataFrame into chunks
num_chunks = 64  # Specify the number of chunks to split into
chunks = np.array_split(data, num_chunks)

# chunk_size = 100000
# chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

for chunk in chunks:
    call_script_with_dataframe(chunk)


