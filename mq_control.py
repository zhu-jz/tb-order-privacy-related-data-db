# -*- coding: utf-8 -*-
"""
Created on Tue May 16 15:28:58 2023

@author: ZHU
"""

import pika


def stop_receiver():
    # Connect to RabbitMQ server
    credentials = pika.PlainCredentials('ZHU', '19840124')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue='control_queue')


    # Publish the DataFrame to the queue
    channel.basic_publish(exchange='', 
                          routing_key='control_queue', 
                          body='stop')

    # Close the connection
    connection.close()

stop_receiver()