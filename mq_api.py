# -*- coding: utf-8 -*-
"""
Created on Tue May 16 17:50:40 2023

@author: ZHU
"""

import requests

# Set up the RabbitMQ management API credentials
api_username = 'ZHU'
api_password = 'password'
api_base_url = 'http://localhost:15672/api'

# Set up the queue and virtual host
queue_name = 'dataframe_queue'
vhost = '%2f'  # URL-encoded value for the default vhost '/'

# Create a session with the management API
session = requests.Session()
session.auth = (api_username, api_password)

# Send a GET request to retrieve queue details
response = session.get(f'{api_base_url}/queues/{vhost}/{queue_name}')

if response.status_code == 200:
    queue_details = response.json()
    consumer_count = queue_details['consumers']
    message_count = queue_details['messages']
else:
    print(f"Failed to retrieve queue details. Response code: {response.status_code}")