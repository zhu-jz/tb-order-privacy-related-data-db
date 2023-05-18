# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 15:00:42 2023

@author: ZHU
"""

import psycopg2
import numpy as np
from tqdm import tqdm
import pandas as pd

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="127.0.0.1",
    database="postgres",
    user="postgres",
    password="password"
)

# Create a cursor object
cur = conn.cursor()

# Update the table with the values in the dataframe
logs = {'order_id': [], 'existing': [], 'updated': [], 'type': [], 'sql_query': []}

tel_data = pd.read_pickle('update_tel.pkl')

for index, row in tqdm(tel_data.iterrows(), total=len(tel_data)):
    order_id = index
    # tel = row['receiver_mobile'].tolist()
    tel = row['receiver_mobile']
    
    cur.execute("SELECT tel FROM tb_privacy_data WHERE order_id = %s", (order_id,))
    result = cur.fetchone()
    if result is not None:
        # Update existing record
        existing = result[0]
        if existing is None:
            existing = []
        
        new = list(set(tel) - set(existing))
        if len(new) > 0:
            updated = existing + new
            
            logs['order_id'].append(order_id)
            logs['existing'].append(existing)
            logs['updated'].append(updated)
            logs['type'].append('update')
            
            sql_query = "UPDATE tb_privacy_data SET tel = %s WHERE order_id = %s"
            params = (updated, order_id)
            logs['sql_query'].append(sql_query % params)
            # cur.execute(sql_query, params)
            
    else:
        # Insert new record
        logs['order_id'].append(order_id)
        logs['existing'].append(np.nan)
        logs['updated'].append(tel)
        logs['type'].append('insert')
        
        sql_query = "INSERT INTO tb_privacy_data (order_id, tel) VALUES (%s, %s)"
        params = (order_id, tel)
        logs['sql_query'].append(sql_query % params)
        # cur.execute(sql_query, params)
    
conn.commit()
cur.close()
conn.close()