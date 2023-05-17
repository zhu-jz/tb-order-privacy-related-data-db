# -*- coding: utf-8 -*-
"""
Created on Wed May 10 15:53:30 2023

@author: ZHU
"""

import psycopg2


# Establish the database connection
conn = psycopg2.connect(
    host="127.0.0.1",
    database="postgres",
    user="postgres",
    password="password"
)

table_name = "tb_privacy_data"
schema = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        主订单ID VARCHAR(50) PRIMARY KEY,
        淘宝ID VARCHAR(50),
        OpenID VARCHAR(50),
        BuyerID VARCHAR(50),
        姓名 VARCHAR(50)[],
        地址 TEXT[],
        手机 VARCHAR(50)[]
    )
""".format(table_name=table_name)

# Execute the schema creation query
with conn.cursor() as cur:
    cur.execute(schema)
    conn.commit()

# Close the database connection
conn.close()
