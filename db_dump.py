# -*- coding: utf-8 -*-
"""
Created on Tue May  9 16:38:11 2023

@author: ZHU
"""

import asyncio
import asyncpg
import pandas as pd
import os
import time
import random
import string
from pathlib import Path
from tqdm import tqdm


async def get_orderid(pool):
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Fetch the order-ids from the database
            result = await conn.fetch("SELECT order_id FROM tb_privacy_data")
            return [row[0] for row in result]


# fetches all order-ids in one go, we assume filename is unique each row, otherwise overwritten occurs
# saves the file outside of the connection scope
async def query_orderid(pool, oids, pbar):
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Build a list of all order-ids to fetch
            placeholders = ",".join(["$"+str(i+1) for i in range(len(oids))])
            query = f"SELECT order_id, tb_id, open_id, buyer_id, name, addr, tel FROM tb_privacy_data WHERE order_id IN ({placeholders})" # TODO
            # Fetch all rows with matching order-ids
            results = await conn.fetch(query, *oids)
            
            # Assuming that `results` is the list of rows returned by asyncpg `fetch` method
            if results:
                # Create a list of column names from the keys of the first row (asyncpg.Record object)
                columns = [key for key in results[0].keys()]
                # Create a pandas DataFrame from the list of rows and column names
                df = pd.DataFrame([dict(row) for row in results], columns=columns)
            else:
                # If no results are returned, create an empty DataFrame with no columns
                df = pd.DataFrame()
            
            # 如果目录不存在，创建目录
            parent_dir = os.path.abspath(os.path.join('.', 'dump_data')) # 父目录
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
            
            pbar.update(1)


async def main():
    # Set up a connection pool
    pool = await asyncpg.create_pool(user='postgres', password='password', database='postgres', host='localhost', max_size=10)

    # Get the list of order-ids
    oids = await get_orderid(pool)

    # Split the list of order-ids into parts
    num_parts = 200
    part_size = len(oids) // num_parts
    oid_parts = [oids[i:i+part_size] for i in range(0, len(oids), part_size)]

    # Query each part of the order-ids list asynchronously and save the data to files
    tasks = []
    with tqdm(total=len(oid_parts)) as pbar:
        for oids in oid_parts:
            tasks.append(asyncio.create_task(query_orderid(pool, oids, pbar)))
    
        await asyncio.gather(*tasks)

    # Close the connection pool
    await pool.close()


if __name__ == "__main__":
    # Retrieve the existing event loop, or create a new one if none exists
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
