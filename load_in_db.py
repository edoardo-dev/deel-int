#!/usr/bin/env python3
import logging
# https://www.psycopg.org/docs/usage.html
import psycopg2
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_files_in_mem(files: list):
    ''' 
    @param files: paths of files containing data
    @returns list of dictionary objects
    '''
    import json
    loaded_files = []
    for file in files:
        with open(file) as f:
            d = json.load(f)
            loaded_files.append(d)
    
    return loaded_files


def create_table(connection, query:str):
    '''
    create table
    @param connection: con to postgres.
    @param query: query to execute.
    @return: None
    '''
    # Open cursor to perform database operation
    # batch load 

    cur = conn.cursor()
    try:
        cur.execute(query)

    except Exception as e:
        logging.error(
            f'Something went wrong in loading data with chunk {idx}\n'
            f'Error: {e}')
    finally:
        cur.close()



def insert_in_db_table(connection, table: str, data: list, chunk_size: int):
    '''
    Insert into table
    @param connection: con to postgres.
    @param table: table to insert data into.
    @param data: object containing data to insert in the table
    @return: None
    '''
    # Open cursor to perform database operation
    # batch load 
    list_chunked = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    cur = connection.cursor()
    try:
        for idx, chunk in enumerate(list_chunked):
            for row in chunk:
                columns = ','.join(list(row.keys())).lower()
                values = ','.join(f"'{e}' " for e in list(row.values()))  
                # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
                # cur.execute(
                #     f'''INSERT INTO {table} ({columns}) 
                #     VALUES ({values}) ;'''
                # )
                cur.execute("INSERT INTO {0} ({1}) VALUES ({2});".format(
                    table, columns, values
                ))

    except Exception as e:
        logging.error(
            f'Something went wrong in loading data with chunk {idx}\n'
            f'Error: {e}')
    finally:
        cur.close()


if __name__ == "__main__":
    start = time.perf_counter()

    files_to_load = ['data/contracts.json', 'data/invoices.json']
    logging.info(f'Loading files: {files_to_load}')
    contracts, invoices = load_files_in_mem(files_to_load)


    # Connect to db
    # conn = psycopg2.connect("dbname=exampledb user=deel")
    conn = psycopg2.connect(
        # database="exampledb",
        user="postgres",
        password="postgres",
    )

    create_table(connection=conn, query=open('create_tables.sql').read())

    logging.info(f'Inserting into contracts')
    insert_in_db_table(connection=conn, table='contracts', data=contracts, chunk_size=100)
    logging.info(f'Inserting into contracts done')

    logging.info(f'Inserting into invoices')
    insert_in_db_table(connection=conn, table='invoices', data=invoices, chunk_size=100)
    logging.info(f'Inserting into invoices done')

    # Close communications with database
    conn.close()

    elapsed = time.perf_counter() - start
    logging.info(f'Program completed in {elapsed:0.5f} seconds.')