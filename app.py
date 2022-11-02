import mysql.connector
import json
from flask import Flask
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, Deel!'


@app.route('/initdb')
def db_init():
    # create db
    mydb = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="p@ssw0rd1"
    )
    cursor = mydb.cursor()

    cursor.execute("DROP DATABASE IF EXISTS deel")
    cursor.execute("CREATE DATABASE deel")
    cursor.close()

    # create tables
    mydb = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="p@ssw0rd1",
        database="deel"
    )
    cursor = mydb.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS contracts")
        cursor.execute("DROP TABLE IF EXISTS invoices")
        cursor.execute(open('schemas/contracts.sql').read())
        cursor.execute(open('schemas/invoices.sql').read())

        cursor.close()
        return 'Database started\n'

    except mysql.connector.Error as err:
        logging.error(f'{err} encountered when dropping/creating init tables')
        cursor.close()
        return (
            'Database not started\n'
            f"Error: '{err}'\n"
        )        


@app.route('/load-contracts')
def load_contracts():

    contracts = load_from_file(path='data/contracts.json')

    add_row = (
        "INSERT INTO contracts "
        "(CONTRACT_ID, CLIENT_ID, CONTRACT_CREATED_AT,STATUS, COMPLETION_DATE, IS_DELETED, RECEIVED_AT) "
        "VALUES (%(CONTRACT_ID)s, %(CLIENT_ID)s, %(CONTRACT_CREATED_AT)s, %(STATUS)s, %(COMPLETION_DATE)s, %(IS_DELETED)s, %(RECEIVED_AT)s)"
        )
    # https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-transaction.html

    mydb = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="p@ssw0rd1",
        database="deel"
    )
    cursor = mydb.cursor()

    try:
        for idx, row in enumerate(contracts):
            cursor.execute("SET SQL_MODE='ALLOW_INVALID_DATES';")
            cursor.execute(add_row, row)

        # Make sure data is committed to the database
        mydb.commit()
        cursor.close()
        
        return 'Contracts loaded\n'

    except mysql.connector.Error as err:
        logging.error(f'{err} encountered when inserting {row} at index {idx}')

        cursor.close()
        return (
            'Contracts not loaded\n' 
            f"Error: '{err}'\n"
        )


@app.route('/contracts')
def get_contracts():
    mydb = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="p@ssw0rd1",
        database="deel"
    )
    cursor = mydb.cursor()

    try:
        cursor.execute("SELECT * FROM contracts")
        results = cursor.fetchall()

        cursor.close()
        return results


    except mysql.connector.Error as err:
        logging.error(f'{err} encountered when querying contracts table')
        cursor.close()
        return (
            'Contracts query failed.\n' 
            f"Error: '{err}'\n"
        )


@app.route('/load-invoices')
def load_invoices():

    invoices = load_from_file(path='data/invoices.json')

    add_row = (
        "INSERT INTO invoices "
        "(INVOICE_ID, CONTRACT_ID, AMOUNT, CURRENCY, IS_EARLY_PAID, IS_DELETED, RECEIVED_AT) "
        "VALUES (%(INVOICE_ID)s, %(CONTRACT_ID)s, %(AMOUNT)s, %(CURRENCY)s, %(IS_EARLY_PAID)s, %(IS_DELETED)s, %(RECEIVED_AT)s)"
        )
    # https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-transaction.html

    mydb = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="p@ssw0rd1",
        database="deel"
    )
    cursor = mydb.cursor()

    try:
        for idx, row in enumerate(invoices):
            cursor.execute("SET SQL_MODE='ALLOW_INVALID_DATES';")
            cursor.execute(add_row, row)

        # Make sure data is committed to the database
        mydb.commit()
        cursor.close()
        
        return 'Invoices loaded\n'

    except mysql.connector.Error as err:
        logging.error(f'{err} encountered when inserting {row} at index {idx}')

        cursor.close()
        return (
            'Invoices not loaded\n' 
            f"Error: '{err}'\n"
        )


@app.route('/invoices')
def get_invoices():
    mydb = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="p@ssw0rd1",
        database="deel"
    )
    cursor = mydb.cursor()

    try:
        cursor.execute("SELECT * FROM invoices")
        results = cursor.fetchall()

        cursor.close()
        return results


    except mysql.connector.Error as err:
        logging.error(f'{err} encountered when querying invoices table')
        cursor.close()
        return (
            'Invoices query failed.\n' 
            f"Error: '{err}'\n"
        )


# helpers
def load_from_file(path: str) -> list:
    with open(path) as f:
        d_list = json.load(f)

    # fix 'RECEIVED_AT\n' in the dict keys
    for d in d_list:
        for k, v in list(d.items()):
            if '\n' in k:
                new_k = k.replace('\n', '')
                d[new_k] = v
                del d[k]
    return d_list

if __name__ == "__main__":
    app.run(host ='0.0.0.0')