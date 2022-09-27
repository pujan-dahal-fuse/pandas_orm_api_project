# we retrieve data from our Retail Store Management database, and try to get meaningful insights into the data
from venv import create
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, MetaData, insert, Table, select
from exceptions import InvalidInput
from sqlalchemy.exc import IntegrityError
# for importing dbaddress
from importlib.machinery import SourceFileLoader
dbaddress = SourceFileLoader('dbaddress', '../dbaddress.py').load_module()

import MySQLdb
import json
import datetime as dt

app = Flask(__name__)


# insert a new record into bill table
@app.route('/api/insert_bill', methods=['POST'])
def insert_bill():
    """Insert new bill into bill table"""

    # get the body of request 
    body = request.get_json()
    bill = Table('bill', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(bill)
    try:
        if body == {}:
            raise InvalidInput('empty input json')

        # user cannot give customer_id in our database
        if body.get('bill_id') is not None:
            raise InvalidInput('bill_id cannot be specified in input json')
        
        # execute the insert statement
        conn.execute(stmt, body)
        
        return jsonify({
            'status': 200,
            'message': 'Successfully inserted bill record into database',
            'data': body
        })


    except InvalidInput as ii:
        return jsonify({
            'status': 400,
            'message': f'Bad request: {ii.get_message()}',
            'data': {}
        })
    
    # integrityerror.orig.args[0] gives us the type of error
    # if it is 1062, it is unique constraint error
    # if it is 1452, it is unique constraint error
    except IntegrityError as ie:
        if ie.orig.args[0] == 1062:
            return jsonify({
                'status': 400,
                'message': 'Bad request: record exists in database',
                'data': {}                      
            })
        elif ie.orig.args[0] == 1452:
            return jsonify({
                'status': 400,
                'message': 'Bad request: invalid foreign keys in input',
                'data': {}                      
            })
        else:
            return jsonify({
                'status': 400,
                'message': 'Bad request: invalid input',
                'data': {}
            })




# insert into store_product table
@app.route('/api/insert_store_product', methods=['POST'])
def insert_store_product():
    """Insert new record into store_product table"""

    # get the body of request
    body = request.get_json()
    store_product = Table('store_product', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(store_product)

    try:
        if body == {}:
            raise InvalidInput('empty input json')

        conn.execute(stmt, body)

        return jsonify({
            'status': 200,
            'message': 'Successfully inserted record into store_product table',
            'data': body
        })

    except InvalidInput as ii:
        return jsonify({
            'status': 400,
            'message': f'Bad request: {ii.get_message()}',
            'data': {}
        })

    except IntegrityError as ie:
        if ie.orig.args[0] == 1062:
            return jsonify({
                'status': 400,
                'message': 'Bad request: record exists in database',
                'data': {}                      
            })
        elif ie.orig.args[0] == 1452:
            return jsonify({
                'status': 400,
                'message': 'Bad request: invalid foreign keys in input',
                'data': {}                      
            })
        else:
            return jsonify({
                'status': 400,
                'message': 'Bad request: invalid input',
                'data': {}
            })
        

# insert product_lot record into product_lot table
@app.route('/api/insert_product_lot', methods=['POST'])
def insert_product_lot():
    """Insert new record into product_lot table"""

    # get the body of request
    body = request.get_json()
    product_lot = Table('product_lot', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(product_lot)

    try:
        if body == {}:
            raise InvalidInput('empty input json')
        
        if body.get('product_lot_id') is not None:
            raise InvalidInput('product_lot_id cannot be specified in input json')

        conn.execute(stmt, body)

        return jsonify({
            'status': 200,
            'message': 'Successfully inserted record into product_lot table',
            'data': {}
        })

    except InvalidInput as ii:
        return jsonify({
            'status': 400,
            'message': f'Bad request: {ii.get_message()}',
            'data': {}
        })
    
    except IntegrityError as ie:
        if ie.orig.args[0] == 1062:
            return jsonify({
                'status': 400,
                'message': 'Bad request: record exists in database',
                'data': {}                      
            })
        elif ie.orig.args[0] == 1452:
            return jsonify({
                'status': 400,
                'message': 'Bad request: invalid foreign keys in input',
                'data': {}                      
            })
        else:
            return jsonify({
                'status': 400,
                'message': 'Bad request: invalid input',
                'data': {}
            })


# insert product_bill record into the database
@app.route('/api/insert_product_bill', methods=['POST'])
def insert_product_bill():
    """Insert product_bill record into the table"""
    
    # when we add a product of certain quantity into the product_bill table, we need to check whether the store from which the quantity is being taken has sufficient product in stock
    # i.e. we need to check the store_product table
    # further we also need to add the number of points collected to customer record because the customer has bought the product
    body = request.get_json()

    # tables that we will use
    store = Table('store', metadata_obj, autoload=True, autoload_with=engine)
    customer = Table('customer', metadata_obj, autoload=True, autoload_with=engine)
    bill = Table('bill', metadata_obj, autoload=True, autoload_with=engine)
    product_bill = Table('product_bill', metadata_obj, autoload=True, autoload_with=engine)
    product_lot = Table('product_lot', metadata_obj, autoload=True, autoload_with=engine)
    store_product = Table('store_product', metadata_obj, autoload=True, autoload_with=engine)
    product = Table('product', metadata_obj, autoload=True, autoload_with=engine)


if __name__ == '__main__':
    # create engine to connect to database
    engine = create_engine(dbaddress.DB_ADDRESS)
    conn = engine.connect()
    # create metadata object
    metadata_obj = MetaData(bind=engine)
    MetaData.reflect(metadata_obj)

    # run app in debug mode
    app.run(debug=True)

