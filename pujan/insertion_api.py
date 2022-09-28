# we retrieve data from our Retail Store Management database, and try to get meaningful insights into the data
from venv import create
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, MetaData, insert, Table, select, update
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


    # check that the quantity doesn't exceed what is in stock
    # we need store_product, store, bill tables
    quantity_joined = (store_product\
                    .join(store, store_product.columns.store_id == store.columns.store_id))\
                    .join(bill, store.columns.store_id == bill.columns.store_id)

    customer_joined = (((product\
                       .join(product_lot, product_lot.columns.product_id == product.columns.product_id))\
                       .join(product_bill, product_bill.columns.product_lot_id == product_lot.columns.product_lot_id))\
                       .join(bill, bill.columns.bill_id == product_bill.columns.bill_id))\
                       .join(customer, customer.columns.customer_id == bill.columns.customer_id)
    
    try:
        bill_product_record_found = False
        for record in conn.execute(select([quantity_joined])).fetchall(): # check each record for the combination of bill_id and product_lot_id
            print(record['bill_id'], record['product_lot_id'])
            if record['bill_id'] == body['bill_id'] and record['product_lot_id'] == body['product_lot_id']:
                bill_product_record_found = True
                if record['in_stock'] < body['quantity']:
                    raise InvalidInput('given quantity is more than what is in stock')

                # insert into product bill
                stmt = insert(product_bill)
                conn.execute(stmt, body)

                # another task is to subtract the quantity of products from store_product table i.e. update operation
                stmt2 = update(store_product)
                stmt2 = stmt2.where((store_product.columns.store_id == record['store_id']) & (store_product.columns.product_lot_id == record['product_lot_id']))
                stmt2 = stmt2.values(in_stock=record['in_stock'] - body['quantity'])
                conn.execute(stmt2)

                # now we have to change the points_collected for customer in customer_table
                for cust_joined_record in conn.execute(select([customer_joined])).fetchall():
                    if cust_joined_record['product_lot_id'] == body['product_lot_id'] and cust_joined_record['bill_id'] == body['bill_id']:
                        stmt3 = update(customer)
                        stmt3 = stmt3.where(customer.columns.customer_id == cust_joined_record['customer_id'])
                        stmt3 = stmt3.values(points_collected = cust_joined_record['points_collected'] + cust_joined_record['points_offered'] * cust_joined_record['quantity'])
                        conn.execute(stmt3)
                        break
                break

        if not bill_product_record_found:
            raise InvalidInput('bill_id and product_lot_id not found')
        
        return jsonify({
            'status': 200,
            'message': 'Successfully inserted into product_bill table',
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

    except:
        return jsonify({
                'status': 400,
                'message': 'Bad request: invalid input',
                'data': {}
            })




if __name__ == '__main__':
    # create engine to connect to database
    engine = create_engine(dbaddress.DB_ADDRESS)
    conn = engine.connect()
    # create metadata object
    metadata_obj = MetaData(bind=engine)
    MetaData.reflect(metadata_obj)

    # run app in debug mode
    app.run(debug=True)

