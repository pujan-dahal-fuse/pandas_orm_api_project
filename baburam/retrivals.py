from heapq import merge
from math import comb
from operator import methodcaller
from tokenize import group
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import select

# for importing dbaddress
from importlib.machinery import SourceFileLoader
dbaddress = SourceFileLoader('dbaddress', '../dbaddress.py').load_module()


import MySQLdb
import json
import pandas as pd
import numpy as np

app = Flask(__name__)

@app.route('/api/',methods=["GET"])
def fun_api():
    return jsonify({
        'status':'success',
        'message':"Api  is Running.....",
    })

# store details retrieve
@app.route('/api/store/', methods=['GET'])
def api_store():
    if request.method=="GET":
        try:
            store_df = pd.read_sql_query('SELECT * FROM store', engine)
            # index
            result_store = store_df.to_json(orient='index') 
            parsed_store = json.loads(result_store)
            response_store = [val for key, val in parsed_store.items()] 
            return jsonify({
                    'status': 200,
                    'message': 'Successfully Store Retrieved ....',
                    'Recoded Data':response_store
                    })
        except:
            return jsonify({
            'status': 'Error..',
            'message': 'Wrong Method',
            })
    else:
        return jsonify({
            'status': 'Error..',
            'message': 'Wrong Method',
            })


# product deails retrieve
@app.route('/api/product/', methods=['GET'])
def api_product():
    if request.method=="GET":
        try:
            product_df = pd.read_sql_query('SELECT * FROM product', engine)
            # index
            result_product = product_df.to_json(orient='index') 
            parsed_product = json.loads(result_product)
            response_product = [val for key, val in parsed_product.items()] 

            return jsonify({
                'status': 200,
                'message': 'Successfully Product Retrieved ....',
                'Recoded Data':response_product
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Bills....',
                })
    else:
        return jsonify({
            'status': 'Error..',
            'message': 'Wrong Method',
            })

# product lot  detailsretrieve
@app.route('/api/product_lot/', methods=['GET'])
def api_product_lot():
    if request.method=="GET":
        try:
            product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
            # index
            result_product_lot = product_lot_df.to_json(orient='index') 
            parsed_product_lot = json.loads(result_product_lot)
            response_product_lot = [val for key, val in parsed_product_lot.items()] 

            return jsonify({
                'status': 200,
                'message': 'Successfully Produc Lot Retrieved ....',
                'Recoded Data':response_product_lot
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product Lots....',
                })
    else:
        return jsonify({
            'status': 'Error..',
            'message': 'Wrong Method',
            })


# store product details retrieve
@app.route('/api/store_product/', methods=['GET'])
def api_store_product():
    if request.method=="GET":
        try:
            store_product_df = pd.read_sql_query('SELECT * FROM store_product', engine)
            # index
            result_store_product = store_product_df.to_json(orient='index') 
            parsed_store_product = json.loads(result_store_product)
            response_store_product = [val for key, val in parsed_store_product.items()] 

            return jsonify({
                'status': 200,
                'message': 'Successfully Store Product Retrieved ....',
                'Recoded Data':response_store_product
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Store Product....',
                })
    else:
        return jsonify({
            'status': 'Error..',
            'message': 'Wrong Method',
            })


# category details retrieve
@app.route('/api/category/', methods=['GET'])
def api_category():
    try:
        if request.method=="GET":
            category_df = pd.read_sql_query('SELECT * FROM category', engine)
            # index
            result_category = category_df.to_json(orient='index') 
            parsed_category = json.loads(result_category)
            response_category = [val for key, val in parsed_category.items()] 

            return jsonify({
                'status': 200,
                'message': 'Successfully Category Retrieved ....',
                'Recoded Data':response_category
                })
        else:
            return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })
    except:
        return jsonify({
            'status': 'Error..',
            'message': 'Error Occured while Retrieving Category....',
            })

       
# customer  detailsretrieve
@app.route('/api/customer/', methods=['GET'])
def api_customer():
    if request.method=="GET":
        try:
            customer_df = pd.read_sql_query('SELECT * FROM customer', engine)
            # index
            result_customer = customer_df.to_json(orient='index') 
            parsed_customer = json.loads(result_customer)
            response_customer = [val for key, val in parsed_customer.items()] 

            return jsonify({
                'status': 'Success',
                'message': 'Successfully Customer Retrieved ....',
                'Recoded Data':response_customer
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Customers....',
                })
    else:
        return jsonify({
            'status': 'Error..',
            'message': 'Wrong Method',
            })

# manufacturer details retrieve
@app.route('/api/manufacturer/', methods=['GET'])
def api_manufacturer():
    if request.method=="GET":
        try:
            manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)
            # index
            result_manufacturer = manufacturer_df.to_json(orient='index') 
            parsed_manufacturer = json.loads(result_manufacturer)
            response_manufacturer = [val for key, val in parsed_manufacturer.items()] 

            return jsonify({
                'status':"Success",
                'message': 'Successfully Manufacturer Retrieved ....',
                'Recoded Data':response_manufacturer
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Manufacturer....',
                })
    else:
        return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })

# bill details retrieve
@app.route('/api/bill/', methods=['GET'])
def api_bill():
    try:
        if request.method=="GET":
            bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
            # index
            result_bill = bill_df.to_json(orient='index') 
            parsed_bill = json.loads(result_bill)
            response_bill = [val for key, val in parsed_bill.items()] 

            return jsonify({
                'status': 200,
                'message': 'Successfully Bills Retrieved ....',
                'Recoded Data':response_bill
                })
        else:
            return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })
    except:
        return jsonify({
            'status': 'Error..',
            'message': 'Error Occured while Retrieving Bills....',
            })


# product deails retrieve
@app.route('/api/product_bill/', methods=['GET'])
def api_product_bill():
    if request.method=="GET":
        try:
            product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
            # index
            result_product_bill = product_bill_df.to_json(orient='index') 
            parsed_product_bill = json.loads(result_product_bill)
            response_product_bill = [val for key, val in parsed_product_bill.items()] 

            return jsonify({
                'status': 200,
                'message': 'Successfully Product Bills Retrieved ....',
                'Recoded Data':response_product_bill
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product Bills....',
                })
    else:
        return jsonify({
            'status': 'Error..',
            'message': 'Wrong Method',
            })
             
if __name__ == '__main__':

    # create engine
    engine = create_engine(dbaddress.DB_ADDRESS)

    # create connection 
    conn = engine.connect()

    # create metadata_object
    metadata_obj = MetaData(bind=engine)
    MetaData.reflect(metadata_obj)

    # run app
    app.run(debug=True)