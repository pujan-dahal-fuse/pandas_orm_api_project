from heapq import merge
from math import comb
from operator import methodcaller
from tokenize import group
from unicodedata import category
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
        'message':"Pandas Api  is Running.....",
    })

# product details  in store
@app.route('/api/store_product_detail/', methods=['GET'])
def api_store_product_detail():
    if request.method=="GET":
        try:
            store_df = pd.read_sql_query('SELECT * FROM store', engine)
            store_product_df=pd.read_sql_query('SELECT * FROM store_product',engine)
            product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
            product_df = pd.read_sql_query('SELECT * FROM product', engine)
            category_df = pd.read_sql_query('SELECT * FROM category', engine)
            manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)

            # merge all the data_frames
            combined_store_product_df = store_df\
                .merge(store_product_df, on='store_id')\
                .merge(product_lot_df, on='product_lot_id')\
                .merge(product_df, on='product_id')\
                .merge(category_df, on='category_id')\
                .merge(manufacturer_df, on='manufacturer_id')

            # index
            combined_store_product_result = combined_store_product_df.to_json(orient='index') 
            combined_store_product_parsed = json.loads(combined_store_product_result)
            combined_store_product_response = [val for key, val in combined_store_product_parsed.items()] 
            return jsonify({
                'status': 'Success',
                'message': 'Successfully Product Details Retrieved ....',
                'Recoded Data':combined_store_product_response
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product details....',
                })
    else:
        return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })

# minimum product  in store
@app.route('/api/min_stock/', methods=['GET'])
def api_min_stock():
    if request.method=="GET":
        try:
            store_df = pd.read_sql_query('SELECT * FROM store', engine)
            store_product_df=pd.read_sql_query('SELECT * FROM store_product',engine)
            product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
            product_df = pd.read_sql_query('SELECT * FROM product', engine)
            category_df = pd.read_sql_query('SELECT * FROM category', engine)
            manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)

            # merge all the data_frames
            combined_store_product_df = store_df\
                .merge(store_product_df, on='store_id')\
                .merge(product_lot_df, on='product_lot_id')\
                .merge(product_df, on='product_id')\
                .merge(category_df, on='category_id')\
                .merge(manufacturer_df, on='manufacturer_id')
            
            combined_store_product_df = combined_store_product_df[["store_id","branch_name",
                "product_name","weight_gm", "description","category_name","in_stock","price","discount",
                "manufacturer_name","manufacture_date","expiry_date","points_offered"]]

            combined_store_product_df=combined_store_product_df.sort_values(by=['in_stock']).head(3)
            combined_store_product_result = combined_store_product_df.to_json(orient='index') 
            combined_store_product_parsed = json.loads(combined_store_product_result)
            combined_store_product_response = [val for key, val in combined_store_product_parsed.items()] 
            return jsonify({
                'status': 'Success',
                'message': 'Successfully Product Details Retrieved ....',
                'Recoded Data':combined_store_product_response
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product details....',
                })
    else:
        return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })



@app.route('/api/max_stock/', methods=['GET'])
def api_max_stock():
    if request.method=="GET":
        try:
            store_df = pd.read_sql_query('SELECT * FROM store', engine)
            store_product_df=pd.read_sql_query('SELECT * FROM store_product',engine)
            product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
            product_df = pd.read_sql_query('SELECT * FROM product', engine)
            category_df = pd.read_sql_query('SELECT * FROM category', engine)
            manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)

            # merge all the data_frames
            combined_store_product_df = store_df\
                .merge(store_product_df, on='store_id')\
                .merge(product_lot_df, on='product_lot_id')\
                .merge(product_df, on='product_id')\
                .merge(category_df, on='category_id')\
                .merge(manufacturer_df, on='manufacturer_id')
            
            combined_store_product_df = combined_store_product_df[["store_id","branch_name",
                "product_name","weight_gm", "description","category_name","in_stock","price","discount",
                "manufacturer_name","manufacture_date","expiry_date","points_offered"]]

            combined_store_product_df=combined_store_product_df.sort_values(by=['in_stock'],ascending=False).head(3)
            combined_store_product_result = combined_store_product_df.to_json(orient='index') 
            combined_store_product_parsed = json.loads(combined_store_product_result)
            combined_store_product_response = [val for key, val in combined_store_product_parsed.items()] 
            return jsonify({
                'status': 'Success',
                'message': 'Successfully Product Details Retrieved ....',
                'Recoded Data':combined_store_product_response
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product details....',
                })
    else:
        return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })
# product details in branch
@app.route('/api/branch/<string:branch>', methods=['GET'])
def api_branch(branch):
    if request.method=="GET":
        try:
            store_df = pd.read_sql_query('SELECT * FROM store', engine)
            store_product_df=pd.read_sql_query('SELECT * FROM store_product',engine)
            product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
            product_df = pd.read_sql_query('SELECT * FROM product', engine)
            category_df = pd.read_sql_query('SELECT * FROM category', engine)
            manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)

            # merge all the data_frames
            combined_store_product_df = store_df\
                .merge(store_product_df, on='store_id')\
                .merge(product_lot_df, on='product_lot_id')\
                .merge(product_df, on='product_id')\
                .merge(category_df, on='category_id')\
                .merge(manufacturer_df, on='manufacturer_id')
            
            combined_store_product_df = combined_store_product_df[["store_id","branch_name",
                "product_name","weight_gm", "description","category_name","in_stock","price","discount",
                "manufacturer_name","manufacture_date","expiry_date","points_offered"]]
            
            combined_store_product_df = combined_store_product_df[combined_store_product_df["branch_name"].str.lower()== branch.lower()]
            combined_store_product_result = combined_store_product_df.to_json(orient='index') 
            combined_store_product_parsed = json.loads(combined_store_product_result)
            
            combined_store_product_response = [val for key, val in combined_store_product_parsed.items()] 
            return jsonify({
                'status': 'Success',
                'message': 'Successfully Product Details Retrieved ....',
                'Recoded Data':combined_store_product_response
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product details....',
                })
    else:
        return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })

# product name search
@app.route('/api/product/<int:id>', methods=['GET'])
def api_product(id):
    if request.method=="GET":
        try:
            store_df = pd.read_sql_query('SELECT * FROM store', engine)
            store_product_df=pd.read_sql_query('SELECT * FROM store_product',engine)
            product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
            product_df = pd.read_sql_query('SELECT * FROM product', engine)
            category_df = pd.read_sql_query('SELECT * FROM category', engine)
            manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)

            # merge all the data_frames
            combined_store_product_df = store_df\
                .merge(store_product_df, on='store_id')\
                .merge(product_lot_df, on='product_lot_id')\
                .merge(product_df, on='product_id')\
                .merge(category_df, on='category_id')\
                .merge(manufacturer_df, on='manufacturer_id')
            
            combined_store_product_df = combined_store_product_df[["store_id","branch_name",
                "product_id","product_name","weight_gm", "description","category_name","in_stock","price","discount",
                "manufacturer_name","manufacture_date","expiry_date","points_offered"]]
            
            combined_store_product_df = combined_store_product_df[id== combined_store_product_df["product_id"]]
            combined_store_product_result = combined_store_product_df.to_json(orient='index') 
            combined_store_product_parsed = json.loads(combined_store_product_result)
            
            combined_store_product_response = [val for key, val in combined_store_product_parsed.items()] 
            return jsonify({
                'status': 'Success',
                'message': 'Successfully Product Details Retrieved ....',
                'Recoded Data':combined_store_product_response
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product details....',
                })
    else:
        return jsonify({
            'status': 'Error',
            'message': 'Wrong Method',
            })


# manufacturer name search
@app.route('/api/menufacturer/<int:id>', methods=['GET'])
def api_menufacturer(id):
    if request.method=="GET":
        try:
            store_df = pd.read_sql_query('SELECT * FROM store', engine)
            store_product_df=pd.read_sql_query('SELECT * FROM store_product',engine)
            product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
            product_df = pd.read_sql_query('SELECT * FROM product', engine)
            category_df = pd.read_sql_query('SELECT * FROM category', engine)
            manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)

            # merge all the data_frames
            combined_store_product_df = store_df\
                .merge(store_product_df, on='store_id')\
                .merge(product_lot_df, on='product_lot_id')\
                .merge(product_df, on='product_id')\
                .merge(category_df, on='category_id')\
                .merge(manufacturer_df, on='manufacturer_id')
            
            combined_store_product_df = combined_store_product_df[[
                "product_name","weight_gm", "description","category_name","price",
                "manufacturer_id","manufacturer_name","manufacture_date","expiry_date",]]
            
            combined_store_product_df = combined_store_product_df[id== combined_store_product_df["manufacturer_id"]]
            combined_store_product_result = combined_store_product_df.to_json(orient='index') 
            combined_store_product_parsed = json.loads(combined_store_product_result)
            
            combined_store_product_response = [val for key, val in combined_store_product_parsed.items()] 
            return jsonify({
                'status': 'Success',
                'message': 'Successfully Product Details Retrieved ....',
                'Recoded Data':combined_store_product_response
                })
        except:
            return jsonify({
                'status': 'Error..',
                'message': 'Error Occured while Retrieving Product details....',
                })
    else:
        return jsonify({
            'status': 'Error',
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