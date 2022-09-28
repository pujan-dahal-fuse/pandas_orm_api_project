# we retrieve data from our Retail Store Management database, and try to get meaningful insights into the data
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

def jprint(obj):
    data = json.dumps(obj, indent=4)
    print(data)

#the total sales done by the manufacturer
@app.route('/api/manufacturer_sales', methods=['GET'])
def manufacturer_sales():
    """Retrieve the total sales done by manufacturer and percentage of sales out of total"""
    
    # we need manufacturer, product, product_lot, product_bill

    manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)
    product_df = pd.read_sql_query('SELECT * FROM product', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)

    joined_df = manufacturer_df\
                .merge(product_df, on='manufacturer_id')\
                .merge(product_lot_df, on='product_id')\
                .merge(product_bill_df, on='product_lot_id')\
                
                
                

    joined_df['payable_price'] = (joined_df['price'] - joined_df['discount']) * joined_df['quantity']

# find number of sales and percentage  in another dataframe
    grouped_df = joined_df.groupby(['manufacturer_id', 'manufacturer_name'])['payable_price'].sum().rename('total_sales')
    grouped_df = grouped_df.reset_index()
    # pct_manufacturer_df = (joined_df.groupby(['manufacturer_id', 'manufacturer_name'])['manufacturer_id'].value_counts(normalize=True).rename('percent_sales'))
    # pct_manufacturer_df = pct_manufacturer_df.reset_index()
    # concated_df = pd.concat([grouped_df, pct_manufacturer_df], axis=1)

# convert dataframe to json
    result = grouped_df.to_json(orient='index')
    parsed = json.loads(result)
    response_list = []

    for k, v in parsed.items():
        manufacturer_dict = dict()
        

        manufacturer_dict['manufacturer_id'] = v['manufacturer_id']
        manufacturer_dict['manufacturer_name'] = v['manufacturer_name']

        manufacturer_dict['total_sales'] = v['total_sales']

        if  manufacturer_dict['total_sales'] == '':
         manufacturer_dict['total_sales'] = 'No record available'

        manufacturer_found = False
        for record in response_list:
            if record['manufacturer'] == manufacturer_dict:
                manufacturer_list = record['manufacturer_list']
                record['manufacturer_list'] = manufacturer_list
                manufacturer_found = True
                break
        if not manufacturer_found:
            outer_dict = dict()
            outer_dict['manufacturer'] = manufacturer_dict
            

            response_list.append(outer_dict)
        
 
    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved total sales record for each manufacturer',
        'data': {
            'num_manufacturer': len(response_list),
            'records': response_list
        }
    })


#the total sales done calculated by category
@app.route('/api/category_sales', methods=['GET'])
def category_sales():
    """Retrieve the total sales done by category and percentage of sales out of total"""
    
    # we need category, product, product_lot, product_bill
    category_df = pd.read_sql_query('SELECT * FROM category', engine)
    product_df = pd.read_sql_query('SELECT * FROM product', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)

    joined_df = category_df\
                .merge(product_df, on='category_id')\
                .merge(product_lot_df, on='product_id')\
                .merge(product_bill_df, on='product_lot_id')\

    joined_df['payable_price'] = (joined_df['price'] - joined_df['discount']) * joined_df['quantity']

# find number of sales and percentage  in another dataframe
    grouped_df = joined_df.groupby(['category_id', 'category_name'])['payable_price'].sum().rename('total_sales')
    grouped_df = grouped_df.reset_index()
    # pct_manufacturer_df = (joined_df.groupby(['category_id', 'category_name'])['total_sales'].value_counts(normalize=True))
    # concated_df = pd.concat([grouped_df, pct_manufacturer_df], axis=1)

# convert dataframe to json
    result = grouped_df.to_json(orient='index')
    parsed = json.loads(result)
    response_list = []

    for k, v in parsed.items():
        category_dict = dict()
        

        category_dict['category_id'] = v['category_id']
        category_dict['category_name'] = v['category_name']

        category_dict['total_sales'] = v['total_sales']

        if  category_dict['total_sales'] == '':
         category_dict['total_sales'] = 'No record available'

        category_found = False
        for record in response_list:
            if record['category'] == category_dict:
                category_list = record['category_list']
                record['category_list'] = category_list
                category_found = True
                break
        if not category_found:
            outer_dict = dict()
            outer_dict['category'] = category_dict
            

            response_list.append(outer_dict)
        
 
    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved total sales record for each category',
        'data': {
            'num_category': len(response_list),
            'records': response_list
        }
    })


    
if __name__ == '__main__':
    # create engine to connect to database
    engine = create_engine(dbaddress.DB_ADDRESS)

    # create connection to perform queries on database to load it to dataframe
    conn = engine.connect()

    # create metadata object
    metadata_obj = MetaData(bind=engine)
    MetaData.reflect(metadata_obj)

    # run app in debug mode
    app.run(debug=True)