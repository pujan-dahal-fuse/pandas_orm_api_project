# we retrieve data from our Retail Store Management database, and try to get meaningful insights into the data

from heapq import merge
from importlib.machinery import SourceFileLoader
from math import comb
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

def jprint(obj):
    data = json.dumps(obj, indent=4)
    print(data)

# retrieve total sales in each store for given year
@app.route('/api/total_sales_store/<int:yr>', methods=['GET'])
def total_sales_by_store(yr):
    """Retrieve total sales in each store for given year"""

    # fetch all table records into pandas data_frames
    store_df = pd.read_sql_query('SELECT * FROM store', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)

    # join all required tables
    combined_df = store_df\
                  .merge(bill_df, how='left', on='store_id')\
                  .merge(product_bill_df, how='inner', on='bill_id')\
                  .merge(product_lot_df, how='inner', on='product_lot_id')

    combined_df['date'] = pd.to_datetime(combined_df['date']) # we need to perform date operations so convert to date
    filtered_df = combined_df[combined_df['date'].dt.year == yr] # select only particular records
    # payable price is calculated by subtracting discount from price
    filtered_df['payable_price'] = filtered_df['price'] - filtered_df['discount']

    # now grouping by store_id and branch_name
    grouped_df = combined_df.groupby(['store_id', 'branch_name'])['payable_price'].sum().rename('total_sales')
    grouped_df = grouped_df.reset_index()

    # for including those years in which the branches have no purchase records
    # join grouped_df to store table (right join)
    grouped_df = grouped_df.merge(store_df, on='store_id', how='right', suffixes=['_left', ''])
    grouped_df = grouped_df.loc[:, ['store_id', 'branch_name', 'total_sales']]
    
    # to fill the null values in total_sales records
    # for some years there may not be any records for some branches, as a result, tehre is null in total_sales, which we will replace by 0
    grouped_df = grouped_df.fillna(0)

    result = grouped_df.to_json(orient='index') # orient the records according to rows i.e. index
    parsed = json.loads(result)

    response_list = [v for k, v in parsed.items()] # value only because key is index i.e. 0, 1, 2 which we don't need

    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved total sales record of all stores in a year',
        'data': {
            'num_branches': len(response_list),
            'year': yr,
            'records': response_list
        }
    })


# retrieve most popular product in each store of all time
@app.route('/api/popular_products', methods=['GET'])
def store_popular_products():
    """Retrieve the most popular product in each store of all time"""
    
    # most popular product in each store is the product that is bought most number of times
    # we need store, bill, product_bill, product_lot and product_tables

    store_df = pd.read_sql_query('SELECT * FROM store', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
    product_df = pd.read_sql_query('SELECT * FROM product', engine)

    # merge all the data_frames
    combined_df = store_df\
                  .merge(bill_df, on='store_id')\
                  .merge(product_bill_df, on='bill_id')\
                  .merge(product_lot_df, on='product_lot_id')\
                  .merge(product_df, on='product_id')
    combined_df['payable_price'] = combined_df['price'] - combined_df['discount']

    grouped_df = combined_df.groupby(['store_id', 'branch_name', 'product_id', 'product_name'])['payable_price'].agg(['count', np.sum])
    grouped_df = grouped_df.rename(columns={'count': 'total_quantity_sold', 'sum': 'total_price_sold'})
    grouped_df = grouped_df.sort_values(by='total_quantity_sold', ascending=False)

    # reset indexes product_id and product_name
    grouped_df = grouped_df.reset_index(level=['product_id', 'product_name'])

    # group again by store_id and branch_name and select only the first record for each branch this time i.e.  the record with maximum quantity_sold
    grouped_df = grouped_df.groupby(level=['store_id', 'branch_name']).first()
    grouped_df = grouped_df.reset_index()


    # what if a branch is newly added and there is not product sale yet
    # join grouped df to store table (right join)
    grouped_df = grouped_df.merge(store_df, on='store_id', how='right', suffixes=['_left', ''])
    grouped_df = grouped_df.loc[:, ['store_id', 'branch_name', 'product_id', 'product_name', 'total_quantity_sold', 'total_price_sold']]

    # change the dataframe to json
    result = grouped_df.to_json(orient='index')
    # load that json for further formatting
    parsed = json.loads(result)

    response_list = []
    for k, v in parsed.items():
        outer_dict = dict()
        store_dict = dict()
        product_dict = dict()

        store_dict['store_id'] = v['store_id']
        store_dict['branch_name'] = v['branch_name']

        product_dict['product_id'] = v['product_id']
        product_dict['product_name'] = v['product_name']
        product_dict['total_quantity_sold'] = v['total_quantity_sold']
        product_dict['total_price_sold'] = v['total_price_sold']

        outer_dict['store'] = store_dict
        outer_dict['most_popular_product'] = product_dict

        response_list.append(outer_dict)


    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved most popular products of all time',
        'data': {
            'num_branches': len(response_list),
            'records': response_list
        }
    })

# most popular products in each store in a particular year
@app.route('/api/popular_products/<int:yr>', methods=['GET'])
def store_popular_products_by_year(yr):
    """Retrieve the most popular products in each store in a particular year"""

    # most popular product in each store is the product that is bought most number of times in that year
    # we need store, bill, product_bill, product_lot and product_tables

    store_df = pd.read_sql_query('SELECT * FROM store', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
    product_df = pd.read_sql_query('SELECT * FROM product', engine)

    # merge all the data_frames
    combined_df = store_df\
                  .merge(bill_df, on='store_id')\
                  .merge(product_bill_df, on='bill_id')\
                  .merge(product_lot_df, on='product_lot_id')\
                  .merge(product_df, on='product_id')

    combined_df['payable_price'] = combined_df['price'] - combined_df['discount']


    # filter out the records for that year
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    filtered_df = combined_df[combined_df['date'].dt.year == yr]

    grouped_df = filtered_df.groupby(['store_id', 'branch_name', 'product_id', 'product_name'])['payable_price'].agg(['count', np.sum])
    grouped_df = grouped_df.rename(columns={'count': 'total_quantity_sold', 'sum': 'total_price_sold'})
    grouped_df = grouped_df.sort_values(by='total_quantity_sold', ascending=False)

    # reset indexes product_id and product_name
    grouped_df = grouped_df.reset_index(level=['product_id', 'product_name'])

    # group again by store_id and branch_name and select only the first record for each branch this time i.e.  the record with maximum quantity_sold
    grouped_df = grouped_df.groupby(level=['store_id', 'branch_name']).first()
    grouped_df = grouped_df.reset_index()

    # what if a branch is newly added and there is not product sale yet
    # join grouped df to store table (right join)
    grouped_df = grouped_df.merge(store_df, on='store_id', how='right', suffixes=['_left', ''])
    grouped_df = grouped_df.loc[:, ['store_id', 'branch_name', 'product_id', 'product_name', 'total_quantity_sold', 'total_price_sold']]

    # change the dataframe to json
    result = grouped_df.to_json(orient='index')
    # load that json for further formatting
    parsed = json.loads(result)

    response_list = []
    for k, v in parsed.items():
        outer_dict = dict()
        store_dict = dict()
        product_dict = dict()

        store_dict['store_id'] = v['store_id']
        store_dict['branch_name'] = v['branch_name']

        product_dict['product_id'] = v['product_id']
        product_dict['product_name'] = v['product_name']
        product_dict['total_quantity_sold'] = v['total_quantity_sold']
        product_dict['total_price_sold'] = v['total_price_sold']

        outer_dict['store'] = store_dict
        outer_dict['most_popular_product'] = product_dict

        response_list.append(outer_dict)


    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved most popular products for all stores in a year',
        'data': {
            'num_branches': len(response_list),
            'year': yr,
            'records': response_list
        }
    })


# average weekly sales for each store in each year
@app.route('/api/average_weekly_sales', methods=['GET'])
def average_weekly_sales_all_stores():
    """Retrieve average weekly sales for all stores in each year"""

    # we need store, bill, product_bill and product_lot tables
    store_df = pd.read_sql_query('SELECT * FROM store', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)




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