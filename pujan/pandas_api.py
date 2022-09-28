# we retrieve data from our Retail Store Management database, and try to get meaningful insights into the data
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, MetaData
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
    filtered_df['payable_price'] = (filtered_df['price'] - filtered_df['discount']) * filtered_df['quantity']

    # now grouping by store_id and branch_name
    grouped_df = filtered_df.groupby(['store_id', 'branch_name'])['payable_price'].sum().rename('total_sales')
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
    combined_df['payable_price'] = (combined_df['price'] - combined_df['discount']) * combined_df['quantity']

    grouped_df = combined_df.groupby(['store_id', 'branch_name', 'product_id', 'product_name']).agg({'quantity': np.sum, 'payable_price': np.sum})
    grouped_df = grouped_df.rename(columns={'quantity': 'total_quantity_sold', 'payable_price': 'total_price_sold'})
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

    grouped_df = grouped_df.fillna('')
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

        if product_dict['product_id'] == '' and product_dict['product_name'] == '' and product_dict['total_quantity_sold'] == '' and product_dict['total_price_sold'] == '':
            product_dict['product_id'] = 'No record available'
            product_dict['product_name'] = 'No record available'
            product_dict['total_quantity_sold'] = 'No record available'
            product_dict['total_price_sold'] = 'No record available'

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

    combined_df['payable_price'] = (combined_df['price'] - combined_df['discount']) * combined_df['quantity']


    # filter out the records for that year
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    filtered_df = combined_df[combined_df['date'].dt.year == yr]

    grouped_df = filtered_df.groupby(['store_id', 'branch_name', 'product_id', 'product_name']).agg({'quantity': np.sum, 'payable_price': np.sum})
    grouped_df = grouped_df.rename(columns={'quantity': 'total_quantity_sold', 'payable_price': 'total_price_sold'})
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

    grouped_df = grouped_df.fillna('')
    print(grouped_df)
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

        if product_dict['product_id'] == '' and product_dict['product_name'] == '' and product_dict['total_quantity_sold'] == '' and product_dict['total_price_sold'] == '':
            product_dict['product_id'] = 'No record available'
            product_dict['product_name'] = 'No record available'
            product_dict['total_quantity_sold'] = 'No record available'
            product_dict['total_price_sold'] = 'No record available'

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


# average monthly sales for each store in each year
@app.route('/api/average_monthly_sales', methods=['GET'])
def average_monthly_sales_each_year():
    """Retrieve average monthly sales for all stores in each year"""

    # we need store, bill, product_bill and product_lot tables
    store_df = pd.read_sql_query('SELECT * FROM store', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)

    # combined the data frames
    combined_df = store_df\
                  .merge(bill_df, on='store_id')\
                  .merge(product_bill_df, on='bill_id')\
                  .merge(product_lot_df, on='product_lot_id')
    
    # convert date to datetime object
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    combined_df['payable_price'] = (combined_df['price'] - combined_df['discount']) * combined_df['quantity']

    # calculate total yearly sales for each store in each year and divide by 12
    grouped_df = combined_df.groupby(['store_id', 'branch_name', combined_df['date'].dt.year])['payable_price'].sum() / 12
    # for each store for each year, is the result obtained
    grouped_df = grouped_df.reset_index()
    grouped_df = grouped_df.rename(columns={'date': 'year', 'payable_price': 'avg_monthly_sales'})
    
    # if some stores have no records at all, then
    grouped_df = grouped_df.merge(store_df, on='store_id', how='right', suffixes=['_left', ''])
    grouped_df = grouped_df.loc[:, ['store_id', 'branch_name', 'year', 'avg_monthly_sales']]

    grouped_df = grouped_df.fillna('')
    
    # change the dataframe to json
    result = grouped_df.to_json(orient='index')

    # load that json for further formatting
    parsed = json.loads(result)

    response_list = []

    for k, v in parsed.items():
        store_dict = dict()
        year_dict = dict()

        store_dict['store_id'] = v['store_id']
        store_dict['branch_name'] = v['branch_name']
        
        year_dict['year'] = v['year']
        year_dict['avg_monthly_sales'] = v['avg_monthly_sales']

        if year_dict['year'] == '' and v['avg_monthly_sales'] == '':
            year_dict['year'] = 'No record available'
            year_dict['avg_monthly_sales'] = 'No record available'
        
        store_record_found = False
        for record in response_list:
            if record['store'] == store_dict:
                year_list = record['year_list']
                year_list.append(year_dict)
                record['year_list'] = year_list
                store_record_found = True
                break

        if not store_record_found:
            outer_dict = dict()
            outer_dict['store'] = store_dict
            outer_dict['year_list'] = []
            outer_dict['year_list'].append(year_dict)
            
            response_list.append(outer_dict)


    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved average monthly sales in each year for all stores',
        'data': {
            'num_branches': len(response_list),
            'records': response_list
        }
    })


# total monthly sales by each month in a given year for all stores
@app.route('/api/total_monthly_sales/<int:yr>', methods=['GET'])
def total_monthly_sales_by_year(yr):
    """Retrieve total monthly sales for a given year for each store"""

    # we need store, bill, product_bill, product_lot tables
    store_df = pd.read_sql_query('SELECT * FROM store', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)

    # join the tables
    combined_df = store_df\
                  .merge(bill_df, on='store_id')\
                  .merge(product_bill_df, on='bill_id')\
                  .merge(product_lot_df, on='product_lot_id')
    
    # convert combined df datetime to datetime
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    combined_df['payable_price'] = (combined_df['price'] - combined_df['discount']) * combined_df['quantity']

    # filter for only given year
    filtered_df = combined_df[combined_df['date'].dt.year == yr]

    # group by month now
    grouped_df = filtered_df.groupby(['store_id', 'branch_name', filtered_df['date'].dt.month_name()])['payable_price'].sum()
    grouped_df = grouped_df.reset_index()
    grouped_df = grouped_df.rename(columns={'payable_price': 'total_sales', 'date': 'month'})

    # suppose there are no records for some stores
    grouped_df = grouped_df.merge(store_df, on='store_id', how='right', suffixes=['_left', ''])
    grouped_df = grouped_df[['store_id', 'branch_name', 'month', 'total_sales']]
    grouped_df = grouped_df.fillna('')

    print(grouped_df)

    # change dataframe to json
    result = grouped_df.to_json(orient='index')
    # load that json for further formatting
    parsed = json.loads(result)
    response_list = []

    for k, v in parsed.items():
        store_dict = dict()
        month_dict = dict()

        store_dict['store_id'] = v['store_id']
        store_dict['branch_name'] = v['branch_name']

        month_dict['month'] = v['month']
        month_dict['total_sales'] = v['total_sales']

        if month_dict['month'] == '' and month_dict['total_sales'] == '':
            month_dict['month'] = 'No record available'
            month_dict['total_sales'] = 'No record available'


        store_record_found = False
        for record in response_list:
            if record['store'] == store_dict:
                month_list = record['month_list']
                month_list.append(month_dict)
                record['month_list'] = month_list
                store_record_found = True
                break
        
        if not store_record_found:
            outer_dict = dict()
            outer_dict['store'] = store_dict
            outer_dict['month_list'] = []
            outer_dict['month_list'].append(month_dict)

            response_list.append(outer_dict)
        
    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved total monthly sales for all stores in a year',
        'data': {
            'num_branches': len(response_list),
            'year': yr,
            'records': response_list
        }
    })


# average sales in each bill
@app.route('/api/avg_bill_sales', methods=['GET'])
def avg_bill_sales():
    """Retrieve average sales in each bill for each store"""

    # we need store, bill, product_bill, product_lot tables
    store_df = pd.read_sql_query('SELECT * FROM store', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)

    # join the tables
    combined_df = store_df\
                  .merge(bill_df, on='store_id')\
                  .merge(product_bill_df, on='bill_id')\
                  .merge(product_lot_df, on='product_lot_id')

    combined_df['payable_price'] = (combined_df['price'] - combined_df['discount']) * combined_df['quantity']

    # first find total of each bills
    grouped_df = combined_df.groupby(['store_id', 'branch_name', 'bill_id'])['payable_price'].sum().rename('bill_total')
    
    grouped_df = grouped_df.reset_index(level='bill_id')

    # now again group by store_id, branch_name and calculate average bill_total
    grouped_df = grouped_df.groupby(level=['store_id', 'branch_name'])['bill_total'].mean().rename('average_bill_sales')
    grouped_df = grouped_df.reset_index()

    # for newly formed branches for which there is no record
    grouped_df = grouped_df.merge(store_df, on='store_id', how='right', suffixes=['_left', ''])
    grouped_df = grouped_df[['store_id', 'branch_name', 'average_bill_sales']]
    grouped_df = grouped_df.fillna(0)
    
    # change the dataframe to json
    result = grouped_df.to_json(orient='index')

    # load that json for further formatting
    parsed = json.loads(result)

    response_list = [v for k, v in parsed.items()]


    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved average bill sales for each store',
        'data': {
            'num_branches': len(response_list),
            'recors': response_list
        }
    })



# different type of products supplied by each manufactuer
@app.route('/api/manufacturer_products', methods=['GET'])
def manufacturer_products():
    """Retrieve number of products made by each manufacturer"""

    manufacturer_df = pd.read_sql_query('SELECT * FROM manufacturer', engine)
    product_df = pd.read_sql_query('SELECT * FROM product', engine)

    joined_df = manufacturer_df.merge(product_df, on='manufacturer_id', how='left')
    grouped_df = joined_df.groupby(['manufacturer_id', 'manufacturer_name'])['product_id'].count().rename('num_of_products')
    grouped_df = grouped_df.reset_index()

    # convert dataframe to json
    result = grouped_df.to_json(orient='index')
    parsed = json.loads(result)
    response_list = [v for k, v in parsed.items()]

    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved all manufacturer records',
        'data': {
            'num_manfacturers': len(response_list),
            'records': response_list
        }
    })


# total sales of each category in each month of a year
@app.route('/api/category_sales/<int:yr>', methods=['GET'])
def category_sales(yr):
    """Retrieve total sales for each category in each month of a year"""

    # we need category, product, product_lot, product_bill, bill tables
    category_df = pd.read_sql_query('SELECT * FROM category', engine)
    product_df = pd.read_sql_query('SELECT * FROM product', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)


    # convert dataframe to json
    joined_df = category_df\
                .merge(product_df, on='category_id')\
                .merge(product_lot_df, on='product_id')\
                .merge(product_bill_df, on='product_lot_id')\
                .merge(bill_df, on='bill_id')
    joined_df['date'] = pd.to_datetime(joined_df['date'])

    # filter the year
    filtered_df = joined_df[joined_df['date'].dt.year == yr]
    filtered_df['payable_price'] = (filtered_df['price'] - filtered_df['discount']) * filtered_df['quantity']

    grouped_df = filtered_df.groupby(['category_id', 'category_name', filtered_df['date'].dt.month_name()])['payable_price'].sum()
    grouped_df = grouped_df.reset_index()
    grouped_df = grouped_df.rename(columns={'date': 'month', 'payable_price': 'total_sales'})
    grouped_df = grouped_df.merge(category_df, on='category_id', how='right', suffixes=['_left', ''])
    grouped_df = grouped_df.fillna('')

    # convert dataframe to json
    result = grouped_df.to_json(orient='index')
    parsed = json.loads(result)

    response_list = []

    for k, v in parsed.items():
        category_dict = dict()
        month_dict = dict()
        
        category_dict['category_id'] = v['category_id']
        category_dict['category_name'] = v['category_name']

        month_dict['month'] = v['month']
        month_dict['total_sales'] = v['total_sales']

        if month_dict['month'] == '' and month_dict['total_sales'] == '':
            month_dict['month'] = 'No record available'
            month_dict['total_sales'] = 'No record available'

        
        category_record_found = False
        for record in response_list:
            if record['category'] == category_dict:
                month_list = record['month_list']
                month_list.append(month_dict)
                record['month_list'] = month_list
                category_record_found = True
                break

        if not category_record_found:
            outer_dict = dict()
            outer_dict['category'] = category_dict
            outer_dict['month_list'] = []
            outer_dict['month_list'].append(month_dict)

            response_list.append(outer_dict)

    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved each category sales',
        'data': {
            'num_categories': len(response_list),
            'year': yr,
            'records': response_list
        }
    })


# Percentage of men and women doing shopping in each category and total shopping done by each gender
@app.route('/api/gender_category', methods=['GET'])
def gender_category_sales():
    """Retrieve the percentage of men and women doing sales in each category and total sales dones by each gender"""
    
    # we need category, product, product_lot, product_bill, bill, customer tables
    category_df = pd.read_sql_query('SELECT * FROM category', engine)
    product_df = pd.read_sql_query('SELECT * FROM product', engine)
    product_lot_df = pd.read_sql_query('SELECT * FROM product_lot', engine)
    product_bill_df = pd.read_sql_query('SELECT * FROM product_bill', engine)
    bill_df = pd.read_sql_query('SELECT * FROM bill', engine)
    customer_df = pd.read_sql_query('SELECT * FROM customer', engine)

    # join all the tables
    joined_df = customer_df\
                .merge(bill_df, on='customer_id')\
                .merge(product_bill_df, on='bill_id')\
                .merge(product_lot_df, on='product_lot_id')\
                .merge(product_df, on='product_id')\
                .merge(category_df, on='category_id')

    joined_df['payable_price'] = (joined_df['price'] - joined_df['discount']) * joined_df['quantity']

    # find number of customers  of each gender and total shopping they did in one dataframe and percentage of each gender in another dataframe
    grouped_df = joined_df.groupby(['category_id', 'category_name', 'gender']).agg({'customer_id': 'count', 'payable_price': np.sum})
    pct_gender_df = (joined_df.groupby(['category_id', 'category_name'])['gender'].value_counts(normalize=True)*100).rename('gender_pct') # to calculate gender percent
    concated_df = pd.concat([grouped_df, pct_gender_df], axis=1)

    concated_df = concated_df.rename(columns={'customer_id': 'num_customers', 'payable_price': 'total_sales'})
    concated_df = concated_df.reset_index()

    # some categories may have no sales records at all, for those, we join category table again
    concated_df = concated_df.merge(category_df, on='category_id', how='right', suffixes=['_left', ''])
    concated_df = concated_df[['category_id', 'category_name', 'gender', 'num_customers', 'gender_pct', 'total_sales']]
    concated_df = concated_df.fillna('')

    # convert dataframe to json
    result = concated_df.to_json(orient='index')
    parsed = json.loads(result)

    response_list = []

    for k, v in parsed.items():
        category_dict = dict()
        gender_dict = dict()

        category_dict['category_id'] = v['category_id']
        category_dict['category_name'] = v['category_name']

        gender_dict['gender'] = v['gender']
        gender_dict['num_customers'] = v['num_customers']
        gender_dict['gender_pct'] = v['gender_pct']
        gender_dict['total_sales'] = v['total_sales']

        if gender_dict['gender'] == '' and gender_dict['num_customers'] == '' and gender_dict['gender_pct'] == '' and gender_dict['total_sales'] == '':
            gender_dict['gender'] = 'No record available'
            gender_dict['num_customers'] = 'No record available'
            gender_dict['gender_pct'] = 'No record available'
            gender_dict['total_sales'] = 'No record available'

        category_found = False
        for record in response_list:
            if record['category'] == category_dict:
                gender_list = record['gender_list']
                gender_list.append(gender_dict)
                record['gender_list'] = gender_list
                category_found = True
                break
        if not category_found:
            outer_dict = dict()
            outer_dict['category'] = category_dict
            outer_dict['gender_list'] = [gender_dict]

            response_list.append(outer_dict)
        
 
    return jsonify({
        'status': 200,
        'message': 'Successfully retrieved gender sales record for each category',
        'data': {
            'num_categories': len(response_list),
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