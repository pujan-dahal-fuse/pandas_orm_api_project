from flask import Flask,jsonify,request 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship,sessionmaker
from sqlalchemy.exc import IntegrityError
import MySQLdb
import pandas as pd
from sqlalchemy import create_engine,Table,Column,Integer,String,ForeignKey,MetaData
from sqlalchemy import insert 
from importlib.machinery import SourceFileLoader
dbaddress = SourceFileLoader('dbaddress', '../dbaddress.py').load_module()

app = Flask(__name__)

# def jprint(obj):
#     data =json.dumps(obj, indent=4)
#     print(data)


#INSERT STORE DATA

@app.route('/api/insert_customer',methods = ['POST'])
def insert_customer():
    body = request.get_json()
    print(body)
    customer = Table('customer', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(customer)
    try:
        conn.execute(stmt, body)
        return jsonify({"message":" data added successfully"})
    
    except IntegrityError as ie:
        return jsonify({
            'status': 400,
            'message': "Duplicate Input: This input is already inserted" if ie.orig.args[0] == 1062\
                        else "Invalid input",
            'data': {}
        })
    except:
        return({
            'status':404,
            'message':'Error'
            }) 
    

@app.route('/api/insert_store',methods = ['POST'])
def insert_store():
    body = request.get_json()
    store = Table('store', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(store)
    try:
        conn.execute(stmt, body)
        return jsonify({"message":" data added successfully"})

    except IntegrityError as ie:
        return jsonify({
            'status': 400,
            'message': "Duplicate Input: This input is already inserted" if ie.orig.args[0] == 1062\
                        else "Invalid input",
            'data': {}
        })
    except:
        return({
            'status':404,
            'message':'Error'
            }) 



@app.route('/api/insert_manufacturer',methods = ['POST'])
def insert_manufacturer():
    body = request.get_json()
    manufacturer = Table('manufacturer', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(manufacturer)
    try:
        conn.execute(stmt, body)
        return jsonify({"message":" data added successfully"})

    except IntegrityError as ie:
        return jsonify({
            'status': 400,
            'message': "Duplicate Input: This input is already inserted" if ie.orig.args[0] == 1062\
                        else "Invalid input",
            'data': {}
        })
    except:
        return({
            'status':404,
            'message':'Error'
            }) 


@app.route('/api/insert_product',methods = ['POST'])
def insert_product():
    body = request.get_json()
    product = Table('product', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(product)
    try:
        conn.execute(stmt, body)
        return jsonify({"message":" data added successfully"})
    
    except IntegrityError as ie:
        return jsonify({
            'status': 400,
            'message': "Duplicate Input: This input is already inserted" if ie.orig.args[0] == 1062\
                        else "Invalid input",
            'data': {}
        })
    except:
        return({
            'status':404,
            'message':'Error'
            }) 




@app.route('/api/insert_category',methods = ['POST'])
def insert_category():
    body = request.get_json()
    category = Table('category', metadata_obj, autoload=True, autoload_with=engine)
    stmt = insert(category)
    try:
        conn.execute(stmt, body)
        return jsonify({"message":" data added successfully"})

    except IntegrityError as ie:
        return jsonify({
            'status': 400,
            'message': "Duplicate Input: This input is already inserted" if ie.orig.args[0] == 1062\
                        else "Invalid input",
            'data': {}
        })
    except:
        return({
            'status':404,
            'message':'Error'
            }) 


if __name__ == '__main__':
    engine = create_engine(dbaddress.DB_ADDRESS)
    conn = engine.connect()
    metadata_obj = MetaData(bind=engine)
    MetaData.reflect(metadata_obj)
    app.run(debug=True)