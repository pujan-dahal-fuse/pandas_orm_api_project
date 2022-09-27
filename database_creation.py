from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy import Column, Integer, Float, String, ForeignKey, Table, Date, UniqueConstraint, Boolean
from sqlalchemy import insert
from dbaddress import DB_ADDRESS
import MySQLdb


def create_schema(engine):
    # creating tables in database
    """
    Tables list:
    1. store:
        store_id (int, PK)
        branch_name (varchar(20))
        address (varchar(255))
        phone_no (varchar(15)) # country code also included
    
    2. product:
        product_id (int, PK)
        product_name (varchar(100))
        description (varchar(1000))
        weight_gm (float)
        points_offered (float)
        category_id (int, FK)
        manufacturer_id (int, FK)

    3. category:
        category_id (int, PK)
        category_name (varchar (50))

    4. manufacturer:
        manufacturer_id (int, PK)
        manufacturer_name (varchar(255))
        address (varchar(255))
        email (varchar(100))
        phone_no (varchar(15)) # country code also included
        country (varchar(100))
    
    5. product_lot:
        lot_id (int, PK)
        product_id (int, FK)
        manufacture_date (date)
        expiry_date (date)
        price (float)
        discount (float)
        

    6. store_product:
        store_id (int, FK, PK)
        product_lot_id (int, FK, PK)
        in_stock (int)

    7. bill:
        bill_id (int, PK)
        customer_id (int, FK)
        date (date)
        transaction_completed(bool)
    
    8. product_bill:
        bill_id (int, FK, PK)
        product_lot_id (int, FK, PK)
    
    9. customer:
        customer_id (int, PK)
        customer_name (varchar (100))
        address (varchar(255))
        email (varchar(100))
        phone_no (varchar(15)) # somebody might add +977
        points_collected (int)
    """

    # create metadata object and bind it to engine
    metadata_obj = MetaData(bind=engine)
    MetaData.reflect(metadata_obj)


    store = Table('store', metadata_obj,
                  Column('store_id', Integer, primary_key=True, autoincrement=True),
                  Column('branch_name', String(20), nullable=False),
                  Column('address', String(255), nullable=False),
                  Column('phone_no', String(15), nullable=False),
                  UniqueConstraint('branch_name', name='unique_key_store')
                  )

    category = Table('category', metadata_obj,
                     Column('category_id', Integer, primary_key=True),
                     Column('category_name', String(50), nullable=False),
                     UniqueConstraint('category_name', name='unique_key_category')
                     )

    manufacturer = Table('manufacturer', metadata_obj,
                         Column('manufacturer_id', Integer, primary_key=True),
                         Column('manufacturer_name', String(255), nullable=False),
                         Column('address', String(255), nullable=False),
                         Column('email', String(100), nullable=False),
                         Column('phone_no', String(15), nullable=False),
                         Column('country', String(100), nullable=False),
                         UniqueConstraint('manufacturer_name', 'address', 'phone_no', 'country', name='unique_key_manufacturer')
                         )

    product = Table('product', metadata_obj,
                    Column('product_id', Integer, primary_key=True, autoincrement=True),
                    Column('product_name', String(100), nullable=False),
                    Column('weight_gm', Float, nullable=False),
                    Column('points_offered', Float, default=0, nullable=False),
                    Column('description', String(1000), nullable=True, default=None),
                    Column('category_id', Integer, ForeignKey('category.category_id', onupdate='CASCADE'), nullable=True),
                    Column('manufacturer_id', Integer, ForeignKey('manufacturer.manufacturer_id', onupdate='CASCADE'), nullable=True),
                    UniqueConstraint('product_name', 'weight_gm', 'points_offered', 'description', 'category_id', 'manufacturer_id', name='unique_key_product')
                    )

    product_lot = Table('product_lot', metadata_obj,
                        Column('product_lot_id', Integer, primary_key=True, autoincrement=True),
                        Column('manufacture_date', Date, nullable=False),
                        Column('expiry_date', Date, nullable=False),
                        Column('price', Float, nullable=False),
                        Column('discount', Float, default=0, nullable=False),
                        Column('product_id', Integer, ForeignKey('product.product_id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False),
                        UniqueConstraint('manufacture_date', 'expiry_date', 'price', 'discount', 'product_id', name='unique_key_product_lot')
                        )

    store_product = Table('store_product', metadata_obj,
                          Column('store_id', Integer, ForeignKey('store.store_id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
                          Column('product_lot_id', Integer, ForeignKey('product_lot.product_lot_id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
                          Column('in_stock', Integer, default=0, nullable=False)
                          )

    customer = Table('customer', metadata_obj,
                     Column('customer_id', Integer, primary_key=True, autoincrement=True),
                     Column('customer_name', String(100), nullable=False),
                     Column('gender', String(1), nullable=False),
                     Column('address', String(255), nullable=False),
                     Column('email', String(100), nullable=False),
                     Column('phone_no', String(15), nullable=False),
                     Column('points_collected', Float, nullable=False),
                     UniqueConstraint('customer_id', 'customer_name', 'gender', 'address', 'email', 'phone_no', 'points_collected', name='unique_key_customer')
                     )

    bill = Table('bill', metadata_obj,
                 Column('bill_id', Integer, primary_key=True, autoincrement=True),
                 Column('date', Date, nullable=False),
                 Column('customer_id', ForeignKey('customer.customer_id', onupdate='CASCADE'), nullable=True),
                 Column('store_id', ForeignKey('store.store_id', onupdate='CASCADE'), nullable=True)
                 )

    # since product_bill has foreign key attribute bill_id, so bill must be created first
    # the newly created bill must have transaction_completed = False
    # if transaction_completed is already True, a record cannot be added to the product_bill table
    # when customer pays money, transaction_completed becomes True
    # when all products are added to products_bill table, we make transaction_completed= True

    product_bill = Table('product_bill', metadata_obj,
                         Column('product_lot_id', ForeignKey('product_lot.product_lot_id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
                         Column('bill_id', ForeignKey('bill.bill_id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
                         Column('quantity', Integer, nullable=False)
                         )
    
    metadata_obj.create_all(engine)



def insert_initial_records(engine):
    """Insert initial records into the database"""
    # create connection to the engine
    conn = engine.connect()

    # create MetaData object for storing database metadata
    metadata_obj = MetaData(bind=engine)
    MetaData.reflect(metadata_obj)

    store = Table('store', metadata_obj, autoload=True, autoload_with=engine)
    product = Table('product', metadata_obj, autoload=True, autoload_with=engine)
    category = Table('category', metadata_obj, autoload=True, autoload_with=engine)
    manufacturer = Table('manufacturer', metadata_obj, autoload=True, autoload_with=engine)
    product_lot = Table('product_lot', metadata_obj, autoload=True, autoload_with=engine)
    store_product = Table('store_product', metadata_obj, autoload=True, autoload_with=engine)
    bill = Table('bill', metadata_obj, autoload=True, autoload_with=engine)
    product_bill = Table('product_bill', metadata_obj, autoload=True, autoload_with=engine)
    customer = Table('customer', metadata_obj, autoload=True, autoload_with=engine)


    store_list = [
        {'store_id': 10000001, 'branch_name': 'Pulchowk', 'address': 'Pulchowk, Lalitpur', 'phone_no': '01-556789'},
        {'store_id': 10000002, 'branch_name': 'Maharajgunj', 'address': 'Maharajgunj, Kathmandu', 'phone_no': '01-455887'},
        {'store_id': 10000003, 'branch_name': 'Naxal', 'address': 'Naxal, Kathmandu', 'phone_no': '01-855798'}
    ]

    category_list = [
        {'category_id': 20000001, 'category_name': 'Body Care'},
        {'category_id': 20000002, 'category_name': 'Bevarage'},
        {'category_id': 20000003, 'category_name': 'Pet Food'},
        {'category_id': 20000004, 'category_name': 'Child Care'},
        {'category_id': 20000005, 'category_name': 'Packaged Food'}
    ]

    manufacturer_list = [
        {'manufacturer_id': 30000001, 'manufacturer_name': 'Patanjali Ayurved Company', 'address': 'Varanasi, India', 'email': 'info@patanjali.in', 'phone_no': '+91 8865530223', 'country': 'India'},
        {'manufacturer_id': 30000002, 'manufacturer_name': 'Johnson and Johnson Pharmaceutical', 'address': 'Atlanta, Georgia, USA', 'email': 'info@jnj.com', 'phone_no': '+1 8795622364', 'country': 'USA'},
        {'manufacturer_id': 30000003, 'manufacturer_name': 'Hanuman Bhuja Udhyog', 'address': 'Birgunj, Parsa, Nepal', 'email': 'hanumanbhuja123@gmail.com', 'phone_no': '+977 023125669', 'country': 'Nepal'}
    ]

    product_list = [
        {
            'product_id': 40000001, 
            'product_name': 'Patanjali Dant Kanti Toothpaste', 
            'weight_gm': 300, 
            'points_offered': 1.5,
            'description': 'A toothpase made by ayurvedic herbs', 
            'category_id': 20000001,
            'manufacturer_id': 30000001
        },
        {
            'product_id': 40000002,
            'product_name': 'Kurum Kurum Chiura',
            'weight_gm': 500,
            'points_offered': 2.5,
            'description': 'Crispy beaten rice',
            'category_id': 20000005,
            'manufacturer_id': 30000003
        },
        {
            'product_id': 40000003,
            'product_name': 'Johnson\'s Baby Powder',
            'weight_gm': 300,
            'points_offered': 5,
            'description': 'Powder to keep childern safe from infections',
            'category_id': 20000004,
            'manufacturer_id': 30000002
        }
    ]

    product_lot_list = [
        {'product_lot_id': 50000001, 'product_id': 40000001, 'manufacture_date': '2022-03-01', 'expiry_date': '2022-12-01', 'price': 200, 'discount': 20},
        {'product_lot_id': 50000002, 'product_id': 40000001, 'manufacture_date': '2022-05-01', 'expiry_date': '2023-02-01', 'price': 220, 'discount': 15},
        {'product_lot_id': 50000003, 'product_id': 40000003, 'manufacture_date': '2021-12-15', 'expiry_date': '2022-12-15', 'price': 300, 'discount': 10},
        {'product_lot_id': 50000004, 'product_id': 40000003, 'manufacture_date': '2022-06-15', 'expiry_date': '2023-06-15', 'price': 350, 'discount': 10},
        {'product_lot_id': 50000005, 'product_id': 40000002, 'manufacture_date': '2022-08-12', 'expiry_date': '2022-11-12', 'price': 80, 'discount': 5},
        {'product_lot_id': 50000006, 'product_id': 40000002, 'manufacture_date': '2022-06-23', 'expiry_date': '2022-09-23', 'price': 70, 'discount': 7}
    ]

    store_product_list = [
        {'store_id': 10000001, 'product_lot_id': 50000001, 'in_stock': 32},
        {'store_id': 10000001, 'product_lot_id': 50000002, 'in_stock': 34},
        {'store_id': 10000001, 'product_lot_id': 50000004, 'in_stock': 23},
        {'store_id': 10000002, 'product_lot_id': 50000001, 'in_stock': 56},
        {'store_id': 10000002, 'product_lot_id': 50000004, 'in_stock': 34},
        {'store_id': 10000002, 'product_lot_id': 50000006, 'in_stock': 0},
        {'store_id': 10000003, 'product_lot_id': 50000001, 'in_stock': 12},
        {'store_id': 10000003, 'product_lot_id': 50000002, 'in_stock': 22},
        {'store_id': 10000003, 'product_lot_id': 50000004, 'in_stock': 456}
    ]

    customer_list = [
        {'customer_id': 60000001, 'customer_name': 'Pujan Dahal', 'gender': 'M', 'address': 'Baneshwor, Kathmandu', 'email': 'pujan.dahal@fusemachines.com', 'phone_no': '9845763125', 'points_collected': 0},
        {'customer_id': 60000002, 'customer_name': 'Shijal Sharma Poudel', 'gender': 'F', 'address': 'Maharajgunj, Kathmandu', 'email': 'shijal@fusemachines.com', 'phone_no': '9785641258', 'points_collected': 0},
        {'customer_id': 60000003, 'customer_name': 'Baburam Shrestha', 'gender': 'M', 'address': 'Sohrakhutte, Kathmandu', 'email': 'baburam@fusemachines.com', 'phone_no': '9875216478', 'points_collected': 11.5}
    ]

    bill_list = [
        {'bill_id': 70000001, 'store_id': 10000001, 'date': '2022-09-23', 'customer_id': 60000001},
        {'bill_id': 70000002, 'store_id': 10000002, 'date': '2022-09-24', 'customer_id': 60000001},
        {'bill_id': 70000003, 'store_id': 10000003, 'date': '2022-09-25', 'customer_id': 60000002},
        {'bill_id': 70000004, 'store_id': 10000001, 'date': '2022-09-25', 'customer_id': 60000002},
        {'bill_id': 70000005, 'store_id': 10000002, 'date': '2022-10-22', 'customer_id': 60000003}
    ]

    product_bill_list = [
        {'bill_id': 70000001, 'product_lot_id': 50000001, 'quantity': 1},
        {'bill_id': 70000001, 'product_lot_id': 50000006, 'quantity': 10},
        {'bill_id': 70000002, 'product_lot_id': 50000002, 'quantity': 21},
        {'bill_id': 70000003, 'product_lot_id': 50000003, 'quantity': 33},
        {'bill_id': 70000004, 'product_lot_id': 50000005, 'quantity': 22}
    ]

    table_dict = {
        store: store_list,
        category: category_list,
        manufacturer: manufacturer_list,
        product: product_list,
        product_lot: product_lot_list,
        store_product: store_product_list,
        customer: customer_list,
        bill: bill_list,
        product_bill: product_bill_list
    }


    # execute all insertion operations in a loop
    for (table, table_list) in table_dict.items():
        conn.execute(insert(table), table_list)


if __name__ == '__main__':
    # engine for connection to database
    engine = create_engine(DB_ADDRESS) # RSM = Retole Store Management

    # create database schema
    create_schema(engine)

    # insert initial records into the database
    insert_initial_records(engine)
