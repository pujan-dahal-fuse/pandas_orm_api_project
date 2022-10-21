# **Pandas, ORM, API Project**
In this project, we use Pandas, ORM and API to create and operate a database for retail store management. The Entity Relationship Diagram (ERD) is given in [`retail_store_managment_erd.png`](./retail_store_management_erd.png). There are 7 main entities; store, product, manufacturer, category, product_lot, bill and customer. To represent some many-to-many relationships, 2 additional entities; store_product and product_bill are also created.

The repository is divided into branches for each team members with their respective names: [baburam](https://www.github.com/pujan-dahal), [pujan](https://www.github.com/baburam-shrestha), [Shijal](https://www.github.com/Shijal12).

Work division is done as follows:
1. Database creation and dummy data fill using ORM - [Pujan Dahal](https://www.github.com/pujan-dahal)
2. Insertion APIs with ORM - [Shijal Sharma Poudel](https://www.github.com/Shijal12)
3. Retrieval APIs with ORM - [Baburam Shrestha](https://www.github.com/baburam-shrestha)
4. Insights into data and relevant API creation - All


## **1. main**
This branch consists of all codes of all members after final merging within the [`merged`](merged/) folder. To run the project, few Python packages should be run using the following command:
```
$ pip install -r requirments.txt
```
The project runs with MySQL Database whose address is given in [`dbaddress.py`](./dbaddress.py). You can install your database driver and provide your database's address into the file to run accordingly. After setting up all this, running the follwing command:
```
$ python database_creation.py
```
creates all the database tables within the provided database address. Few dummy data are also loaded into the tables.

### **Merged files**
Project merged files are within the [`merged`](merged/) folder and can be run from there using the command:
```
$ python merged/<file_name>.py
```
This runs the flask APIs at address [localhost:5000](https://localhost:5000) and corresponding api's can be accessed according to the API URLs given at the branches descriptions.

# **2. baburam**
This branch consists of APIs that were written by [Baburam Shrestha](https://www.github.com/baburam-shrestha). Respective APIs are present in baburam folder of `main` branch as well as in `baburam` branch. Also, merged folder consists of sames APIs.

Routes for accessing retrieval APIs (after merge) from[`retrievals_api.py`](`merged/retrieval_api.py`) after running
```
$ python merged/retrieval_api.py
```
are as follows:
|Route|Method|Operation Description|
|-------|--------|-----------------|
|`/api/store`|GET|Retrieve all records from `store` table|
|`/api/product`|GET|Retrieve all records form `product` table|
|`/api/product_lot`|GET|Retrieve all records from `product_lot` table|
|`/api/store_product`|GET|Retreive all records from `store_product` table|
|`/api/category`|GET|Retrieve all records from `category` table|
|`/api/customer`|GET|Retrieve all records from `customer` table|
|`/api/manufacturer`|GET|Retrieve all records from `manufacturer` table|
|`/api/bill`|GET|Retrieve all records from `bill` table|
|`/api/product_bill`|GET|Retrieve all records from `product_bill` table|


Routes for accessing insights APIs (after merge) from [`pandas_api.py`](merged/pandas_api.py) after running
```
$ python merged/pandas_api.py
```
are as follows:
|Route|Method|Operation Description|
|------|-------|---------------------|
|`/api/store_product_detail`|GET|Retrieve details about products in each store|
|`/api/min_stock`|GET|Retrieve the products which are in minimum stock in each store|
|`/api/max_stock`|GET|Retrieve the products which are in maxmimum stock in each store|
|`/api/branch/<string:branch>`|GET|Retrieve the details of products in the given branch|
|`/api/product/<int:id>`|GET|Retrieve the product details for given product id|
|`/api/manufacturer/<int:id>`|GET|Retrieve the manufacturer details for given manufacturer id|


# **3. Pujan**
This branch consists of APIs written by [Pujan Dahal](https://www.github.com/pujan-dahal). Respective APIs are present in pujan folder of `main` branch as well as in `pujan` branch. Also, merged folder consists of sames APIs.

Routes for accessing insights APIs (after merge) from [`pandas_api.py`](merged/pandas_api.py) after running
```
$ python merged/pandas_api.py
```
are as follows:
|Route|Method|Operation Description|
|-------|-------|---------------------|
|`/api/total_sales_store/<int:yr>`|GET|Retrieve total sales in each store for the given year|
|`/api/popular_products`|GET|Retrieve the most popular products i.e. bought in max quantity in each store for all time|
|`/api/popular_products/<int:yr>`|GET|Retrieve the most popular products i.e. bought in max quantity in each store for given year|
|`/api/average_monthly_sales`|GET|Retireve the average monthly sales for each store in each year|
|`/api/total_monthly_sales/<int:yr>`|GET|Retrieve the total monthly sales for each store in the given year|
|`/api/avg_bill_sales`|GET|Retreive the average overall sales of each bill|
|`/api/manufacturer_products`|GET|Retrieve the different products supplied by each manufacturer|
|`/api/category_sales/<int:yr>`|GET|Retreive the total sales of each category in each month of a year|
|`/api/gender_category`|GET|Retrieve the percentage of men and women doing shopping in each category and total shopping done by each gender|


# **4. Shijal**
This branch consists of APIs written by [Shijal Sharma Poudel](https://www.github.com/Shijal12). Respective APIs are present in shijal folder of `main` branch as well as in `Shijal` branch. Also, merged folder consists of sames APIs.

Routes for accessing insertion APIs (after merge) from [`insertion_api.py`](merged/insertion_api.py) after running
```
$ python merged/insertion_api.py
```
are as follows:
|Route|Method|Operation Description|
|-------|--------|---------------------|
|`/api/insert_customer`|POST|Insert a record into `customer` table|
|`/api/insert_store`|POST|Insert a record into `store` table|
|`/api/insert_manufacturer`|POST|Insert a record into `manufacturer` table|
|`/api/insert_product`|POST|Insert a record into `product` table|
|`/api/insert_category`|POST|Insert a record into `category` table|
|`/api/insert_bill`|POST|Insert a record into `bill` table|
|`/api/insert_store_product`|POST|Insert a record into `store_product` table|
|`/api/insert_product_lot`|POST|Insert a record into `product_lot` table|
|`/api/insert_product_bill`|POST|Insert a record into `product_bill` table|


Further, routes for acccessing insights APIs (after merge) from [`pandas_api.py`](merged/pandas_api.py) after running:
```
$ python merged/pandas_api.py
```
are as follows:
|Route|Method|Operation Description|
|-------|--------|---------------------|
|`/api/total_manufacturer_sales`|GET|Retrieve total sales done for each manufacturer|
|`/api/total_category_sales`|GET|Retrieve total sales done for each category|