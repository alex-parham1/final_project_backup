from extraction import get_data_frame, etl, clean_the_data

import pandas as pd
from yaspin import yaspin
import sys
from dotenv import load_dotenv
import time
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
import numpy

# adds the word IGNORE after INSERT in sqlalchemy
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kw)

main_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(f"{main_dir}")

from src.database import get_connection, close_connection, commit_connection

con = get_connection()

def df_to_sql(df:pd.DataFrame, table_name):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@localhost:3307/thirstee")
    db_engine = engine.execution_options(autocommit=True)
    df.to_sql(name=table_name, con=db_engine, if_exists='append', index=False, schema='thirstee') 

def df_from_sql_table(table_name):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@localhost:3307/thirstee")
    return pd.read_sql_table(table_name,engine)

def df_from_sql_query(table_name,start_time,end_time):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@localhost:3307/thirstee")
    sql = f"SELECT * from {table_name} WHERE date_time >= {start_time} and date_time =< {end_time}"
    print('executing')
    return pd.read_sql_query(sql,engine)

def get_store_id(store,stores:pd.DataFrame):
    id = stores.query(f"name=='{store}'", inplace=False)
    return str(id.values.tolist()[0][0])

def get_customer_id(name,customers:pd.DataFrame):
    #name = df["customer_name"]
    #print(name)
    id = customers.query(f"name=='{name}'",inplace=False)
    return str(id.values.tolist()[0][0])

def get_product_id(df:pd.Series,products:pd.DataFrame):
    p_id = products.query(f"name == '{df['product_name']}' and size == '{df['size']}' and flavour == '{df['flavour']}' and price == {df['price']} ")
    return str(p_id.values.tolist()[0][0])

def get_transaction_id(df:pd.Series,transactions:pd.DataFrame):
    #timestamp, name, customer, total
    t_id = transactions.query(f"date_time == '{df['date']}' and store_id == {df['location']} and customer_id == {df['customer_id']}")
    
    return str(t_id.values.tolist()[0][0])

# def set_foreign_keys(df: pd.DataFrame,cust:pd.DataFrame,stores:pd.DataFrame):
#     print(df.head(10))
#     print(df.columns)
    


@yaspin(text="Inserting order to DB...")
def insert_transactions(trans_df):
    #store tables in memory for comparison
    users = df_from_sql_table("customers")
    users = users.drop_duplicates(subset="name")
    stores = df_from_sql_table("store")
    stores = stores.drop_duplicates(subset='name')

    #get customer ids by looking up a matching customer in the database
    trans_df["customer_id"] = trans_df["customer_name"].apply(get_customer_id,args=(users,))
    #get store id by looking up a matching store in the database
    trans_df["location"] = trans_df["location"].apply(get_store_id,args=(stores,))

    #make a new df that matches the layout and format of the table in the database
    trans_table = trans_df.drop(columns=['customer_name','product_name','flavour','size','price','card_number'])
    trans_table.columns=['date_time','store_id','total','payment_method','customer_id']
    
    trans_table = trans_table.drop_duplicates()
    print('uploading transactions')
    df_to_sql(trans_table,'transactions')
    print('uploaded transactions')
    
    #baskets starts here
    start_time = trans_df['date'].head(1).values.tolist()[0]
    end_time = trans_df['date'].tail(1).values.tolist()[0]
    transactions = df_from_sql_table("transactions")
    transactions = transactions.drop_duplicates()
    products = df_from_sql_table("products")
    products = products.drop_duplicates()
    baskets = pd.DataFrame()
    print('creating baskets')
    baskets["transaction_id"]= trans_df.apply(get_transaction_id,args=(transactions,),axis=1)
    baskets["product_id"] = trans_df.apply(get_product_id,args=(products,),axis=1)

    print('uploading baskets')
    df_to_sql(baskets,'basket')
    print('baskets uploaded')    

    print("Transactions and Baskets inserted OK")


if __name__ == "__main__":
    insert_transactions()