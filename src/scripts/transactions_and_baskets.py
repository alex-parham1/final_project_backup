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

def get_store_id(store,stores:pd.DataFrame):
    id = stores.query(f"name=='{store}'", inplace=False)
    return str(id.values.tolist()[0][0])

def get_customer_id(name,customers:pd.DataFrame):
    #name = df["customer_name"]
    #print(name)
    id = customers.query(f"name=='{name}'",inplace=False)
    return str(id.values.tolist()[0][0])

# def set_foreign_keys(df: pd.DataFrame,cust:pd.DataFrame,stores:pd.DataFrame):
#     print(df.head(10))
#     print(df.columns)
    


@yaspin(text="Inserting order to DB...")
def insert_transactions():
    users = df_from_sql_table("customers")
    users = users.drop_duplicates(subset="name")
    stores = df_from_sql_table("store")
    stores = stores.drop_duplicates(subset='name')
    trans_df = clean_the_data()

    trans_df["customer_id"] = trans_df["customer_name"].apply(get_customer_id,args=(users,))
    
    trans_df["location"] = trans_df["location"].apply(get_store_id,args=(stores,))

    trans_table = trans_df.drop(columns=['customer_name','product_name','flavour','size','price','card_number'])
    trans_table.columns=['date_time','store_id','total','payment_method','customer_id']
    #trans_table = trans_table[['date_time','store_id','total','payment_method','customer_id']]
    
    trans_table = trans_table.drop_duplicates()
    print(trans_table.head(10))
    print('uploading transactions')
    df_to_sql(trans_table,'transactions')
    print('uploaded transactions')
    

    #     #baskets begins here
    #     product_ids = []
    #     cursor = con.cursor()
    #     sql_get_product_id = f"""
    #     SELECT product_id FROM products 
    #         WHERE name = '{order[3]}' AND flavour = '{order[4]}' AND size = '{order[5]}' AND price = {order[6]}"""
    #     cursor.execute(sql_get_product_id)
    #     prod_id_t = cursor.fetchone()
    #     product_ids.append(prod_id_t[0])
    #     cursor.close()

    #     cursor = con.cursor()
    #     for ID in product_ids:
    #         sql_get_trans_id = f"""
    #         SELECT transaction_id from transactions
    #             WHERE customer_id = {customer_id} AND store_id = {store_id} AND date_time = '{order[0]}'"""

    #         cursor.execute(sql_get_trans_id)
    #         trans_id_t = cursor.fetchone()
    #         transaction_id = trans_id_t[0]

    #         sql_check_basket_exists = f"""
    #         SELECT transaction_id FROM basket
    #             WHERE product_id = {ID} AND transaction_id = {transaction_id}"""
    #         cursor.execute(sql_check_basket_exists)
    #         is_exists = cursor.fetchone()

    #         if is_exists == None:
    #             print("Inserted basket")
    #             sql_insert_into_basket = f"""
    #             INSERT into basket (transaction_id, product_id)
    #                 VALUES ({transaction_id}, {ID})"""

    #             cursor.execute(sql_insert_into_basket)
    #         else:
    #             pass
    #     cursor.close()
    #     con.commit()

    print("Transactions and Baskets inserted OK")


if __name__ == "__main__":
    insert_transactions()
