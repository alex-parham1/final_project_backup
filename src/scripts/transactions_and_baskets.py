from extraction import get_data_frame, clean_products, etl

import pandas as pd
from yaspin import yaspin
import sys
from dotenv import load_dotenv
import time
import os

main_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(f"{main_dir}")

from src.database import get_connection, close_connection, commit_connection

con = get_connection()


@yaspin(text="Inserting order to DB...")
def insert_transactions():
    trans_df = get_data_frame()
    for order in trans_df.values.tolist():
        sql_get_customer_id = f"""
        SELECT customer_id FROM customers
        WHERE name = '{order[2]}'"""

        sql_get_store_id = f"""
        SELECT store_id from store
        WHERE name = '{order[1]}'"""

        cursor = con.cursor()
        cursor.execute(sql_get_customer_id)
        customer_id_t = cursor.fetchone()
        cursor.close()

        cursor = con.cursor()
        cursor.execute(sql_get_store_id)
        store_id_t = cursor.fetchone()
        cursor.close()

        customer_id = customer_id_t[0]
        store_id = store_id_t[0]
        insert_transaction_sql = f"""
        INSERT INTO transactions (date_time, customer_id, store_id, total, payment_method)
            VALUES ('{order[0]}', {customer_id}, {store_id}, {order[4]}, '{order[5]}')"""

        cursor = con.cursor()
        cursor.execute(insert_transaction_sql)
        cursor.close()
        con.commit()

        prods_order_list = []
        prods_order_list.append(order[3])
        prod_df = pd.DataFrame(prods_order_list, columns=["products"])
        clean_prods = clean_products(prod_df)
        clean_prods = clean_prods.values.tolist()
        
        
        product_ids = []
        cursor = con.cursor()
        for item in clean_prods:
            sql_get_product_id = f"""
            SELECT product_id FROM products 
                WHERE name = '{item[1]}' AND size = '{item[0]}' AND flavour = '{item[2]}' AND price = {item[3]}"""
            cursor.execute(sql_get_product_id)
            prod_id_t = cursor.fetchone()
            product_ids.append(prod_id_t[0])
        cursor.close()
        
        cursor = con.cursor()
        for ID in product_ids:
            sql_get_trans_id = f"""
            SELECT transaction_id from transactions
                WHERE customer_id = {customer_id} AND store_id = {store_id} AND date_time = '{order[0]}'"""
            
            cursor.execute(sql_get_trans_id)
            trans_id_t = cursor.fetchone()
            transaction_id = trans_id_t[0]

            sql_insert_into_basket = f"""
            INSERT into basket (transaction_id, product_id)
                VALUES ({transaction_id}, {ID})"""

            cursor.execute(sql_insert_into_basket)
        cursor.close()
        con.commit()

    print("Transactions and Baskets inserted OK")

