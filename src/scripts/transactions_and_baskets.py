from extraction import get_data_frame, clean_products, etl, clean_the_data

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
    trans_df = clean_the_data()
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

        check_existing_trans_sql = f"""
        SELECT transaction_id FROM transactions 
            WHERE customer_id = {customer_id} AND store_id = {store_id} AND date_time = '{order[0]}'"""
        cursor = con.cursor()
        cursor.execute(check_existing_trans_sql)
        is_exists = cursor.fetchone()

        if is_exists == None:
            insert_transaction_sql = f"""
            INSERT INTO transactions (date_time, customer_id, store_id, total, payment_method)
                VALUES ('{order[0]}', {customer_id}, {store_id}, {order[7]}, '{order[8]}')"""
    
            cursor = con.cursor()
            cursor.execute(insert_transaction_sql)
            cursor.close()
            con.commit()
        else:
            pass
        
        product_ids = []
        cursor = con.cursor()
        sql_get_product_id = f"""
        SELECT product_id FROM products 
            WHERE name = '{order[3]}' AND flavour = '{order[4]}' AND size = '{order[5]}' AND price = {order[6]}"""
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
            
            sql_check_basket_exists = f"""
            SELECT transaction_id FROM basket
                WHERE product_id = {ID} AND transaction_id = {transaction_id}"""
            cursor.execute(sql_check_basket_exists)
            is_exists = cursor.fetchone()

            if is_exists == None:
                print("Inserted basket")
                sql_insert_into_basket = f"""
                INSERT into basket (transaction_id, product_id)
                    VALUES ({transaction_id}, {ID})"""
    
                cursor.execute(sql_insert_into_basket)
            else:
                pass
        cursor.close()
        con.commit()

    print("Transactions and Baskets inserted OK")

if __name__ == "__main__":
    insert_transactions()