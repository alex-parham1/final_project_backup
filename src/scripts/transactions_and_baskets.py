from extraction import get_data_frame, clean_products
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


@yaspin(text="Inserting orders to transaction table...")
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
        print(order)
        insert_transaction_sql = f"""
        INSERT INTO transactions (date_time, customer_id, store_id, total, payment_method)
            VALUES ('{order[0]}', {customer_id}, {store_id}, {order[4]}, '{order[5]}')"""

        cursor = con.cursor()
        cursor.execute(insert_transaction_sql)

    con.commit()
    print("Transactions inserted OK")


insert_transactions()
