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
    
    sql_get_customer_id = f"""
    SELECT customer_id FROM customers
    WHERE name = '{trans_df[['customer_name']]}'"""
    cursor = con.cursor()
    cursor.execute(sql_get_customer_id)
    customer_id = cursor.fetchone()
    print(customer_id)

insert_transactions()
