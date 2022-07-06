import pandas as pd
import pandas_profiling
import os
from database import get_connection, close_connection, commit_connection
from dotenv import load_dotenv
from yaspin import yaspin
import time

connection = get_connection()
# This function forms the **E** from ETL - it extracts the data and puts it into a dataframe.

@yaspin(text='Cleaning data...')
def get_data_frame():
    df = pd.DataFrame()
    for filename in os.listdir("../data"):
        temp_df = pd.read_csv(f"../data/{filename}")
        df = pd.concat([df, temp_df], axis=0)
    df.columns = [
        "date",
        "location",
        "customer_name",
        "products",
        "payment_type",
        "total",
        "card",
    ]
    df.reset_index()
    # This part of the function begins to work on the transform
    df[["card_type", "card_number"]] = df["card"].str.split(",", expand=True)
    df.dropna()
    return df


# Gets the data frame to check the code is working
df = get_data_frame()

"""
WE NEED:

into CARDS:
- number
- type

into TRANSACTION:
- timestamp
- customer id (foreign key)
- product id (foreign key)
- store id (foreign key)
- total

into PRODUCTS:
- name
"""


@yaspin(text='Inserting names into DB...')
def insert_names(connection):
    for name in df["customer_name"]:
        sql_query = f"""
        INSERT into customers (name)
            VALUES ('{name}')"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
    connection.commit()
    print('Names inserted OK')

@yaspin(text='Inserting cards into DB...')
def insert_cards(connection):
    for card_type in df["card_type"]:
        for card_number in df["card_number"]:
            sql_query = f"""
            INSERT INTO cards (card_number, card_type)
                VALUES ('{card_number}', '{card_type}')"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
        connection.commit()
        print('Cards inserted OK')

@yaspin(text='Inserting stores into DB...')
def insert_store(connection):
    for store in df["location"]:
        sql_query = f"""
        INSERT INTO store (name)
        VALUES ('{store}') """
        cursor = connection.cursor()
        cursor.execute(sql_query)
        connection.commit()
        print('Stores inserted OK')

insert_names(connection)
insert_cards(connection)
insert_store(connection)