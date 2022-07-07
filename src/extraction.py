import pandas as pd

# import pandas_profiling
import os
from database import get_connection, close_connection, commit_connection
from dotenv import load_dotenv
from yaspin import yaspin
import time

connection = get_connection()
# This function forms the **E** from ETL - it extracts the data and puts it into a dataframe.


@yaspin(text="Cleaning data...")
def get_data_frame():
    time.sleep(1)
    df = pd.DataFrame()
    target = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    for filename in os.listdir(f"{target}/data"):
        temp_df = pd.read_csv(f"{target}/data/{filename}")
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


def get_df_customers(df):
    customer_df = df[["customer_name"]]
    return customer_df


def get_df_location(df):
    location_df = df[["location"]]
    location_df = location_df.drop_duplicates()
    return location_df


def get_df_cards(df):
    cards_df = df[["card_type", "card_number"]]
    return cards_df


def get_df_products(df):
    products_df = df[["products"]]
    return products_df


# def insert_products(connection, products_df):
#     for product in products_df:


def get_df_transaction(df):
    transaction_df = df[["date", "payment_type", "total"]]
    return transaction_df

@yaspin(text='Creating dataframes...')
def get_table_df(df):
    time.sleep(1)
    customer_df = get_df_customers(df)
    location_df = get_df_location(df)
    cards_df = get_df_cards(df)
    products_df = get_df_products(df)
    transaction_df = get_df_transaction(df)
    return customer_df, location_df, cards_df, products_df, transaction_df


@yaspin(text="Cleaning products...")
def clean_products(products_df):
    time.sleep(1)
    products = []
    for order in products_df["products"]:
        order_split = order.split(",")
        order_split = seperate_products(order_split)
        for item in order_split:
            if item[0] == "Regular," or item[0] == "Large,":
                continue
            elif item[0] == "":
                order_split[order_split.index(item)][0] = "Small"
        products.append(order_split)
    clean_products_df = pd.DataFrame(columns=["SIZE", "NAME", "PRICE"])
    for product in products:
        temp_df = pd.DataFrame(product, columns=["SIZE", "NAME", "PRICE"])
        clean_products_df = pd.concat([clean_products_df, temp_df])
    clean_products_df = clean_products_df.drop_duplicates(ignore_index=True)
    clean_products_df = clean_products_df.sort_values("NAME")
    return clean_products_df


def seperate_products(products_df):
    rule = [1, 2, 3]
    products = []
    total = int(len(products_df) / 3)
    for value in range(total):
        buffer = []
        for num in rule:
            buffer.append(products_df[0])
            products_df.pop(0)
        products.append(buffer)
    return products


@yaspin(text="Inserting names into DB...")
def insert_names(connection):
    for name in df["customer_name"]:
        sql_query = f"""
        INSERT into customers (name)
            VALUES ('{name}')"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
    print("Names inserted OK")


@yaspin(text="Inserting cards into DB...")
def insert_cards(connection):
    for card_type in df["card_type"]:
        for card_number in df["card_number"]:
            sql_query = f"""
            INSERT INTO cards (card_number, card_type)
                VALUES ('{card_number}', '{card_type}')"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
    print("Cards inserted OK")


@yaspin(text="Inserting stores into DB...")
def insert_store(connection):
    for store in df["location"]:
        sql_query = f"""
        INSERT INTO store (name)
        VALUES ('{store}') """
        cursor = connection.cursor()
        cursor.execute(sql_query)
    print("Stores inserted OK")

@yaspin(text="Inserting products into DB...")
def insert_products(connection, products_df:pd.DataFrame):
    print(products_df)
    for product in products_df.values.tolist():
        sql_query = f'''
        INSERT INTO products (size, name, price)
            VALUES ('{product[0]}', '{product[1]}', {product[2]})'''
        cursor = connection.cursor()
        cursor.execute(sql_query)
            

df = get_data_frame()
customer_df, location_df, cards_df, products_df, transaction_df = get_table_df(df)
products_df = clean_products(products_df)
insert_products(connection, products_df)
connection.commit()
