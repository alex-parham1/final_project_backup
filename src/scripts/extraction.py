import pandas as pd

# import pandas_profiling
import sys
from dotenv import load_dotenv
from yaspin import yaspin
import time
import os

main_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(f"{main_dir}")

from src.database import get_connection, commit_and_close, execute_cursor, get_cursor

# This function forms the **E** from ETL - it extracts the data and puts it into a dataframe.

# lets the user know what is happening when the code is just 'doing stuff'
@yaspin(text="Cleaning data...")
def get_data_frame():
    time.sleep(1)
    df = pd.DataFrame()
    # target path relative to this file (where to find the csv)
    target = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    )
    for filename in os.listdir(f"{target}/data"):
        # for each file, put all the data into a dataframe and concat it into our main dataframe
        temp_df = pd.read_csv(f"{target}/data/{filename}")
        df = pd.concat([df, temp_df], axis=0)
    df.columns = [
        "date",
        "location",
        "customer_name",
        "products",
        "payment_type",
        "total",
        "card_number",
    ]
    df.reset_index()
    # This part of the function begins to work on the transform
    # separates card type and number into different columns
    # df[["card_type", "card_number"]] = df["card"].str.split(",", expand=True)
    df.dropna()
    return df


# gets a series of dataframes, one for each table in our database
# ----------------------------------------------------------------
def get_df_customers(df):
    customer_df = df[["customer_name"]]
    customer_df = customer_df.drop_duplicates()
    return customer_df


def get_df_location(df):
    location_df = df[["location"]]
    location_df = location_df.drop_duplicates()
    return location_df


def get_df_cards(df):
    cards_df = df[["card_number"]]
    # cards_df = cards_df.drop_duplicates()
    # commented out temporarily while transaction table not yet made
    return cards_df


def get_df_products(df):
    products_df = df[["products"]]
    return products_df


def get_df_transaction(df):
    transaction_df = df[["date", "payment_type", "total"]]
    return transaction_df


# -------------------------------------------------------------------------
# function that creates all of the individual dataframes (calls the above functions)
@yaspin(text="Creating dataframes...")
def get_table_df(
    df,
    get_df_customers=get_df_customers,
    get_df_location=get_df_location,
    get_df_cards=get_df_cards,
    get_df_products=get_df_products,
    get_df_transaction=get_df_transaction,
):
    time.sleep(1)
    customer_df = get_df_customers(df)
    location_df = get_df_location(df)
    cards_df = get_df_cards(df)
    products_df = get_df_products(df)
    transaction_df = get_df_transaction(df)
    return customer_df, location_df, cards_df, products_df, transaction_df


# each product consists of three values: size, name and price
# this function separates each product from an transaction and makes it into a list, to be stored in another list
def separate_products(products_df):
    new_split = []
    for prod in products_df:
        prod = prod.strip()
        new_split.append(prod.split(" - "))

    for prod in new_split:
        if len(prod) == 2:
            index = new_split.index(prod)
            new_split[index].insert(1, "None")

    new_split_df = pd.DataFrame(new_split, columns=["product", "flavour", "price"])
    new_split_df[["size", "name"]] = new_split_df["product"].str.split(
        " ", n=1, expand=True
    )

    new_split_df = new_split_df.drop(columns=["product"])
    new_split_df = new_split_df[["size", "name", "flavour", "price"]]

    return new_split_df.values.tolist()


# cleaning our product data
@yaspin(
    text="Cleaning products...",
)
def clean_products(products_df, separate_products=separate_products):
    time.sleep(1)
    products = []

    for order in products_df["products"]:
        order_split = list(order.split(","))
        # split each row of products into a list, separated by commas
        order_split = separate_products(order_split)
        products.append(order_split)
    # make a dataframe to house our products and provide the column names
    clean_products_df = pd.DataFrame(columns=["SIZE", "NAME", "FLAVOUR", "PRICE"])
    for product in products:
        # itterate through and add each product to the new dataframe
        temp_df = pd.DataFrame(product, columns=["SIZE", "NAME", "FLAVOUR", "PRICE"])
        clean_products_df = pd.concat([clean_products_df, temp_df])
    # drop any duplicates and sort by name, so it is easier to read
    clean_products_df = clean_products_df.drop_duplicates(ignore_index=True)
    clean_products_df = clean_products_df.sort_values("NAME")

    return clean_products_df


# individual functions to isert into all the different tables
@yaspin(text="Inserting names into DB...")
def insert_names(
    connection,
    customer_df: pd.DataFrame,
    get_cursor=get_cursor,
    execute_cursor=execute_cursor,
):
    cursor = get_cursor(connection)
    for name in customer_df.values.tolist():
        sql_query = f"""
        INSERT INTO customers (name)
            VALUES ('{name[0]}')"""
        execute_cursor(cursor, sql_query)
    print("Names inserted OK")


@yaspin(text="Inserting cards into DB...")
def insert_cards(
    connection,
    cards_df: pd.DataFrame,
    get_cursor=get_cursor,
    execute_cursor=execute_cursor,
):
    cursor = get_cursor(connection)
    for cards in cards_df.values.tolist():
        sql_query = f"""
        INSERT into cards (card_number)
            VALUES ('{cards[0]}')"""
        execute_cursor(cursor, sql_query)
    print("Cards inserted OK")


@yaspin(text="Inserting stores into DB...")
def insert_store(
    connection,
    location_df: pd.DataFrame,
    get_cursor=get_cursor,
    execute_cursor=execute_cursor,
):
    cursor = get_cursor(connection)
    for store in location_df.values.tolist():
        sql_query = f"""
        INSERT into store (name)
            VALUES ('{store[0]}')"""
        execute_cursor(cursor, sql_query)
    print("Stores inserted OK")


@yaspin(text="Inserting products into DB...")
def insert_products(
    connection,
    products_df: pd.DataFrame,
    get_cursor=get_cursor,
    execute_cursor=execute_cursor,
):
    cursor = get_cursor(connection)
    for product in products_df.values.tolist():
        sql_query = f"""
        INSERT INTO products (size, name, flavour, price)
            VALUES ('{product[0]}', '{product[1]}', '{product[2]}', {product[3]})"""
        execute_cursor(cursor, sql_query)
    print("Products inserted OK")


# -----------------------------------------------------

def etl(
    get_data_frame=get_data_frame,
    get_table_df=get_table_df,
    clean_products=clean_products,
    get_connection=get_connection,
    insert_names=insert_names,
    insert_cards=insert_cards,
    insert_store=insert_store,
    insert_products=insert_products,
    commit_and_close=commit_and_close,
):
    # generate our dataframes
    df = get_data_frame()
    customer_df, location_df, cards_df, products_df, transaction_df = get_table_df(df)

    # clean our product data
    products_df = clean_products(products_df)

    connection = get_connection()

    # each of these executes a series of sql commands to insert the data into our database
    # insert_names(connection, customer_df)
    # insert_cards(connection, cards_df)
    # insert_store(connection, location_df)
    insert_products(connection, products_df)

    commit_and_close(connection)


# ---------------------------------------------------
# --------------functions end here-------------------
# this file just runs this one command
if __name__ == "__main__":
    etl()

