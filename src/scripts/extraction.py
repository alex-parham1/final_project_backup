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
    target = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    )
    df = pd.DataFrame()
    for filename in os.listdir(f"{target}/data"):
        # for each file, put all the data into a dataframe and concat it into our main dataframe
        temp_df = pd.read_csv(f"{target}/data/{filename}")
        df = pd.concat([df, temp_df], axis=0)
    # df = pd.read_csv(f"{target}/data/*.csv")
    df.columns = [
        "date",
        "location",
        "customer_name",
        "products",
        "total",
        "payment_type",
        "card_number",
    ]
    return df


def separate_products(prod):
    prod = prod.strip()
    prod = prod.split(", ")
    return prod


def extract_size(prod):
    prod_list = prod.split(" ")
    return prod_list[0]


def extract_price(prod):
    prod_list = prod.split(" - ")
    return prod_list[-1]


def extract_flavour(prod):
    prod_list = prod.split(" - ")
    if len(prod_list) == 2:
        add = prod_list.insert(1, "None")
    if len(prod_list) == 3:
        add = prod_list[1]
    return add


def extract_name(prod):
    prod_list = prod.split(" - ")
    if len(prod_list) == 3:
        prod_list.pop(-1)
    prod_list.pop(-1)
    prod_list = prod_list[0].split(" ")
    prod_list.pop(0)
    name = " ".join(prod_list)
    return name


def clean_the_data():
    df = get_data_frame()
    df["separate_products"] = df["products"].apply(separate_products)
    df_exploded = df.explode("separate_products")
    df_exploded["size"] = df_exploded["separate_products"].apply(extract_size)
    df_exploded["price"] = df_exploded["separate_products"].apply(extract_price)
    df_exploded["flavour"] = df_exploded["separate_products"].apply(extract_flavour)
    df_exploded["product_name"] = df_exploded["separate_products"].apply(extract_name)
    df_exploded.drop("products", axis=1, inplace=True)
    # df_exploded = df_exploded.drop("separate_products", axis=1, inplace=True)
    df_exploded = df_exploded[
        [
            "date",
            "location",
            "customer_name",
            "product_name",
            "flavour",
            "size",
            "price",
            "total",
            "payment_type",
            "card_number",
        ]
    ]
    df_exploded.columns = [
        "date",
        "location",
        "customer_name",
        "product_name",
        "flavour",
        "size",
        "price",
        "total",
        "payment_type",
        "card_number",
    ]
    return df_exploded


# gets a series of dataframes, one for each table in our database
# ----------------------------------------------------------------
def get_df_customers(df):
    customer_df = df[["customer_name"]]
    customer_df = customer_df.drop_duplicates()
    print("Customers DF OK")
    return customer_df


def get_df_location(df):
    location_df = df[["location"]]
    location_df = location_df.drop_duplicates()
    print("Location DF OK")
    return location_df


def get_df_cards(df):
    cards_df = df[["card_number"]]
    cards_df = cards_df.drop_duplicates()
    # commented out temporarily while transaction table not yet made
    print("Cards DF OK")
    return cards_df


def get_df_products(df):
    print(df.head())
    products_df = df[["product_name", "flavour", "size", "price"]]
    products_df = products_df.drop_duplicates(ignore_index=True)
    products_df = products_df.sort_values("product_name")
    print("Products DF OK")
    return products_df


def get_df_transaction(df):
    transaction_df = df[["date", "payment_type", "total"]]
    print("Transaction DF OK")
    return transaction_df


# -------------------------------------------------------------------------
# function that creates all of the individual dataframes (calls the above functions)
@yaspin(text="Creating dataframes...")
def get_table_df(
    df,
    df_exploded,
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
    products_df = get_df_products(df_exploded)
    transaction_df = get_df_transaction(df)
    return customer_df, location_df, cards_df, products_df, transaction_df


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
    sql_check_customers_query = """
    SELECT name FROM customers"""
    cursor.execute(sql_check_customers_query)
    names = cursor.fetchall()
    for name in names:
        customer_df = customer_df.drop(
            customer_df.index[customer_df["customer_name"] == name[0]]
        )
    cursor.close()
    cursor = connection.cursor()
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
    sql_check_cards_query = """
    SELECT card_number FROM cards"""
    cursor.execute(sql_check_cards_query)
    card_nums = cursor.fetchall()
    for card in card_nums:
        cards_df = cards_df.drop(cards_df.index[cards_df["card_number"] == card[0]])
    cursor.close()
    cursor = connection.cursor()
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
    sql_check_stores_query = """
    SELECT name FROM store"""
    cursor.execute(sql_check_stores_query)
    locations = cursor.fetchall()
    for location in locations:
        location_df = location_df.drop(
            location_df.index[location_df["location"] == location[0]]
        )
    cursor.close()
    cursor = connection.cursor()
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
    cursor = connection.cursor()
    for product in products_df.values.tolist():
        cursor = get_cursor(connection)

        sql_check_prods_query = f"""
        SELECT product_id FROM products
            WHERE name = '{product[0]}' AND flavour = '{product[1]}' AND size = '{product[2]}'"""

        cursor.execute(sql_check_prods_query)
        products = cursor.fetchall()
        print(products)
        if products == ():
            print("Unique product found, entering into DB")
            sql_query = f"""
            INSERT INTO products (name, flavour, size, price)
                VALUES ('{product[0]}', '{product[1]}', '{product[2]}', '{product[3]}')"""
            execute_cursor(cursor, sql_query)

        else:
            continue

    print("Products inserted OK")


# -----------------------------------------------------


def etl(
    df_exploded,
    get_data_frame=get_data_frame,
    get_table_df=get_table_df,
    # clean_products=clean_products,
    get_connection=get_connection,
    insert_names=insert_names,
    insert_cards=insert_cards,
    insert_store=insert_store,
    insert_products=insert_products,
    commit_and_close=commit_and_close,
):
    # generate our dataframes
    df = get_data_frame()
    customer_df, location_df, cards_df, products_df, transaction_df = get_table_df(
        df, df_exploded
    )

    # clean our product data
    connection = get_connection()
    # each of these executes a series of sql commands to insert the data into our database
    insert_names(connection, customer_df)
    insert_cards(connection, cards_df)
    insert_store(connection, location_df)
    insert_products(connection, products_df)
    commit_and_close(connection)


# ---------------------------------------------------
# --------------functions end here-------------------
# this file just runs this one command
if __name__ == "__main__":
    df_exploded = clean_the_data()
    etl(df_exploded)
