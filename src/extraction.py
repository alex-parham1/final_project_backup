import pandas as pd

# import pandas_profiling
import os
from database import get_connection, close_connection, commit_connection
from dotenv import load_dotenv
from yaspin import yaspin
import time

# This function forms the **E** from ETL - it extracts the data and puts it into a dataframe.

#lets the user know what is happening when the code is just 'doing stuff'
@yaspin(text="Cleaning data...")
def get_data_frame():
    time.sleep(1)
    df = pd.DataFrame()
    # target path relative to this file (where to find the csv)
    target = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    for filename in os.listdir(f"{target}/data"):
        #for each file, put all the data into a dataframe and concat it into our main dataframe
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
    # separates card type and number into different columns
    df[["card_type", "card_number"]] = df["card"].str.split(",", expand=True)
    df.dropna()
    return df

# gets a series of dataframes, one for each table in our database
#----------------------------------------------------------------
def get_df_customers(df):
    customer_df = df[["customer_name"]]
    customer_df = customer_df.drop_duplicates()
    return customer_df


def get_df_location(df):
    location_df = df[["location"]]
    location_df = location_df.drop_duplicates()
    return location_df


def get_df_cards(df):
    cards_df = df[["card_number", "card_type"]]
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
def get_table_df(df):
    time.sleep(1)
    customer_df = get_df_customers(df)
    location_df = get_df_location(df)
    cards_df = get_df_cards(df)
    products_df = get_df_products(df)
    transaction_df = get_df_transaction(df)
    return customer_df, location_df, cards_df, products_df, transaction_df

#cleaning our product data
@yaspin(text="Cleaning products...")
def clean_products(products_df):
    time.sleep(1)
    products = []
    for order in products_df["products"]:
        order_split = order.split(",")
        #split each row of products into a list, separated by commas
        order_split = seperate_products(order_split)
        for item in order_split:
            if item[0] == "Regular," or item[0] == "Large,":
                continue
            #some entries have no size, we prodided some data so that it fits the schema. can be changed if needed
            elif item[0] == "":
                order_split[order_split.index(item)][0] = "Small"
        # add this row of producs to the main products list
        products.append(order_split)
    #make a dataframe to house our products and provide the column names
    clean_products_df = pd.DataFrame(columns=["SIZE", "NAME", "PRICE"])
    for product in products:
        #itterate through and add each product to the new dataframe
        temp_df = pd.DataFrame(product, columns=["SIZE", "NAME", "PRICE"])
        clean_products_df = pd.concat([clean_products_df, temp_df])
    #drop any duplicates and sort by name, so it is easier to read
    clean_products_df = clean_products_df.drop_duplicates(ignore_index=True)
    clean_products_df = clean_products_df.sort_values("NAME")
    return clean_products_df

#each product consists of three values: size, name and price
#this function separates each product from an transaction and makes it into a list, to be stored in another list
def seperate_products(products_df):
    rule = [1, 2, 3]
    products = []
    total = int(len(products_df) / 3)
    for value in range(total):
        buffer = []
        for num in rule:
            #add the first three entries to the buffer, popping each after they are used (three items each time)
            buffer.append(products_df[0])
            products_df.pop(0)
        # append to prodcuts list, rinse an repeat until all productsin transaction are stored
        products.append(buffer)
    return products

# individual functions to isert into all the different tables
@yaspin(text="Inserting names into DB...")
def insert_names(connection, customer_df: pd.DataFrame):
    for name in customer_df.values.tolist():
        sql_query = f"""
        INSERT INTO customers (name)
            VALUES ('{name[0]}')"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
    print("Names inserted OK")


@yaspin(text="Inserting cards into DB...")
def insert_cards(connection, cards_df: pd.DataFrame):
    for cards in cards_df.values.tolist():
        sql_query = f"""
        INSERT into cards (card_number, card_type)
            VALUES ('{cards[0]}', '{cards[1]}')"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
    print("Cards inserted OK")


@yaspin(text="Inserting stores into DB...")
def insert_store(connection, location_df):
    for store in location_df.values.tolist():
        sql_query = f"""
        INSERT into store (name)
            VALUES ('{store[0]}')"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
    print("Stores inserted OK")


@yaspin(text="Inserting products into DB...")
def insert_products(connection, products_df: pd.DataFrame):
    for product in products_df.values.tolist():
        sql_query = f"""
        INSERT INTO products (size, name, price)
            VALUES ('{product[0]}', '{product[1]}', {product[2]})"""
        cursor = connection.cursor()
        cursor.execute(sql_query)
    print("Products inserted OK")
#-----------------------------------------------------

def etl():
    #generate our dataframes
    df = get_data_frame()
    customer_df, location_df, cards_df, products_df, transaction_df = get_table_df(df)
    
    #clean our product data
    products_df = clean_products(products_df)

    connection = get_connection()

    #each of these executes a series of sql commands to insert the data into our database
    insert_names(connection, customer_df)
    insert_cards(connection, cards_df)
    insert_store(connection, location_df)
    insert_products(connection, products_df)

    connection.commit()
    connection.close()
#---------------------------------------------------
#--------------functions end here-------------------
#this file just runs this one command
etl()
