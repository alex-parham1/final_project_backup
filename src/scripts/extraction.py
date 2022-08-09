import pandas as pd
import os
import traceback
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# adds the word IGNORE after INSERT in sqlalchemy - fallback just in case some duplicate protection fails. likely no longer triggered though
# ----------------------------------------------------------------
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with("IGNORE"), **kw)


# This function forms the **E** from ETL - it extracts the data and puts it into a dataframe.
# lets the user know what is happening when the code is just 'doing stuff'


# get the snowflake credentials form the envrionment vars on aws
try:
    snow_user = os.environ.get("SNOWFLAKE_USER")
    snow_password = os.environ.get("SNOWFLAKE_PASS")
except:
    print("Failed to find snowflake credentials. Skipping.")


# craetes a connection to our snowflake database, and uploads data to a specified table
def connect_and_push_snowflake(
    table,
    database,
    df,
    user=snow_user,
    password=snow_password,
    account="sainsburys-bootcamp",
    warehouse="BOOTCAMP_WH",
    schema="PUBLIC",
):
    ctx = connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    print("capitolizing columns")

    # columns need to be in all caps to match our schema
    cols = df.columns
    upper_cols = []
    for col in cols:
        upper_cols.append(col.upper())
    df.columns = upper_cols

    print("writing to pandas")
    # the code that pushes a dataframe to snowflake. we provide connection, data, target table, schema and database
    success, nchunks, nrows, _ = write_pandas(
        ctx, df, table_name=table, database=database, schema=schema
    )
    print(
        f"Successfully uploaded to snowflake: {success}, Number of rows updated (if any): {nrows} using {nchunks} chunks."
    )
    # dont forget to close that connection!
    ctx.close()


# This function connects and returns whichever table you specify from the DB
# ----------------------------------------------------------------
def df_from_sql_table(table_name, create_engine=create_engine):
    # get environment variables
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")

    # create the sql alchemy engine connection
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")

    # attempt to pull data from db, if it fails, raise an error
    try:
        ret = pd.read_sql_table(table_name, engine)
    except Exception as e:
        print(f"Unable to access table {table_name}")
        raise e

    # dont forget to close that connection
    engine.dispose()
    return ret


# These functions separate out the products columnn into new columns, and cleans cards
# lots of string splicing and splitting
# ----------------------------------------------------------------
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
    if len(prod_list) == 4:
        add = prod_list[2]
        return add
    if len(prod_list) == 2:
        add = prod_list.insert(1, "None")
    if len(prod_list) == 3:
        add = prod_list[1]
    return add


def extract_name(prod):
    prod_list = prod.split(" - ")
    return prod_list[1]


def clean_cards(cards):
    cards = str(cards).rstrip(".0")
    return cards


# This function uses the above to return our dataframe with all the new columns
# ----------------------------------------------------------------
def clean_the_data(df):
    df["separate_products"] = df["products"].apply(separate_products)
    df_exploded = df.explode("separate_products")
    # using the apply method to create new columns with our product details in
    df_exploded["size"] = df_exploded["separate_products"].apply(extract_size)
    df_exploded["price"] = df_exploded["separate_products"].apply(extract_price)
    df_exploded["flavour"] = df_exploded["separate_products"].apply(extract_flavour)
    df_exploded["product_name"] = df_exploded["separate_products"].apply(extract_name)

    # remove unnescessary column
    df_exploded.drop("products", axis=1, inplace=True)
    # re-order columns
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


#  removes any dupes by comparing to the db
# ----------------------------------------------------------------
def drop_dupe_cards(df: pd.Series, cards: pd.DataFrame):
    # queries the passed table for a specific card number
    card_number = df["card_number"]
    card = cards.query(f"card_number == '{card_number}'")

    # if there are any results then puts true in a new column
    if not card.empty or not str(card_number).isdigit():
        return True
    else:
        return False


def drop_dupe_customers(df: pd.Series, customers: pd.DataFrame):
    name = df["name"]
    customer = customers.query(f"name == '{name}'")

    if not customer.empty or str(name).isdigit():
        return True
    else:
        return False


def drop_dupe_location(df: pd.Series, location: pd.DataFrame):
    name = df["name"]
    locations = location.query(f"name == '{name}'")

    if not locations.empty or str(name).isdigit():
        return True
    else:
        return False


def drop_dupe_prods(df: pd.Series, prods: pd.DataFrame):
    name = df["name"]
    flavour = df["flavour"]
    size = df["size"]
    price = df["price"]
    prod = prods.query(
        f"name=='{name}' and size == '{size}' and flavour == '{flavour}' and price == {price}"
    )

    if not prod.empty:
        return True
    else:
        return False


def get_df_products(
    df, df_from_sql_table=df_from_sql_table, drop_dupe_prods=drop_dupe_prods
):
    print("getting products table")
    # gets the table from the database
    prods_table = df_from_sql_table("products")

    # gets required data from dataframe and renames columns, dropping any duplicates
    products_df = df[["product_name", "flavour", "size", "price"]]
    products_df.columns = ["name", "flavour", "size", "price"]
    products_df = products_df.drop_duplicates(ignore_index=True)

    # create the duplicate column and then drop everyting with a true value
    products_df["duplicate"] = products_df.apply(
        drop_dupe_prods, args=(prods_table,), axis=1
    )
    products_df = products_df[products_df["duplicate"] == False]
    products_df = products_df.drop("duplicate", axis=1)

    print("Products DF OK")
    return products_df


# gets a series of dataframes, one for each table in our database
# ----------------------------------------------------------------


def get_df_cards(
    df, df_from_sql_table=df_from_sql_table, drop_dupe_cards=drop_dupe_cards
):
    print("getting cards table")
    # gets the table from the database
    cards_table = df_from_sql_table("cards")

    # gets required data from dataframe, dropping any duplicates
    cards_df = df[["card_number"]]
    cards_df = cards_df.drop_duplicates(ignore_index=True)

    # create the duplicate column and then drop everyting with a true value
    cards_df["duplicate"] = cards_df.apply(drop_dupe_cards, args=(cards_table,), axis=1)
    cards_df = cards_df[cards_df["duplicate"] == False]
    cards_df = cards_df.drop("duplicate", axis=1)
    print("cards DF OK")
    return cards_df


def get_df_customers(
    df, df_from_sql_table=df_from_sql_table, drop_dupe_customers=drop_dupe_customers
):
    print("getting customers table")
    # gets the table from the database
    customer_table = df_from_sql_table("customers")

    # gets required data from dataframe, dropping any duplicates
    customer_df = df[["customer_name"]]
    customer_df.columns = ["name"]
    customer_df = customer_df.drop_duplicates(ignore_index=True)

    # create the duplicate column and then drop everyting with a true value
    customer_df["duplicate"] = customer_df.apply(
        drop_dupe_customers, args=(customer_table,), axis=1
    )
    customer_df = customer_df[customer_df["duplicate"] == False]
    customer_df = customer_df.drop("duplicate", axis=1)
    print("customer DF OK")
    return customer_df


def get_df_location(
    df, df_from_sql_table=df_from_sql_table, drop_dupe_location=drop_dupe_location
):
    print("getting location table")
    # gets the table from the database
    location_table = df_from_sql_table("store")
    # gets required data from dataframe, dropping any duplicates
    location_df = df[["location"]]
    location_df.columns = ["name"]
    location_df = location_df.drop_duplicates(ignore_index=True)
    # create the duplicate column and then drop everyting with a true value
    location_df["duplicate"] = location_df.apply(
        drop_dupe_location, args=(location_table,), axis=1
    )
    location_df = location_df[location_df["duplicate"] == False]
    location_df = location_df.drop("duplicate", axis=1)
    print("location DF OK")
    return location_df


def get_df_products(
    df, df_from_sql_table=df_from_sql_table, drop_dupe_prods=drop_dupe_prods
):
    print("getting products table")
    # gets the table from the database
    prods_table = df_from_sql_table("products")
    # gets required data from dataframe, dropping any duplicates
    products_df = df[["product_name", "flavour", "size", "price"]]
    products_df.columns = ["name", "flavour", "size", "price"]
    products_df = products_df.drop_duplicates(ignore_index=True)
    # create the duplicate column and then drop everyting with a true value
    products_df["duplicate"] = products_df.apply(
        drop_dupe_prods, args=(prods_table,), axis=1
    )
    products_df = products_df[products_df["duplicate"] == False]
    products_df = products_df.drop("duplicate", axis=1)
    print("Products DF OK")
    return products_df


# function that creates all of the individual dataframes (calls the above functions)
# -------------------------------------------------------------------------
def get_table_df(
    df,
    get_df_customers=get_df_customers,
    get_df_location=get_df_location,
    get_df_cards=get_df_cards,
    get_df_products=get_df_products,
):
    customer_df = get_df_customers(df)
    location_df = get_df_location(df)
    cards_df = get_df_cards(df)
    products_df = get_df_products(df)
    return customer_df, location_df, cards_df, products_df


def df_to_sql(df, table_name, create_engine=create_engine):
    # get envritonment variables from lambda
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")

    # create sqlalchdemy engine/connection
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")

    # use pandas to_sql, append mode, to add dataframe onto the end of the table in rds
    try:
        df.to_sql(con=engine, if_exists="append", name=table_name, index=False)
    except Exception as error:
        print(f"Unable to add to table {table_name}")
        raise error

    # dont forget to close those connections!
    engine.dispose()


# individual functions to isert into all the different tables
# -------------------------------------------------------------------------
def insert_names(customer_df: pd.DataFrame, df_to_sql=df_to_sql):
    df_to_sql(customer_df, "customers")
    print("Names inserted OK")


def insert_cards(cards_df: pd.DataFrame, df_to_sql=df_to_sql, clean_cards=clean_cards):
    cards_df["card_number"] = cards_df["card_number"].apply(clean_cards)
    df_to_sql(cards_df, "cards")
    print("Cards inserted OK")


def insert_store(
    location_df: pd.DataFrame,
    df_to_sql=df_to_sql,
):
    df_to_sql(location_df, "store")
    print("Stores inserted OK")


def insert_products(products_df: pd.DataFrame, df_to_sql=df_to_sql):
    df_to_sql(products_df, "products")
    print("Products inserted OK")


# final function to insert into all of the tables
# -----------------------------------------------------
def etl(
    df_exploded,
    get_table_df=get_table_df,
    # clean_products=clean_products,
    insert_names=insert_names,
    insert_cards=insert_cards,
    insert_store=insert_store,
    insert_products=insert_products,
    connect_and_push_snowflake=connect_and_push_snowflake,
):
    # generate our dataframes
    customer_df, location_df, cards_df, products_df = get_table_df(df_exploded)
    # each of these executes a series of sql commands to insert the data into our database
    # checks if products is empty, if not, then uploads

    if not customer_df.empty:
        insert_names(customer_df)
    else:
        print("no new customers")

    if not cards_df.empty:
        insert_cards(cards_df)
    else:
        print("no new cards")

    if not location_df.empty:
        insert_store(location_df)
    else:
        print("no new stores")

    if not products_df.empty:
        insert_products(products_df)

    else:
        print("no new products")

    # snowflake cannot take empty dataframes, so we should only try to upload if there is data in them
    try:
        if not customer_df.empty:
            print("connecting to snowflake to upload customers")
            connect_and_push_snowflake("CUSTOMERS", "YOGHURT_DB", customer_df)

        if not cards_df.empty:
            print("connecting to snowflake to upload cards")
            connect_and_push_snowflake("CARDS", "YOGHURT_DB", cards_df)

        if not location_df.empty:
            print("connecting to snowflake to upload cards")
            connect_and_push_snowflake("STORE", "YOGHURT_DB", location_df)

        if not products_df.empty:
            print("connecting to snowflake to upload products")
            connect_and_push_snowflake("PRODUCTS", "YOGHURT_DB", products_df)

    except Exception as e:
        print("Failed to connect to snowflake. Pushing to RDS.")
        print(traceback.format_exc())
        print(e)
        pass


# # ---------------------------------------------------
# # --------------functions end here-------------------
# # this file just runs this one command
# if __name__ == "__main__":
#     df_exploded = clean_the_data()
#     etl(df_exploded)
