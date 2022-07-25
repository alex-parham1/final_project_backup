import pandas as pd

# import pandas_profiling
from yaspin import yaspin
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

# adds the word IGNORE after INSERT in sqlalchemy
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kw)

# This function forms the **E** from ETL - it extracts the data and puts it into a dataframe.
# lets the user know what is happening when the code is just 'doing stuff'

def df_from_sql_table(table_name, create_engine=create_engine):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    ret = pd.read_sql_table(table_name, engine)
    engine.dispose()
    return ret


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
    cards=str(cards).rstrip('.0')
    return cards



def clean_the_data(df):
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
    customer_df.columns = ["name"]
    customer_df = customer_df.drop_duplicates()
    print("Customers DF OK")
    return customer_df


def get_df_location(df):
    location_df = df[["location"]]
    location_df.columns = ["name"]
    location_df = location_df.drop_duplicates()
    print("Location DF OK")
    return location_df


def get_df_cards(df):
    cards_df = df[["card_number"]]
    cards_df = cards_df.drop_duplicates()
    # commented out temporarily while transaction table not yet made
    print("Cards DF OK")
    return cards_df

def drop_dupe_prods(df:pd.Series,prods:pd.DataFrame):
    name = df['name']
    flavour = df['flavour']
    size = df['size']
    price = df['price']    
    prod = prods.query(f"name=='{name}' and size == '{size}' and flavour == '{flavour}' and price == {price}")

    if not prod.empty:
        return True
    else:
        return False
    

def get_df_products(df):
    prods_table = df_from_sql_table('products')
    products_df = df[["product_name", "flavour", "size", "price"]]
    products_df.columns = ["name", "flavour", "size", "price"]
    products_df = products_df.drop_duplicates(ignore_index=True)
    products_df['duplicate'] = products_df.apply(drop_dupe_prods,args=(prods_table,),axis=1)
    products_df = products_df[products_df["duplicate"] == False]
    products_df = products_df.drop('duplicate',axis=1)
    print("Products DF OK")
    return products_df        
    

# -------------------------------------------------------------------------
# function that creates all of the individual dataframes (calls the above functions)
#@yaspin(text="Creating dataframes...")
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

def df_to_sql(df, table_name):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    df.to_sql(con=engine, if_exists='append', name=table_name, index=False)
    engine.dispose()

# individual functions to isert into all the different tables
#@yaspin(text="Inserting names into DB...")
def insert_names(
    customer_df: pd.DataFrame,
    df_to_sql=df_to_sql
):
    df_to_sql(customer_df, "customers")
    print("Names inserted OK")


#@yaspin(text="Inserting cards into DB...")
def insert_cards(
    cards_df: pd.DataFrame,
    df_to_sql=df_to_sql,
    clean_cards=clean_cards
):
    cards_df["card_number"] =cards_df["card_number"].apply(clean_cards)
    df_to_sql(cards_df, "cards")
    print("Cards inserted OK")


#@yaspin(text="Inserting stores into DB...")
def insert_store(
    location_df: pd.DataFrame,
    df_to_sql=df_to_sql,
):
    df_to_sql(location_df, "store")
    print("Stores inserted OK")


#@yaspin(text="Inserting products into DB...")
def insert_products(
    products_df: pd.DataFrame,
    df_to_sql=df_to_sql
):
    df_to_sql(products_df, "products")
    print("Products inserted OK")


# -----------------------------------------------------


def etl(
    df_exploded,
    get_table_df=get_table_df,
    # clean_products=clean_products,
    insert_names=insert_names,
    insert_cards=insert_cards,
    insert_store=insert_store,
    insert_products=insert_products,
):
    # generate our dataframes
    customer_df, location_df, cards_df, products_df = get_table_df(df_exploded)

    # clean our product data
    # each of these executes a series of sql commands to insert the data into our database

    insert_names(customer_df)
    insert_cards(cards_df)
    insert_store(location_df)
    if not products_df.empty:
        print('tried to insert :)')
        insert_products(products_df)
    else:
        print('no new products')


# ---------------------------------------------------
# --------------functions end here-------------------
# this file just runs this one command
if __name__ == "__main__":
    df_exploded = clean_the_data()
    etl(df_exploded)
