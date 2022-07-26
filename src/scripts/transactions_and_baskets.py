import pandas as pd
from yaspin import yaspin
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
import numpy

# adds the word IGNORE after INSERT in sqlalchemy
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with("IGNORE"), **kw)


def df_to_sql(df: pd.DataFrame, table_name, create_engine=create_engine):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")
    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    )
    db_engine = engine.execution_options(autocommit=True)
    df.to_sql(
        name=table_name,
        con=db_engine,
        if_exists="append",
        index=False,
        schema="thirstee",
    )
    db_engine.dispose()
    engine.dispose()


def df_from_sql_table(
    table_name, create_engine=create_engine, read_sql_table=pd.read_sql_table
):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")
    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    )
    ret = pd.read_sql_table(table_name, engine)
    engine.dispose()
    return ret

def transaction_duplicate_protection(transaction, table: pd.DataFrame):
    date = transaction["date_time"]
    customer = transaction["customer_id"]
    store = transaction["store_id"]
    total = transaction["total"]
    method = transaction["payment_method"]
    trans = table.query(
        f"date_time == '{date}' and customer_id == {customer} and store_id == {store} and total == {total} and payment_method == '{method}'",
        inplace=False,
    )
    if not trans.empty:
        print("duplicate found")
        return True
    else:
        print("new entry")
        return False

def df_from_sql_query(
    table_name,
    start_time,
    end_time,
    create_engine=create_engine,
    read_sql_query=pd.read_sql_query,
):

    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")

    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    )
    sql = f"SELECT * from {table_name} WHERE date_time between '{start_time}' and '{end_time}'"
    print("executing query")
    ret = pd.read_sql_query(sql, engine)

def get_store_id(store, stores: pd.DataFrame):
    id = stores.query(f"name=='{store}'", inplace=False)
    return str(id.values.tolist()[0][0])


def get_customer_id(name, customers: pd.DataFrame):
    id = customers.query(f"name=='{name}'", inplace=False)
    return str(id.values.tolist()[0][0])


def get_product_id(df: pd.Series, products: pd.DataFrame):
    p_id = products.query(
        f"name == '{df['product_name']}' and size == '{df['size']}' and flavour == '{df['flavour']}' and price == {df['price']} "
    )
    return str(p_id.values.tolist()[0][0])


def get_transaction_id(df: pd.Series, transactions: pd.DataFrame):
    # timestamp, name, customer, total
    t_id = transactions.query(
        f"date_time == '{df['date']}' and store_id == {df['location']} and customer_id == {df['customer_id']}"
    )

    return str(t_id.values.tolist()[0][0])


# def set_foreign_keys(df: pd.DataFrame,cust:pd.DataFrame,stores:pd.DataFrame):
#     print(df.head(10))
#     print(df.columns)


def insert_transactions(trans_df):
    # store tables in memory for comparison
    print("getting customers")
    users = df_from_sql_table("customers")
    users = users.drop_duplicates(subset="name")
    print("getting stores")
    stores = df_from_sql_table("store")
    stores = stores.drop_duplicates(subset="name")

    start_time = trans_df["date"].head(1).values.tolist()[0]
    end_time = trans_df["date"].tail(1).values.tolist()[0]

    print("downloading transactions")
    transactions = df_from_sql_query("transactions", start_time, end_time)
    transactions = transactions.drop_duplicates()
    print("transactions downloaded")

    # get customer ids by looking up a matching customer in the database
    print("getting customer ids")
    trans_df["customer_id"] = trans_df["customer_name"].apply(
        get_customer_id, args=(users,)
    )
    # get store id by looking up a matching store in the database
    print("getting store id")
    trans_df["location"] = trans_df["location"].apply(get_store_id, args=(stores,))

    # make a new df that matches the layout and format of the table in the database
    trans_table = trans_df.drop(
        columns=[
            "customer_name",
            "product_name",
            "flavour",
            "size",
            "price",
            "card_number",
        ]
    )
    trans_table.columns = [
        "date_time",
        "store_id",
        "total",
        "payment_method",
        "customer_id",
    ]

    trans_table = trans_table.drop_duplicates()
    # duplicate protection
    trans_table["duplicate"] = trans_table.apply(
        transaction_duplicate_protection, args=(transactions,), axis=1
    )
    trans_table = trans_table[trans_table["duplicate"] == False]
    trans_table = trans_table.drop("duplicate", axis=1)
    print(trans_table.shape)
    print("uploading transactions")
    df_to_sql(trans_table, "transactions")
    print("uploaded transactions")

    # baskets starts here

    print("updating transactions")
    transactions = df_from_sql_query("transactions", start_time, end_time)
    transactions = transactions.drop_duplicates()
    print("transactions updated")
    print("downloading products")
    products = df_from_sql_table("products")
    products = products.drop_duplicates()
    print("products downloaded")
    baskets = pd.DataFrame()

    print("creating baskets")
    baskets["transaction_id"] = trans_df.apply(
        get_transaction_id, args=(transactions,), axis=1
    )
    baskets["product_id"] = trans_df.apply(get_product_id, args=(products,), axis=1)
    print("baskets created")

    print("uploading baskets")
    df_to_sql(baskets, "basket")
    print("baskets uploaded")

    print("Transactions and Baskets inserted OK")


if __name__ == "__main__":
    insert_transactions()
