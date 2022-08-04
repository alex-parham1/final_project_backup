import pandas as pd
import traceback
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
import numpy
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# adds the word IGNORE after INSERT in sqlalchemy
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with("IGNORE"), **kw)


try:
    snow_user = os.environ.get("SNOWFLAKE_USER")
    snow_password = os.environ.get("SNOWFLAKE_PASS")
    print('sf credentials aquired')
except:
    print("Failed to find snowflake credentials. Skipping.")


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
    print('getting connection')
    ctx = connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    print('capitolizing columns')
    cols = df.columns
    upper_cols = []
    for col in cols:
        upper_cols.append(col.upper())
    df.columns = upper_cols
    print('writing to pandas')
    success, nchunks, nrows, _ = write_pandas(ctx, df, table_name=table,database=database,schema=schema)
    print(
        f"Successfully uploaded to snowflake: {success}, Number of rows updated (if any): {nrows} using {nchunks} chunks."
    )
    ctx.close()


def df_to_sql(df: pd.DataFrame, table_name, create_engine=create_engine):
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
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
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
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
        return True
    else:
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

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    sql = f"SELECT * from {table_name} WHERE date_time between '{start_time}' and '{end_time}'"
    print("executing query")
    ret = read_sql_query(sql, engine)
    return ret


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


def get_table_drop_dupes(table_name, df_from_sql_table=df_from_sql_table):
    print(f"getting {table_name} table")
    table = df_from_sql_table(table_name)
    try:
        table = table.drop_duplicates(subset="name")
    except Exception:
        print(f"Table {table_name} has no column named 'name'")
    return table


def get_timeframe_transactions(
    start_time, end_time, df_from_sql_query=df_from_sql_query
):
    transactions = df_from_sql_query("transactions", start_time, end_time)
    transactions = transactions.drop_duplicates()
    return transactions


def remove_duplicate_transactions(
    transactions,
    trans_table,
    transaction_duplicate_protection=transaction_duplicate_protection,
):
    trans_table = trans_table.drop_duplicates()
    # duplicate protection
    trans_table["duplicate"] = trans_table.apply(
        transaction_duplicate_protection, args=(transactions,), axis=1
    )
    trans_table = trans_table[trans_table["duplicate"] == False]
    trans_table = trans_table.drop("duplicate", axis=1)
    return trans_table


def insert_baskets(
    trans_df: pd.DataFrame,
    start_time,
    end_time,
    df_from_sql_query=df_from_sql_query,
    get_transaction_id=get_transaction_id,
    get_product_id=get_product_id,
    df_to_sql=df_to_sql,
    df_from_sql_table=df_from_sql_table,
):

    print("updating transactions")
    transactions = df_from_sql_query("transactions", start_time, end_time)
    transactions = transactions.drop_duplicates()
    print("transactions updated")
    print("downloading products")
    products = df_from_sql_table("products")
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
    try:
        connect_and_push_snowflake("BASKET", "YOGHURT_DB", baskets)
    except Exception as e:
        print("Failed to upload to Snowflake")
        print(traceback.format_exc())
        print(e)
    print("baskets uploaded")

    print("Transactions and Baskets inserted OK")


def insert_transactions(
    trans_df: pd.DataFrame,
    get_customer_id=get_customer_id,
    get_store_id=get_store_id,
    get_table_drop_dupes=get_table_drop_dupes,
    get_timeframe_transactions=get_timeframe_transactions,
    remove_duplicate_transactions=remove_duplicate_transactions,
    df_to_sql=df_to_sql,
    insert_baskets=insert_baskets,
):
    # store tables in memory for comparison
    users = get_table_drop_dupes("customers")
    stores = get_table_drop_dupes("store")

    start_time = trans_df["date"].head(1).values.tolist()[0]
    end_time = trans_df["date"].tail(1).values.tolist()[0]

    print("downloading transactions")
    transactions = get_timeframe_transactions(start_time, end_time)
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

    trans_table = remove_duplicate_transactions(transactions, trans_table)
    print("uploading transactions")
    df_to_sql(trans_table, "transactions")
    try:
        connect_and_push_snowflake("TRANSACTIONS", "YOGHURT_DB", trans_table)
    except Exception as e:
        print("Failed to upload to Snowflake")
        print(traceback.format_exc())
        print(e)
    print("uploaded transactions")
    insert_baskets(trans_df, start_time, end_time)
