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


# tries to grab snowflake details from aws environment variables
try:
    snow_user = os.environ.get("SNOWFLAKE_USER")
    snow_password = os.environ.get("SNOWFLAKE_PASS")
    print("sf credentials aquired")
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
    # getting connection....
    print("getting connection")
    ctx = connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

    # columns need to be in all caps to match our schema
    print("capitolizing columns")
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


def df_to_sql(df: pd.DataFrame, table_name, create_engine=create_engine):
    # get environment variables from lambda
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")

    # create sqlalchemy engine/connection - turning on auto-commit
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    db_engine = engine.execution_options(autocommit=True)

    # use pandas to_sql, append mode, to add dataframe onto then end of the table in rds
    df.to_sql(
        name=table_name,
        con=db_engine,
        if_exists="append",
        index=False,
        schema="thirstee",
    )
    # dont forget to close the connections!
    db_engine.dispose()
    engine.dispose()


def df_from_sql_table(
    table_name, create_engine=create_engine, read_sql_table=pd.read_sql_table
):
    # get environment variables from lambda
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")

    # create sqlalchemy engine/connection
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")

    # create dataframe using specified table in our database
    ret = pd.read_sql_table(table_name, engine)

    # close connection and return dataframe
    engine.dispose()
    return ret


# used via .apply in pandas to create a duplicate flag column by comparing two dataframes
def transaction_duplicate_protection(transaction, table: pd.DataFrame):
    # grab some variables from the passed in series / row
    date = transaction["date_time"]
    customer = transaction["customer_id"]
    store = transaction["store_id"]
    total = transaction["total"]
    method = transaction["payment_method"]

    # get any entries from the passed in dataframe that have the exact same data
    trans = table.query(
        f"date_time == '{date}' and customer_id == {customer} and store_id == {store} and total == {total} and payment_method == '{method}'",
        inplace=False,
    )

    # if there were any result for the query, return true, else return false
    if not trans.empty:
        return True
    else:
        return False


def df_from_sql_query(
    table_name,
    start_time,
    end_time,
    store,
    create_engine=create_engine,
    read_sql_query=pd.read_sql_query,
):
    # gather environment variables from lambda
    user = os.environ.get("mysql_user")
    password = os.environ.get("mysql_pass")
    host = os.environ.get("mysql_host")
    port = os.environ.get("mysql_port")
    db = os.environ.get("mysql_db")

    # create sqlalchemy engine/connection
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")


    # creqte sql querey, get any entries from the table between the provided date range 
    sql = f"SELECT * from {table_name} WHERE store_id = {store} and date_time between '{start_time}' and '{end_time}'"
    
    #execute sql querey, retur dataframe

    print("executing query")
    ret = read_sql_query(sql, engine)
    return ret
    
# ----------- These functions are used via the .apply method in pandas datafarmes ---------
# ----------- they get the primary keys of the values already in the database -------------
# ------ they querey a dataframe that contains all the table data againsts all of the new entries ------


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


# calls df_from_sql_table to pull a table from the database, drops duplicates based on name
def get_table_drop_dupes(table_name, df_from_sql_table=df_from_sql_table):
    print(f"getting {table_name} table")
    table = df_from_sql_table(table_name)
    try:
        table = table.drop_duplicates(subset="name")
    except Exception:
        print(f"Table {table_name} has no column named 'name'")
    return table


# calls df_from_sql_query, drops any duplicate entries
# hardcoded to pull the transactions table
def get_timeframe_transactions(
    start_time, end_time, store_id, df_from_sql_query=df_from_sql_query
):
    transactions = df_from_sql_query("transactions", start_time, end_time, store_id)
    transactions = transactions.drop_duplicates()
    return transactions


# duplicate protection for transactions table
def remove_duplicate_transactions(
    transactions,
    trans_table,
    transaction_duplicate_protection=transaction_duplicate_protection,
):
    # makes sure trans_table has no duplicates, potentially no longer required
    trans_table = trans_table.drop_duplicates()

    # duplicate protection
    # compare each line of df to the table, if any duplicate entries found, tag with True
    trans_table["duplicate"] = trans_table.apply(
        transaction_duplicate_protection, args=(transactions,), axis=1
    )
    # drop anything with a True flag, then drop the column and return
    trans_table = trans_table[trans_table["duplicate"] == False]
    trans_table = trans_table.drop("duplicate", axis=1)
    return trans_table


# trans_df is the processed new transactions pulled from the trigger csv file
# start time is the first date_time in the csv, end_time is the last
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
    print(trans_df.columns)
    store_id = trans_df['location'].head(1).values.tolist()[0]

    # grab all of todays transactions by pulling from the database
    print("updating transactions")
    transactions = df_from_sql_query("transactions", start_time, end_time,store_id)
    transactions = transactions.drop_duplicates()
    print("transactions updated")

    # grab all products by pulling from the database
    print("downloading products")
    products = df_from_sql_table("products")
    print("products downloaded")

    # create empty dataframe :)
    baskets = pd.DataFrame()

    # creates column in new datafarme with the return value of get_transaction_id when applied to our trans dataframe
    print("creating baskets")
    baskets["transaction_id"] = trans_df.apply(
        get_transaction_id, args=(transactions,), axis=1
    )
    # creates column in new datafarme with the return value of get_product_id when applied to our trans dataframe
    baskets["product_id"] = trans_df.apply(get_product_id, args=(products,), axis=1)
    print("baskets created")

    # push the newly filled out dataframe to the database with df_to_sql
    print("uploading baskets")
    df_to_sql(baskets, "basket")

    # try and except block for connecting and pushing the data to our snowflake database
    # does not end code if it fails.
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

    # get the first and last date_time entry for the file
    start_time = trans_df["date"].head(1).values.tolist()[0]
    end_time = trans_df["date"].tail(1).values.tolist()[0]

    # get customer ids by looking up a matching customer in the database
    print("getting customer ids")
    trans_df["customer_id"] = trans_df["customer_name"].apply(
        get_customer_id, args=(users,)
    )
    # get store id by looking up a matching store in the database
    print("getting store id")
    trans_df["location"] = trans_df["location"].apply(get_store_id, args=(stores,))

    store_id = trans_df['location'].head(1).values.tolist()[0]

    # get all transactions from the database with the same date as the file being processed
    print("downloading transactions")
    transactions = get_timeframe_transactions(start_time, end_time, store_id)
    print("transactions downloaded")

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

    # removes any duplicated transactions and pushes to the database with to_sql
    trans_table = remove_duplicate_transactions(transactions, trans_table)
    print("uploading transactions")
    df_to_sql(trans_table, "transactions")

    # tries to push to our snowflake database, if this fails, code continues anyway
    try:
        connect_and_push_snowflake("TRANSACTIONS", "YOGHURT_DB", trans_table)
    except Exception as e:
        print("Failed to upload to Snowflake")
        print(traceback.format_exc())
        print(e)
    print("uploaded transactions")

    # now transactions are completed, time to make the baskets table
    insert_baskets(trans_df, start_time, end_time)
