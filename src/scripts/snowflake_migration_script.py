import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from extraction import df_from_sql_table
from extraction import connect_and_push_snowflake
from dotenv import load_dotenv

load_dotenv()

print(
    "USE EXTREME CAUTION RUNNING THIS SCRIPT. SNOWFLAKE DOES NOT ENFORCE DUPLICATES. BE CERTAIN THAT THE DATA YOU ARE QUERYING IS WHAT YOU WANT TO GO INTO SNOWFLAKE."
)
migrate = input("Do you wish to migrate a table to snowflake? Y/N: ").upper()
if migrate == "Y":
    migrate = True
else:
    migrate = False

try:
    snow_user = os.environ.get("SNOWFLAKE_USER")
    snow_password = os.environ.get("SNOWFLAKE_PASS")
except:
    print("Failed to find snowflake credentials. Skipping.")


while migrate:
    table_name_rds = input(
        "Enter the exact name of the RDS table you wish to copy *CASE SENSITIVE*:"
    )
    table_name_snowflake = input(
        "Enter the exact name of the SNOWFLAKE table you wish to copy into *CASE SENSITIVE*:"
    )
    db_name_snowflake = input("Enter the snowflake DB to be used:")

    print(f"Downloading {table_name_rds}.")
    rds_df = df_from_sql_table(table_name_rds)
    print("Download Successful.")
    print(f"Pushing to Snowflake table {table_name_snowflake} on {db_name_snowflake}")
    columns = rds_df.columns
    upper_columns = []
    for column in columns:
        upper_columns.append(column.upper())
    rds_df.columns = upper_columns
    print(rds_df.head())
    go = input(
        "Verify the above columns match what you expect in SNOWFLAKE. Press Y to continue or N to abort. > "
    ).upper()
    if go == "Y":
        connect_and_push_snowflake(
            table_name_snowflake,
            db_name_snowflake,
            rds_df,
            user=snow_user,
            password=snow_password,
        )
        print("Success")
    else:
        pass

    cont = input("Do you have another table to copy? Y/N: ")
    if cont == "Y":
        migrate = True
    else:
        migrate = False
        print("Thank you, Goodbye!")
