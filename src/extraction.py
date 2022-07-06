import pandas as pd
import pandas_profiling
import os

# This function forms the **E** from ETL - it extracts the data and puts it into a dataframe.


def get_data_frame():
    df = pd.DataFrame()
    for filename in os.listdir("../data"):
        temp_df = pd.read_csv(f"../data/{filename}")
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
    df[["card_type", "card-number"]] = df["card"].str.split(",", expand=True)
    df.dropna()
    return df


# Gets the data frame to check the code is working
df = get_data_frame()

'''
WE NEED:
into CUSTOMERS:
- Customer Name

into CARDS:
- number
- type

into STORE:
- name

into TRANSACTION:
- timestamp
- customer id (foreign key)
- product id (foreign key)
- store id (foreign key)
- total

into PRODUCTS:
- name
'''
for name in df['customer_name']:
    sql_query = f'''
    INSERT into customers (customer_name)
        VALUES ('{name}')'''