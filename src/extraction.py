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
        "DATE",
        "LOCATION",
        "CUSTOMER NAME",
        "PRODUCTS",
        "PAYMENT METHOD",
        "TOTAL",
        "CARD",
    ]
    df.reset_index()
    # This part of the function begins to work on the transform
    df[["CARD", "CARD NUMBER"]] = df["CARD"].str.split(",", expand=True)
    df.dropna()
    return df


# Gets the data frame to check the code is working
df = get_data_frame()
