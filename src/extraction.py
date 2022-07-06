import pandas as pd 
import pandas_profiling
import os 

def get_data_frame():
    df = pd.DataFrame(
        columns = ['DATE', 'LOCATION', 'CUSTOMER NAME', 'PRODUCTS', 'PAYMENT METHOD', 'TOTAL', 'CARD'])
    for filename in os.listdir('../data'):
        temp_df = pd.read_csv(f'../data/{filename}')
        df = pd.concat([df, temp_df], axis=0)
    return df

df = get_data_frame()
print(df.shape)
