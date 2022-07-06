import pandas as pd 
import pandas_profiling
import os 

def get_data_frame():
    df = pd.DataFrame()
    for filename in os.listdir('../data'):
        temp_df = pd.read_csv(f'../data/{filename}')
        df = pd.concat([df, temp_df], axis=0)
    return df

df = get_data_frame()
df.columns = ['DATE', 'LOCATION', 'CUSTOMER NAME', 'PRODUCTS', 'PAYMENT METHOD', 'TOTAL', 'CARD']
df.reset_index()
print(df.shape)
print(df.sample(5))


