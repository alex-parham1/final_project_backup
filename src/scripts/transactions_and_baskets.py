from extraction import get_data_frame, clean_products
import pandas as pd
import yaspin

@yaspin(text="Inserting orders to transaction table...")
def insert_transactions():
    trans_df = get_data_frame()
    for entry in trans_df['products']:
        df_constructor_list = []
        df_constructor_list.append(entry)
        entry_df = pd.DataFrame(df_constructor_list, columns=['products'])
        cleaned_prods_df = clean_products(entry_df)
        print(f"\n {cleaned_prods_df.head}")
        input()