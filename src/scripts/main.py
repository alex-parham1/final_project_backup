import transactions_and_baskets as tb
import extraction as ex

df = ex.clean_the_data()
ex.etl(df)
tb.insert_transactions()
