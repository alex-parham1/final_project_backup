import transactions_and_baskets as tb
import extraction as ex

if __name__ == "__main__":
    df = ex.clean_the_data()
    ex.etl(df)
    tb.insert_transactions()