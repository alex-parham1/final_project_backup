import extraction as e
import transactions_and_baskets as t_n_b

if __name__ == "__main__":
    e.etl()
    t_n_b.insert_transactions()