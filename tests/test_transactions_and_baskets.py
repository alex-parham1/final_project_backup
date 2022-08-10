import pytest
from unittest.mock import Mock, patch, call
import sys

sys.path.append("../")
from src.scripts import transactions_and_baskets as tb
import pandas as pd

# --------df_to_sql--------
@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.DataFrame.to_sql")
def test_df_to_sql(mock_table: Mock, mock_get: Mock):
    df = pd.DataFrame()
    name = "test_table"
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)

    tb.df_to_sql(df, name, create_engine=engine_caller)

    calls = [
        call("mysql_user"),
        call("mysql_pass"),
        call("mysql_host"),
        call("mysql_port"),
        call("mysql_db"),
    ]
    # asserts
    mock_engine.assert_called_once()
    mock_get.assert_has_calls(calls)
    mock_table.assert_called_once()


# -------df_from_sql_table-------
@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.read_sql_table", side_effect=["table"])
@patch("pandas.read_sql_query")
def test_df_from_sql_query(mock_query: Mock, mock_table: Mock, mock_get: Mock):
    name = "test_table"
    store = 1377
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    tb.df_from_sql_query(
        name,
        "Test",
        "Test",
        store,
        create_engine=engine_caller,
        read_sql_query=mock_query,
    )
    mock_query.assert_called()


# -------df_from_sql_table-------
@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.read_sql_table", side_effect=["table"])
def test_df_from_sql_table(mock_table: Mock, mock_get: Mock):
    name = "test_table"
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    expected = "table"
    result = tb.df_from_sql_table(
        name, create_engine=engine_caller, read_sql_table=mock_table
    )
    assert result == expected


# ----get_store_id------
def test_get_store_id():
    data = {
        "store_id": [1, 2, 3, 4, 5],
        "name": ["Holborn", "Manchester", "Antsy", "Lochside", "Widnes"],
    }
    stores = pd.DataFrame(data)

    expected = "1"

    actual = tb.get_store_id("Holborn", stores)
    assert expected == actual


# Unhappy 1 - Store ID doesn't exist
def test_get_store_id_unhappy():
    data = {
        "store_id": [1, 2, 3, 4, 5],
        "name": ["Holborn", "Manchester", "Antsy", "Lochside", "Widnes"],
    }
    stores = pd.DataFrame(data)

    with pytest.raises(IndexError):
        tb.get_store_id("London", stores)


# ------get_customer_id--------
def test_get_customer_id():
    data = {
        "customer_id": [1, 2, 3, 4, 5],
        "name": [
            "John James Sainsbury",
            "Mary Ann Sainsbury",
            "Simon Roberts",
            "Clodagh Moriarty",
            "Phil Jordan",
        ],
    }
    customers = pd.DataFrame(data)

    expected = "4"

    actual = tb.get_customer_id("Clodagh Moriarty", customers)
    assert expected == actual


# Unhappy Path 1 - Customer ID doesn't exist
def test_get_customer_id_unhappy():
    data = {
        "customer_id": [1, 2, 3, 4, 5],
        "name": [
            "John James Sainsbury",
            "Mary Ann Sainsbury",
            "Simon Roberts",
            "Clodagh Moriarty",
            "Phil Jordan",
        ],
    }
    customers = pd.DataFrame(data)

    with pytest.raises(IndexError):
        tb.get_customer_id("Helen Hunter", customers)


# -------get_product_id-----------
def test_get_product_id():
    data = {
        "product_id": [2],
        "name": "Test2",
        "flavour": "None",
        "size": "Regular",
        "price": 2,
    }

    products = pd.DataFrame(data, index=[0])

    test_prod = {
        "product_name": "Test2",
        "flavour": "None",
        "size": "Regular",
        "price": 2,
    }

    expected = "2"

    actual = tb.get_product_id(test_prod, products)

    assert expected == actual


# Unhappy 1 - Product ID doesn't exist
def test_get_product_id_happy():
    data = {
        "product_id": [2],
        "name": "Test2",
        "flavour": "None",
        "size": "Regular",
        "price": 2,
    }

    products = pd.DataFrame(data, index=[0])

    test_prod = {
        "product_name": "Test2",
        "flavour": "None",
        "size": "Regular",
        "price": 2,
    }

    expected = "2"

    actual = tb.get_product_id(test_prod, products)
    assert expected == actual


# ---get_transaction_id----
def test_get_transaction_id():
    assemble_data = {"date": "1970/01/01 09:00", "location": 1, "customer_id": 1}

    transactions_data = {
        "transaction_id": 5,
        "date_time": "1970/01/01 09:00",
        "customer_id": 1,
        "store_id": 1,
    }

    transactions = pd.DataFrame(transactions_data, index=[0])

    expected = "5"

    actual = tb.get_transaction_id(assemble_data, transactions)
    print(actual)
    assert expected == actual


# ----get_table_drop_dupes----
def test_get_table_drop_dupes():
    mock_df_from_sql_table = Mock()

    mock_df = {
        "name": ["value1", "value1", "value2"],
        "column_2": ["c2_value1", "c2_value1", "value2"],
    }

    df = pd.DataFrame(mock_df)
    mock_df_from_sql_table.return_value = df
    expected_df = {"name": ["value1", "value2"], "column_2": ["c2_value1", "value2"]}

    expected = pd.DataFrame(expected_df)
    expected = expected.reset_index(drop=True)

    actual = tb.get_table_drop_dupes("test", df_from_sql_table=mock_df_from_sql_table)
    actual = actual.reset_index(drop=True)

    assert expected.equals(actual)


@patch("builtins.print")
def test_get_table_drop_dupes_happy2(mock_print: Mock):
    mock_df_from_sql_table = Mock()

    mock_df = {
        "column_1": ["value1", "value1", "value2"],
        "column_2": ["c2_value1", "c2_value1", "value2"],
    }

    df = pd.DataFrame(mock_df)
    mock_df_from_sql_table.return_value = df
    expected_df = {
        "column_1": ["value1", "value1", "value2"],
        "column_2": ["c2_value1", "c2_value1", "value2"],
    }

    expected = pd.DataFrame(expected_df)
    expected = expected.reset_index(drop=True)

    actual = tb.get_table_drop_dupes("test", df_from_sql_table=mock_df_from_sql_table)
    actual = actual.reset_index(drop=True)

    mock_print.assert_called_with("Table test has no column named 'name'")
    assert expected.equals(actual)

    expected = pd.DataFrame(expected_df)
    expected = expected.reset_index(drop=True)

    actual = tb.get_table_drop_dupes("test", df_from_sql_table=mock_df_from_sql_table)
    actual = actual.reset_index(drop=True)

    mock_print.assert_called_with("Table test has no column named 'name'")
    assert expected.equals(actual)


# ----get_timeframe_transactions-----
def test_get_timeframe_transactions():
    mock_df_from_sql_query = Mock()

    store = 1377

    mock_df = {
        "name": ["value1", "value1", "value2"],
        "column_2": ["c2_value1", "c2_value1", "value2"],
    }

    df = pd.DataFrame(mock_df)
    mock_df_from_sql_query.return_value = df
    expected_df = {"name": ["value1", "value2"], "column_2": ["c2_value1", "value2"]}

    expected = pd.DataFrame(expected_df)
    expected = expected.reset_index(drop=True)

    actual = tb.get_timeframe_transactions(
        "00:00", "00:00", store, df_from_sql_query=mock_df_from_sql_query
    )
    actual = actual.reset_index(drop=True)

    assert expected.equals(actual)


# ----remove_duplicate_transactions----
def test_remove_duplicate_transactions():
    mock_transaction_duplicate_protection = Mock(side_effect=[False, True])

    transactions = pd.DataFrame()

    trans_data = {"name": ["Test", "Test2"]}

    trans_table = pd.DataFrame(trans_data)

    expected_data = {"name": ["Test"]}

    expected = pd.DataFrame(expected_data)
    expected = expected.reset_index(drop=True)

    actual = tb.remove_duplicate_transactions(
        transactions,
        trans_table,
        transaction_duplicate_protection=mock_transaction_duplicate_protection,
    )
    actual = expected.reset_index(drop=True)

    assert expected.equals(actual)


# -----test_transaction_duplicate_protection-----
def test_transaction_duplicate_protection():
    data = {
        "date_time": ["2022/07/28 09:00", "2022/07/28 09:01", "2022/07/28 09:02"],
        "customer_id": [1, 2, 3],
        "store_id": [1, 1, 1],
        "total": [7.50, 12.30, 5],
        "payment_method": ["CARD", "CASH", "CASH"],
    }

    transactions = pd.DataFrame(data)

    test_data = {
        "date_time": ["2022/07/28 09:00", "2022/07/28 09:03", "2022/07/28 09:02"],
        "customer_id": [1, 4, 3],
        "store_id": [1, 1, 1],
        "total": [7.50, 10, 5],
        "payment_method": ["CARD", "CARD", "CASH"],
    }

    trans_table = pd.DataFrame(test_data)

    expected_data = {
        "date_time": ["2022/07/28 09:00", "2022/07/28 09:03", "2022/07/28 09:02"],
        "customer_id": [1, 4, 3],
        "store_id": [1, 1, 1],
        "total": [7.50, 10, 5],
        "payment_method": ["CARD", "CARD", "CASH"],
        "duplicate": [True, False, True],
    }

    expected = pd.DataFrame(expected_data)

    trans_table["duplicate"] = trans_table.apply(
        tb.transaction_duplicate_protection, args=(transactions,), axis=1
    )

    actual = trans_table

    assert expected.equals(actual)


# ----insert_transactions----


def test_insert_transactions():
    users_data = {"name": ["Test Name"]}
    stores_data = {"name": ["Test Store"]}
    trans_data = {
        "date": ["2022/07/28 09:00"],
        "customer_name": ["Sian Moore"],
        "product_name": ["Cortado"],
        "flavour": ["None"],
        "size": ["Large"],
        "location": ["Folkestone"],
        "price": [1.50],
        "card_number": [1234],
        "customer_id": [5],
        "store_id": [1],
        "total": [1.50],
    }
    users = pd.DataFrame(users_data)
    stores = pd.DataFrame(stores_data)
    trans_df = pd.DataFrame(trans_data)

    mock_get_table_drop_dupes = Mock(side_effect=[users, stores])

    transactions_table_data = {
        "transaction_id": [1, 2],
        "date_time": ["2022/07/27 09:00", "2022/07/27 09:01"],
        "customer_id": [1, 2],
        "store_id": [1, 1],
        "total": [4.75, 5],
        "payment_method": ["CASH", "CARD"],
    }

    transactions = pd.DataFrame(transactions_table_data)

    mock_get_timeframe_transactions = Mock(side_effect=[transactions])

    mock_get_customer_id = Mock(side_effect=[5])

    mock_get_store_id = Mock(side_effect=[1])

    mock_remove_duplicate_transactions = Mock()
    mock_df_to_sql = Mock()
    mock_insert_baskets = Mock()

    tb.insert_transactions(
        trans_df,
        get_customer_id=mock_get_customer_id,
        get_store_id=mock_get_store_id,
        get_table_drop_dupes=mock_get_table_drop_dupes,
        remove_duplicate_transactions=mock_remove_duplicate_transactions,
        df_to_sql=mock_df_to_sql,
        get_timeframe_transactions=mock_get_timeframe_transactions,
        insert_baskets=mock_insert_baskets,
    )

    mock_get_customer_id.assert_called_once()
    mock_get_store_id.assert_called_once()
    mock_get_table_drop_dupes.assert_called()
    mock_remove_duplicate_transactions.assert_called_once()
    mock_df_to_sql.assert_called_once()
    mock_get_timeframe_transactions.assert_called_once()
    mock_insert_baskets.assert_called_once()


@patch("builtins.print")
def test_insert_baskets(mock_print):
    trans_data = {
        "transaction_id": 1,
        "date_time": "1993/06/29 12:10",
        "customer_id": 2,
        "location": 3,
        "total": 7.50,
        "payment_method": "CARD",
    }
    transactions = pd.DataFrame(trans_data, index=[0])

    prod_data = {
        "product_id": 123,
        "name": "Coffee",
        "flavour": "None",
        "size": "Large",
        "price": 1.50,
    }
    products = pd.DataFrame(prod_data, index=[0])

    mock_df_from_sql_query = Mock(side_effect=[transactions])
    mock_df_from_sql_table = Mock(side_effect=[products])

    mock_get_transaction_id = Mock()
    mock_get_transaction_id.return_value = 1

    mock_get_product_id = Mock()
    mock_get_product_id.return_value = 11

    mock_df_to_sql = Mock()

    tb.insert_baskets(
        trans_df=transactions,
        start_time="2022/07/28 11:52",
        end_time="2022/07/28 12:52",
        df_from_sql_query=mock_df_from_sql_query,
        get_transaction_id=mock_get_transaction_id,
        get_product_id=mock_get_product_id,
        df_to_sql=mock_df_to_sql,
        df_from_sql_table=mock_df_from_sql_table,
    )

    mock_df_from_sql_query.assert_called()
    mock_df_from_sql_table.assert_called()
    mock_get_transaction_id.assert_called()
    mock_get_product_id.assert_called()
    mock_df_to_sql.assert_called()
    mock_print.assert_called()
