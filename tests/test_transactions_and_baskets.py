import pytest
from unittest.mock import Mock, patch, call
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
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    tb.df_from_sql_query(
        name, "Test", "Test", create_engine=engine_caller, read_sql_query=mock_query
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
    print(actual)
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

#----get_table_drop_dupes----
def test_get_table_drop_dupes():
    mock_df_from_sql_table = Mock()

    mock_df = {
        "name" : ['value1', 'value1', 'value2'],
        "column_2" : ["c2_value1", "c2_value1", "value2"]
    }

    df = pd.DataFrame(mock_df)
    mock_df_from_sql_table.return_value = df
    expected_df = {
        "name" : ['value1', 'value2'],
        "column_2" : ["c2_value1", "value2"]
    }

    expected = pd.DataFrame(expected_df)
    expected = expected.reset_index(drop=True)

    actual = tb.get_table_drop_dupes("test", df_from_sql_table=mock_df_from_sql_table)
    actual = actual.reset_index(drop=True)

    assert expected.equals(actual)

#----get_timeframe_transactions-----
def test_get_timeframe_transactions():
    mock_df_from_sql_query = Mock()

    mock_df = {
        "name" : ['value1', 'value1', 'value2'],
        "column_2" : ["c2_value1", "c2_value1", "value2"]
    }

    df = pd.DataFrame(mock_df)
    mock_df_from_sql_query.return_value = df
    expected_df = {
        "name" : ['value1', 'value2'],
        "column_2" : ["c2_value1", "value2"]
    }

    expected = pd.DataFrame(expected_df)
    expected = expected.reset_index(drop=True)

    actual = tb.get_timeframe_transactions("00:00", "00:00", df_from_sql_query=mock_df_from_sql_query)
    actual = actual.reset_index(drop=True)

    assert expected.equals(actual)




    @patch("builtins.print")
    def mock_insert_baskets(mock_print):
        trans_data = {
            "transaction_id" : 1,
            "date_time" : "1993/06/29 12:10",
            "customer_id" : 2,
            "store_id" : 3,
            "total" : 7.50,
            "payment_method" : "CARD"
        }
        transactions = pd.DataFrame(trans_data)
       
        prod_data = {
            "product_id" : 123,
            "name" : "Coffee",
            "flavour" : "None",
            "size" : "Large",
            "price" : 1.50
        }
        products = pd.DataFrame(prod_data)

        mock_df_from_sql_query = Mock(side_effect=[transactions])
        mock_df_from_sql_table = Mock(side_effect=[products])
        
        mock_get_transaction_id = Mock()
        mock_get_transaction_id.return_value = 1

        mock_get_product_id = Mock()
        mock_get_product_id.return_value = 11

        mock_df_to_sql = Mock()
            
        mock_print(1)
        mock_print(2)
        mock_print(3)
        mock_print(4)
        mock_print(5)
        mock_print(6)
        mock_print(7)
        mock_print(8)
        mock_print(9)
        calls = [mock_print(1),mock_print(2),mock_print(3), mock_print(4),mock_print(5), mock_print(6), mock_print(7), mock_print(8), mock_print(9)]
        
        tb.insert_baskets(
            trans_df=transactions, 
            start_time="2022/07/28 11:52", 
            end_time="2022/07/28 12:52",
            df_from_sql_query=mock_df_from_sql_query,
            get_transaction_id=mock_get_transaction_id,
            get_product_id=mock_get_product_id,
            df_to_sql=mock_df_to_sql,
            df_from_sql_table=mock_df_from_sql_table)

        mock_df_from_sql_query.assert_called()
        mock_df_from_sql_table.assert_called()
        mock_get_transaction_id.assert_called()
        mock_get_product_id.assert_called()
        mock_df_to_sql.asert_called()
        mock_print.assert_has_calls(calls, any_order=False)


