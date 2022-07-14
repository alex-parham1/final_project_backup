from src.scripts import extraction as ex
import pymysql
import pandas as pd
import pytest
from unittest.mock import Mock, MagicMock, patch, call

# ----------get_df_customers-------------------


def test_get_df_customers_happy_path_drops_duplicates():
    list = [1, 1, 4, 5]
    data = pd.DataFrame(list, columns=["customer_name"])
    expected_list = [1, 4, 5]
    expected_data = pd.DataFrame(expected_list, columns=["customer_name"])
    result_dataframe = ex.get_df_customers(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_customers_happy_path():
    list = [1, 4, 5]
    data = pd.DataFrame(list, columns=["customer_name"])
    expected_list = [1, 4, 5]
    expected_data = pd.DataFrame(expected_list, columns=["customer_name"])
    result_dataframe = ex.get_df_customers(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_customers_empty():
    list = []
    data = pd.DataFrame(list, columns=["customer_name"])
    expected_list = []
    expected_data = pd.DataFrame(expected_list, columns=["customer_name"])
    result_dataframe = ex.get_df_customers(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_customers_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 4, 5]
        data = pd.DataFrame(list, columns=["CUSTOMER_NAME"])
        result_dataframe = ex.get_df_customers(data)


# ----------get_df_location--------------------


def test_get_df_location_happy_path():
    list = ["London", "Folkestone", "Bournemouth", "Hardwicke"]
    data = pd.DataFrame(list, columns=["location"])
    expected_list = ["London", "Folkestone", "Bournemouth", "Hardwicke"]
    expected_data = pd.DataFrame(expected_list, columns=["location"])
    result_dataframe = ex.get_df_location(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_location_happy_path_drops_duplicate():
    list = ["London", "London", "Folkestone", "Folkestone", "Bournemouth", "Hardwicke"]
    data = pd.DataFrame(list, columns=["location"])
    expected_list = ["London", "Folkestone", "Bournemouth", "Hardwicke"]
    expected_data = pd.DataFrame(expected_list, columns=["location"])
    result_dataframe = ex.get_df_location(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_location_empty():
    list = []
    data = pd.DataFrame(list, columns=["location"])
    expected_list = []
    expected_data = pd.DataFrame(expected_list, columns=["location"])
    result_dataframe = ex.get_df_location(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_location_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 2, 3]
        data = pd.DataFrame(list, columns=["LOCATION"])
        result_dataframe = ex.get_df_location(data)


# ------get_df_cards-------------------


def test_get_df_cards():
    list = ["1212", "12333", "123432"]
    data = pd.DataFrame(list, columns=["card_number"])
    expected_list = ["1212", "12333", "123432"]
    expected_data = pd.DataFrame(expected_list, columns=["card_number"])
    result_dataframe = ex.get_df_cards(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_cards_empty():
    list = []
    data = pd.DataFrame(list, columns=["card_number"])
    expected_list = []
    expected_data = pd.DataFrame(expected_list, columns=["card_number"])
    result_dataframe = ex.get_df_cards(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_cards_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 2, 3]
        data = pd.DataFrame(list, columns=["CARD NUMBER"])
        result_dataframe = ex.get_df_cards(data)


# --------get_df_products-----------------


def test_get_df_products():
    list = ["test", "test", "test"]
    data = pd.DataFrame(list, columns=["products"])
    expected_list = ["test", "test", "test"]
    expected_data = pd.DataFrame(expected_list, columns=["products"])
    result_dataframe = ex.get_df_products(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_products_empty_list():
    list = []
    data = pd.DataFrame(list, columns=["products"])
    expected_list = []
    expected_data = pd.DataFrame(expected_list, columns=["products"])
    result_dataframe = ex.get_df_products(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_products_unhappy_path():
    with pytest.raises(Exception):
        list = ["test", "test"]
        data = pd.DataFrame(list, columns=["PRODUCTS"])
        result_dataframe = ex.get_df_products(data)


# --------separate_products----------------------


def test_separate_products_happy_path():
    data = ["Regular Mocha - 2.30", "Large tea - Caramel - 3.25"]
    expected = [
        ["Regular", "Mocha", "None", "2.30"],
        ["Large", "tea", "Caramel", "3.25"],
    ]
    result = ex.separate_products(data)
    print(result)
    assert result == expected


def test_separate_products_happy_path2():
    data = ["Regular Mocha - 2.30", " Large tea - Caramel - 3.25"]
    expected = [
        ["Regular", "Mocha", "None", "2.30"],
        ["Large", "tea", "Caramel", "3.25"],
    ]
    result = ex.separate_products(data)
    print(result)
    assert result == expected


def test_separate_products_unhappy_path():
    data = ["Regular Mocha : 2.30", "Large tea : Caramel : 3.25"]
    with pytest.raises(Exception):
        result = ex.separate_products(data)


def test_separate_products_unhappy_path_empty():
    data = []
    with pytest.raises(Exception):
        result = ex.separate_products(data)


# --------get_table_df---------------------


def test_get_table_df():
    list = ["test", "test"]
    df = pd.DataFrame(list, columns=["PRODUCTS"])

    mock_customers = Mock(side_effect=[1])
    mock_location = Mock(side_effect=[2])
    mock_cards = Mock(side_effect=[3])
    mock_products = Mock(side_effect=[4])
    mock_transaction = Mock(side_effect=[5])

    result = ex.get_table_df(
        df,
        get_df_customers=mock_customers,
        get_df_location=mock_location,
        get_df_cards=mock_cards,
        get_df_products=mock_products,
        get_df_transaction=mock_transaction,
    )

    expected = (1, 2, 3, 4, 5)

    assert expected == result


def test_get_table_df_foo_calls():
    list = ["test", "test"]
    df = pd.DataFrame(list, columns=["PRODUCTS"])

    mock_customers = Mock(side_effect=[1])
    mock_location = Mock(side_effect=[2])
    mock_cards = Mock(side_effect=[3])
    mock_products = Mock(side_effect=[4])
    mock_transaction = Mock(side_effect=[5])

    result = ex.get_table_df(
        df,
        get_df_customers=mock_customers,
        get_df_location=mock_location,
        get_df_cards=mock_cards,
        get_df_products=mock_products,
        get_df_transaction=mock_transaction,
    )

    mock_customers.assert_called_once_with(df)
    mock_location.assert_called_once_with(df)
    mock_cards.assert_called_once_with(df)
    mock_products.assert_called_once_with(df)
    mock_transaction.assert_called_once_with(df)


# ------etl----------------------
def test_etl_foo_calls():
    list = ["test", "test"]
    df = pd.DataFrame(list, columns=["PRODUCTS"])

    get_data_frame = Mock(side_effect=[df])
    get_table_df = Mock(side_effect=[(1, 1, 1, 1, 1)])
    clean_products = Mock(side_effect=[3])
    get_connection = Mock(side_effect=["mock_connection"])
    insert_names = Mock(side_effect=[5])
    insert_cards = Mock(side_effect=[6])
    insert_store = Mock(side_effect=[7])
    insert_products = Mock(side_effect=[8])
    commit_and_close = Mock()

    result = ex.etl(
        get_data_frame=get_data_frame,
        get_table_df=get_table_df,
        clean_products=clean_products,
        get_connection=get_connection,
        insert_names=insert_names,
        insert_cards=insert_cards,
        insert_store=insert_store,
        insert_products=insert_products,
        commit_and_close=commit_and_close,
    )

    get_data_frame.assert_called_once()
    get_table_df.assert_called_once_with(df)
    clean_products.assert_called_once_with(1)
    get_connection.assert_called_once()
    insert_names.assert_called_once_with("mock_connection", 1)
    insert_cards.assert_called_once_with("mock_connection", 1)
    insert_store.assert_called_once_with("mock_connection", 1)
    insert_products.assert_called_once_with("mock_connection", 3)
    commit_and_close.assert_called_once_with("mock_connection")


# ------insert names----------------------
@patch("builtins.print")
def test_insert_names(mock_print: Mock):
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    connection = "connection"
    calls = [
        call(
            "cursor",
            """
        INSERT INTO customers (name)
            VALUES ('test')""",
        ),
        call(
            "cursor",
            """
        INSERT INTO customers (name)
            VALUES ('test')""",
        ),
    ]
    list = ["test", "test"]
    df = pd.DataFrame(list, columns=["PRODUCTS"])

    ex.insert_names(
        connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
    )

    mock_print.assert_called_once_with("Names inserted OK")
    get_cursor.assert_called_once_with(connection)
    execute_cursor.assert_has_calls(calls)


@patch("builtins.print")
def test_insert_names_big_data(mock_print: Mock):
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    connection = "connection"
    calls = [
        call(
            "cursor",
            """
        INSERT INTO customers (name)
            VALUES ('test')""",
        ),
        call(
            "cursor",
            """
        INSERT INTO customers (name)
            VALUES ('test')""",
        ),
    ]
    list = [["test", "testing"], ["test", "testing"]]
    df = pd.DataFrame(list, columns=["PRODUCTS", "name"])

    ex.insert_names(
        connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
    )

    mock_print.assert_called_once_with("Names inserted OK")
    get_cursor.assert_called_once_with(connection)
    execute_cursor.assert_has_calls(calls)


# ---------------insert cards
@patch("builtins.print")
def test_insert_cards(mock_print: Mock):
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    connection = "connection"
    calls = [
        call(
            "cursor",
            """
        INSERT into cards (card_number)
            VALUES ('test')""",
        ),
        call(
            "cursor",
            """
        INSERT into cards (card_number)
            VALUES ('test')""",
        ),
    ]

    list = ["test", "test"]
    df = pd.DataFrame(list, columns=["PRODUCTS"])

    ex.insert_cards(
        connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
    )

    mock_print.assert_called_once_with("Cards inserted OK")
    get_cursor.assert_called_once_with(connection)
    execute_cursor.assert_has_calls(calls)


@patch("builtins.print")
def test_insert_cards_big_data(mock_print: Mock):
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    connection = "connection"
    calls = [
        call(
            "cursor",
            """
        INSERT into cards (card_number)
            VALUES ('test')""",
        ),
        call(
            "cursor",
            """
        INSERT into cards (card_number)
            VALUES ('test')""",
        ),
    ]

    list = [["test", "testing"], ["test", "testing"]]
    df = pd.DataFrame(list, columns=["PRODUCTS", "name"])

    ex.insert_cards(
        connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
    )

    mock_print.assert_called_once_with("Cards inserted OK")
    get_cursor.assert_called_once_with(connection)
    execute_cursor.assert_has_calls(calls)


# -----------insert store---------------
@patch("builtins.print")
def test_insert_store(mock_print: Mock):
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    connection = "connection"
    calls = [
        call(
            "cursor",
            """
        INSERT into store (name)
            VALUES ('test')""",
        ),
        call(
            "cursor",
            """
        INSERT into store (name)
            VALUES ('test')""",
        ),
    ]

    list = ["test", "test"]
    df = pd.DataFrame(list, columns=["PRODUCTS"])

    ex.insert_store(
        connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
    )

    mock_print.assert_called_once_with("Stores inserted OK")
    get_cursor.assert_called_once_with(connection)
    execute_cursor.assert_has_calls(calls)


@patch("builtins.print")
def test_insert_store_big_data(mock_print: Mock):
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    connection = "connection"
    calls = [
        call(
            "cursor",
            """
        INSERT into store (name)
            VALUES ('test')""",
        ),
        call(
            "cursor",
            """
        INSERT into store (name)
            VALUES ('test')""",
        ),
    ]

    list = [["test", "testing"], ["test", "testing"]]
    df = pd.DataFrame(list, columns=["PRODUCTS", "name"])

    ex.insert_store(
        connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
    )

    mock_print.assert_called_once_with("Stores inserted OK")
    get_cursor.assert_called_once_with(connection)
    execute_cursor.assert_has_calls(calls)


# -------------insert products-----------------
@patch("builtins.print")
def test_insert_products(mock_print: Mock):
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    sql = """
        INSERT INTO products (size, name, flavour, price)
            VALUES ('test', 'testing', 'tested', 3)"""
    connection = "connection"
    calls = [call("cursor", sql), call("cursor", sql)]

    list = [["test", "testing", "tested", 3], ["test", "testing", "tested", 3]]
    df = pd.DataFrame(list, columns=["PRODUCTS", "name", "size", "price"])

    ex.insert_products(
        connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
    )

    mock_print.assert_called_once_with("Products inserted OK")
    get_cursor.assert_called_once_with(connection)
    execute_cursor.assert_has_calls(calls)


def test_insert_products_bad_data():
    get_cursor = Mock(side_effect=["cursor"])
    execute_cursor = Mock()
    connection = "connection"
    # calls = [call("cursor", sql), call("cursor", sql)]

    list = [["test", "testing", "tested"], ["test", "testing", "tested"]]
    df = pd.DataFrame(list, columns=["PRODUCTS", "name", "size"])
    with pytest.raises(IndexError):
        ex.insert_products(
            connection, df, get_cursor=get_cursor, execute_cursor=execute_cursor
        )


# ----------get_data_frame------------

# def test_get_data_frame():
#     pass

# ----------clean_products-------------


def test_clean_products():
    pass
