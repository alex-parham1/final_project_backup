import pytest
from unittest.mock import Mock, patch, call
from src.scripts import extraction as ex
import pandas as pd

# --------df_from_sql_table-----------------
@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.read_sql_table", side_effect=["table"])
def test_df_from_sql_table(mock_table: Mock, mock_get: Mock):
    name = "test_table"
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    expected = "table"
    result = ex.df_from_sql_table(name, create_engine=engine_caller)
    assert result == expected


@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.read_sql_table", side_effect=["table"])
def test_df_from_sql_table_calls(mock_table: Mock, mock_get: Mock):
    name = "test_table"
    # mocks
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    # calls
    address = "mysql+pymysql://test:pass@localhost:3307/test"
    calls = [
        call("mysql_user"),
        call("mysql_pass"),
        call("mysql_host"),
        call("mysql_port"),
        call("mysql_db"),
    ]
    table_calls = call(name, mock_engine)
    result = ex.df_from_sql_table(name, create_engine=engine_caller)
    # asserts
    mock_engine.assert_called_once()
    mock_get.assert_has_calls(calls)
    mock_table.assert_called_once()


# -----------separate_products--------------
def test_separate_products_happy_path():
    test = " test,test"
    expected = ["test,test"]
    result = ex.separate_products(test)
    assert expected == result


def test_separate_products_unhappy_path():
    test = "test test test"
    expected = ["test test test"]
    result = ex.separate_products(test)
    assert expected == result


# -----------extract_size--------------
def test_extract_size_happy_path():
    test = "test, test1, "
    expected = "test,"
    result = ex.extract_size(test)
    assert expected == result


def test_extract_size_unhappy_path():
    test = "test, test"
    expected = "test,"
    result = ex.extract_size(test)
    assert expected == result


# ---------extract_price-----------------
def test_extract_price_happy_path():
    list = "test - test,test"
    expected = "test,test"
    result = ex.extract_price(list)
    assert expected == result


def test_extract_price_unhappy_path():
    list = "test - test,test"
    expected = "test,test"
    result = ex.extract_price(list)
    assert expected == result


# --------extract_flavour---------------------
def test_extract_flavour_happy_path_4_long():
    list = "test1 - test2 - test3 - test4"
    expected = "test3"
    result = ex.extract_flavour(list)
    assert result == expected


def test_extract_flavour_happy_path_3_long():
    list = "test1 - test2 - test3"
    expected = "test2"
    result = ex.extract_flavour(list)
    assert result == expected


def test_extract_flavour_happy_path_2_long():
    list = "test1 - test2"
    expected = "None"
    result = ex.extract_flavour(list)
    assert result == expected


def test_extract_flavour_unhappy_path_more_than_4():
    with pytest.raises(Exception):
        list = "test1 - test2 - test3 - test4 - test5"
        result = ex.extract_flavour(list)


def test_extract_flavour_unhappy_path_only_one():
    with pytest.raises(Exception):
        list = "test1"
        result = ex.extract_flavour(list)


def test_extract_flavour_unhappy_path_4_long():
    list = "test1 - test2 - test3 - test4"
    expected = "test3"
    result = ex.extract_flavour(list)
    assert result == expected


def test_extract_flavour_unnhappy_path_3_long():
    list = "test1 - test2 - test3"
    expected = "test2"
    result = ex.extract_flavour(list)
    assert result == expected


def test_extract_flavour_unhappy_path_2_long():
    list = "test1 - test2"
    expected = "None"
    result = ex.extract_flavour(list)
    assert result == expected


# --------extract_name--------------
def test_extract_name_happy_path():
    list = "test - test,test"
    expected = "test,test"
    result = ex.extract_name(list)
    assert expected == result


def test_extract_name_unhappy_path():
    list = "test - test,test"
    expected = "test,test"
    result = ex.extract_name(list)
    assert expected == result


# --------cards------------------
def test_cards_happy_path():
    card = 12345678.0
    expected = "12345678"
    result = ex.clean_cards(card)
    assert expected == result


def test_cards_unhappy_path():
    card = "12345678.0"
    expected = "12345678"
    result = ex.clean_cards(card)
    assert expected == result


def test_cards_unhappier_path():
    card = "12345678"
    expected = "12345678"
    result = ex.clean_cards(card)
    assert expected == result


# --------get_df_customers---------------------------------
def test_get_df_customers_happy_path_drops_duplicates():
    list = [1, 1, 4, 5]
    data = pd.DataFrame(list, columns=["customer_name"])
    expected_list = [1, 4, 5]
    expected_data = pd.DataFrame(expected_list, columns=["name"])
    result_dataframe = ex.get_df_customers(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_customers_happy_path():
    list = [1, 4, 5]
    data = pd.DataFrame(list, columns=["customer_name"])
    expected_list = [1, 4, 5]
    expected_data = pd.DataFrame(expected_list, columns=["name"])
    result_dataframe = ex.get_df_customers(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_customers_empty():
    list = []
    data = pd.DataFrame(list, columns=["customer_name"])
    expected_list = []
    expected_data = pd.DataFrame(expected_list, columns=["name"])
    result_dataframe = ex.get_df_customers(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_customers_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 4, 5]
        data = pd.DataFrame(list, columns=["CUSTOMER_NAME"])
        result_dataframe = ex.get_df_customers(data)


# --------get_df_location---------------------
def test_get_df_location_happy_path():
    list = ["London", "Folkestone", "Bournemouth", "Hardwicke"]
    data = pd.DataFrame(list, columns=["location"])
    expected_list = ["London", "Folkestone", "Bournemouth", "Hardwicke"]
    expected_data = pd.DataFrame(expected_list, columns=["name"])
    result_dataframe = ex.get_df_location(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_location_happy_path_drops_duplicate():
    list = ["London", "London", "Folkestone", "Folkestone", "Bournemouth", "Hardwicke"]
    data = pd.DataFrame(list, columns=["location"])
    expected_list = ["London", "Folkestone", "Bournemouth", "Hardwicke"]
    expected_data = pd.DataFrame(expected_list, columns=["name"])
    result_dataframe = ex.get_df_location(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_location_empty():
    list = []
    data = pd.DataFrame(list, columns=["location"])
    expected_list = []
    expected_data = pd.DataFrame(expected_list, columns=["name"])
    result_dataframe = ex.get_df_location(data)
    assert result_dataframe.reset_index(drop=True).equals(
        expected_data.reset_index(drop=True)
    )


def test_get_df_location_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 2, 3]
        data = pd.DataFrame(list, columns=["LOCATION"])
        result_dataframe = ex.get_df_location(data)


# --------get_df_cards-------------
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


def test_drop_duplicate_prods_happy_path():
    data = {"name": ["name1"], "flavour": ["fla1"], "size": ["size1"], "price": [1]}

    data_1 = pd.DataFrame(data)
    data_2 = pd.DataFrame(data)
    data_1["duplicate"] = data_1.apply(ex.drop_dupe_prods, args=(data_2,), axis=1)
    expected = {
        "name": ["name1"],
        "flavour": ["fla1"],
        "size": ["size1"],
        "price": [1],
        "duplicate": [True],
    }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)


def test_drop_duplicate_prods_happier_path():
    data = {"name": ["name1"], "flavour": ["fla1"], "size": ["size1"], "price": [4]}
    data_2 = {"name": ["name1"], "flavour": ["fla1"], "size": ["size1"], "price": [1]}
    data_1 = pd.DataFrame(data)
    wrong_data = pd.DataFrame(data_2)
    data_1["duplicate"] = data_1.apply(ex.drop_dupe_prods, args=(wrong_data,), axis=1)
    expected = {
        "name": ["name1"],
        "flavour": ["fla1"],
        "size": ["size1"],
        "price": [4],
        "duplicate": [False],
    }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)


def test_drop_duplicate_prods_unhappy_path():
    data = {
        "name": ["name1"],
        "flavour": ["fla1"],
        "size": ["size1"],
        "price": ["string"],
    }
    data_1 = pd.DataFrame(data)
    data_2 = pd.DataFrame(data)
    with pytest.raises(Exception):
        data_1["duplicate"] = data_1.apply(ex.drop_dupe_prods, args=(data_2,), axis=1)


def test_get_table_df_happy_path():
    customer_df = Mock(side_effect=["customer"])
    location_df = Mock(side_effect=["location"])
    cards_df = Mock(side_effect=["cards"])
    products_df = Mock(side_effect=["products"])

    data = {
        "name": ["name1"],
        "flavour": ["fla1"],
        "size": ["size1"],
        "price": ["string"],
    }
    data_1 = pd.DataFrame(data)

    expected = ("customer", "location", "cards", "products")

    result = ex.get_table_df(
        data_1,
        get_df_customers=customer_df,
        get_df_location=location_df,
        get_df_cards=cards_df,
        get_df_products=products_df,
    )

    assert result == expected


def test_table_df_happier_path():
    customer_df = Mock(side_effect=["customer"])
    location_df = Mock(side_effect=["location"])
    cards_df = Mock(side_effect=["cards"])
    products_df = Mock(side_effect=["products"])

    data = {
        "name": ["name1"],
        "flavour": ["fla1"],
        "size": ["size1"],
        "price": ["string"],
    }
    data_1 = pd.DataFrame(data)

    ex.get_table_df(
        data_1,
        get_df_customers=customer_df,
        get_df_location=location_df,
        get_df_cards=cards_df,
        get_df_products=products_df,
    )

    customer_df.assert_called_once_with(data_1)
    location_df.assert_called_once_with(data_1)
    cards_df.assert_called_once_with(data_1)
    products_df.assert_called_once_with(data_1)
