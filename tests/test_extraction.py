from src.scripts import extraction as ex
import pandas as pd
import pytest
from unittest.mock import Mock


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
