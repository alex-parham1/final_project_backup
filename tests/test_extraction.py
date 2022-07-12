from src.scripts import extraction as ex
import pandas as pd
import pytest


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


# def test_get_df_cards():
#     list = ["1212", "12333", "123432"]
#     data = pd.DataFrame(list, columns=["card_number"])
#     expected_list = ["1212", "12333", "123432"]
#     expected_data = pd.DataFrame(expected_list, columns=["card_number"])
#     result_dataframe = ex.get_df_cards(data)
#     assert result_dataframe.reset_index(drop=True).equals(
#         expected_data.reset_index(drop=True)
#     )

# def test_get_df_cards_empty():
#     list = []
#     data = pd.DataFrame(list, columns=["card_number"])
#     expected_list = []
#     expected_data = pd.DataFrame(expected_list, columns=["card_number"])
#     result_dataframe = ex.get_df_cards(data)
#     assert result_dataframe.reset_index(drop=True).equals(
#         expected_data.reset_index(drop=True)
#     )


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
