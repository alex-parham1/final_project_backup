from ast import Raise
import pytest
from src.scripts import extraction as ex
import pandas as pd

def test_separate_products_happy_path():
    test = (" test,test")
    expected = ['test,test']
    result = ex.separate_products(test)
    assert expected == result

def test_separate_products_unhappy_path():
    test = ("test test test")
    expected = ['test test test']
    result = ex.separate_products(test)
    assert expected == result

def test_extract_size_happy_path():
    test = "test, test1, "
    expected = "test,"
    result = ex.extract_size(test)
    assert expected == result

def test_extract_size_unhappy_path():
    test = ("test, test")
    expected = ("test,")
    result = ex.extract_size(test)
    assert expected == result

def test_extract_price_happy_path():
    list = "test - test,test"
    expected = "test,test"
    result = ex.extract_price(list)
    assert expected == result

def test_extract_price_unhappy_path():
    list = ("test - test,test")
    expected = ("test,test")
    result = ex.extract_price(list)
    assert expected == result

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
    list = ("test1 - test2 - test3 - test4")
    expected = "test3"
    result = ex.extract_flavour(list)
    assert result == expected

def test_extract_flavour_unnhappy_path_3_long():
    list = ("test1 - test2 - test3")
    expected = "test2"
    result = ex.extract_flavour(list)
    assert result == expected

def test_extract_flavour_unhappy_path_2_long():
    list = ("test1 - test2")
    expected = "None"
    result = ex.extract_flavour(list)
    assert result == expected

def test_extract_name_happy_path():
    list = "test - test,test"
    expected = "test,test"
    result = ex.extract_name(list)
    assert expected == result

def test_extract_name_unhappy_path():
    list = ("test - test,test")
    expected = "test,test"
    result = ex.extract_name(list)
    assert expected == result

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









# def test_drop_duplicate_prods_happy_path():
#     list_name = ["name1", "name1", "name2" ]
#     list_flavour = ["fla1", "fla1", "fla2"]
#     list_size = ["size1", "size1", "size2"]
#     list_price = ["price1", "price1", "price2"]
#     name_data = pd.DataFrame(list_name, columns=['name'])
#     flavour_data = pd.DataFrame(list_flavour, columns=['flavour'])
#     size_data = pd.DataFrame(list_size, columns=['size'])
#     price_data = pd.DataFrame(list_price, columns=['price'])
#     mock_db = Mock(side_effect=["name1", ])