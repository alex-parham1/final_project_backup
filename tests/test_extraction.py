import pytest
from unittest.mock import Mock, patch, call
from src.scripts import extraction as ex
import pandas as pd

# df_from_sql_table
# ------------------------------------------------
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
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
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
    mock_engine.assert_called_once()
    mock_get.assert_has_calls(calls)
    mock_table.assert_called_once()

# separate_products
# ------------------------------------------------
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

# extract_size
# ------------------------------------------------
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

# extract_price
# ------------------------------------------------
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

# extract_flavour
# --------------------------------------------
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

# extract_name
# ------------------------------------------------
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

# clean_cards
# ------------------------------------------------
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

# get_df_customers
# ------------------------------------------------
def test_get_df_customers_happy_path_drops_duplicates():
    customer_table = pd.DataFrame()
    mock_get_customer_table = Mock(side_effect=[customer_table])
    mock_drop_dupe_customers = Mock(side_effect=[False])
    customer_data = {
        'customer_name' : ["test name"]
    }
    df = pd.DataFrame(customer_data)
    expected_data = {
        'name' : ["test name"]
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_customers(df, df_from_sql_table=mock_get_customer_table, drop_dupe_customers=mock_drop_dupe_customers)
    assert expected.equals(actual)
   
def test_get_df_customers_happier_path():
    customer_table = pd.DataFrame()
    mock_get_customer_table = Mock(side_effect=[customer_table])
    mock_drop_dupe_customers = Mock(side_effect=[False, True])
    customer_data = {
        'customer_name' : ["test name", "test name"]
    }
    df = pd.DataFrame(customer_data)
    expected_data = {
        'name' : ["test name"]
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_customers(df, df_from_sql_table=mock_get_customer_table, drop_dupe_customers=mock_drop_dupe_customers)
    assert expected.equals(actual)
   
def test_get_df_customers_unhappy_path():
    customer_table = pd.DataFrame()
    mock_get_customer_table = Mock(side_effect=[customer_table])
    mock_drop_dupe_customers = Mock(side_effect=[False, True])
    customer_data = {
        'customer_name' : ["test name", "1234"]
    }
    df = pd.DataFrame(customer_data)
    expected_data = {
        'name' : ["test name"]
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_customers(df, df_from_sql_table=mock_get_customer_table, drop_dupe_customers=mock_drop_dupe_customers)
    assert expected.equals(actual)

def test_get_df_unhappier_path():
    customer_table = pd.DataFrame()
    mock_get_customer_table = Mock(side_effect=[customer_table])
    mock_drop_dupe_customers = Mock()
    customer_data = {
        'customer_name' : []
    }
    df = pd.DataFrame(customer_data)
    expected_data = {
        'name' : []
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_customers(df, df_from_sql_table=mock_get_customer_table, drop_dupe_customers=mock_drop_dupe_customers)
    assert expected.equals(actual)

def test_get_df_customers_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 4, 5]
        data = pd.DataFrame(list, columns=["CUSTOMER_NAME"])
        result_dataframe = ex.get_df_customers(data)

# get_df_location
# ------------------------------------------------

def test_get_df_location_happy_path_drop_duplicates():
    location_table = pd.DataFrame()
    mock_get_location_table = Mock(side_effect=[location_table])
    mock_drop_dupe_location = Mock(side_effect=[False])
    location_data = {
        'location' : ["test"]
    }
    df = pd.DataFrame(location_data)
    expected_data = {
        'name' : ["test"]
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_location(df, df_from_sql_table=mock_get_location_table, drop_dupe_location=mock_drop_dupe_location)
    assert expected.equals(actual)

def test_get_df_location_happy_path():
    location_table = pd.DataFrame()
    mock_get_location_table = Mock(side_effect=[location_table])
    mock_drop_dupe_location = Mock(side_effect=[False, True])
    location_data = {
        'location' : ["test", "test"]
    }
    df = pd.DataFrame(location_data)
    expected_data = {
        'name' : ["test"]
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_location(df, df_from_sql_table=mock_get_location_table, drop_dupe_location=mock_drop_dupe_location)
    assert expected.equals(actual)

def test_get_df_location_unhappy_path():
    location_table = pd.DataFrame()
    mock_get_location_table = Mock(side_effect=[location_table])
    mock_drop_dupe_location = Mock(side_effect=[False, True])
    location_data = {
        'location' : ["test", "1234"]
    }
    df = pd.DataFrame(location_data)
    expected_data = {
        'name' : ["test"]
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_location(df, df_from_sql_table=mock_get_location_table, drop_dupe_location=mock_drop_dupe_location)
    assert expected.equals(actual)

def test_get_df_location_unhappier_path():
    location_table = pd.DataFrame()
    mock_get_location_table = Mock(side_effect=[location_table])
    mock_drop_dupe_location = Mock(side_effect=[])
    location_data = {
        'location' : []
    }
    df = pd.DataFrame(location_data)
    expected_data = {
        'name' : []
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_location(df, df_from_sql_table=mock_get_location_table, drop_dupe_location=mock_drop_dupe_location)
    assert expected.equals(actual)

def test_get_df_location_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 2, 3]
        data = pd.DataFrame(list, columns=["LOCATION"])
        result_dataframe = ex.get_df_location(data)


# get_cards_df
# ------------------------------------------------
def test_get_df_cards_happy_path_drops_duplicates():
    cards_table = pd.DataFrame()
    mock_get_cards_table = Mock(side_effect=[cards_table])
    mock_drop_dupe_cards = Mock(side_effect=[False])
    cards_data = {
        'card_number' : ['1234']
    }
    df = pd.DataFrame(cards_data)
    expected_data = {
        'card_number' : ['1234']
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_cards(df, df_from_sql_table=mock_get_cards_table, drop_dupe_cards=mock_drop_dupe_cards)
    assert expected.equals(actual)

def test_get_df_cards_happy_path():
    cards_table = pd.DataFrame()
    mock_get_cards_table = Mock(side_effect=[cards_table])
    mock_drop_dupe_cards = Mock(side_effect=[False, True])
    cards_data = {
        'card_number' : ['1234', '1234']
    }
    df = pd.DataFrame(cards_data)
    expected_data = {
        'card_number' : ['1234']
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_cards(df, df_from_sql_table=mock_get_cards_table, drop_dupe_cards=mock_drop_dupe_cards)
    assert expected.equals(actual)

def test_get_df_cards_unhappy_path():
    cards_table = pd.DataFrame()
    mock_get_cards_table = Mock(side_effect=[cards_table])
    mock_drop_dupe_cards = Mock(side_effect=[False, True])
    cards_data = {
        'card_number' : ['1234', 'test']
    }
    df = pd.DataFrame(cards_data)
    expected_data = {
        'card_number' : ['1234']
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_cards(df, df_from_sql_table=mock_get_cards_table, drop_dupe_cards=mock_drop_dupe_cards)
    assert expected.equals(actual)

def test_get_df_cards_unhappier_path():
    cards_table = pd.DataFrame()
    mock_get_cards_table = Mock(side_effect=[cards_table])
    mock_drop_dupe_cards = Mock(side_effect=[])
    cards_data = {
        'card_number' : []
    }
    df = pd.DataFrame(cards_data)
    expected_data = {
        'card_number' : []
    }
    expected = pd.DataFrame(expected_data)
    actual = ex.get_df_cards(df, df_from_sql_table=mock_get_cards_table, drop_dupe_cards=mock_drop_dupe_cards)
    assert expected.equals(actual)

def test_get_df_cards_unhappy_path():
    with pytest.raises(Exception):
        list = [1, 2, 3]
        data = pd.DataFrame(list, columns=["CARD NUMBER"])
        result_dataframe = ex.get_df_cards(data)

# drop_dupe_cards
# ------------------------------------------------
def test_drop_duplicate_cards_happy_path():
    data = {
        'card_number' : ['1234']
        }
    data_1 = pd.DataFrame(data)
    data_2 = pd.DataFrame(data)
    data_1['duplicate'] = data_1.apply(ex.drop_dupe_cards,args=(data_2,),axis=1)
    expected = {
        'card_number' : ['1234'],
        'duplicate' : [True]
        }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)

def test_drop_duplicate_cards_happier_path():
    data = {
        'card_number' :['1234']
        }
    data_2 = {
        'card_number' :['1235']
        }   
    data_1 = pd.DataFrame(data)
    wrong_data = pd.DataFrame(data_2)
    data_1['duplicate'] = data_1.apply(ex.drop_dupe_cards,args=(wrong_data,),axis=1)
    expected = {
        'card_number' :['1234'], 
        'duplicate' : [False]
        }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)


# drop_dupe_location
# ------------------------------------------------
def test_drop_duplicate_location_happy_path():
    data = {
        'name' : ['London']
        }
    data_1 = pd.DataFrame(data)
    data_2 = pd.DataFrame(data)
    data_1['duplicate'] = data_1.apply(ex.drop_dupe_location,args=(data_2,),axis=1)
    expected = {
        'name' : ['London'],
        'duplicate' : [True]
        }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)

def test_drop_duplicate_location_happier_path():
    data = {
        'name' :['London']
        }
    data_2 = {
        'name' :['Bristol']
        }   
    data_1 = pd.DataFrame(data)
    wrong_data = pd.DataFrame(data_2)
    data_1['duplicate'] = data_1.apply(ex.drop_dupe_location,args=(wrong_data,),axis=1)
    expected = {
        'name' :['London'], 
        'duplicate' : [False]
        }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)

def test_drop_duplicate_location_unhappy_path():
    data = {
        'name' :[4]
        }
    data_1 = pd.DataFrame(data)
    data_2 = pd.DataFrame(data)
    data_1['duplicate'] = data_1.apply(ex.drop_dupe_location,args=(data_2,),axis=1)
    expected = {
        'name' : [4],
        'duplicate' : [True]
    }
    
    assert data_1.equals(pd.DataFrame(expected))

# drop_dupe_customers
# ------------------------------------------------
def test_drop_duplicate_customers_happy_path():
    data = {
        'name' : ["test name"]
        }
    data_1 = pd.DataFrame(data)
    data_2 = pd.DataFrame(data)
    data_1['duplicate'] = data_1.apply(ex.drop_dupe_customers,args=(data_2,),axis=1)
    expected = {
        'name' : ['test name'],
        'duplicate' : [True]
        }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)

def test_drop_duplicate_customers_happier_path():
    data = {
        'name' :['test name']
        }
    data_2 = {
        'name' :['test surname']
        }   
    data_1 = pd.DataFrame(data)
    wrong_data = pd.DataFrame(data_2)
    data_1['duplicate'] = data_1.apply(ex.drop_dupe_customers,args=(wrong_data,),axis=1)
    expected = {
        'name' :['test name'], 
        'duplicate' : [False]
        }
    data_expected = pd.DataFrame(expected)
    assert data_1.equals(data_expected)

# drop_dupe_products
# ------------------------------------------------
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

# test_get_df
# ------------------------------------------------
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

# clean_the_data
# ------------------------------------------------
def test_clean_the_data():
    data = {
        "date": ["01/01/1970 09:00", "01/01/1970 09:01"],
        "location": ["London Holborn", "Milton Keynes"],
        "customer_name": ["Simon Roberts", "Phil Jordan"],
        "products": [
            "Regular - Test - Vanilla - 1.50, Large - Testing - Chocolate - 2.00",
            "Regular - Test - Vanilla - 1.50, Large - Testing - Chocolate - 2.00",
        ],
        "total": [3.50, 3.50],
        "payment_type": ["CARD", "CARD"],
        "card_number": [3456, 1234],
    }
    df = pd.DataFrame(data)
    expected = {
        "date": [
            "01/01/1970 09:00",
            "01/01/1970 09:00",
            "01/01/1970 09:01",
            "01/01/1970 09:01",
        ],
        "location": [
            "London Holborn",
            "London Holborn",
            "Milton Keynes",
            "Milton Keynes",
        ],
        "customer_name": [
            "Simon Roberts",
            "Simon Roberts",
            "Phil Jordan",
            "Phil Jordan",
        ],
        "product_name": ["Test", "Testing", "Test", "Testing"],
        "flavour": ["Vanilla", "Chocolate", "Vanilla", "Chocolate"],
        "size": ["Regular", "Large", "Regular", "Large"],
        "price": ["1.50", "2.00", "1.50", "2.00"],
        "total": [3.5, 3.5, 3.5, 3.5],
        "payment_type": ["CARD", "CARD", "CARD", "CARD"],
        "card_number": [3456, 3456, 1234, 1234],
    }

    expected_df = pd.DataFrame(expected)
    actual = ex.clean_the_data(df)
    expected_df = expected_df.reset_index(drop=True)
    actual_df = actual.reset_index(drop=True)
    assert expected_df.equals(actual_df)

def test_clean_the_data_unhappy_1():
    data = {
        "date": ["01/01/1970 09:00", "01/01/1970 09:01"],
        "customer_name": ["Simon Roberts", "Phil Jordan"],
        "products": [
            "Regular - Test - Vanilla - 1.50, Large - Testing - Chocolate - 2.00",
            "Regular - Test - Vanilla - 1.50, Large - Testing - Chocolate - 2.00",
        ],
        "total": [3.50, 3.50],
        "payment_type": ["CARD", "CARD"],
        "card_number": [3456, 1234],
    }
    df = pd.DataFrame(data)
    with pytest.raises(KeyError):
        ex.clean_the_data(df)

def test_clean_the_data_unhappy_1():
    data = {
        "date": ["01/01/1970 09:00", "01/01/1970 09:01"],
        "customer_name": ["Simon Roberts", "Phil Jordan"],
        "location": ["London Holborn", "Milton Keynes"],
        "products": [
            "Regular, Test, Vanilla, 1.50, Large, Testing, Chocolate, 2.00",
            "Regular, Test, Vanilla, 1.50, Large, Testing, Chocolate, 2.00",
        ],
        "total": [3.50, 3.50],
        "payment_type": ["CARD", "CARD"],
        "card_number": [3456, 1234],
    }
    df = pd.DataFrame(data)
    with pytest.raises(UnboundLocalError):
        ex.clean_the_data(df)

# get_df_products
# ------------------------------------------------
def test_get_df_products():
    mock_get_prods_table = Mock()
    prods_table_data = {
        "product_id": [12345, 12346, 12347],
        "name": ["Test prod", "Tea", "Testing Water"],
        "flavour": ["None", "None", "Chocolate"],
        "size": ["Large", "Regular", "Large"],
        "price": [1.20, 1, 1.40],
    }
    prods_table = pd.DataFrame(prods_table_data)
    mock_get_prods_table.return_value = prods_table
    products_df_data = {
        "product_name": ["Test"],
        "flavour": ["Test"],
        "size": ["Regular"],
        "price": [1.2],
    }
    df = pd.DataFrame(products_df_data)
    expected_data = {
        "name": ["Test"],
        "flavour": ["Test"],
        "size": ["Regular"],
        "price": [1.2],
    }
    expected_df = pd.DataFrame(expected_data)
    actual = ex.get_df_products(df, df_from_sql_table=mock_get_prods_table)
    actual = actual.reset_index(drop=True)
    expected = expected_df.reset_index(drop=True)

    assert actual.equals(expected)

def test_get_df_products_dupe():
    mock_get_prods_table = Mock()
    prods_table_data = {
        "product_id": [12345, 12346, 12347],
        "name": ["Test prod", "Tea", "Testing Water"],
        "flavour": ["None", "None", "Chocolate"],
        "size": ["Large", "Regular", "Large"],
        "price": [1.20, 1, 1.40],
    }
    prods_table = pd.DataFrame(prods_table_data)
    mock_get_prods_table.return_value = prods_table
    products_df_data = {
        "product_name": ["Tea"],
        "flavour": ["None"],
        "size": ["Regular"],
        "price": [1],
    }
    df = pd.DataFrame(products_df_data)
    actual = ex.get_df_products(df, df_from_sql_table=mock_get_prods_table)
    actual = actual.reset_index(drop=True)

    assert actual.empty

def test_get_df_products_unhappy_1():
    mock_get_prods_table = Mock()
    prods_table_data = {
        "product_id": [12345, 12346, 12347],
        "name": ["Test prod", "Tea", "Testing Water"],
        "flavour": ["None", "None", "Chocolate"],
        "size": ["Large", "Regular", "Large"],
        "price": [1.20, 1, 1.40],
    }
    prods_table = pd.DataFrame(prods_table_data)
    mock_get_prods_table.return_value = prods_table
    products_df_data = {
        "product_name": ["Tea"],
        "flavour": ["None"],
        "size": ["Regular"],
    }
    df = pd.DataFrame(products_df_data)
    with pytest.raises(KeyError):
        ex.get_df_products(df, df_from_sql_table=mock_get_prods_table)


# Unhappy 2 - Missing Table of Products from DB
# def test_get_df_products_unhappy_2():
#    mock_get_prods_table = Mock()
#
#    prods_table_data = ()
#    prods_table = pd.DataFrame(prods_table_data)
#    mock_get_prods_table.return_value = prods_table
#    products_df_data = {
#        "product_name": ["Tea"],
#       "flavour": ["None"],
#        "price": [1.20],
#        "size": ["Regular"],
#  }
#   df = pd.DataFrame(products_df_data)
#
#    with pytest.raises(ValueError):
#        ex.get_df_products(df, df_from_sql_table=mock_get_prods_table)


# df_to_sql
# ------------------------------------------------
@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.DataFrame.to_sql")
def test_df_to_sql(mock_table: Mock, mock_get: Mock):
    df = pd.DataFrame()
    name = "test_table"
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    ex.df_to_sql(df, name, create_engine=engine_caller)
    calls = [
        call("mysql_user"),
        call("mysql_pass"),
        call("mysql_host"),
        call("mysql_port"),
        call("mysql_db"),
    ]
    mock_engine.assert_called_once()
    mock_get.assert_has_calls(calls)
    mock_table.assert_called_once()

#test inserts
# ------------------------------------------------
@patch("builtins.print")
def test_insert_names(mock_print: Mock):
    mock_df_to_sql = Mock()
    df = pd.DataFrame()
    ex.insert_names(df, df_to_sql=mock_df_to_sql)
    mock_print.assert_called_once()
    mock_df_to_sql.assert_called_once()

@patch("builtins.print")
def test_insert_cards(mock_print: Mock):
    mock_clean_cards = Mock()
    mock_df_to_sql = Mock()
    df_data = {"card_number": [1234]}
    df = pd.DataFrame(df_data)
    ex.insert_cards(df, df_to_sql=mock_df_to_sql, clean_cards=mock_clean_cards)
    mock_print.assert_called_once()
    mock_clean_cards.assert_called_once()
    mock_df_to_sql.assert_called_once()

@patch("builtins.print")
def test_insert_store(mock_print: Mock):
    mock_df_to_sql = Mock()
    df = pd.DataFrame()
    ex.insert_store(df, df_to_sql=mock_df_to_sql)
    mock_print.assert_called_once()
    mock_df_to_sql.assert_called_once()

@patch("builtins.print")
def test_insert_products(mock_print: Mock):
    mock_df_to_sql = Mock()
    df = pd.DataFrame()
    ex.insert_products(df, df_to_sql=mock_df_to_sql)
    mock_print.assert_called_once()
    mock_df_to_sql.assert_called_once()

# etl
# ------------------------------------------------
@patch("builtins.print")
def test_etl(mock_print: Mock):
    mock_get_table_df = Mock()
    mock_insert_names = Mock()
    mock_insert_cards = Mock()
    mock_insert_store = Mock()
    mock_insert_products = Mock()
    customer_data = {"name": ["John J Sainsbury"]}
    location_data = {"name": ["Holborn"]}
    cards_data = {"card_number": [5678]}
    products_data = {
        "name": ["Test"],
        "flavour": ["None"],
        "size": ["Regular"],
        "price": 1.50,
    }
    customer_df = pd.DataFrame(customer_data)
    location_df = pd.DataFrame(location_data)
    cards_df = pd.DataFrame(cards_data)
    products_df = pd.DataFrame(products_data)
    df_exploded = pd.DataFrame()
    mock_get_table_df.return_value = customer_df, location_df, cards_df, products_df
    ex.etl(
        df_exploded=df_exploded,
        get_table_df=mock_get_table_df,
        insert_names=mock_insert_names,
        insert_cards=mock_insert_cards,
        insert_store=mock_insert_store,
        insert_products=mock_insert_products,
    )
    mock_print.assert_called_once()
    mock_get_table_df.assert_called_once()
    mock_insert_names.assert_called_once()
    mock_insert_cards.assert_called_once()
    mock_insert_store.assert_called_once()
    mock_insert_products.assert_called_once()


@patch("builtins.print")
def test_etl_happy_1(mock_print: Mock):
    mock_get_table_df = Mock()
    mock_insert_names = Mock()
    mock_insert_cards = Mock()
    mock_insert_store = Mock()
    mock_insert_products = Mock()
    customer_data = {"name": ["John J Sainsbury"]}
    location_data = {"name": ["Holborn"]}
    cards_data = {"card_number": [5678]}
    products_data = {}
    customer_df = pd.DataFrame(customer_data)
    location_df = pd.DataFrame(location_data)
    cards_df = pd.DataFrame(cards_data)
    products_df = pd.DataFrame(products_data)
    df_exploded = pd.DataFrame()
    mock_get_table_df.return_value = customer_df, location_df, cards_df, products_df
    ex.etl(
        df_exploded=df_exploded,
        get_table_df=mock_get_table_df,
        insert_names=mock_insert_names,
        insert_cards=mock_insert_cards,
        insert_store=mock_insert_store,
        insert_products=mock_insert_products,
    )
    mock_print.assert_called_once()
    mock_get_table_df.assert_called_once()
    mock_insert_names.assert_called_once()
    mock_insert_cards.assert_called_once()
    mock_insert_store.assert_called_once()
    mock_insert_products.assert_not_called()
