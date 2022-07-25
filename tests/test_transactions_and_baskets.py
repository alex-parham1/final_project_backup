import pytest
from unittest.mock import Mock, patch, call
from src.scripts import transactions_and_baskets as tb
import pandas as pd

#--------df_to_sql--------
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

#-------df_from_sql_table-------
@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.read_sql_table", side_effect=["table"])
@patch("pandas.read_sql_query")
def test_df_from_sql_query(mock_query:Mock, mock_table: Mock, mock_get: Mock):
    name = "test_table"
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    tb.df_from_sql_query(name, "Test", "Test", create_engine=engine_caller, read_sql_query=mock_query)
    mock_query.assert_called()

#-------df_from_sql_table-------
@patch("os.environ.get", side_effect=["test", "pass", "localhost", "3307", "test"])
@patch("pandas.read_sql_table", side_effect=["table"])
def test_df_from_sql_table(mock_table: Mock, mock_get: Mock):
    name = "test_table"
    dispose = Mock(side_effect="engine_closed")
    mock_engine = Mock()
    mock_engine.attach_mock(dispose, "dispose")
    engine_caller = Mock(side_effect=mock_engine)
    expected = "table"
    result = tb.df_from_sql_table(name, create_engine=engine_caller, read_sql_table=mock_table)
    assert result == expected