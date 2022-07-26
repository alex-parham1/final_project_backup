from src.scripts.Lambdas.Retrival_lambda import cleaning as cl
import pandas as pd

def test_clean_date_time():
    input = "06/07/2022 09:00"

    expected = "2022/07/06 09:00"

    result = cl.clean_date_time(input)
    
    assert expected == result

def test_clean_date_time_apply():
    input = {"date":["06/07/2022 09:00","06/07/2022 09:00"]}
    df = pd.DataFrame(input)
    expected = {"date":["2022/07/06 09:00","2022/07/06 09:00"]}
    e_df = pd.DataFrame(expected)
    df["date"] = df["date"].apply(cl.clean_date_time)
    
    assert df.equals(e_df)