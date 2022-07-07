from src.scripts import temp
from src.scripts import notebook_utils




def test_temp():
    expected = "hello world"
    result = temp.test_func()

    assert expected == result
