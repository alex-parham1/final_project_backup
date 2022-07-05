from src.scripts import temp


def test_temp():
    expected = "hello world"
    result = temp.test_func()

    assert expected == result
