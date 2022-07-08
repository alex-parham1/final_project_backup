from src.scripts import notebook_utils as nu


def test_get_labels_and_values():
    data = (("tables", 444), ("for", 555), ("testing", 666))

    expected = (["tables", "for", "testing"], [444, 555, 666])
    result = nu.get_labels_and_values(data)

    assert expected == result


def test_get_labels_and_values_unhappy():
    data = (("tables", 444), ("for", 555), ("testing"))

    expected = ValueError
    result = nu.get_labels_and_values(data)

    assert expected == result


def test_get_labels_and_values_unhappy_list():
    data = (["tables", 444], ("for", 555), ("testing"))

    expected = ValueError
    result = nu.get_labels_and_values(data)

    assert expected == result
