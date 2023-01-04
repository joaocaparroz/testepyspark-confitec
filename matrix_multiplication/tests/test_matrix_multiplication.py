import pytest

from matrix_multiplication.matrix_multiplication import matrix_multiplication, pretty_format_matrix


@pytest.fixture()
def data():
    data = [
        [1, 20, 300],
        [4, 50, 6000],
        [7, 80, 90]
    ]
    return data


def test_matrix_multiplication(data):
    expected_result = [
        [2181, 25020, 147300],
        [42204, 482580, 841200],
        [957, 11340, 490200]
    ]

    result = matrix_multiplication(data, data)
    assert (result, expected_result)


def test_pretty_format_matrix(data):
    expected_result = '1\t20\t300 \n4\t50\t6000\n7\t80\t90  '

    result = pretty_format_matrix(data)
    assert (result, expected_result)
