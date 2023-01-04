import random


def generate_square_matrix(n: int = 4) -> list:
    matrix = []
    row = []

    for i in range(n):
        for j in range(n):
            row.append(random.randint(1, 9))
        matrix.append(row)
        row = []
    return matrix


def matrix_multiplication(matrix_a: list, matrix_b: list) -> list:
    result_matrix = []
    for i in range(len(matrix_a)):
        result_row = []
        for j in range(len(matrix_b[0])):
            matrix_a_values = matrix_a[i]
            matrix_b_values = list(zip(*matrix_b))[j]
            result_value_list = [ai * bj for ai, bj in zip(matrix_a_values, matrix_b_values)]
            result_value = sum(result_value_list)
            result_row.append(result_value)
        result_matrix.append(result_row)

    return result_matrix


def pretty_format_matrix(matrix: list):
    s = [[str(x) for x in row] for row in matrix]
    lengths = [max(map(len, col)) for col in zip(*s)]
    fmt = '\t'.join('{{:{}}}'.format(x) for x in lengths)
    pf_matrix = [fmt.format(*row) for row in s]
    return '\n'.join(pf_matrix)


a = generate_square_matrix()
b = generate_square_matrix()
result = matrix_multiplication(a, b)

print(f'A:\n{pretty_format_matrix(a)}\n\nB:\n{pretty_format_matrix(b)}\n\nResult:\n{pretty_format_matrix(result)}\n\n')
