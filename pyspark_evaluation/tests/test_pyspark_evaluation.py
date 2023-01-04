from datetime import date, datetime

import pytest
from chispa.dataframe_comparer import assert_df_equality

from pyspark.sql import Row, SparkSession
from pyspark_evaluation.pyspark_evaluation import step_1, step_2, step_3, step_5


@pytest.fixture(scope='session')
def spark_session():
    return (
        SparkSession
        .builder
        .master('local')
        .appName('Confitec PySpark')
        .getOrCreate()
    )


def test_test_1(spark_session):
    data = [
        {'Premiere': '1-Apr-20', 'dt_inclusao': '2021-03-16T21:20:24.000-03:00'},
        {'Premiere': '30-Aug-21', 'dt_inclusao': '2021-03-17T21:20:24.000-03:00'},
        {'Premiere': '15-Jul-22', 'dt_inclusao': '2021-03-18T21:20:24.000-03:00'}
    ]
    df = spark_session.createDataFrame(Row(**x) for x in data)

    expected_data = [
        {'Premiere': date(2020, 4, 1), 'dt_inclusao': datetime(2021, 3, 16, 21, 20, 24)},
        {'Premiere': date(2021, 8, 30), 'dt_inclusao': datetime(2021, 3, 17, 21, 20, 24)},
        {'Premiere': date(2022, 7, 15), 'dt_inclusao': datetime(2021, 3, 18, 21, 20, 24)}
    ]
    expected_result = spark_session.createDataFrame(Row(**x) for x in expected_data)
    result = step_1(df)
    assert_df_equality(result, expected_result)


def test_test_2(spark_session):
    data = [
        {'Ativo': 1, 'Gênero': 'Drama'},
        {'Ativo': 0, 'Gênero': 'Action'},
        {'Ativo': 1, 'Gênero': 'Action'}
    ]
    df = spark_session.createDataFrame(Row(**x) for x in data)

    expected_data = [
        {'Ativo': 1, 'Gênero': 'Action'},
        {'Ativo': 1, 'Gênero': 'Drama'},
        {'Ativo': 0, 'Gênero': 'Action'}
    ]
    expected_result = spark_session.createDataFrame(Row(**x) for x in expected_data)
    result = step_2(df)
    assert_df_equality(result, expected_result)


def test_test_3(spark_session):
    data = [
        {'Seasons': '2', 'Gênero': 'Drama'},
        {'Seasons': 'TBA', 'Gênero': 'Action'},
        {'Seasons': '5', 'Gênero': 'Action'},
        {'Seasons': '5', 'Gênero': 'Action'}
    ]
    df = spark_session.createDataFrame(Row(**x) for x in data)

    expected_data = [
        {'Seasons': '5', 'Gênero': 'Action'},
        {'Seasons': '2', 'Gênero': 'Drama'},
        {'Seasons': 'a ser anunciado', 'Gênero': 'Action'}
    ]
    expected_result = spark_session.createDataFrame(Row(**x) for x in expected_data)
    result = step_3(df)
    assert_df_equality(result, expected_result)


def test_test_5(spark_session):
    data = [
        {'Seasons': '2', 'Gênero': 'Drama', 'Premiere': date(2020, 4, 1)},
        {'Seasons': 'TBA', 'Gênero': 'Action', 'Premiere': date(2021, 5, 2)},
        {'Seasons': '5', 'Gênero': 'Action', 'Premiere': date(2022, 6, 3)}
    ]
    df = spark_session.createDataFrame(Row(**x) for x in data)

    rename_dict = {
        'Premiere': 'Estréia',
        'Seasons': 'Temporadas',
        'SeasonsParsed': 'Temporadas Analisadas',
        'EpisodesParsed': 'Episódios Analisados'
    }

    expected_data = [
        {'Temporadas': '2', 'Gênero': 'Drama', 'Estréia': date(2020, 4, 1)},
        {'Temporadas': 'TBA', 'Gênero': 'Action', 'Estréia': date(2021, 5, 2)},
        {'Temporadas': '5', 'Gênero': 'Action', 'Estréia': date(2022, 6, 3)}
    ]
    expected_result = spark_session.createDataFrame(Row(**x) for x in expected_data)
    result = step_5(df, rename_dict)
    assert_df_equality(result, expected_result)
