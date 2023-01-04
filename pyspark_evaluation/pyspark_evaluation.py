# Library imports
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, desc, when, current_timestamp

# Spark environment configuration
spark = (
    SparkSession
    .builder
    .config('spark.jars.packages', 'org.apache.spark:spark-hadoop-cloud_2.13:3.3.1')
    .config('spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled', True)
    .master('local')
    .appName('Confitec PySpark')
    .getOrCreate()
)

# S3 Configuration
hc = spark.sparkContext._jsc.hadoopConfiguration()
hc.set('fs.s3a.access.key', os.getenv('AWS_KEY_ID'))
hc.set('fs.s3a.secret.key', os.getenv('AWS_SECRET_KEY'))
hc.set('fs.s3a.endpoint', 's3.amazonaws.com')


# Defining steps functions
def step_1(df: DataFrame) -> DataFrame:
    df = (
        df
        .withColumn('Premiere', to_date(col('Premiere'), 'd-LLL-yy'))
        .withColumn('dt_inclusao', to_timestamp(col('dt_inclusao')))
    )
    return df


def step_2(df: DataFrame) -> DataFrame:
    df = (
        df
        .orderBy([desc(col('Ativo')), col('Gênero')])
    )
    return df


def step_3(df: DataFrame) -> DataFrame:
    df = (
        df
        .dropDuplicates()
        .withColumn('Seasons', when(col('Seasons') == 'TBA', 'a ser anunciado')
                    .otherwise(col('Seasons')))
    )
    return df


def step_4(df: DataFrame) -> DataFrame:
    df = (
        df
        .withColumn('Data de Alteração', current_timestamp())
    )
    return df


def step_5(df: DataFrame, rename_dictionary: dict) -> DataFrame:
    rename_select = [col(x).alias(rename_dictionary[x]) if x in rename_dictionary.keys() else col(x) for x in
                     df.columns]
    df = (
        df
        .select(rename_select)
    )
    return df


if __name__ == '__main__':
    # Loading the file
    df_netflix_originais = spark.read.parquet('./data/OriginaisNetflix.parquet')

    # Transforming the data
    df_netflix_originais = step_1(df_netflix_originais)

    df_netflix_originais = step_3(df_netflix_originais)

    df_netflix_originais = step_4(df_netflix_originais)

    # Dictionary for renaming the columns
    rename_dict = {
        'Title': 'Título',
        'Genre': 'Gênero',
        'GenreLabels': 'Rótulos de Gênero',
        'Premiere': 'Estréia',
        'Seasons': 'Temporadas',
        'SeasonsParsed': 'Temporadas Analisadas',
        'EpisodesParsed': 'Episódios Analisados',
        'Length': 'Duração',
        'MinLength': 'Duração Mínima',
        'MaxLength': 'Duração Máxima',
        'Active': 'Ativo',
        'Table': 'Tabela',
        'Language': 'Idioma',
        'dt_inclusao': 'Data de Inclusão'
    }
    df_netflix_originais = step_5(df_netflix_originais, rename_dict)

    # This step was delayed because the steps before would reorder the DF
    df_netflix_originais = step_2(df_netflix_originais)

    # Step 6
    df_netflix_originais.show()

    # Configuring csv
    sv = (
        df_netflix_originais
        .coalesce(1)
        .write
        .mode('overwrite')
        .options(header=True, sep=';'))

    # Step 7
    sv.csv('./data/csv')

    # Step 8
    sv.csv(f's3a://{os.getenv("S3_PATH")}')
