# Confitec Python Test

## .env file

Before using this project, replace the environment variables in the `.env` file.

- `AWS_KEY_ID` - Your AWS access key ID
- `AWS_SECRET_KEY` - Your AWS access key secret
- `SPARK_HOME`- Path of your spark installation
- `PYSPARK_PYTHON`- What type of Python you are using (probably you don't need to change this)
- `JAVA_HOME`- Path of your java installation (use Java 8 only!)
- `HADOOP_HOME` - Path of your Hadoop installation
- `S3_PATH` - Bucket/folder where the csv should be copied

## Installation

This project uses pipenv for environment management. If you don't have it installed, use the
command `pip install pipenv`.
After that, navigate to the project folder and create the environment using `pipenv install`. If you intend to run unit
tests, add the flag `-d`.
