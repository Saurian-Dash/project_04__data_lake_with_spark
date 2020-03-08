import glob
import os

import biapp.core.logger.log as log
from biapp.core.operators.spark import SparkOperator
from biapp.core.queries.sql import (
    create_dim_artists,
    create_dim_songs,
    create_dim_time,
    create_dim_users,
    create_fact_songplays,
    profile_query,
    songplay_test_query,
    stage_log_data,
    stage_song_data,
)
from biapp.core.schema.json import schema_log_data, schema_song_data
from biapp.settings.config import (
    S3_INPUT_DATA,
    S3_OUTPUT_DATA
)


logger = log.setup_custom_logger(__name__)


def get_filepaths(filepath, extension):
    """
    Walks over the specified directory and returns a list of filepaths
    matching the specified extension.

    Args:
        filepath (str):
            The directory containing the filepaths to list.

        extension (str):
            File extension to filter the results by.

    Returns:
        list
    """
    filepaths = []

    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, f'*.{extension}'))
        for f in files:
            filepaths.append(os.path.abspath(f))

    return filepaths


def process_song_data(spark, input_data, output_data):
    """
    ETL operation for the Sparkify song data. JSON data is loaded from S3,
    staged in Spark DataFrames, transformed into a dimensional model and
    saved back to S3 as parquet files.

    Args:
        spark (pyspark.SparkSession):
            A PySpark session configured to run on AWS EMR.

        input_data (str):
            The URI of the S3 bucket containing the input files.

        output_data (str):
            The URI of the S3 bucket to write output files.
    """

    # get filepath for song data files
    input_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    logger.info(f'Input data path: {input_data}')

    # read json data into a spark dataframe
    df = spark.stage_json_data(input_data=input_data,
                               schema=schema_song_data(),
                               query=stage_song_data(),
                               table_name='stage_song_data')

    # run sql query to create songs dimension
    clean_df = spark.execute_sql(df=df, query=create_dim_songs())

    # profiling query: check for duplicate song_id
    spark.execute_sql(df=clean_df,
                      query=profile_query(key='song_id')).show(1)

    # write songs table to parquet files
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='dim_songs',
                              partition=('year', 'artist_name'))

    # run sql query to create artists dimension
    clean_df = spark.execute_sql(df=df, query=create_dim_artists())

    # profiling query: check for duplicate artist_id
    spark.execute_sql(df=clean_df,
                      query=profile_query(key='artist_id')).show(1)

    # write artists table to parquet files
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='dim_artists')


def process_log_data(spark, input_data, output_data):
    """
    ETL operation for the Sparkify log data. JSON data is loaded from S3,
    staged in Spark DataFrames, transformed into a dimensional model and
    saved back to S3 as parquet files.

    Args:
        spark (pyspark.SparkSession):
            A PySpark session configured to run on AWS EMR.

        input_data (str):
            The URI of the S3 bucket containing the input files.

        output_data (str):
            The URI of the S3 bucket to write output files.
    """

    # get filepath for log data files
    input_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read json data into a spark dataframe
    df = spark.stage_json_data(input_data=input_data,
                               schema=schema_log_data(),
                               query=stage_log_data(),
                               table_name='stage_log_data')

    # run sql query to create users dimension
    clean_df = spark.execute_sql(df=df, query=create_dim_users())

    # profiling query: check for duplicate user_id
    spark.execute_sql(df=clean_df,
                      query=profile_query(key='user_id')).show(1)

    # write users table to parquet files
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='dim_users')

    # run sql query to create time dimension
    clean_df = spark.execute_sql(df=df, query=create_dim_time())

    # write time table to parquet files
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='dim_time',
                              partition=('year', 'month'))

    # run sql query to create songplays fact table
    clean_df = spark.execute_sql(df=df,  query=create_fact_songplays())

    # profiling query: show populated song_id (expecting one match)
    spark.execute_sql(df=clean_df,
                      query=songplay_test_query()).show(1)

    # write fact table to parquet files
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='fact_songplays',
                              partition=('year', 'month'))


def main():

    spark = SparkOperator()

    process_song_data(spark=spark,
                      input_data=S3_INPUT_DATA,
                      output_data=S3_OUTPUT_DATA)

    process_log_data(spark=spark,
                     input_data=S3_INPUT_DATA,
                     output_data=S3_OUTPUT_DATA)


if __name__ == "__main__":
    main()
