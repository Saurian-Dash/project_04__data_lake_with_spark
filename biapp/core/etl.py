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


logger = log.setup_custom_logger(__name__)


def get_filepaths(filepath, extension):
    """
    Walks over a directory and returns a list of filepaths matching the
    specified extension.

    Args:
        filepath (str): The directory containing the filepaths to list.

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
    saved to disk in parquet format.
    """

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

    # write songs table to parquet files partitioned by year and artist
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='dim_songs',
                              partition=('year', 'artist_name'))

    # run sql query to artists dimension
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
    saved to disk in parquet format.
    """

    # read json data into a spark dataframe
    df = spark.stage_json_data(input_data=input_data,
                               schema=schema_log_data(),
                               query=stage_log_data(),
                               table_name='stage_log_data')

    # run sql query to clean users data
    clean_df = spark.execute_sql(df=df, query=create_dim_users())

    # profiling query: check for duplicate user_id
    spark.execute_sql(df=clean_df,
                      query=profile_query(key='user_id')).show(1)

    # write users table to parquet files
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='dim_users',
                              partition=('gender', 'level'))

    # run sql query to clean time data
    clean_df = spark.execute_sql(df=df, query=create_dim_time())

    # write time table to parquet files
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='dim_time',
                              partition=('year', 'month'))

    # extract columns from joined song and log datasets to create songplays
    clean_df = spark.execute_sql(df=df,  query=create_fact_songplays())

    # profiling query: count populated song_id
    spark.execute_sql(df=clean_df,
                      query=songplay_test_query()).show(1)

    # write songplays table to parquet files partitioned by year, month, day
    spark.write_parquet_files(df=clean_df,
                              output_path=output_data,
                              table_name='fact_songplays',
                              partition=('year', 'month'))


def main():

    spark = SparkOperator()
    # input_data = "s3a://udacity-dend/"
    # output_data = ""

    log_data = get_filepaths(filepath='data/log-data', extension='json')
    song_data = get_filepaths(filepath='data/song-data', extension='json')
    output_data = os.path.join(os.getcwd(), 'sparkify-warehouse')

    process_song_data(spark=spark,
                      input_data=song_data,
                      output_data=output_data)

    process_log_data(spark=spark,
                     input_data=log_data,
                     output_data=output_data)


if __name__ == "__main__":
    main()