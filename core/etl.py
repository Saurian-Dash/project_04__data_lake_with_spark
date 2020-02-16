import os

from core.operators.spark import SparkOperator
from core.queries.sql import (
     create_dim_artists,
     create_dim_songs,
     create_fact_songplays,
     create_dim_time,
     create_dim_users,
     profile_query,
     songplay_test_query,
     stage_log_data,
     stage_song_data,
)
from core.schema.json import schema_log_data, schema_song_data


def process_song_data(sc, input_data, output_data):

    # read json data
    df = sc.session.read.json(input_data, schema_song_data())
    df.show(1)

    # clean whitespace from columns
    df = sc.clean_dataframe(df)

    # run sql query to stage songs data
    df = sc.execute_sql(df=df, query=stage_song_data())

    # add a temporary view to the session catalogue
    df.createOrReplaceTempView('stage_song_data')

    # run sql query to create songs dimension
    clean_df = sc.execute_sql(df=df, query=create_dim_songs())
    clean_df.show(1)

    # profiling query: check for duplicate song_id
    sc.execute_sql(df=clean_df,
                   query=profile_query(key='song_id')).show()

    # write songs table to parquet files partitioned by year and title
    sc.write_parquet_file(df=clean_df,
                          output_path=output_data,
                          table_name='dim_songs',
                          partition=('year', 'title'))

    # run sql query to artists dimension
    clean_df = sc.execute_sql(df=df, query=create_dim_artists())
    clean_df.show(1)

    # profiling query: check for duplicate artist_id
    sc.execute_sql(df=clean_df,
                   query=profile_query(key='artist_id')).show()

    # write artists table to parquet files
    sc.write_parquet_file(df=clean_df,
                          output_path=output_data,
                          table_name='dim_artists',
                          partition=('artist_id'))

    # test to see if song_data still exists
    catalog = sc.session.catalog
    print('LISTING TABLES')
    print(catalog.listTables())


def process_log_data(sc, input_data, output_data):
    # test to see if song_data still exists
    catalog = sc.session.catalog
    print('LISTING TABLES')
    print(catalog.listTables())

    # read json data
    df = sc.session.read.json(input_data, schema_log_data())
    df.show(1)

    # clean whitespace from columns
    df = sc.clean_dataframe(df)

    # run sql query to stage log data
    df = sc.execute_sql(df=df, query=stage_log_data())

    # add a temporary view to the session catalogue
    df.createOrReplaceTempView('stage_log_data')

    # run sql query to clean users data
    clean_df = sc.execute_sql(df=df, query=create_dim_users())
    clean_df.show(1)

    # profiling query: check for duplicate user_id
    sc.execute_sql(df=clean_df,
                   query=profile_query(key='user_id')).show()

    # write users table to parquet files
    sc.write_parquet_file(df=clean_df,
                          output_path=output_data,
                          table_name='dim_users',
                          partition=('user_id'))

    # run sql query to clean time data
    clean_df = sc.execute_sql(df=df, query=create_dim_time())
    clean_df.show(1)

    # write time table to parquet files
    sc.write_parquet_file(df=clean_df,
                          output_path=output_data,
                          table_name='dim_time',
                          partition=('start_time'))

    # extract columns from joined song and log datasets to create songplays
    clean_df = sc.execute_sql(df=df, query=create_fact_songplays())
    clean_df.show(1)

    # profiling query: count populated song_id
    sc.execute_sql(df=clean_df,
                   query=songplay_test_query()).show()

    # write songplays table to parquet files partitioned by year and month
    sc.write_parquet_file(df=clean_df,
                          output_path=output_data,
                          table_name='fact_songplays',
                          partition=('start_time'))


def main():
    sc = SparkOperator()
    # input_data = "s3a://udacity-dend/"
    # output_data = ""

    log_data = 'data/log-data/*.json'
    song_data = 'data/song-data/*/*/*/*.json'
    output_data = os.path.join(os.getcwd(), 'sparkify-warehouse')

    process_song_data(sc=sc,
                      input_data=song_data,
                      output_data=output_data)

    process_log_data(sc=sc,
                     input_data=log_data,
                     output_data=output_data)


if __name__ == "__main__":
    main()
