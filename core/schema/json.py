from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


def schema_log_data():
    """
    Docstring
    """

    return StructType(
        [
            StructField('artist', StringType(), True),
            StructField('auth', StringType(), True),
            StructField('firstName', StringType(), True),
            StructField('gender', StringType(), True),
            StructField('itemInSession', StringType(), True),
            StructField('lastName', StringType(), True),
            StructField('length', StringType(), True),
            StructField('level', StringType(), True),
            StructField('location', StringType(), True),
            StructField('method', StringType(), True),
            StructField('page', StringType(), True),
            StructField('registration', StringType(), True),
            StructField('sessionId', StringType(), True),
            StructField('song', StringType(), True),
            StructField('status', StringType(), True),
            StructField('ts', StringType(), True),
            StructField('userAgent', StringType(), True),
            StructField('userId', StringType(), True),
        ]
    )


def schema_song_data():
    """
    Docstring
    """

    return StructType(
        [
            StructField('num_songs', StringType(), True),
            StructField('artist_id', StringType(), True),
            StructField('artist_latitude', StringType(), True),
            StructField('artist_longitude', StringType(), True),
            StructField('artist_location', StringType(), True),
            StructField('artist_name', StringType(), True),
            StructField('song_id', StringType(), True),
            StructField('title', StringType(), True),
            StructField('duration', StringType(), True),
            StructField('year', StringType(), True),
        ]
    )
