

def stage_log_data():

    return (
        """
        SELECT CAST(artist AS STRING) AS artist,
               CAST(auth AS STRING) AS auth,
               CAST(firstName AS STRING) AS first_name,
               CAST(gender AS STRING) AS gender,
               CAST(itemInSession AS INT) AS item_in_session,
               CAST(lastName AS STRING) AS last_name,
               CAST(length AS DOUBLE) AS length,
               CAST(level AS STRING) AS level,
               CAST(location AS STRING) AS location,
               CAST(method AS STRING) AS method,
               CAST(page AS STRING) AS page,
               FROM_UNIXTIME(
                   CAST(registration AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss'
               ) AS registration,
               CAST(sessionId AS BIGINT) AS session_id,
               CAST(song AS STRING) AS song,
               CAST(status AS STRING) AS status,
               FROM_UNIXTIME(
                   CAST(ts AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss'
               ) AS ts,
               CAST(userAgent AS STRING) AS user_agent,
               CAST(userId AS BIGINT) AS user_id
        FROM stage
        """
    )


def stage_song_data():

    return (
        """
        SELECT CAST(num_songs AS INT) AS num_songs,
               CAST(artist_id AS STRING) AS artist_id,
               CAST(artist_latitude AS DOUBLE) AS artist_latitude,
               CAST(artist_longitude AS DOUBLE) AS artist_longitude,
               CAST(artist_location AS STRING) AS artist_location,
               CAST(artist_name AS STRING) AS artist_name,
               CAST(song_id AS STRING) AS song_id,
               CAST(title AS STRING) AS title,
               CAST(duration AS DOUBLE) AS duration,
               CAST(year AS SMALLINT) AS year
        FROM stage
        """
    )


def create_dim_artists():

    return (
        """
        SELECT DISTINCT artist_id,
                        artist_name,
                        artist_location AS location,
                        artist_latitude AS latitude,
                        artist_longitude AS longitude
        FROM stage
        WHERE artist_id IS NOT NULL
        ORDER BY 1
        """
    )


def create_dim_songs():

    return (
        """
        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        CASE
                            WHEN year = 0 THEN NULL
                            ELSE year
                        END AS year,
                        duration
        FROM stage
        WHERE song_id IS NOT NULL
        ORDER BY 1
        """
    )


def create_dim_time():

    return (
        """
        SELECT DISTINCT ts AS start_time,
                        HOUR(ts) AS hour,
                        DAYOFMONTH(ts) AS day,
                        DAYOFWEEK(ts) AS weekday,
                        WEEKOFYEAR(ts) AS week,
                        MONTH(ts) AS month,
                        YEAR(ts) AS year
        FROM stage
        WHERE ts IS NOT NULL
            AND page = 'NextSong'
        ORDER BY 1
        """
    )


def create_dim_users():

    return (
        """
        SELECT DISTINCT user_id,
                        first_name,
                        last_name,
                        gender,
                        level
        FROM stage t1
        WHERE t1.user_id IS NOT NULL
            AND t1.ts =
            (
                SELECT MAX(ts)
                FROM stage t2
                WHERE t1.user_id = t2.user_id
            )
        ORDER BY 1
        """
    )


def create_fact_songplays():

    return (
        """
        SELECT t1.ts AS start_time,
            t1.user_id AS user_id,
            t1.level AS level,
            t2.song_id AS song_id,
            t2.artist_id AS artist_id,
            t1.session_id AS session_id,
            t1.location AS location,
            t1.user_agent AS user_agent
        FROM stage t1
        LEFT JOIN stage_song_data t2 ON
            UCASE(t2.artist_name) = UCASE(t1.artist)
            AND UCASE(t2.title) = UCASE(t1.song)
        WHERE t1.page = 'NextSong'
            AND t1.ts IS NOT NULL
        ORDER BY 1
        """
    )


def profile_query(key):

    return (
        f"""
        SELECT
            {key},
            COUNT(*)
        FROM stage
        GROUP BY 1
        HAVING COUNT(*) > 1
        """
    )


def songplay_test_query():

    return (
        """
        SELECT *
        FROM stage
        WHERE song_id IS NOT NULL
        """
    )
