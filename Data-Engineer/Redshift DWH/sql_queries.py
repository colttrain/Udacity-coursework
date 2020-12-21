import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplays;"
user_table_drop = "DROP TABLE IF EXISTS dimUsers;"
song_table_drop = "DROP TABLE IF EXISTS dimSongs;"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists;"
time_table_drop = "DROP TABLE IF EXISTS dimTime;"

# CREATE TABLES

##Table for staging all log_data
staging_events_table_create= ("""CREATE TABLE staging_events (
    artist        VARCHAR,
    auth          VARCHAR,
    firstName     VARCHAR,
    gender        CHAR(1),
    ItemInSession INT,
    lastName      VARCHAR,
    length        FLOAT,
    level         VARCHAR,
    location      VARCHAR,
    method        CHAR(20),
    page          CHAR(20),
    registration  BIGINT,
    sessionid     INT,
    song          VARCHAR,
    status        INT,
    ts            TIMESTAMP,
    userAgent     VARCHAR,
    userId        INT
    )
""")

##Table for staging all songs_data
staging_songs_table_create = ("""CREATE TABLE staging_songs (
    artist_id         VARCHAR,
    artist_latitude   FLOAT,
    artist_location   VARCHAR,
    artist_longitude  FLOAT,
    artist_name       VARCHAR,
    duration          FLOAT,
    num_songs         SMALLINT,
    song_id           VARCHAR,
    title             VARCHAR,
    year              SMALLINT
    )
""")

songplay_table_create = ("""CREATE TABLE factSongplays (
    songplay_key  INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time    TIMESTAMP NOT NULL, 
    user_key      INTEGER NOT NULL, 
    song_key      VARCHAR NOT NULL, 
    artist_key    VARCHAR NOT NULL, 
    session_id    INTEGER NOT NULL, 
    location      VARCHAR,
    user_agent    VARCHAR,
    length        FLOAT
    )
""")

user_table_create = ("""CREATE TABLE dimUsers (
    user_key      INT PRIMARY KEY,
    first_name    VARCHAR, 
    last_name     VARCHAR, 
    gender        CHAR(1),
    level         VARCHAR
    )
""")

song_table_create = ("""CREATE TABLE dimSongs (
    song_key      VARCHAR PRIMARY KEY, 
    title         VARCHAR, 
    artist_name   VARCHAR, 
    year          SMALLINT,
    duration      FLOAT
    )
    
""")

artist_table_create = ("""CREATE TABLE dimArtists (
    artist_key    VARCHAR PRIMARY KEY, 
    artist_name   VARCHAR, 
    location      VARCHAR, 
    latitude      FLOAT, 
    longitude     FLOAT
    )
""")

time_table_create = ("""CREATE TABLE dimTime (
    start_time    TIMESTAMP PRIMARY KEY, 
    hour          INTEGER NOT NULL, 
    day           INTEGER NOT NULL, 
    week          INTEGER NOT NULL, 
    month         INTEGER NOT NULL, 
    year          INTEGER NOT NULL, 
    weekday       INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH')
            )

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE off
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO factSongplays (start_time, user_key, song_key,                                                                                                       artist_key, session_id, location, user_agent, length)
                            SELECT DISTINCT e.ts, e.userId, s.song_id, e.artist, e.sessionid, 
                                            s.artist_location, e.userAgent, s.duration
                            FROM staging_events AS e 
                            JOIN staging_songs AS s ON (e.artist = s.artist_name AND
                                                        e.song = s.title AND
                                                        e.length = s.duration)                         
                            WHERE e.page = 'NextSong';
                         """)

user_table_insert = ("""INSERT INTO dimUsers (user_key, first_name, last_name, gender, level)
                        SELECT DISTINCT e.userId, e.firstName, e.lastName, e.gender, e.level
                        FROM staging_events AS e
                        WHERE e.page = 'NextSong'
                        AND e.userId NOT IN (SELECT DISTINCT user_key FROM dimUsers);
                     """)

song_table_insert = ("""INSERT INTO dimSongs (song_key, title, artist_name, year, duration)
                        SELECT DISTINCT s.song_id, s.title, s.artist_name, s.year, s.duration
                        FROM staging_songs AS s
                        WHERE s.song_id NOT IN (SELECT DISTINCT song_key FROM dimSongs);
                     """)

artist_table_insert = ("""INSERT INTO dimArtists (artist_key, artist_name, location, latitude, longitude)
                        SELECT DISTINCT s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
                        FROM staging_songs AS s
                        WHERE s.artist_id NOT IN (SELECT DISTINCT artist_key FROM dimArtists);
                       """)

time_table_insert = ("""INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT s.start_time,
                                        EXTRACT(hour FROM s.start_time),
                                        EXTRACT(day FROM s.start_time),
                                        EXTRACT(week FROM s.start_time),
                                        EXTRACT(month FROM s.start_time),
                                        EXTRACT(year FROM s.start_time),
                                        EXTRACT(dow FROM s.start_time)
                        FROM factSongplays AS s
                        AND s.start_time NOT IN (SELECT DISTINCT start_time FROM dimTime);
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
