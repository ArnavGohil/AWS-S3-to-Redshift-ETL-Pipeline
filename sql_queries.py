import configparser


# Importing CONFIG Data
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLE Queries
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES Queries
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INT,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId INT,
song VARCHAR,
status INT,
ts VARCHAR,
userAgent VARCHAR,
userId VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id VARCHAR,
num_songs INT,
title VARCHAR,
artist_name VARCHAR,
artist_latitude FLOAT,
year INT,
duration FLOAT,
artist_id VARCHAR,
artist_longitude FLOAT,
artist_location VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id VARCHAR PRIMARY KEY, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR, 
level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
lattitude FLOAT,
longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP PRIMARY KEY,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INT IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP, 
user_id VARCHAR, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id VARCHAR, 
location VARCHAR, 
user_agent VARCHAR,
FOREIGN KEY (user_id) REFERENCES users(user_id),
FOREIGN KEY (song_id) REFERENCES songs(song_id),
FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
FOREIGN KEY (start_time) REFERENCES time(start_time)
);
""")

# STAGING TABLES
# Coping data from JSON Files stored in S3 bucket to staging tables
staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {}
REGION 'us-west-2' 
COMPUPDATE OFF
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' truncatecolumns
REGION 'us-west-2' 
COMPUPDATE OFF
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
# Coping data from Staging tables to Main tables
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location,user_agent)
SELECT
    (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time,
    se.userId AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionId AS session_id,
    se.location AS location,
    se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title
AND se.artist = ss.artist_name
AND se.length = ss.duration
WHERE se.userId IS NOT NULL
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT 
    DISTINCT userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
AND page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL ;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT
    DISTINCT artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS lattitude,
    ss.artist_longitude AS longitude
FROM staging_songs ss
WHERE song_id IS NOT NULL ;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEK FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    EXTRACT(WEEKDAY FROM start_time) AS weekday
FROM(   SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time 
        FROM staging_events
        WHERE ts IS NOT NULL
    )
;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
