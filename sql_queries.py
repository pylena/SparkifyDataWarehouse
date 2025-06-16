import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
     CREATE TABLE staging_events (
          artist VARCHAR,
          auth VARCHAR,
          firstName VARCHAR,
          gender CHAR(1),
          itemInSession INT,
          lastName VARCHAR,
          length FLOAT,
          level VARCHAR,
          location VARCHAR,
          method VARCHAR,
          page VARCHAR,
          registration BIGINT,
          sessionId INT,
          song VARCHAR,
          status INT,
          ts BIGINT,
          user_agent TEXT,
          user_id INT
         );                         
            
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR ,
        title VARCHAR,
        duration FLOAT,
        year INT
    );                          
""")

# we used SERIAL type for songplay_id to auto-generate unique IDs
songplay_table_create = ("""
      CREATE TABLE songplays (
       songplay_id INT IDENTITY(0,1) PRIMARY KEY,
       start_time TIMESTAMP REFERENCES time(start_time),
       user_id INT REFERENCES users(user_id),
       level VARCHAR,
       song_id VARCHAR REFERENCES songs(song_id),
       artist_id VARCHAR REFERENCES artists(artist_id),
       session_id INT,
       location VARCHAR,
       user_agent TEXT
    );                                    
""")

user_table_create = ("""
    CREATE TABLE users (                 
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1), 
    level VARCHAR   
    );              
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR ,
        artist_id VARCHAR REFERENCES artists(artist_id),
        year INT,
        duration FLOAT
    ) ;                
""")

artist_table_create = ("""
   CREATE TABLE artists (
         artist_id VARCHAR PRIMARY KEY,
         name VARCHAR,
         location VARCHAR,
         latitude FLOAT,
         longitude FLOAT
   ) ;                   
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );              
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON '{}'
REGION 'us-west-2'
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
    e.user_id ,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.user_agent
FROM staging_events e
JOIN staging_songs s ON
    e.song = s.title AND
    e.artist = s.artist_name AND
    e.length = s.duration
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE user_id IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time,
    EXTRACT(hour FROM start_time),
    EXTRACT(day FROM start_time),
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time),
    EXTRACT(year FROM start_time),
    EXTRACT(weekday FROM start_time)
FROM (
    SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time
    FROM staging_events
    WHERE page = 'NextSong'
) t;
""")


# Example usage: explotaory data analysis:

#query1:  Are there any gender preferences for artists?
gender_artist_preference =  ("""
    SELECT 
        u.gender,
        a.artist_id,
        a.name AS artist_name,
        COUNT(*) AS songplays
    FROM songplays sp
    JOIN users u ON sp.user_id = u.user_id
    JOIN artists a ON sp.artist_id = a.artist_id
    GROUP BY u.gender, a.artist_id, a.name
    ORDER BY plays DESC;
    LIMIT 5;
""")

# query2:  What time of day has the most listening activity?
time_of_day_activity = ("""
    t.hour,
    COUNT(*) AS num_plays
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY num_plays DESC;
lIMIT 5;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
sample_analysis_queries = [gender_artist_preference, time_of_day_activity]


