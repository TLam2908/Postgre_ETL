# DROP TABLES


songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
times_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

users_create_table = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )                      
""")

songs_create_table = (""" 
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar,
        year int,
        duration float
    )             
""")

artists_create_table = (""" 
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude float,
        longitude float
    )                        
""")

times_create_table = (""" 
    CREATE TABLE IF NOT EXISTS times (
        start_time bigint PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )                     
""")

songplays_create_table = (""" 
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int PRIMARY KEY,
        start_time bigint REFERENCES times(start_time) ON DELETE RESTRICT,
        user_id int REFERENCES users(user_id) ON DELETE RESTRICT,
        level varchar,
        song_id varchar REFERENCES songs(song_id) ON DELETE RESTRICT,
        artist_id varchar REFERENCES artists(artist_id) ON DELETE RESTRICT,
        session_id int,
        location varchar,
        user_agent varchar
    )
""")

# INSERT RECORDS
songs_table_insert = (""" 
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)   
    ON CONFLICT (song_id) DO NOTHING
""")

users_table_insert = (""" 
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level                      
""")

artists_table_insert = (""" 
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING
""")

times_table_insert = (""" 
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING 
""")

songplays_table_insert = (""" 
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) DO NOTHING
""")

# FIND SONGS

songs_select = (""" 
    SELECT songs.song_id, artists.artist_id FROM songs
    JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [times_create_table, users_create_table, songs_create_table, artists_create_table, songplays_create_table]
drop_table_queries = [songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, times_table_drop]
