from queries_sql import *
import os
import glob
import psycopg2
import pandas as pd

def process_song_file(cur, filepath):
    """
    Process song file and insert data into tables.
    Each song file has 1 record
    """

    # Open song file
    song_df = pd.read_json(filepath, lines=True)
    
    # Insert song record
    song_data = list(song_df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(songs_table_insert, song_data)
    
    # Insert artist record
    artist_data = list(song_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artists_table_insert, artist_data)
    
def process_log_file(cur, filepath):
    """
    Process log file and insert data into tables.
    Each log file has multiple records.
    """
    
    # Open log file
    log_df = pd.read_json(filepath, lines=True)
    
    # Filter by NextSong action
    log_df = log_df[log_df['page'] == 'NextSong']
    t = pd.to_datetime(log_df['ts'])
    
    time_data = [(x, x.hour, x.day, x.week, x.month, x.year, x.weekday()) for x in t]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # Insert time data records
    time_df = pd.DataFrame(time_data, columns=column_labels)
    for i, row in time_df.iterrows():
        cur.execute(times_table_insert, row)
    
    # Insert user records
    user_df = log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for i, row in user_df.iterrows():
        cur.execute(users_table_insert, row)
        
    # Insert songplay records
    for i, row in log_df.iterrows():
        # Get song_id and artist_id
        cur.execute(songs_select, (row.song, row.artist, row.length))
        results = cur.fetchone()    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (i, row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'],
                     row['location'], row['userAgent'])
        cur.execute(songplays_table_insert, songplay_data)
    
def process_data(cur, conn, filepath, func):
    """
    Process data from a given filepath and execute a given function.
    """
    
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    # Get total number of files found        
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed'.format(i, num_files))
    
def main():
    """
    Connect to database and process data.
    """
    
    conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user=postgres password=admin123")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    conn.close()

if __name__ == "__main__":
    main()