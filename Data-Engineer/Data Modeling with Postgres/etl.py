import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads song_files from 'data/song_data' into df using pd.read_json
    - Inserts song_data into song_table using iloc
    - Inserts artist_data in artist_table using iloc
    - SQL queries for join located in sql_queries.py
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.iloc[[0],[7, 8, 0, 9, 5]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.iloc[[0],[0, 4, 2, 1, 3]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads song_files from 'data/log_data' into df using pd.read_json
    - Filters df for NextSong action
    - Converts ts (timestamp) column from milliseconds to traditional timestamp format
    - Inserts time data into time_table from a dict created via ts column
    - Inserts user data into user_table using iterrows() on user_df
    - Inserts songplay data into songplay_table by joining artist and song tables to retrieve artist_id and song_id
    SQL queries for join located in sql_queries.py
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query('page == "NextSong"')
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    data = {'timestamp' : t.values, 'hour' : t.dt.hour.values, 'day' : t.dt.day.values, 'week_of_year' : t.dt.week.values, 'month' : t.dt.month.values, 'year' : t.dt.year.values, 'weekday' : t.dt.weekday.values}
    time_df = pd.DataFrame.from_dict(data)
 
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Process song/log file data 
    - Prints total files found for processing
    - Prints run completions
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connects/closes to sparkify db
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()