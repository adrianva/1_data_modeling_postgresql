import os
import glob
import json

import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process a single song file, insertint its data in
    both the songs and artists tables
    
    :param cur: database cursor
    :param filepath:path where the song file is located
    """
    # open song file
    df = _open_song_file(filepath)
    
    # insert artist record
    artist_data = {
        "artist_id": df.loc[0]["artist_id"],
        "name": df.loc[0]["artist_name"],
        "location": df.loc[0]["artist_location"],
        "latitude": df.loc[0]["artist_latitude"],
        "longitude": df.loc[0]["artist_longitude"]
    }
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = {
        "song_id": df.loc[0]["song_id"],
        "title": df.loc[0]["title"],
        "artist_id": df.loc[0]["artist_id"],
        "year": int(df.loc[0]["year"]),
        "duration": int(df.loc[0]["duration"])
    }
    cur.execute(song_table_insert, song_data)
   

def _open_song_file(filepath: str) -> pd.DataFrame:
    """
    Open song file a returns a Dataframe with the data
    
    :param filepath: path where the song file is located
    """
    # open song file
    with open(filepath, "r") as json_file:
        data = json.load(json_file)
    
    df = pd.DataFrame.from_dict([data])
    return df
    
    
def process_log_file(cur, filepath):
    """
    Process a single song file, insertint its data in
    both the songs and artists tables
    
    :param cur: database cursor
    :param filepath:path where the log file is located
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]
    
    # convert timestamp column to datetime
    df["date"] = (df["ts"]/1000).apply(datetime.datetime.fromtimestamp) 
    
    # insert time data records
    data = {
        "start_time": list(df["ts"].values),
        "hour": list(df["date"].dt.hour.values),
        "day": list(df["date"].dt.day.values),
        "year": list(df["date"].dt.year.values),
        "month": list(df["date"].dt.month.values),
        "week": list(df["date"].dt.week.values),
        "weekday": list(df["date"].dt.weekday.values),
    }
    time_df = pd.DataFrame(data)

    for i, row in time_df.iterrows():
        row = {
            "start_time": int(row[0]),
            "hour": int(row[1]),
            "day": int(row[2]),
            "year": int(row[3]),
            "month": int(row[4]),
            "week": int(row[5]),
            "weekday": int(row[6]),
        }
        cur.execute(time_table_insert, row)

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        row = {
            "user_id": int(row[0]),
            "first_name": str(row[1]),
            "last_name": str(row[2]),
            "gender": str(row[3]),
            "level": str(row[4])
        }
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        data = {"song_title": row.song, "artist_name": row.artist}
        cur.execute(song_select, data)
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = {
            "start_time": row.ts,
            "user_id": int(row.userId),
            "level": str(row.level),
            "song_id": songid,
            "artist_id": artistid,
            "session_id": int(row.sessionId),
            "location": str(row.location),
            "user_agent": str(row.userAgent)
        }
        if songid or artistid:
            print(songplay_data)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process the songs and logs files, inserting its data into the database
    
    :param cur: Database cursor
    :param conn: Database connection
    :param filepath: path where the files are located
    :param func: function object which allows to process the file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
