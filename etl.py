import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime
import time

#filepath = 'data/song_data/A/A/B/TRAABLR128F423B7E3.json'
#song_files = get_files(filepath)

def process_song_file(cur, filepath):
    # open song file
    filepath = 'data/song_data/A/A/B/TRAABLR128F423B7E3.json'
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

#filepath = 'data/log_data/2018/11/2018-11-01-events.json'
#log_files = get_files(filepath)

def process_log_file(cur, filepath):
    # open log file
    filepath = 'data/log_data/2018/11/2018-11-01-events.json'
    #log_files = get_files(filepath)
    df1 =  pd.read_json(filepath, lines=True) 

    # filter by NextSong action
    df2 = df1[df1['page']=='NextSong'] 

    # convert timestamp column to datetime
    G = pd.to_datetime(df2['ts'], unit='ms')
    t = G.dt.strftime('%d-%m-%Y %H:%W:%w')
    t = pd.DataFrame(t)
    t['ts'] = pd.to_datetime(t['ts'])
    t['Hour'] = t['ts'].dt.strftime('%H')
    t['Date'] = t['ts'].dt.strftime('%d')
    t['Week'] = t['ts'].dt.strftime('%W')
    t['Month'] = t['ts'].dt.strftime('%m')
    t['Year'] = t['ts'].dt.strftime('%Y')
    t['Weekday'] = t['ts'].dt.strftime('%w')
    t = pd.DataFrame(t)
    t= t.drop(['ts'], axis =1) 
    
    # insert time data records
    df3 = df2[['ts']]
    time_df = pd.concat([df3, t], axis=1)
    column_labels = ["start_time","hour","date","week","month","year","weekday"] 
    time_df.columns = column_labels


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df =  df1[['userId','firstName','lastName','gender','level']]
    col_names = ['user_id','first_name','last_name','gender','level']
    user_df.columns = col_names 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df1.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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