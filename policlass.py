import sqlite3 as sql 
from pytube import Playlist, YouTube
import os 
import time 
import sys 
from googleapiclient.discovery import build

global api_key
api_key = 'YOUR API KEY'



class Database:
    def __init__(self) -> None:
        pass

    def create_database(db_filename):
        # Create a connection to the database (or create a new database if it doesn't exist)
        conn = sql.connect(db_filename)

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        ## Create an "Channels" table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Channels (
                related_channel_id INTEGER PRIMARY KEY,
                channel_title TEXT,
                channel_publish_date TEXT, 
                channel_views INTEGER              
            )
        ''')

        # Create a "Playlists" table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Playlists (
                playlist_id INTEGER PRIMARY KEY,
                playlist_title TEXT,
                playlist_length TEXT,
                playlist_link TEXT,
                related_channel_id INTEGER,  -- Reference the correct column name here
                FOREIGN KEY (related_channel_id) REFERENCES Channels (related_channel_id)
            )
        ''')

        # Create a "Vids" table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Vids (
                video_id INTEGER PRIMARY KEY,
                title TEXT,
                author TEXT,
                views INTEGER,
                length INTEGER,
                likes INTEGER,
                dislikes INTEGER,
                publish_date TEXT,
                link TEXT,
                playlist_id INTEGER,  -- Reference the correct column name here
                FOREIGN KEY (playlist_id) REFERENCES Playlists (playlist_id)
            )
        ''')

        # Create the "Relations" table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Relations (
                relation_id INTEGER PRIMARY KEY,
                related_channel_id INTEGER,
                playlist_id INTEGER,
                video_id INTEGER,
                FOREIGN KEY (related_channel_id) REFERENCES Channels (related_channel_id),
                FOREIGN KEY (playlist_id) REFERENCES Playlists (playlist_id),
                FOREIGN KEY (video_id) REFERENCES Vids (video_id)
            )
        ''')




        # Commit the changes and close the database connection
        conn.commit()
        conn.close()

        print(f"Database {db_filename} and tables have been created.")

    def get_last_row_id_for_table(self, cursor, table_name):
        # This query retrieves the last row ID for a specified table
        query = f"SELECT rowid FROM {table_name} ORDER BY rowid DESC LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return None

class Save_data:
    def __init__(self) -> None:
        pass

    def Save_video_data(self, video_data, database, relational_key):
        """
        Save the detail-data of the song-vid.

        Parameters:
        - video_data (tuple): A tuple containing details of the video, such as title, duration, and URL.
        - database (str): The path or connection information for the database where the video data will be saved.
         """
        print("__Save_video_data__")

        connection = sql.connect(database)
        cursor = connection.cursor()

        # Adjust the video_data tuple to match the number of placeholders
        adjusted_video_data = (video_data[0], video_data[1], video_data[2], video_data[3], video_data[4], video_data[5], video_data[6], video_data[7])
        # Add the details of the video into the Vids table
        cursor.execute('''
            INSERT INTO Vids (title, author, views, length, likes, dislikes, publish_date, link) VALUES (?,?,?,?,?,?,?,?)''', adjusted_video_data)
        # Add the relation in the relations table
        relations_key = (relational_key[0], relational_key[1], cursor.lastrowid) 
        cursor.execute('''
            INSERT INTO Relations (related_channel_id, playlist_id, video_id)
            VALUES (?, ?, ?)''', relations_key)
        connection.commit()
        connection.close() 

    def Save_playlist(self, url, database, related_channel_id):
        """
        Save playlist details to the database.

        Parameters:
        - url (str): The URL of the playlist.
        - database (str): The path or connection information for the database.
        - related_channel_id (int): The ID of the related channel in the database.
        """
        print("__Save_playlist__")
        # Create playlist_ object
        try:
            playlist_obj = Playlist(url)
            # get the necessary data and create a tuple to store
            playlist_title = playlist_obj.title
            playlist_length = len(playlist_obj.video_urls)
            print(f"__Playlist length = {playlist_length}__")      
         
            playlist_data = (playlist_title, playlist_length, url) 

            # save the data 
            connection = sql.connect(database)
            cursor = connection.cursor()
            # Save the data of the playlist
            cursor.execute('''
                INSERT INTO Playlists (playlist_title, playlist_length, playlist_link) VALUES (?,?,?)''', playlist_data)
            # Save the relaitions between new Playlist and the coresponding Channel 
            # Retrieve the playlist_id of the last inserted row in the playlists table
            # Get the last playlist_id from the Playlists table
            playlist_id = Database().get_last_row_id_for_table(cursor, "Playlists")

            # Save the relationship between the new Playlist and the corresponding Channel
            cursor.execute("INSERT INTO Relations (related_channel_id, playlist_id) VALUES (?, ?)", (related_channel_id, playlist_id))

            connection.commit()
            connection.close()
        except Exception as e:
            print(e)

        # Now we need to iterate through all the videos of the playlist and store the data of each one of them in a relaitional way as parts of the above playlist
        count = 0
        for video_url in playlist_obj.video_urls: 
            count+=1 
            print(count) 
            video_data = self.get_video_data(video_url)
            # lastrowid = Database().get_last_row_id_for_table( cursor, "")
            relational_key = (related_channel_id, playlist_id)
            self.Save_video_data(video_data, database, relational_key)

    def Save_channel_details(self, channel_data, database):
        """
        Save channel details to the database.

        Parameters:
        - channel_data (tuple): A tuple containing information about the channel, such as title, publish date, and views.
        - database (str): The path or connection information for the database.
        - table (str): The name of the table in the database where channel details will be saved.
        """
        connection = sql.connect(database)
        cursor = connection.cursor()
        # save the data to the database
        query = '''
            INSERT INTO Channels (channel_title, channel_publish_date, channel_views)
            VALUES (?, ?, ?)
        '''
        cursor.execute(query, channel_data )

        connection.commit()
        connection.close()
        print(f'Channel "{channel_data[0]}" details have been saved!')

class Extract_data_url(Save_data):
    def __init__(self)-> None:
        super().__init__()

    def get_yt_id(self, url, ignore_playlist=False):
        import re
        from urllib.parse import urlparse, parse_qs
        from contextlib import suppress

        query = urlparse(url)
        if query.hostname == 'youtu.be': return query.path[1:]
        if query.hostname in {'www.youtube.com', 'youtube.com', 'music.youtube.com'}:
            if not ignore_playlist:
            # use case: get playlist id not current video in playlist
                with suppress(KeyError):
                    return parse_qs(query.query)['list'][0]
            if query.path == '/watch': return parse_qs(query.query)['v'][0]
            if query.path[:7] == '/watch/': return query.path.split('/')[1]
            if query.path[:7] == '/embed/': return query.path.split('/')[2]
            if query.path[:3] == '/v/': return query.path.split('/')[2]

    def get_likes_dislikes(self, url):
        """Takes in the a youtube video url and returns the counter of likes and dislikes if they available. """
        video_id = self.get_yt_id(url)
        # Your API key or OAuth credentials go here
        api_key = 'AIzaSyCiMFa2EWVvJiyxRDhvoDvzBuzdPyqTvU8'

        # Create a YouTube Data API client
        youtube = build('youtube', 'v3', developerKey=api_key)


        # Call the API to retrieve video details
        video_request = youtube.videos().list(
            id=video_id,
            part='statistics,snippet'

        )
        video_response = video_request.execute()

        snippet = video_response['items'][0]['snippet']
        video_title = snippet.get('title', 'N/A')
        try:
            # Extract likes and dislikes from the response
            likes = video_response['items'][0]['statistics']['likeCount']
        except:
            # print(f"No likes counter for video = {video_title}")
            likes = "Not available"
        try:
            dislikes = video_response['items'][0]['statistics']['dislikeCount']
        except:
            # print(f"No dislikes counter for video = {video_title}")
            dislikes = "Not available"

        return likes, dislikes
    
    def get_video_data(self, url):
        print("__get_video_data__")
        try:
            # Create a YouTube object
            video = YouTube(url)

            # Get the video details
            title = str(video.title)
            author = str(video.author)
            views = int(video.views)
            length = int(video.length)
            likes, dislikes = self.get_likes_dislikes(url)
            publish_date = str(video.publish_date)
            raiting = video.rating
            publish_date = video.publish_date
            print(f"{title}") 
            # Save the detail-data of the song-vid.
            video_data = (title, author, views, length, likes, dislikes ,publish_date, url)
            return video_data
        except Exception as e:
            print("Error") 
            print(e) 
            return None 

    def getSave_channel_details(self, yt_channel_id, database): 
        """
        Retrieve and return channel details from the database.

        Parameters:
        - yt_channel_id (str): The YouTube channel ID.
        - database (str): The path or connection information for the database.
        - table (str): The name of the table in the database where channel details are stored.

        Returns:
        - tuple: A tuple containing the information of channel title, publish date, and views.
        """
        youtube = build('youtube', 'v3', developerKey=api_key)
        # Call the API to retrieve channel details
        channel_request = youtube.channels().list(
            id=yt_channel_id,
            part='statistics,snippet'
        )
        channel_response = channel_request.execute()
        channel_title = channel_response['items'][0]['snippet']['title']
        # Extract view count from the response

        try:
            view_count = channel_response['items'][0]['statistics']['viewCount']
            publish_date = channel_response['items'][0]['snippet']['publishedAt']
        except KeyError:
            print(f'View count information not available for the channel {channel_title}.')
        channel_data = (channel_title, publish_date, view_count )
        Save_data().Save_channel_details(channel_data, database)

    def get_all_playlist_from_channel(self, yt_channel_id):# Here the yt_channel_id refears to youtube channel ids
        youtube = build('youtube', 'v3', developerKey=api_key)

        # Initial request to get the first page of playlists
        playlists_request = youtube.playlists().list(
            channelId=yt_channel_id,
            part='snippet,contentDetails',
            maxResults=50
        )
        playlists_response = playlists_request.execute()

        # Extract playlist IDs from the first page
        playlist_ids = [item['id'] for item in playlists_response.get('items', [])]

        # Continue making requests until all pages are retrieved
        while 'nextPageToken' in playlists_response:
            next_page_token = playlists_response['nextPageToken']
            playlists_request = youtube.playlists().list(
                channelId=yt_channel_id,
                part='snippet,contentDetails',
                maxResults=50,
                pageToken=next_page_token
            )
            playlists_response = playlists_request.execute()

            # Extract playlist IDs from the current page
            playlist_ids.extend(item['id'] for item in playlists_response.get('items', []))

        # Construct playlist URLs using the retrieved playlist IDs
        playlist_urls = [f'https://www.youtube.com/playlist?list={playlist_id}' for playlist_id in playlist_ids]

        return playlist_urls

    def getSave_playlists_data(self, urls, database, related_channel_id):
        """
        Get the data of each video in the playlist and save the data using the Save_playlist function.
        If the parameter urls is a list of playlist URLs, the function iterates through all of the playlists.

        Parameters:
        - urls (list of str): A list of playlist URLs.
        - database (str): The path or connection information for the database.
        - related_channel_id (int): The ID of the related channel in the database.
        """
        print("__get_playlist_data__")

        def get_single_playlist_data(playlist_url, database):
            # SAve the playlist to the database with the function Save_playlist
            self.Save_playlist(playlist_url, database, related_channel_id)
            # Create a Playlist object to work with
            playlist = Playlist(playlist_url)
            # Iterate through the videos in the playlist and get details
            for video_url in playlist.video_urls:
                try:
                    video = YouTube(video_url)           
                    # Get the video details
                    title = video.title
                    author = video.author
                    views = video.views
                    length = video.length
                    likes, dislikes = self.get_likes_dislikes(video_url) 
                    publish_date = video.publish_date
                    raiting = video.rating
                    publish_date = video.publish_date

                    video_data = (title, author, views, length,likes, dislikes ,publish_date, video_url)
                    print(video_data)
                    cursor = sql.Connection(database).cursor()
                    playlist_id = Database().get_last_row_id_for_table(cursor, "Playlists")
                    relational_key = (related_channel_id, playlist_id)
                    # SAve the video details in the database
                    self.Save_video_data(video_data, database, relational_key)

                except Exception as e:
                    print(e)
                    

        try: # If we have many playlist urls in the urls parameter we are going to call several time the grt_single_playlist_data 
            len_urls = len(urls) 
            for url in urls:
                print("loop of saving data of playlist")
                get_single_playlist_data(url, database) 
            return
        except:
            get_single_playlist_data(urls, database)

# Create nd database 
# nd_database = "nd_data.db"
# Create poli big database
# try_channels = "try_channels.db" 
# Save_data.create_database(try_channels)
# Extract_inst = Extract_data_url()
# snippet = ("UCuyKcrRBIz28qF2IqC3T4Cw", try_channels, "Channels")
# Extract_inst.getSave_channel_details( snippet[0], snippet[1], snippet[2])

# sys.exit() 


yt_channel_ids = [
    "UCC5GG5tZf0APYlBbzup9J9A",
    "UCN_lRCj-sCu9HZvSz-TxikA",
    "UCuyKcrRBIz28qF2IqC3T4Cw",
]

def get_big_data(yt_channel_ids, database):
    Extract_inst = Extract_data_url()
    connection = sql.connect(database)
    cursor = connection.cursor()
    
    for yt_channel_id in yt_channel_ids:
        time.sleep(10) 
        count=0
        try:
            Extract_inst.getSave_channel_details(yt_channel_id, database) # Append the channel to the database 
            urls_of_partyi = Extract_inst.get_all_playlist_from_channel(yt_channel_id) # Get all the playlists of the channel 
            print(f" Number of playlists in the channel = {len(urls_of_partyi)}. ")
        except Exception as e:
            print(e) 
            time.sleep(20)

        try:
            related_channel_id = Database().get_last_row_id_for_table(cursor, "Channels") # retreive the relational key of the channel in the database
            Extract_inst.getSave_playlists_data(urls_of_partyi, database, related_channel_id)   
        except Exception as e:
            print(e)  
            related_channel_id = Database().get_last_row_id_for_table(cursor, "Channels") # retreive the relational key of the channel in the database
            Extract_inst.getSave_playlists_data(urls_of_partyi, database, related_channel_id)   
    print("Fuck yes man you nailed it!!!") 
    return 

poli_full_db = "poli_full1.db"
nd_database = "nd_database.db"
Extract_inst = Extract_data_url()

if __name__ == "__main__":
    # Database.create_database(poli_full_db) 
    # sys.exit()

    get_big_data(yt_channel_ids, poli_full_db)

    # yt_channel_id = "UCuyKcrRBIz28qF2IqC3T4Cw"
    # playlists = Extract_inst.get_all_playlist_from_channel(yt_channel_id)
    # print(len(playlists) )
    # conn = sql.Connection(nd_database) 
    # cursor = conn.cursor()

    # Extract_data_url().getSave_channel_details(yt_channel_id, nd_database)
    # related_channel_id = Database().get_last_row_id_for_table(cursor, "Channels") 
    # Extract_inst.getSave_playlists_data(playlists, nd_database, related_channel_id)

# "UCG_zxydHuzlhgO4yquCOsjQ",
#     "UCZ5e8t-YR8woifcKACklYlg",
#     "UCJkuXSz3IIBir_IkHSyQ-2w",
#     "UC_wSXkcCtfqvuFcmhGWTBEg",
#     "UC5XLm_i_Bflbhc-xPzQoQdA"]

























