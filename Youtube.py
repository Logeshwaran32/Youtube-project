# Getting channel details from youtube using API

from googleapiclient.discovery import build

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyABMPZEru_2QH6iAaUlXOVQt_TH1BYO72U"

def channel_info(channel_id):
    youtube = build(api_service_name, api_version, developerKey=api_key)

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    
    for i in response ['items']:
        mongo=dict(Channel_Name=i ['snippet']['title'],
                   Channel_Id=i['id'],
                   Subscription_Count=i['statistics']['subscriberCount'],
                   Channel_Views=i['statistics']['viewCount'],
                   Video_count=i['statistics']['videoCount'],
                   Channel_Description=i['snippet']['description'],
                   Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return mongo

# Getting Videos_id from youtube using API
# Playlist id will not get directly using content details option and next page token-Clarified in doubt session
# This playlist id will use to get Video details

youtube = build(api_service_name, api_version, developerKey=api_key)
def get_video_ids(channel_id):
    videos_ids=[]
    videos=youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    playlist_id = videos['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None  # Initialize the next_page_token variable

    while True:
        playlist_items = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=25,
            pageToken=next_page_token
        ).execute()

        for item in playlist_items['items']:
            videos_ids.append(item['snippet']['resourceId']['videoId'])

        next_page_token = playlist_items.get('nextPageToken')

        if not next_page_token:
            break
    return videos_ids


# Get video information from youtube using API

youtube = build(api_service_name, api_version, developerKey=api_key)
def get_video_info(videos_ids):
    video_data=[]
    for video_info in videos_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_info
        )
        response = request.execute()

        for item in response['items']:
            mongo1=dict(Channel_Name=item['snippet']['channelTitle'],Channel_Id=item['snippet']['channelId'],
                     Video_Id=item['id'],Video_Name=item['snippet']['title'],Video_Description=item['snippet'].get('description'),
                     Tags=item['snippet'].get('tags'),PublishedAt=item['snippet']['publishedAt'],View_Count=item['statistics']['viewCount'],
                     Like_Count=item['statistics'].get('likeCount'),Dislike_Count=item['statistics'].get('dislikeCount'),Favorite_Count=item['statistics']['favoriteCount'],
                     Comment_Count=item['statistics'].get('commentCount'),Duration=item['contentDetails']['duration'],Thumbnail=item['snippet']['thumbnails']['default']['url'],
                     Caption_Status=item['contentDetails']['caption'])

            video_data.append(mongo1)
            
    return video_data

# Getting comments details from youtube API

youtube = build(api_service_name, api_version, developerKey=api_key)
def get_commment_info(videos_ids):
    comments_detail=[]
    try:
        for comments_id in videos_ids:
            #video_id = 'RV2r7pds06A'
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=comments_id,
                maxResults=50
            )

            response = request.execute()

            for item in response['items']:
                mongo3=dict(Comment_Id=item['id'],Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_Text=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                          Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                comments_detail.append(mongo3)
    except:
        pass
    
    return comments_detail

# Getting playlist details from youtube using API

youtube = build(api_service_name, api_version, developerKey=api_key)

def get_playlist_details(channel_id):

    full_details=[]

    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25,
        #mine=True
    )

    response = request.execute()

    for item in response['items']:
        mongo4=dict(playlist_id = item['id'],
        channel_id = item['snippet']['channelId'],
        playlist_name = item['snippet']['title'],
        channel_name = item['snippet']['channelTitle'],
        playlist_published_at = item['snippet']['publishedAt'],
        video_count = item['contentDetails']['itemCount'])

        full_details.append(mongo4)
        
    return full_details 
    
#-----------------------------------streamlite DataFrame Code---------------------------------------------#

#view table for channel in streamlite 

import streamlit as st
from pymongo import MongoClient
import pandas as pd

# Define a function to fetch data and create DataFrame
def channel_view():
    # MongoDB configuration
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['youtube_data']
    mongo_collection = mongo_db['channel_data']
    ch_list = []
    
    # Fetch data from MongoDB
    for ch_data in mongo_collection.find({}, {"_id": 0, "channel_info": 1}):
        ch_list.append(ch_data["channel_info"])

    # Create DataFrame from MongoDB data
    df = pd.DataFrame(ch_list)
    return df


import streamlit as st
from pymongo import MongoClient
import pandas as pd

# Define a function to fetch data and create DataFrame
def playlist_view():
    # MongoDB configuration
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['youtube_data']
    mongo_collection = mongo_db['channel_data']
    pl_list = []

    # Fetch data from MongoDB
    for pl_data in mongo_collection.find({}, {"_id": 0, "playlist_info": 1}):
        for i in range(len(pl_data["playlist_info"])):
            pl_list.append(pl_data["playlist_info"][i])

    # Create DataFrame from MongoDB data
    df1 = pd.DataFrame(pl_list)
    return df1


#view table for video in streamlite

import streamlit as st
from pymongo import MongoClient
import pandas as pd

# Define a function to fetch data and create DataFrame
def video_view():
    # MongoDB configuration
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['youtube_data']
    mongo_collection = mongo_db['channel_data']
    vd_list = []

    # Fetch data from MongoDB
    for vd_data in mongo_collection.find({}, {"_id": 0, "video_info": 1}):
        for i in range(len(vd_data["video_info"])):
            vd_list.append(vd_data["video_info"][i])

    # Create DataFrame from MongoDB data
    df2 = pd.DataFrame(vd_list)
    return df2

#view table for comments in streamlite

import streamlit as st
from pymongo import MongoClient
import pandas as pd

# Define a function to fetch data and create DataFrame
def comment_view():
    # MongoDB configuration
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['youtube_data']
    mongo_collection = mongo_db['channel_data']
    cmt_list = []

    # Fetch data from MongoDB
    for cmt_data in mongo_collection.find({}, {"_id": 0, "comment_info": 1}):
        for i in range(len(cmt_data["comment_info"])):
            cmt_list.append(cmt_data["comment_info"][i])

    # Create DataFrame from MongoDB data
    df3 = pd.DataFrame(cmt_list)
    return df3



     #-----------------------------------------Streamlite code Start------------------------------------#
import streamlit as st
from pymongo import MongoClient
import mysql.connector
import pandas as pd
from datetime import datetime

# Function to fetch channel information from MongoDB
def get_channel_info(channel_id):
    try:
        # MongoDB configuration
        mongo_client = MongoClient('mongodb://localhost:27017/')
        mongo_db = mongo_client['youtube_data']
        mongo_collection = mongo_db['channel_data']

        # Query MongoDB for channel information based on the provided channel ID
        result = mongo_collection.find_one({"channel_info.Channel_Id": channel_id}, {"_id": 0, "channel_info": 1})
        
        # Check if the result is found
        if result:
            return result['channel_info']
        else:
            return f"No channel found with ID: {channel_id}"
    
    except Exception as e:
        return f"Error fetching channel information: {e}"
    
    except Exception as e:
        return f"Error fetching channel information: {e}"

# Function to insert new channel ID into MongoDB and MySQL
def insert_channel_id(channel_id):
    try:
        # MongoDB configuration
        mongo_client = MongoClient('mongodb://localhost:27017/')
        mongo_db = mongo_client['youtube_data']
        mongo_collection = mongo_db['channel_data']

        # Check if channel_id already exists in MongoDB
        existing_channel = mongo_collection.find_one({"channel_info.Channel_Id": channel_id})

        if existing_channel:
            return "Channel ID already exists in MongoDB. Skipped insertion."

    except Exception as e:
        return f"Error occurred: {e}"
    try:
        channel=channel_info(channel_id)
        playlist=get_playlist_details(channel_id)
        v_id=get_video_ids(channel_id)
        v_info=get_video_info(v_id)
        comment=get_commment_info(v_id)

        # Insert the list of dictionaries into MongoDB
        mongo_collection.insert_one({"channel_info":channel,"playlist_info":playlist,"video_info":v_info,"comment_info":comment})

        return "mongodb upload completed"  # Happy path

    except Exception as e:
        return f"Error occurred: {e}"  #Unhappy path

        # Insert into MySQL (You will need to add the MySQL insertion logic here)
def insert_channel_into_mysql():
        # MongoDB configuration
        mongo_client = MongoClient('mongodb://localhost:27017/')
        mongo_db = mongo_client['youtube_data']
        mongo_collection = mongo_db['channel_data']
        ch_list = []

        try:
            # MySQL connection
            mysql_conn = mysql.connector.connect(
                host='127.0.0.1',
                user='root',
                password='PXXXXXXXXXX'
            )
            mysql_cursor = mysql_conn.cursor()

            # Create MySQL database if not exists
            mysql_cursor.execute("CREATE DATABASE IF NOT EXISTS prj_youtube_data")
            mysql_conn.commit()  # Commit the database creation

            # Use the database
            mysql_cursor.execute("USE prj_youtube_data")

            # Create MySQL table if not exists
            mysql_cursor.execute("""
                CREATE TABLE IF NOT EXISTS channel (
                    Channel_Name VARCHAR(255),
                    Channel_Id VARCHAR(255) PRIMARY KEY,
                    Subscription_Count BIGINT,
                    Channel_Views BIGINT,
                    Video_count BIGINT,
                    Channel_Description TEXT,
                    Playlist_Id VARCHAR(255)
                )
            """)
            mysql_conn.commit()  # Commit the table creation
            print("Table created or exists already")

             # Fetch data from MongoDB
            for ch_data in mongo_collection.find({}, {"_id": 0, "channel_info": 1}):
                if 'channel_info' in ch_data:
                    ch_list.append(ch_data["channel_info"])
            else:
                print("Skipping document without 'channel_info' key")


            # Create DataFrame from MongoDB data
            df = pd.DataFrame(ch_list)

            # Insert data into MySQL table from DataFrame
            for index, row in df.iterrows():
                insert_query = '''INSERT INTO prj_youtube_data.channel
                                (Channel_Name,
                                Channel_Id,
                                Subscription_Count,
                                Channel_Views,
                                Video_count,
                                Channel_Description,
                                Playlist_Id)

                                VALUES (%s, %s, %s, %s, %s, %s, %s)'''
                values = (row['Channel_Name'], row['Channel_Id'], row['Subscription_Count'], row['Channel_Views'], row['Video_count'], row['Channel_Description'],
                        row['Playlist_Id'])

                try:
                    mysql_cursor.execute(insert_query, values)
                    mysql_conn.commit()  # Commit the transaction
                    print("Data insert done")

                except mysql.connector.Error as e:
                    if e.errno == 1062:  # MySQL error number for duplicate entry
                        print(f"Duplicate entry found for Channel_Id: {row['Channel_Id']}. Skipping insertion.")
                    else:
                        print(f"An error occurred: {str(e)}")

        
        except mysql.connector.Error as error:
            print(f"Error connecting to MySQL: {error}")

    #Creating playlist table and data insert in My SQL

        try:
            # MongoDB configuration
            mongo_client = MongoClient('mongodb://localhost:27017/')
            mongo_db = mongo_client['youtube_data']
            mongo_collection = mongo_db['channel_data']
            pl_list = []

            # Fetch data from MongoDB
            for pl_data in mongo_collection.find({}, {"_id": 0, "playlist_info": 1}):
                #for i in range(len(pl_data["playlist_info"])):
                 #   pl_list.append(pl_data["playlist_info"][i])
                if 'playlist_info' in pl_data:
                    pl_list.extend(pl_data["playlist_info"])
            else:
                print("Skipping document without 'playlist_info' key")
            # Create DataFrame from MongoDB data
            df1 = pd.DataFrame(pl_list)

            if df1.empty:
                print("No data retrieved from MongoDB.")
                return

            # MySQL connection
            mysql_conn = mysql.connector.connect(
                host='127.0.0.1',
                user='root',
                password='PXXXXXXXXXX',
                database='prj_youtube_data'
            )
            mysql_cursor = mysql_conn.cursor()

            # Create MySQL table if not exists
            mysql_cursor.execute("""
                CREATE TABLE IF NOT EXISTS playlist (
                    Channel_Name VARCHAR(255),
                    Channel_Id VARCHAR(255),
                    Playlist_Id VARCHAR(255) PRIMARY KEY,
                    Playlist_Name VARCHAR(255)
                )
            """)

            for index, row in df1.iterrows():
                insert_query = '''INSERT INTO playlist
                                (playlist_id, channel_id, Playlist_Name, Channel_Name)
                                VALUES (%s, %s, %s, %s)'''

                values = (row['playlist_id'], row['channel_id'], row['playlist_name'], row['channel_name'])

                try:
                    mysql_cursor.execute(insert_query, values)
                    mysql_conn.commit()
                    print("Data inserted successfully")

                except mysql.connector.Error as e:
                    if e.errno == 1062:  # MySQL error number for duplicate entry
                        print(f"Duplicate entry found for playlist_id: {row['playlist_id']}. Skipping insertion.")
                    else:
                        print(f"An error occurred while inserting into MySQL: {str(e)}")

        except mysql.connector.Error as error:
            print(f"Error connecting to MySQL: {error}")

    #Creating Video table and Data insert in My SQL.

        try:
            # MongoDB configuration
            mongo_client = MongoClient('mongodb://localhost:27017/')
            mongo_db = mongo_client['youtube_data']
            mongo_collection = mongo_db['channel_data']
            vd_list = []

            # Fetch data from MongoDB
            for vd_data in mongo_collection.find({}, {"_id": 0, "video_info": 1}):
                #for i in range(len(vd_data["video_info"])):
                 #   vd_list.append(vd_data["video_info"][i])
                if 'video_info' in vd_data:
                    vd_list.extend(vd_data["video_info"])
            else:
                print("Skipping document without 'video_info' key")

            # Create DataFrame from MongoDB data
            df2 = pd.DataFrame(vd_list)

            if df2.empty:
                print("No data retrieved from MongoDB.")
                return

            # MySQL connection
            mysql_conn = mysql.connector.connect(
                host='127.0.0.1',
                user='root',
                password='PXXXXXXXXXX',
                database='prj_youtube_data'
            )
            mysql_cursor = mysql_conn.cursor()

            # Create MySQL table if not exists
            mysql_cursor.execute("""
                CREATE TABLE IF NOT EXISTS video (
                    Channel_Name varchar(255),
                    Channel_Id varchar(255),
                    Video_Id varchar(255) primary key,
                    Video_Name varchar(255),
                    Video_Description text,
                    PublishedAt datetime,
                    View_Count bigint,
                    Like_Count int,
                    Dislike_Count int,
                    Favorite_Count int,
                    Comment_Count int,
                    Duration varchar(255),
                    Thumbnail varchar(255),
                    Caption_Status varchar(255)
                )
            """)

            for index, row in df2.iterrows():
                # Convert PublishedAt string to the required MySQL datetime format
                datetime_obj = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
                formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

                insert_query = '''INSERT INTO video
                                (Channel_Name,
                                Channel_Id,
                                Video_Id,
                                Video_Name,
                                Video_Description,
                                PublishedAt,
                                View_Count,
                                Like_Count,
                                Dislike_Count,
                                Favorite_Count,
                                Comment_Count,
                                Duration,
                                Thumbnail,
                                Caption_Status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

                values = (row['Channel_Name'], row['Channel_Id'], row['Video_Id'], row['Video_Name'],
                        row['Video_Description'], formatted_datetime, row['View_Count'], row['Like_Count'],
                        row['Dislike_Count'], row['Favorite_Count'], row['Comment_Count'], row['Duration'],
                        row['Thumbnail'], row['Caption_Status'])

                try:
                    mysql_cursor.execute(insert_query, values)
                    mysql_conn.commit()
                    print("Data inserted successfully")

                except mysql.connector.Error as e:
                    if e.errno == 1062:    # MySQL error number for duplicate entry
                        print(f"Duplicate entry found for Video_Id: {row['Video_Id']}. Skipping insertion.")
                    else:
                        print(f"An error occurred while inserting into MySQL: {str(e)}")

        except mysql.connector.Error as error:
            print(f"Error connecting to MySQL: {error}")


    #creating comments table and data entry in My SQL.
        try:
            # MongoDB configuration
            mongo_client = MongoClient('mongodb://localhost:27017/')
            mongo_db = mongo_client['youtube_data']
            mongo_collection = mongo_db['channel_data']
            cmt_list = []

            # Fetch data from MongoDB
            for cmt_data in mongo_collection.find({}, {"_id": 0, "comment_info": 1}):
                #for i in range(len(cmt_data["comment_info"])):
                 #   cmt_list.append(cmt_data["comment_info"][i])
                if 'comment_info' in cmt_data:
                    cmt_list.extend(cmt_data["comment_info"])
            else:
                print("Skipping document without 'comment_info' key")


            # Create DataFrame from MongoDB data
            df3 = pd.DataFrame(cmt_list)

            if df3.empty:
                print("No data retrieved from MongoDB.")
                return

            # MySQL connection
            mysql_conn = mysql.connector.connect(
                host='127.0.0.1',
                user='root',
                password='PXXXXXXXXXX',
                database='prj_youtube_data'
            )
            mysql_cursor = mysql_conn.cursor()

            # Create MySQL table if not exists
            mysql_cursor.execute("""
                CREATE TABLE IF NOT EXISTS comment (
                        Comment_Id varchar(255) primary key,
                        Video_Id   varchar(255),                   
                        Comment_Text text,
                        Comment_Author varchar(255),
                        Comment_PublishedAt datetime
                )
            """)

            for index, row in df3.iterrows():
                
                datetime_obj = datetime.strptime(row['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
                formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
                
                insert_query = '''INSERT INTO comment
                                (Comment_Id,
                                Video_Id,
                                Comment_Text,
                                Comment_Author,
                                Comment_PublishedAt)
                                VALUES (%s, %s, %s, %s, %s)'''

                values = (row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'],formatted_datetime)

                try:
                    mysql_cursor.execute(insert_query, values)
                    mysql_conn.commit()
                    print("Data inserted successfully")

                except mysql.connector.Error as e:
                    if e.errno == 1062:
                        print(f"Duplicate entry found for Comment_Id: {row['Comment_Id']}. Skipping insertion.")
                    else:
                        print(f"An error occurred while inserting into MySQL: {str(e)}")

        except mysql.connector.Error as error:
            print(f"Error connecting to MySQL: {error}")

            return "Channel ID inserted into MongoDB and MySQL successfully."
        
        except Exception as e:
            return f"Error inserting channel ID: {e}"

    # Function to fetch answers from MySQL based on selected question

# Streamlit app
st.title('YouTube Data')

# Display channel information from MongoDB
def display_channel_info(channel_id):
    if channel_id:
        channel_data = get_channel_info(channel_id)
        st.write("Channel Information:")
        st.write(channel_data)

# User input for viewing channel information
st.header("View Channel Information")
channel_id = st.text_input("Enter Channel ID to view information:")

if st.button("View"):
    display_channel_info(channel_id)

# User input for uploading new channel ID to MongoDB and MySQL
st.header("Upload New Channel ID")
new_channel_id = st.text_input("Enter Channel ID:")

if st.button("Upload"):
    upload_result = insert_channel_id(new_channel_id)
    st.write(upload_result)

# Button to insert channel information into MySQL
if st.button("Insert Channel into MySQL"):
    insert_result=insert_channel_into_mysql()
    st.write(insert_result)

# Dropdown to select a question and get the answer from MySQL
question = st.selectbox(
    "Select your question",
    [
        "1. What are the names of all the videos and their corresponding channels?",
        "2. Which channels have the most number of videos, and how many videos do they have?",
        "3. What are the top 10 most viewed videos and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022?",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]
)

# Execute SQL queries based on the selected question
    # Connect to MySQL
def fetch_answers():
    try:
        mysql_conn = mysql.connector.connect(
                host='127.0.0.1',
                user='root',
                password='PXXXXXXXXXX',
                database='prj_youtube_data'
            )
        mysql_cursor = mysql_conn.cursor()
        if question == "1. What are the names of all the videos and their corresponding channels?":
            query1 = "SELECT Channel_Name, Video_Name FROM prj_youtube_data.video"
            mysql_cursor.execute(query1)
            result1 = mysql_cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=["Channel Name", "Video Title"])
            st.write(df1)

        elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
            query2 = "SELECT Channel_Name, Video_count FROM prj_youtube_data.channel Order by Video_count desc"
            mysql_cursor.execute(query2)
            result2 = mysql_cursor.fetchall()
            df2 = pd.DataFrame(result2, columns=["Channel Name", "No.of.Videos"])
            st.write(df2)

        elif question == "3. What are the top 10 most viewed videos and their respective channels?":
            query3 = "SELECT Channel_Name, View_Count, Video_Name FROM prj_youtube_data.video order by View_Count desc limit 10"
            mysql_cursor.execute(query3)
            result3 = mysql_cursor.fetchall()
            df3 = pd.DataFrame(result3, columns=["Channel Name", "Top Viewed Videos","title"])
            st.write(df3)

        elif question == "4. How many comments were made on each video, and what are their corresponding video names":
            query4 = "SELECT Comment_Count, Video_Name FROM prj_youtube_data.video where Comment_Count is not null"
            mysql_cursor.execute(query4)
            result4 = mysql_cursor.fetchall()
            df4 = pd.DataFrame(result4, columns=["comments", "title"])
            st.write(df4)

        elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
            query5 = "SELECT Video_Name, Channel_Name, Like_Count FROM prj_youtube_data.video order by Like_Count desc"
            mysql_cursor.execute(query5)
            result5 = mysql_cursor.fetchall()
            df5 = pd.DataFrame(result5, columns=["title","channelname","mostlike count"])
            st.write(df5)

        elif question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
            query6 = "SELECT Video_Name, Dislike_Count, Like_Count FROM prj_youtube_data.video"
            mysql_cursor.execute(query6)
            result6 = mysql_cursor.fetchall()
            df6 = pd.DataFrame(result6, columns=["title","dislike count","like count"])
            st.write(df6)

        elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
            query7 = "SELECT Channel_Name,Channel_Views FROM prj_youtube_data.channel"
            mysql_cursor.execute(query7)
            result7 = mysql_cursor.fetchall()
            df7 = pd.DataFrame(result7, columns=["channelname","total views"])
            st.write(df7)

        elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
            query8 = "SELECT Channel_Name,PublishedAt FROM prj_youtube_data.video WHERE YEAR(PublishedAt) = 2022"
            mysql_cursor.execute(query8)
            result8 = mysql_cursor.fetchall()
            df8 = pd.DataFrame(result8, columns=["channelname","2022 Video"])
            st.write(df8)

        elif question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query9 = "SELECT Channel_Name,AVG(Duration) FROM prj_youtube_data.video group by Channel_Name"
            mysql_cursor.execute(query9)
            result9 = mysql_cursor.fetchall()
            df9 = pd.DataFrame(result9, columns=["channelname","Average"])
            st.write(df9)

        elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
            query10 = "SELECT Channel_Name, Comment_Count FROM prj_youtube_data.video order by Comment_Count desc"
            mysql_cursor.execute(query10)
            result10 = mysql_cursor.fetchall()
            df10 = pd.DataFrame(result10, columns=["channelname","Average"])
            st.write(df10)

    except mysql.connector.Error as e:
            return f"Error retrieving data from MySQL: {str(e)}"
if st.button("Get Answer"):
    answer = fetch_answers()
    st.write("Answer:")
    st.write(answer)

show_table = st.radio("select the option to view the table", ["Channels", "playlist", "Videos", "Comments"])

if show_table == "Channels":
    channel_data = channel_view()
    st.dataframe(channel_data)
        
elif show_table == "playlist":
    playlist_data = playlist_view()
    st.dataframe(playlist_data)
        
elif show_table == "Videos":
    Video_data=video_view()
    st.dataframe(Video_data)   
elif show_table == "Comments":
    comment_data=comment_view()
    st.dataframe(comment_data)

