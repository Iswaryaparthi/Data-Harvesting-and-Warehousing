import pymongo
import mysql.connector
import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime

# working code as of 22/03/2024

# Function to connect to YouTube API
def connect_to_youtube_api(api_key):
    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube

# Function to get channel details from YouTube API
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for item in response['items']:
        channel_data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_ID': item['id'],
            'Subscribers': item['statistics']['subscriberCount'],
            'Video_count': item['statistics']['videoCount'],
            'Total_views': item['statistics']['viewCount'],
            'Description': item['snippet']['description'],
            'Playlist_ID': item['contentDetails']['relatedPlaylists']['uploads']
        }
    return channel_data

# Function to get video IDs from YouTube API
def get_video_ids(youtube, channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response1['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if not next_page_token:
            break
    return video_ids

# Function to get video information from YouTube API
def get_video_info(youtube, video_ids):
    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response['items']:
            data = {
                'Channel_Name': item['snippet']['channelTitle'],
                'Channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Video_title': item['snippet']['title'],
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet'].get('description'),
                'PublishedAt': item['snippet']['publishedAt'],
                'Duration': item['contentDetails']['duration'],
                'View_count': item['statistics'].get('viewCount'),
                'Like_count': item['statistics'].get('likeCount'),
                'Comments_count': item['statistics'].get('commentCount'),
                'Caption': item['contentDetails']['caption']
            }
            video_data.append(data)
    return video_data

# Function to get comment details from YouTube API
def get_comments_info(youtube, video_ids):
    comment_data = []

    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = {
                    'Comment_Id': item['snippet']['topLevelComment']['id'],
                    'Video_Id': item['snippet']['videoId'],
                    'Comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comment_data.append(data)
    except:
        pass

    return comment_data

# connecting MongoDB
# Function to connect to MongoDB
def connect_to_mongodb():
    client = pymongo.MongoClient("mongodb+srv://username:password@cluster0.qyejfm2.mongodb.net/?retryWrites=true&w=majority")
    mongo_db = client["Youtube_Data"]
    mongo_collection = mongo_db["youtube_data1"]
    return mongo_collection

# Function to save data to MongoDB
def save_to_mongodb(youtube, channel_id):
    chl_details = get_channel_info(youtube, channel_id)
    vid_details = get_video_ids(youtube, channel_id)
    vidinfo_details = get_video_info(youtube, vid_details)
    comm_details = get_comments_info(youtube, vid_details)

    coll = connect_to_mongodb()
    coll.insert_one({"channel_information": chl_details, "video_information": vidinfo_details,
                     "comment_information": comm_details})
    return "Data uploaded successfully"

# Function to fetch all channel names from MongoDB
def get_all_channel_names():
    coll = connect_to_mongodb()
    channels = coll.distinct("channel_information.Channel_Name")
    return channels
# Function to insert video information into MySQL
def insert_video_info_to_mysql(channel_id, video_info):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='youtube_data',
            user='username',
            password='password'
        )
        cursor = connection.cursor()

        insert_video_query = """INSERT INTO videos 
                                (Channel_Name, Channel_Id, Video_Id, Video_title, Thumbnail, Description, 
                                PublishedAt, Duration, View_count, Like_count, Comments_count, Caption) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        # Convert ISO  datetime string to MySQL datetime format
        published_at = datetime.fromisoformat(video_info['PublishedAt'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert 'true'/'false' string to boolean value
        caption = True if video_info['Caption'].lower() == 'true' else False

        # Extract Channel_Name and Channel_Id from video_info
        channel_name = video_info.get('Channel_Name', 'Unknown')
        channel_id = video_info.get('Channel_Id', 'Unknown')

        video_data = (channel_name, channel_id, video_info['Video_Id'], 
                      video_info['Video_title'], video_info['Thumbnail'], video_info['Description'], 
                      published_at, video_info['Duration'], video_info['View_count'], 
                      video_info['Like_count'], video_info['Comments_count'], caption)
        cursor.execute(insert_video_query, video_data)
        connection.commit()
        print("Video information inserted successfully into MySQL")

    except mysql.connector.Error as e:
        print("Error inserting video information into MySQL:", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Function to insert comment information into MySQL
def insert_comment_info_to_mysql(channel_id, comment_info):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='youtube_data',
            user='username',
            password='password'
        )
        cursor = connection.cursor()

        insert_comment_query = """INSERT INTO comments 
                                  (Channel_Id, Video_Id, Comment_text, Comment_author, Comment_publishedAt) 
                                  VALUES (%s, %s, %s, %s, %s)"""
        # Convert ISO  datetime string to MySQL datetime format
        comment_published_at = datetime.fromisoformat(comment_info['Comment_publishedAt'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
        
        comment_data = (channel_id, comment_info['Video_Id'], 
                        comment_info['Comment_text'], comment_info['Comment_author'], 
                        comment_published_at)
        cursor.execute(insert_comment_query, comment_data)
        connection.commit()
        print("Comment information inserted successfully into MySQL")

    except mysql.connector.Error as e:
        print("Error inserting comment information into MySQL:", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def insert_channel_info_to_mysql(channel_info):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='youtube_data',
            user='username',
            password='password'
        )
        cursor = connection.cursor()

        # Insert channel information into 'channels' table
        insert_channel_query = """INSERT INTO channels 
                                  (Channel_Name, Channel_ID, Subscribers, Video_count, Total_views, Description, Playlist_ID) 
                                  VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        channel_data = (channel_info['Channel_Name'], channel_info['Channel_ID'], 
                        channel_info['Subscribers'], channel_info['Video_count'], 
                        channel_info['Total_views'], channel_info['Description'], 
                        channel_info['Playlist_ID'])
        cursor.execute(insert_channel_query, channel_data)

        connection.commit()
        print("Channel information inserted successfully into MySQL")

        # Print the length of video_information and comment_information arrays
        print("Number of videos to insert:", len(channel_info.get('video_information', [])))
        print("Number of comments to insert:", len(channel_info.get('comment_information', [])))

        # Insert video information into 'videos' table
        for video in channel_info.get('video_information', []):
            insert_video_info_to_mysql(channel_info['Channel_ID'], video)  

        # Insert comment information into 'comments' table
        for comment in channel_info.get('comment_information', []):
            insert_comment_info_to_mysql(channel_info['Channel_ID'], comment) 

        print("Video and comment information inserted successfully into MySQL")

    except mysql.connector.Error as e:
        print("Error inserting data into MySQL:", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Function to execute MySQL queries
def execute_query(query):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='youtube_data',
            user='username',
            password='password'
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except mysql.connector.Error as e:
        print(f"Error executing query: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Streamlit UI
def main():
    st.title(":rainbow[YouTube Data Harvesting and Warehousing]")

    option = st.sidebar.selectbox(":red[Select Option]", [
        "Upload Data", 
        "View Channels", 
        "Insert into MySQL", 
        "Answer Questions"
    ])

    if option == "Upload Data":
        st.subheader(":green[Upload Data]")
        # Your upload data functionality goes here
        st.write("Enter the Channel ID:")
        channel_id = st.text_input("Channel ID")
        if st.button("Save to MongoDB"):
            if channel_id:
                youtube = connect_to_youtube_api("apikey")
                result = save_to_mongodb(youtube, channel_id)
                st.success(result)
            else:
                st.error("Please enter a valid Channel ID")

    elif option == "View Channels":
        st.subheader(":green[View Channels]")
        # Your view channels functionality goes here
        channels = get_all_channel_names()
        if channels:
            st.write("Available Channels:")
            for channel in channels:
                st.write(channel)
        else:
            st.write("No channels found in the database.")

    elif option == "Insert into MySQL":
        st.subheader(":green[Insert into MySQL]")
        channels = get_all_channel_names()
        selected_channel = st.selectbox("Select Channel", channels)
        if st.button("Insert into MySQL"):
            if selected_channel:
                # Fetch channel information based on the selected channel name
                coll = connect_to_mongodb()
                channel_info = coll.find_one({"channel_information.Channel_Name": selected_channel})
                print(channel_info)  # Print channel_info for debugging
                if channel_info:
                    insert_channel_info_to_mysql(channel_info['channel_information'])
                    # Insert video information
                    for video_info in channel_info.get('video_information', []):
                        insert_video_info_to_mysql(channel_info.get('Channel_ID', ''), video_info)  # Passing channel_id and video_info
                    # Insert comment information
                    for comment_info in channel_info.get('comment_information', []):
                        insert_comment_info_to_mysql(channel_info.get('Channel_ID', ''), comment_info)  # Passing channel_id and comment_info
                    st.success("Data inserted into MySQL successfully")
                else:
                    st.error("Channel information not found in MongoDB")
            else:
                st.error("Please select a channel from the list")

    elif option == "Answer Questions":
        st.subheader(":green[Answer Questions]")
        question = st.selectbox("Select a question", [
            "All the videos and their channel names",
            "Channels which have more number of videos",
            "10 most viewed videos and the channel names",
            "No.of Comments on each videos and their names",
            "Videos have highest like and channel names",
            "Total number of likes and their video names",
            "Total number of views for each channel and their names",
            "Channels have published videos in the year 2022",
            "Average duration of all videos and their channel names",
            "Videos having highest number of comments and their channel name"
        ])

        if st.button("Get Answer"):
            # Execute query based on selected question and display result
            if question == "All the videos and their channel names":
                query_result = execute_query("SELECT Video_title, Channel_Name FROM videos")
                st.write("All the videos and their channel names:")
                st.dataframe(query_result)

            elif question == "Channels which have more number of videos":
                query_result = execute_query('''SELECT Channel_Name, COUNT(*) AS num_videos FROM videos
                                              GROUP BY Channel_Name ORDER BY num_videos DESC''')
                st.write("Channels with more number of videos:")
                st.dataframe(query_result)

            elif question == "10 most viewed videos and the channel names":
                query_result = execute_query('''SELECT v.Video_title, v.View_count, c.Channel_Name
                                                FROM videos v
                                                JOIN channels c ON v.Channel_Id = c.Channel_ID
                                                ORDER BY v.View_count DESC
                                                LIMIT 10;''')
                st.write("Channels with most viewed videos:")
                st.dataframe(query_result)

            elif question == "No.of Comments on each videos and their names":
                query_result = execute_query('''SELECT v.Video_title, v.Comments_count, c.Channel_Name
                                                FROM videos v
                                                JOIN channels c ON v.Channel_Id = c.Channel_ID;''')
                st.write("No.of Comments on each videos with names:")
                st.dataframe(query_result)

            elif question == "Videos have highest like and channel names":
                query_result = execute_query('''SELECT v.Video_title, v.Like_count, c.Channel_Name
                                                FROM videos v
                                                JOIN channels c ON v.Channel_Id = c.Channel_ID
                                                ORDER BY v.Like_count DESC
                                                LIMIT 10;''')
                st.write("Videos with highest likes:")
                st.dataframe(query_result)

            elif question == "Total number of likes and their video names":
                query_result = execute_query('''SELECT v.Video_title, SUM(v.Like_count) AS Total_likes
                                                FROM videos v
                                                GROUP BY v.Video_title;''')
                st.write("Total likes with their video names:")
                st.dataframe(query_result)

            elif question ==  "Total number of views for each channel and their names":
                query_result = execute_query('''SELECT c.Channel_Name, SUM(v.View_count) AS Total_views
                                                FROM videos v
                                                JOIN channels c ON v.Channel_Id = c.Channel_ID
                                                GROUP BY c.Channel_Name;''')
                st.write("Number of Views with Channel names:")
                st.dataframe(query_result)

            elif question ==  "Channels have published videos in the year 2022":
                query_result = execute_query('''SELECT c.Channel_Name
                                                FROM channels c
                                                JOIN videos v ON c.Channel_ID = v.Channel_Id
                                                WHERE YEAR(v.PublishedAt) = 2022
                                                GROUP BY c.Channel_Name;''')
                st.write("Channels published videos in the year 2022:")
                st.dataframe(query_result)

            elif question ==  "Average duration of all videos and their channel names":
                query_result = execute_query('''SELECT c.Channel_Name, AVG(v.Duration)
                                                AS Average_duration
                                                FROM videos v
                                                JOIN channels c ON v.Channel_Id = c.Channel_ID
                                                GROUP BY c.Channel_Name;''')
                st.write("Average duration of videos with channel names:")
                st.dataframe(query_result)

            elif question ==  "Videos having the highest number of comments and their channel name":
                query_result = execute_query('''SELECT v.Video_title, v.Comments_count, c.Channel_Name
                                                FROM videos v
                                                JOIN channels c ON v.Channel_Id = c.Channel_ID
                                                ORDER BY v.Comments_count DESC
                                                LIMIT 10;''')
                st.write("Channel have highest number of comments:")
                st.dataframe(query_result)


if __name__ == "__main__":
    main()
 
