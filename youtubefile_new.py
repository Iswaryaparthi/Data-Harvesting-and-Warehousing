from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime
import streamlit as st
from sqlalchemy import create_engine


# youtube API key connection

# atlas pwd- Xy17N06oDPFF3m3G

def API_connect():
   
    api_id = "AIzaSyCfmn_wOHerUVOKxXkaAOsHnmncqSi0zTA"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=api_id)
    return youtube

youtube_result = API_connect()    

# get channel_ids and channel data

def getChannel_Info(channel_id):
    request = youtube_result.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()

    for i in response['items']:
        channel_data = dict(Channel_Name=i['snippet']['title'],
                    Channel_ID = i['id'],
                    Subscribers = i['statistics']['subscriberCount'],
                    Video_count = i['statistics']['videoCount'],
                    Total_views=i['statistics']['viewCount'],
                    Description=i['snippet']['description'],
                    Playlist_ID = i['contentDetails']['relatedPlaylists']['uploads']
                    )
        return channel_data
        
# get video_ids


def getVideo_Ids(channel_id):
    video_ids=[]
    response = youtube_result.channels().list(id =channel_id,
                                    part = 'contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube_result.playlistItems().list(
                                part = 'snippet',
                                playlistId = Playlist_Id,
                                maxResults = 50,
                                pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break    
    return video_ids


# get videos information

# get function helps to give none value if the channel off the tags,views,comments key..
def getVideoInfo(Video):
  video_data = []

  for video_id in Video:
      request = youtube_result.videos().list(
                              part="snippet,contentDetails,statistics",
                              id = video_id
      )
      response = request.execute()
      for item in response['items']:
          data = dict(Channel_Name=item['snippet']['channelTitle'],
                      Channel_Id = item['snippet']['channelId'],
                      Video_Id = item['id'],
                      Video_title = item['snippet']['title'],
                      #Tags = item['snippet'].get('tags'),
                      Thumbnail = item['snippet']['thumbnails']['default']['url'],
                      Description = item['snippet'].get('description'),
                      PublishedAt = item['snippet']['publishedAt'],
                      Duration = item['contentDetails']['duration'],
                      View_count = item['statistics'].get('viewCount'),
                      Like_count = item['statistics'].get('likeCount'),
                      Comments_count = item['statistics'].get('commentCount'),
                      Caption = item['contentDetails']['caption']
                
                    )
          video_data.append(data)
  return video_data   


 #get comments_details

def get_Commentsinfo(result_video_IDs):
    Comment_data =[]
    try:

        for comment_id in result_video_IDs:
            request = youtube_result.commentThreads().list(
                                        part ="snippet",
                                        videoId = "TJbpI3mFbYY",
                                        maxResults = 50            
                    )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['videoId'],
                            Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author= item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_publishedAt  = item['snippet']['topLevelComment']['snippet']['publishedAt']

                            )
                Comment_data.append(data)
    except:
        pass    
    return Comment_data   

# connecting MongoDB

client = pymongo.MongoClient("mongodb+srv://iswaryaparthiban:pwd123456@cluster0.qyejfm2.mongodb.net/?retryWrites=true&w=majority")
#client = pymongo.MongoClient("mongodb://localhost:27017") 
mongo_db = client["Youtube_Data"]
mongo_collection = mongo_db["youtube_data1"]

# new function for insert data into mongodb

def youtube_data1(channel_id):
    chl_details= getChannel_Info(channel_id)
    vid_details= getVideo_Ids(channel_id)
    vidinfo_details= getVideoInfo(vid_details)
    comm_detals= get_Commentsinfo(vid_details)

    coll = mongo_client["youtube_data1"]
    coll.insert_one({"channel_information":chl_details,"video_information":vidinfo_details,
                     "comment_information":comm_detals})
    return "data uploaded successfully"


# mysql connection
#creating channels1 table in mysql 
# updated

def channels_table():
    sqldb = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = "Pwd@123456",
            database = "Youtube_Data",
            port = "3306"

        )
    mycursor = sqldb.cursor()

        # to insert multiple channel details use drop query, to avoid duplicate 

    drop_query = ''' drop table if exists Channels'''
    mycursor.execute(drop_query)
    sqldb.commit()

        # Table creation query for creating channels,videos,comments
    create_query = '''create table if not exists Channels(Channel_Name varchar(100),
                                                Channel_ID varchar(70) primary key,
                                                Subscribers bigint,
                                                Video_count int,
                                                Total_views bigint,
                                                Description text,
                                                Playlist_ID varchar(70))'''
    mycursor.execute(create_query)

    sqldb.commit()

    # insert channel data from mongodb atlas into mysql table

    ch_list =[]
    mongo_client = client["Youtube_Data"]
    coll = mongo_client["youtube_data1"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)    

    # insert data into channels table in mysql

    for  index,row in df.iterrows():
        insert_query = '''insert into Channels(Channel_Name,
                                            Channel_ID, 
                                            Subscribers,
                                            Video_count,
                                            Total_views,
                                            Description,
                                            Playlist_ID)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        # df column names
                                            
        values = (row['Channel_Name'],
                row['Channel_ID'],
                row['Subscribers'],
                row['Video_count'],
                row['Total_views'],
                row['Description'],
                row['Playlist_ID']
                )   
        try:
            mycursor.execute(insert_query,values) 
            sqldb.commit()

        except:
            print("data inserted to the table successfully!")                                    
        
    sqldb.close()    

# working code for video_information, inserted into table - videos

import pymongo
import mysql.connector
from datetime import datetime

# Connect to MongoDB
#mongo_client = pymongo.MongoClient("mongodb+srv://iswaryaparthiban:pwd123456@cluster0.qyejfm2.mongodb.net/?retryWrites=true&w=majority")
#mongo_db = mongo_client["Youtube_Data"]
#mongo_collection = mongo_db["youtube_data1"]

# Connect to MySQL
def videos_table():
    mysql_db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Pwd@123456",
        database="Youtube_Data",
        port="3306"
    )
    mysql_cursor = mysql_db.cursor()

    # Retrieve data from MongoDB
    mongo_data = mongo_collection.find()

    # to insert multiple channel details use drop query, to avoid duplicate 

    drop_query = ''' drop table if exists videos'''
    mysql_cursor.execute(drop_query)
    mysql_db.commit()

    # create query
    create_query = ''' create table if not exists videos(Channel_Name varchar(255),
                                                            Channel_Id varchar(255), 
                                                            Video_Id varchar(255), 
                                                            Video_title varchar(255),
                                                            Thumbnail varchar(255),
                                                            Description text,
                                                            PublishedAt datetime,
                                                            Duration varchar(10),
                                                            View_count int, 
                                                            Like_count int, 
                                                            Comments_count int, 
                                                            Caption tinyint(1)
                                        )'''
    mysql_cursor.execute(create_query)
    mysql_db.commit()

    # Iterate through the MongoDB data and extract 'video_information'
    for document in mongo_data:
        video_information_list = document.get('video_information', [])
        
        # Insert video_information into MySQL table
        for video_info in video_information_list:

            # Extract 'PublishedAt' and convert it to MySQL datetime format
            published_at_iso = video_info.get('PublishedAt', '')
            try:
                published_at = datetime.strptime(published_at_iso, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                published_at = None  # Handle the case where the datetime string is invalid

            # Convert 'Caption' to a boolean value
            caption_value = video_info.get('Caption', False)
            caption_int = 1 if caption_value else 0  # Convert boolean to 1 or 0

            # Assuming 'your_mysql_table' is the MySQL table where you want to insert data
            mysql_insert_query = '''INSERT INTO videos (Channel_Name,
                                                                    Channel_Id,
                                                                    Video_Id,
                                                                    Video_title,
                                                                    Thumbnail,
                                                                    Description,
                                                                    PublishedAt,
                                                                    Duration,
                                                                    View_count,
                                                                    Like_count,
                                                                    Comments_count,
                                                                    Caption)
                                                                VALUES (%s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s)'''
            
            # Replace 'column1', 'column2', ... with your actual column names in the MySQL table
            mysql_values = (
                video_info.get('Channel_Name', ''),
                video_info.get('Channel_Id', ''),
                video_info.get('Video_Id', ''),
                video_info.get('Video_title', ''),
                video_info.get('Thumbnail', ''),
                video_info.get('Description', ''),
                published_at,
                video_info.get('Duration', ''),
                video_info.get('View_count', ''),
                video_info.get('Like_count', ''),
                video_info.get('Comments_count', ''),
                caption_int  
                )   
            # Execute the insert query
            mysql_cursor.execute(mysql_insert_query, mysql_values)

    # Commit the changes to MySQL
    mysql_db.commit()

    # Close connections
    mysql_cursor.close()
    mysql_db.close()
    #mongo_client.close()


# working code for comment_information, inserted into table comments

import pymongo
import mysql.connector
from datetime import datetime

# Connect to MongoDB
#mongo_client = pymongo.MongoClient("mongodb+srv://iswaryaparthiban:pwd123456@cluster0.qyejfm2.mongodb.net/?retryWrites=true&w=majority")
#mongo_db = mongo_client["Youtube_Data"]
#mongo_collection = mongo_db["youtube_data1"]

# Connect to MySQL
def comments_table():
    mysql_db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Pwd@123456",
        database="Youtube_Data",
        port="3306"
    )
    mysql_cursor = mysql_db.cursor()

    # Retrieve data from MongoDB
    mongo_data = mongo_collection.find()


    # to insert multiple comment details use drop query, to avoid duplicate 

    drop_query = ''' drop table if exists comments'''
    mysql_cursor.execute(drop_query)
    mysql_db.commit()

    # create query
    create_query = ''' create table if not exists comments(Comment_Id varchar(255),
                                                            Video_Id varchar(255),
                                                            Comment_text varchar(255),
                                                            Comment_author varchar(255),
                                                            Comment_publishedAt datetime
                                        )'''
    mysql_cursor.execute(create_query)
    mysql_db.commit()


    # Iterate through the MongoDB data and extract 'video_information'
    for document in mongo_data:
        comment_information_list = document.get('comment_information', [])
        
        # Insert video_information into MySQL table
        for comment_info in comment_information_list:

            # Extract 'PublishedAt' and convert it to MySQL datetime format
            published_at_iso = comment_info.get('Comment_publishedAt', '')
            try:
                published_at = datetime.strptime(published_at_iso, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                published_at = None  # Handle the case where the datetime string is invalid

            # Assuming 'your_mysql_table' is the MySQL table where you want to insert data
            mysql_insert_query = '''INSERT INTO comments(Comment_Id,
                                                            Video_Id,
                                                            Comment_text,
                                                            Comment_author,
                                                            Comment_publishedAt)
                                                                VALUES (%s, %s, %s, %s,%s)'''
            
            # Replace 'column1', 'column2', ... with your actual column names in the MySQL table
            mysql_values = (
                comment_info.get('Comment_Id', ''),
                comment_info.get('Video_Id', ''),
                comment_info.get('Comment_text', ''),
                comment_info.get('Comment_author', ''),
                published_at
            )   
            # Execute the insert query
            mysql_cursor.execute(mysql_insert_query, mysql_values)

    # Commit the changes to MySQL
    mysql_db.commit()

    # Close connections
    mysql_cursor.close()
    mysql_db.close()
    #mongo_client.close()

# bring all table creation function in one function

def all_tables():
    channels_table()
    videos_table()
    comments_table()

    return "tables created successfully"


# creation of  channel_table_dataframe to view in streamlit app

def show_channels_table():
    ch_list =[]
    mongo_client = client["Youtube_Data"]
    coll = mongo_client["youtube_data1"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data["channel_information"])             
    df = st.dataframe(ch_list) 

    return df       

# creation of  videos_table_dataframe to view in streamlit app

def show_videos_table():
        vid_list =[]
        mongo_client = client["Youtube_Data"]
        coll = mongo_client["youtube_data1"]
        for vid_data in coll.find({}, {"_id": 0, "video_information": 1}):
            for i in range(len(vid_data["video_information"])):
                vid_list.append(vid_data["video_information"][i])
        df1 = pd.DataFrame(vid_list)

        return df1


# creation of  comments_table_dataframe to view in streamlit app

def show_comments_table():
    com_list =[]
    mongo_client = client["Youtube_Data"]
    coll = mongo_client["youtube_data1"]
    for com_data in coll.find({},{"_id":0,"comment_information":1}):
            for i in range(len(com_data["comment_information"])):
                    com_list.append(com_data["comment_information"][i])
    df2 = pd.DataFrame(com_list)  

    return df2


# streamlit codes

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills learned")
    st.caption("Youtube API Integration")
    st.caption("Data collection")
    st.caption("MongoDB Atlas")
    st.caption("Python Scripting")        
    st.caption("Data manipulation using MongoDB and MySQL")
    st.caption("Streamlit Application")

channel_id = st.text_input("Enter the channel Id") 

# button creation in streamlit

if st.button("collect and store data"):
    ch_ids=[]
    mongo_client = client["Youtube_Data"]
    coll = mongo_client["youtube_data1"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_ID"])

# check whether channel id is exists or not        

    if channel_id in ch_ids:
        st.success("Channel already exists")

    else:
        insert = youtube_data1(channel_id)    
        st.success(insert)

if st.button("Migrate to MySQL"):
    Table = all_tables()
    st.success(Table)  

show_table = st.radio("select the table for display",("Channels","Videos", "Comments"))

if show_table == "Channels":
    show_channels_table()

elif  show_table == "Videos":
    show_videos_table()    
    
elif  show_table == "Comments":
    show_comments_table()


# SQL queries for 10 questions in problem statement

# mysql connection

sqldb = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = "Pwd@123456",
            database = "Youtube_Data",
            port = "3306"

        )
mycursor = sqldb.cursor(buffered=True)

Questions = st.selectbox("Select your question",("1. All the videos and their channel name",
                                                 "2. Channels which have more number of videos",
                                                 "3. 10 most viewed videos and the channel name",
                                                 "4. No.of Comments on each videos and their names",
                                                 "5. Videos have highest like and channel names",
                                                 "6. Total number of likes and their video names",
                                                 "7. Total number of views for each channel and their names",
                                                 "8. Channels have published videos in the year 2022",
                                                 "9. Average duration of all videos and their channel names",
                                                 "10.Videos having highest number of comments and their channel name"))

if Questions=="1. All the videos and their channel name":

    ques1_query = '''select video_title as videos,channel_name as channelname from videos'''

    mycursor.execute(ques1_query)
    sqldb.commit()
    tbl1 = mycursor.fetchall()
    df1 = pd.DataFrame(tbl1,columns=["video title","channel name"])
    st.write(df1)

elif Questions=="2. Channels which have more number of videos":

    ques2_query = '''select Channel_Name, Video_count from channels
                        order by Video_count desc;'''

    mycursor.execute(ques2_query)
    sqldb.commit()
    tbl2 = mycursor.fetchall()
    df2 = pd.DataFrame(tbl2,columns=["channel name", "no of videos"])
    st.write(df2)    

elif Questions=="3. 10 most viewed videos and the channel name":

    ques3_query = '''SELECT video_title, view_count, channel_name
                        FROM (
                            SELECT video_title, view_count, channel_name
                            FROM videos
                            ORDER BY view_count DESC
                        ) AS ordered_videos
                        LIMIT 10;'''

    mycursor.execute(ques3_query)
    sqldb.commit()
    tbl3 = mycursor.fetchall()
    df3 = pd.DataFrame(tbl3,columns=["video_title", "most_viewed","channel name"])
    st.write(df3)  

elif Questions=="4. No.of Comments on each videos and their names":

    ques4_query = '''SELECT Comments_count AS commentscount, Video_title AS videoname
                    FROM videos
                    WHERE Comments_count IS NOT NULL;'''

    mycursor.execute(ques4_query)
    sqldb.commit()
    tbl4 = mycursor.fetchall()
    df4 = pd.DataFrame(tbl4,columns=["commentscount", "videoname"])
    st.write(df4)    

elif Questions=="5. Videos have highest like and channel names":

    ques5_query = '''SELECT v.video_title, v.like_count, v.channel_name
                    FROM videos v
                    ORDER BY v.like_count DESC;;'''

    mycursor.execute(ques5_query)
    sqldb.commit()
    tbl5 = mycursor.fetchall()
    df5 = pd.DataFrame(tbl5,columns=["videotitle","highestlike", "channelname"])
    st.write(df5)      


elif Questions=="6. Total number of likes and their video names":

    ques6_query = '''select Like_count, Video_title from videos'''

    mycursor.execute(ques6_query)
    sqldb.commit()
    tbl6 = mycursor.fetchall()
    df6 = pd.DataFrame(tbl6,columns=["likecounts", "videoname"])
    st.write(df6)

elif Questions=="7. Total number of views for each channel and their names":

    ques7_query = '''select Total_views, Channel_Name from channels'''

    mycursor.execute(ques7_query)
    sqldb.commit()
    tbl7 = mycursor.fetchall()
    df7 = pd.DataFrame(tbl7,columns=["totalviews", "channelname"])
    st.write(df7)    

elif  Questions=="8. Channels have published videos in the year 2022":

    ques8_query = '''select Channel_Name, PublishedAt , Video_title from videos
                    WHERE SUBSTRING(PublishedAt, 1, 4) = '2022';
                    '''
    mycursor.execute(ques8_query)
    sqldb.commit()
    tbl8 = mycursor.fetchall()
    df8 = pd.DataFrame(tbl8,columns=["channelname", "publishedAt","videotitle"])
    df8= pd.read_sql_query(ques8_query, sqldb)
    st.write(df8)

elif Questions == "9. Average duration of all videos and their channel names":

    ques9_query = '''SELECT channel_name, AVG(duration_in_seconds) AS average_duration
                    FROM (
                        SELECT channel_name,
                            TIME_TO_SEC(
                                CONCAT(
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'T', -1),
                                    ' ',
                                    SUBSTRING_INDEX(Duration, 'S', -1)
                                )
                            ) AS duration_in_seconds
                        FROM videos
                    ) AS subquery
                    GROUP BY channel_name'''

    mycursor.execute(ques9_query)
    sqldb.commit()
    tbl9 = mycursor.fetchall()
    df9 = pd.DataFrame(tbl9,columns=["channelname", "average_duration"])
    st.write(df9)    

elif Questions == "10.Videos having highest number of comments and their channel name":

    ques10_query = '''select comments_count, video_title, channel_name 
                        from videos
                        order by comments_count desc'''

    mycursor.execute(ques10_query)
    sqldb.commit()
    tbl10 = mycursor.fetchall()
    df10 = pd.DataFrame(tbl10,columns=["comments_count","video_id","Channel_Name"])
    st.write(df10)



    
