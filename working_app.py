from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime
import streamlit as st
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

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

# function for insert data into mongodb

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
    # insert channel data from mongodb atlas into mysql table

    ch_list = []
    mongo_client = client["Youtube_Data"]
    coll = mongo_client["youtube_data1"]
    for ch_data in coll.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

# insert data into channels table in mysql

    for index, row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
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
            mycursor.execute(insert_query, values) 
            sqldb.commit()
        except:
            print("data inserted to the table successfully!")                                    

    sqldb.close()


# working code for video_information, inserted into table - videos

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
    mysql_cursor.close()
    mysql_db.close()
   

# working code for comment_information, inserted into table comments

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

    # Iterate through the MongoDB data and extract 'comment_information'
    for document in mongo_data:
        comment_information_list = document.get('comment_information', [])
        
        # Insert comment_information into MySQL table
        for comment_info in comment_information_list:

            # Extract 'Comment_publishedAt' and convert it to MySQL datetime format
            published_at_iso = comment_info.get('Comment_publishedAt', '')
            try:
                published_at = datetime.strptime(published_at_iso, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                published_at = None  # Handle the case where the datetime string is invalid

            # Assuming 'comments' is the MySQL table where you want to insert data
            mysql_insert_query = '''INSERT INTO comments(Comment_Id,
                                                            Video_Id,
                                                            Comment_text,
                                                            Comment_author,
                                                            Comment_publishedAt)
                                                                VALUES (%s, %s, %s, %s,%s)'''
            
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

    return "Inserted successfully"

# creation of channel_table_dataframe to view in streamlit app

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

st.title(":rainbow[YouTube Data Collection App]")

st.header(":green[Introduction]")

st.markdown('''The project aims to retrieve Youtube channel data through Youtube API. This project is working 
            on Streamlit Web Application, if the user provides the channel ID of any Youtube channel and in button 
            click they will able to store the data to MongoDB collections. After that the user can transfer those 
            unstructured data to structured entities as MySQL table. Got immense exposure of handling unstructure 
            and structure data through Python.''')

st.markdown('''As per the problem statement, aimed to get some information through visualizations from channels
             and their videos details as which channel having more subscribers, more videos, published in the 
            particular year,most liked videos, total number of views etc.,''')

st.header(":orange[Technologies Used]")

st.markdown(''' Python, MongoDB Atlas, MySQL, Pandas, Google API, Streamlit''')

channel_id = st.text_input("Enter Channel ID:")
if st.button("Fetch and Store Data"):
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

if st.button("Insert into MySQL"):
    Table = all_tables()
    st.success(Table)  

show_table = st.radio(":green[Select the table:]",("Channels","Videos", "Comments"))

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
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x="channel name", y="no of videos", data=df2, ax=ax)
    st.pyplot(fig)

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
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x="video_title", y="most_viewed", hue="channel name", data=df3, ax=ax)
    ax.tick_params(axis='x', rotation=45)
    st.pyplot(fig) 

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
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x="channelname", y="highestlike", hue="channelname", data=df5, ax=ax)
    ax.tick_params(axis='x', rotation=45)
    st.pyplot(fig)    


elif Questions=="6. Total number of likes and their video names":

    ques6_query = '''select Like_count, Video_title from videos'''

    mycursor.execute(ques6_query)
    sqldb.commit()
    tbl6 = mycursor.fetchall()
    df6 = pd.DataFrame(tbl6,columns=["likecounts", "videoname"])
    df6.fillna(0, inplace=True)    
    st.write(df6)
    
elif Questions=="7. Total number of views for each channel and their names":

    ques7_query = '''select Total_views, Channel_Name from channels'''

    mycursor.execute(ques7_query)
    sqldb.commit()
    tbl7 = mycursor.fetchall()
    df7 = pd.DataFrame(tbl7,columns=["totalviews", "channelname"])
    st.write(df7)
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x="channelname", y="totalviews", hue="channelname", data=df7, ax=ax)
    ax.tick_params(axis='x', rotation=45)
    st.pyplot(fig)     

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
    # Count the number of videos published by each channel
    channel_video_counts = df8['Channel_Name'].value_counts()

    # Create a bar chart
    fig, ax = plt.subplots(figsize=(10, 6))
    channel_video_counts.plot(kind='bar', ax=ax)
    ax.set_xlabel('Channel Name')
    ax.set_ylabel('Number of Videos Published')
    ax.set_title('Number of Videos Published by Each Channel in 2022')
    plt.xticks(rotation=45)
    st.pyplot(fig)
        

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
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x="Channel_Name", y="comments_count", hue="Channel_Name", data=df10, ax=ax)
    ax.tick_params(axis='x', rotation=45)
    st.pyplot(fig) 



    
