#  Capstone 1
## YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit
                                  
  ### Description of the project:
  
The purpose of this project is to work around with MongoDB and MySQL databases using Python as the programming language. From YouTube API the data were collected, stored, retrieved, and displayed in the Streamlit application in browser.

### Skills take away from this project:
1. YouTube Integration.
2. Account, Cluster, Database, Collection - creation in MongoDB Atlas.
3. Getting data from YouTube API using API key.
4. Load the data into MongoDB. 
5. Python scripting.
6. MySQL table creation and inserting data.
7. Streamlit application.
  
### Installation:
- Few packages, software’s need to be installed/import for the completion of the project:

- For YouTube API integration, imports the below package
##### from googleapiclient.discovery import build

- Getting data from MongoDB Atlas
##### import pymongo

- Connecting with MySQL
##### import mysql.connector

- Package for Pandas, using pip install Pandas in terminal and import this package
##### import pandas as pd

- For handling datetime fields
##### from datetime import datetime

- Run the Streamlit application from Python environment
##### import streamlit as st

### Usage of the Project:
1. The project aims to provide the connection between YouTube API and MongoDB databases.
2. The project should play around the data from Unstructured database to Structured database.
3. From Mongo DB , using python need to create tables and insert the data from mongo DB to MySQL DB.
4. Here, I created three tables in MySQL database as Channels,Videos,Comments.
5. The datas from MySQL should be display as table format in Streamlit application.
6. While providing ChannelID in streamlit application, on button click it should get the channel details from API and stored in MongoDB.
7. On click Migrate to MySQL button, the newly added channel details are retrieved and stored in MySQL table and display in Streamlit application.

Thank you all!

