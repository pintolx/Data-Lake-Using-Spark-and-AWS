This file has instructions on how to run the project
# dl.cfg
This file contains the secret key and password that help connect the project to an AWS account
# etl.py
This file connects the spark engine for data normalization and parquet file writing. The ETL job helps process the song files then the log files. The song files are iterated over entering relevant information in the artists and the song folders in parquet. The log files are filtered by the NextSong action. The timestamp for each record is processed to extract the date, time, year. The extracted fields and records are then entered into the time, users and songplays folders in parquet for analysis.
# data - 
A folder that cointains two zip files, that are being used for data exploration
# Examples of queries
a) This would show you the count of users in the users table grouped by level and gender
select level, gender, count(distinct userId) as user_count from user_table group by 1,2
b) Number of unique artists we have in the artists table
Select count(distinct artist_id) as count_of_artists from artists_table
# Data structure of all tables have been uploaded as images.
Images of songplays table, users, time, artists and songs table have been uploaded to the project file





