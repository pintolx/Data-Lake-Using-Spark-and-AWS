import configparser
from datetime import datetime
import os
import calendar
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

"""This function scans all the spark components"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


"""This function processes the song and artist data from the json file that is stored in the S3 bucket"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "./data/song_data/*/*/*/*.json"
    
    """Creating the song_data file schema that we are going to add to spark"""
    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    
    # reading song data file json structure
    df = spark.read.json(song_data, schema=songSchema)

    """Filtering out only the needed columns for the songs table"""
    song_fields = ["title", "artist_id","year", "duration"]
    
    print('Creating the songs table and dropping duplicates')
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    print("--- All duplicate songs have been dropped and the songs table created ---")
    print('Printing some rows from the songs_table')
    songs_table.show(15)
    print('Saving the songs table to the s3 bucket')
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs")
    print("--- songs.parquet completed ---")
    """Filtering out only the needed columns for the artists table"""
    artists_data = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    print("--- Starting to drop duplicate artists....")
    artists_table = df.selectExpr(artists_data).dropDuplicates()
    print("All duplicate artists have been dropped......")
    
    print('Printing some rows from the artists_table')
    artists_table.show(15)
    
    """writing the artists table to the parquets file"""
    artists_table.write.parquet(output_data + "artists")
    print("--- artists.parquet completed ---")
    print("*** process_song_data completed ***\n\n")


def process_log_data(spark, input_data, output_data):
    """ Processing log data from the JSON file provided in the S3 bucket"""  
    # get filepath to log data file
    log_data = input_data + 'data/*.json'
    
    """Creating the song_data file schema that we are going to add to spark"""
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", LongType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])


    """Reading the log_data file and creating a dataframe from which we are going to create tables"""
    df = spark.read.json(log_data, schema=log_data_schema)
    
    """Filtering out onlys records where the page is equal to the next page"""
    df = df.filter(col("page") == 'NextSong')
    """Extracting columns for users table from the dataframe created above """   
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()
    print("--- All duplicate users have been dropped completed ---")

    print('Printing some rows from the users_table')
    users_table.show(15)
    
    """Writing the users table to the parquet file"""
    users_table.write.parquet(output_data+"users")
    print("--- users.parquet completed ---")
    
    print("--- creating the time table ---")
    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", F.to_timestamp(F.from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp"))

    time_table = df.select("start_time").dropDuplicates()
    time_table = time_table.withColumn("month", F.month("start_time"))
    time_table = time_table.withColumn("year", F.year("start_time"))
    time_table = time_table.withColumn("day", F.dayofmonth("start_time"))
    time_table = time_table.withColumn("hour", F.hour("start_time"))
    time_table = time_table.withColumn("week", F.weekofyear("start_time"))
    time_table = time_table.withColumn("weekday", F.dayofweek("start_time"))
    
    print('Printing some rows from the time_table')
    time_table.show(15)
                    
    time_table.write.partitionBy("year", "month").parquet(output_data +"time")
    print("--- time.parquet completed ---")
    
    """Reading in the songs data used to create the songsplay table"""
    song_df = input_data+"./data/song_data/*/*/*/*.json"
    # read in song data to use for songplays table
    song_df = spark.read.json(song_df)
    print("--- song df data has been read in ---")

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    songplays_table = df['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent',  month(col("start_time")).alias("month"),
           year(col("start_time")).alias("year")]
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
    print("--- Basics of the songplays table have been set up ---")
    print('Printing some rows from the songplays_table')
    songplays_table.show(15)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays")
    #songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("--- songplays.parquet completed ---")
    print("*** process_log_data completed ***\n\nEND")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacitys3byron/"
    input_data =''
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
