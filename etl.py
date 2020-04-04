import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

try:
    config = configparser.ConfigParser()
    config.read('dl.cfg')
except Exception as e:
    error(f"Error reading config file: {e}")
    exit()

try:
    os.environ['AWS_ACCESS_KEY_ID']=config['KEY']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['SECRET']['AWS_SECRET_ACCESS_KEY']
except Exception as e:
    error(f"Error setting AWS credential Environment Variables: {e}")
    exit()

def create_spark_session():
    """
    Get or create a spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads song data from s3 bucket and extracts relevant fields to create dimension tables - songs and artists.
    Also writes dimension tables to s3 bucket in parquet format
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    try:
        df = spark.read.json(song_data)
    except Exception as e:
        error(f"Error reading songs data files while processing songs data: {e}")
        exit()

    # extract columns to create songs table
    try:
        songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                        .dropDuplicates()
    except Exception as e:
        error(f"Error creating songs_table dataframe: {e}")
        exit()
    
    # write songs table to parquet files partitioned by year and artist
    try:
        songs_table.write.partitionBy('year', 'artist_id') \
                   .parquet(path = output_data + '/songs', mode = 'overwrite')
    except Exception as e:
        error(f"Error writing songs table to S3: {e}")
        exit()

    # extract columns to create artists table
    try:
        artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude').dropDuplicates()
    except Exception as e:
        error(f"Error creating artists_table dataframe: {e}")
        exit()
    
    # write artists table to parquet files
    try:
        artists_table.write.parquet(path = output_data + '/artists', mode = 'overwrite')
    except Exception as e:
        error(f"Error writing artists table to S3: {e}")
        exit()

def process_log_data(spark, input_data, output_data):
    """
    Reads log data from s3 bucket and extracts relevant fields to create dimension tables - users and time & fact table - songplays.
    Also writes dimension and fact tables to s3 bucket in parquet format
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    try:
        df = spark.read.json(log_data)
    except Exception as e:
        error(f"Error reading log data files: {e}")
        exit()
    
    # filter by actions for song plays
    try:
        df = df.filter(df.page == 'NextSong')
        df = df.withColumnRenamed('userId', 'user_id') \
               .withColumnRenamed('firstName', 'first_name') \
               .withColumnRenamed('lastName', 'last_name') \
               .withColumnRenamed('userAgent', 'user_agent') \
               .withColumnRenamed('sessionId', 'session_id') \
               .withColumnRenamed('itemInSession', 'item_in_session')
    except Exception as e:
        error(f"Error filtering log dataframe: {e}")
        exit()
        
    # extract columns for users table    
    try:
        users_table = df.select('user_id', 'first_name', 'last_name',
                            'gender', 'level').dropDuplicates()
    except Exception as e:
        error(f"Error creating users_table dataframe: {e}")
        exit()
        
    # write users table to parquet files
    try:
        users_table.write.parquet(os.path.join(output_data, '/users'), 'overwrite')
    except Exception as e:
        error(f"Error writing users table to S3: {e}")
        exit()
        
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    
    try:
        df = df.withColumn('start_time', get_timestamp(df.ts))
    except Exception as e:
        error(f"Error adding 'timestamp' column to log dataframe: {e}")
        exit()
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    df = df.withColumn('hour', F.hour(df.datetime))
    df = df.withColumn('day', F.dayofmonth(df.datetime))
    df = df.withColumn('week', F.weekofyear(df.datetime))
    df = df.withColumn('month', F.month(df.datetime))
    df = df.withColumn('year', F.year(df.datetime))
    df = df.withColumn('weekday', F.dayofweek(df.datetime))
    
    
    # extract columns to create time table
    try:
        time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') \
                       .dropDuplicates()
    except Exception as e:
        error(f"Error creating time_table dataframe: {e}")
        exit()
    
    
    # write time table to parquet files partitioned by year and month
    try:
        time_table.write.partitionBy('year', 'month') \
                  .parquet(path = output_data + '/time', mode = 'overwrite')
    except Exception as e:
        error(f"time_table write error: {e}")
        exit()

    # read in song data to use for songplays table
    try:
        song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    except Exception as e:
        error(f"Error reading songs data files while processing log data: {e}")
        exit()
        
    # extract columns from joined song and log datasets to create songplays table 
    try:
        songplays_table = df.join(song_df, df.artist == song_df.artist_name)
        songplays_table = songplays_table.withColumn("songplay_id",F.monotonically_increasing_id())
        songplays_table = songplays_table.select('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 
                                      'artist_id', 'session_id', 'location', 'user_agent')
    except Exception as e:
        error(f"Error creating songplays dataframe: {e}")
        exit()
        

    # write songplays table to parquet files partitioned by year and month
    try:
        songplays_table.write.partitionBy('year', 'month') \
                       .parquet(path = output_data + '/song_plays', mode = 'overwrite')
    except Exception as e:
        error(f"Error writing songplays table to S3: {e}")
        exit()
        
def main():
    """
    1.) Get or create a spark session
    1.) Read the song and log data from s3
    2.) take the data and transform them into tables
    which will then be written to parquet files
    """
    
    try:
        spark = create_spark_session()
    except Exception as e:
        error(f"Error creating SparkSession {e}")
        exit()
        
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://gy-nano-bkt/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
