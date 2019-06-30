####
# Importing all the necessary libraries
####
import configparser
from datetime import datetime
import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

# ----------------------------------------------------------------------------

# getting all files list in the directory

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):  
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files

# # reading all files list in the directory
# def read_data():
#     import pyarrow.parquet as pq
#     import s3fs
#     s3 = s3fs.S3FileSystem()
#     pandas_dataframe = pq.ParquetDataset('s3://your-bucket/', filesystem=s3).read_pandas().to_pandas()


# ----------------------------------------------------------------------------

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']
#YOUR_DATAFRAME.write.parquet("s3a://YOUR_BUCKET/SUB_DIRECTORY",mode="overwrite")

# ----------------------------------------------------------------------------

def create_spark_session():
    """
    Description: This function can be used to create spark session.

    Arguments:
        None
    Returns:
        spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    # sparkContext.setLogLevel('ERROR')
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info and used to populate the songs and artists dim tables.

    Arguments:
        spark: the spark session object. 
        input_data: song data file path. 
        output_data:
    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    print("Processing Song Data File ...\n")
    print(" -- Spark able to read JSON file successfully!\n")
    print(" -- Number of records in song_data file is: {} \n".format(df.count()))
#     print(df.printSchema())
#     print(df.take(5))
    
    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
#     print(songs_table.printSchema())
    print(" -- songs_table data extracted successfully!\n")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(os.path.join(output_data,"song_table/"))
    print(" -- songs_table data written successfully!\n")
    
    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location',\
                               'artist_latitude','artist_longitude'])
    print(" -- artists_table data extracted successfully!\n")
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artist_table/"))
    print(" -- artists_table data written successfully!\n")

def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the file in the filepath (data/log_data) 
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        spark: the spark session object. 
        input_data: song data file path. 
        output_data:
    Returns:
        None
    """   
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/2018/11/*.json")

    # read log data file
    print("Processing Log Data File ...\n")
    df = spark.read.json(log_data)
    print(" -- Spark able to read JSON file successfully!\n")
    print(" -- Number of records in log_data file is: {} \n".format(df.count())) 
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")
    print(" -- Number of records in log_data file is: {} \n".format(df.count()))
    
    # extract columns for users table    
    users_table =df.select(['userId','firstname','lastname','gender','level'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,"users_table/"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0),TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("datetime", get_timestamp(df.ts))
    print(df.take(2))
    
    # extract columns to create time table
    time_table = df.select('timestamp',\
                           hour('datetime').alias('hour'),\
                           dayofmonth('datetime').alias('day'),\
                           weekofyear('datetime').alias('week'),\
                           month('datetime').alias('month'),\
                           year('datetime').alias('year'),\
                           date_format('datetime','F').alias('weekday')
                          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,"time_table/"))

    # read in song data to use for songplays table
    song_df = ""

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ""

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    #SparkContext.setLogLevel('ERROR')
    spark = create_spark_session()
    print(" -- Spark session created successfully!\n")
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-data-lake-310/"
    
#     process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
#     read_data(output_data,)
#     import boto
#     s3 = boto.connect_s3()
#     bucket = s3.get_bucket(output_data)
#     bucketListResultSet = bucket.list(prefix="")
#     result = bucket.delete_keys([key.name for key in bucketListResultSet])

if __name__ == "__main__":
    main()
