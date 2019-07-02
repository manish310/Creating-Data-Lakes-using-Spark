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

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']

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
    print("Processing Song Data File ...\n")
    df = spark.read.json(song_data)
    
    print(" -- Spark able to read JSON file successfully!")
    print(" -- Number of records in song_data file is: {} ".format(df.count()))
#     print(df.printSchema())
#     print(df.take(5))
    
    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
#     print(songs_table.printSchema())
    print(" -- songs_table data extracted successfully!")
    
    songs_table=songs_table.dropDuplicates()
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet\
                    (os.path.join(output_data,"song_table/"),mode="overwrite")
    print(" -- songs_table data written successfully!")
    
    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location',\
                               'artist_latitude','artist_longitude'])
    print(" -- artists_table data extracted successfully!")
    
    artists_table=artists_table.dropDuplicates()
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artist_table/"),mode="overwrite")
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
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    print("Processing Log Data File ...\n")
    df = spark.read.json(log_data)
    print(" -- Spark able to read JSON file successfully!")
    #print(" -- Number of records in log_data file is: {} ".format(df.count())) 
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")
    print(" -- Number of records in log_data file is: {} ".format(df.count()))
    
    # extract columns for users table    
    users_table =df.select(['userId','firstname','lastname','gender','level'])
    print(" -- users_table data extracted successfully!")
    
    #users_table=users_table.dropDuplicates()
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,"users_table/"),mode="overwrite")
    print(" -- users_table data written successfully!")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0),TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("datetime", get_timestamp(df.ts))
    #print(df.take(2))
    
    # extract columns to create time table
    time_table = df.select('timestamp',\
                           hour('datetime').alias('hour'),\
                           dayofmonth('datetime').alias('day'),\
                           weekofyear('datetime').alias('week'),\
                           month('datetime').alias('month'),\
                           year('datetime').alias('year'),\
                           date_format('datetime','F').alias('weekday')
                          )
    print(" -- time_table data extracted successfully!")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data,"time_table/"),mode="overwrite")
    print(" -- time_table data written successfully!")
    
    # read in song data to use for songplays table
    song_df = spark.read.option("basePath", os.path.join(output_data,"song_table")).\
              load(os.path.join(output_data,"song_table/*"))
    artist_df = spark.read.option("basePath", os.path.join(output_data,"artist_table")).\
              load(os.path.join(output_data,"artist_table/*"))
    
    song_df.createOrReplaceTempView("song")
    df.createOrReplaceTempView("log")
    artist_df.createOrReplaceTempView("artist")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT timestamp(l.ts) AS start_time,
                                    year(timestamp(l.ts)) AS year,
                                    month(timestamp(l.ts)) AS month,
                                    l.userid,
                                    l.level,
                                    s.song_id,
                                    a.artist_id,
                                    l.sessionid,
                                    l.location,
                                    l.useragent
                                    FROM song  s
                                    LEFT OUTER JOIN artist a ON a.artist_id=s.artist_id
                                    LEFT OUTER JOIN log l ON (s.title=l.song
                                    AND a.artist_name=l.artist
                                    AND s.duration=l.length)
                                 """)
    print(" -- songplays_table data extracted successfully!")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet\
                    (os.path.join(output_data,"songplays_table/"),mode="overwrite")
    print(" -- songplays_table data written successfully!")
    
    
# reading all files list in the directory
def read_data(spark,output_data,table_name):
    """
    Description: This function can be used to read the file in the filepath (data/log_data) 
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        spark: the spark session object.  
        output_data:
        table_name:
    Returns:
        None
    """ 
    df=spark.read.option("basePath", os.path.join(output_data,table_name)).\
              load(os.path.join(output_data,table_name,"*"))
    df.show(n=5)

# reading all files list in the directory
def read_summ(spark,output_data,table_name):
    """
    Description: This function can be used to read the file in the filepath (data/log_data) 
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        spark: the spark session object.  
        output_data:
        table_name:
    Returns:
        None
    """ 
    df=spark.read.load(os.path.join(output_data,table_name))
    return (df.count())

def main():
    #SparkContext.setLogLevel('ERROR')
    spark = create_spark_session()
    print("\n -- Spark session created successfully!\n")
    print(" *** Spark Version is :: {} ***\n".format(spark.sparkContext.version))
    input_data =  "s3a://udacity-dend/"
    output_data = "s3a://dend-data-lake-310/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    t_list=['song_table','artist_table','users_table','time_table','songplays_table']
    print("\n -- Data Quality Checks \n")
    for t in t_list:
        print(" {}:".format(t))
        read_data(spark,output_data,t)
    
    print("\n******************************TABLE STATS*****************************\n")
    for t in t_list:
        print("Numbers of records in {} is : {}".\
               format(t,read_summ(spark,output_data,t)))
    print("\n**********************************************************************\n")

if __name__ == "__main__":
    main()
