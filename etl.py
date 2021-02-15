import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id


# config = configparser.ConfigParser()
# config.read('~/credentials.cfg')
#
# os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
# os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_table")
    songs_table = spark.sql("""
        SELECT DISTINCT song_id,
        title, 
        artist_id, 
        year, 
        duration
        FROM song_table
    """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
        .partitionBy("year", "artist_id")\
        .mode('overwrite')\
        .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df['artist_id',
                       'artist_name',
                       'artist_location',
                       'artist_latitude',
                       'artist_longitude']

    # write artists table to parquet files
    artists_table.write\
        .mode('overwrite')\
        .parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df_filtered = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df_filtered['userId',
                              'firstName',
                              'lastName',
                              'gender',
                              'level']

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df_filtered = df_filtered.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    @udf
    def get_datetime(value):
        return datetime.fromtimestamp(value)
    df_date = df_filtered.withColumn('date', get_datetime(df_filtered.timestamp))

    # extract columns to create time table
    @udf
    def datetimeToYear(value):
        return value.year if value is not None else None

    @udf
    def datetimeToHour(value):
        return value.hour if value is not None else None

    # weekofyear
    @udf
    def datetimeToWeek(value):
        return value.strftime("%W") if value is not None else None

    # dayofmonth
    @udf
    def datetimeToDay(value):
        return value.strftime("%d") if value is not None else None

    @udf
    def datetimeToMonth(value):
        return value.strftime("%B") if value is not None else None

    @udf
    def datetimeToWeekDay(value):
        return value.strftime("%A") if value is not None else None

    df_date = df_date.withColumn('year', datetimeToYear(df_date.date))

    df_date = df_date.withColumn('hour', datetimeToHour(df_date.date))

    df_date = df_date.withColumn('week', datetimeToWeek(df_date.date))

    df_date = df_date.withColumn('day', datetimeToDay(df_date.date))

    df_date = df_date.withColumn('month', datetimeToMonth(df_date.date))

    df_date = df_date.withColumn('weekday', datetimeToWeekDay(df_date.date))

    df_date.createOrReplaceTempView("date_table")

    time_table = spark.sql("""
          SELECT DISTINCT timestamp as start_time, 
          day, 
          year, 
          hour, 
          week, 
          month, 
          weekday     
          FROM date_table
          GROUP BY timestamp, day, year, hour, week, month, weekday
    """)

    # write time table to parquet files partitioned by year and month
    time_table.write\
        .partitionBy("year", "month")\
        .mode('overwrite')\
        .parquet(os.path.join(output_data, 'time'))

    # songplays dataframe
    songplays_df = df_filtered['timestamp',
                               'userId',
                               'level',
                               'song',
                               'artist',
                               'sessionId',
                               'location',
                               'userAgent']

    # read in song data to use for songplays table
    songs_table = spark.read.parquet(os.path.join(output_data, 'songs'))
    songplays_df = songplays_df.join(songs_table, songplays_df.song==songs_table.title, how='left')

    # extract columns from joined song and log datasets to create songplays table
    songplays_df = songplays_df['timestamp',
                                'userId',
                                'level',
                                'song_id',
                                'artist_id',
                                'sessionId',
                                'location',
                                'userAgent']
    songplays_df = songplays_df.withColumn('songplay_id', monotonically_increasing_id())
    songplays_df = songplays_df['songplay_id',
                                'timestamp',
                                'artist_id',
                                'song_id',
                                'userId',
                                'level',
                                'sessionId',
                                'location',
                                'userAgent']
    songplays_df = songplays_df.join(time_table, songplays_df.timestamp == time_table.start_time, how='left')
    songplays_df = songplays_df['songplay_id',
                                'timestamp',
                                'artist_id',
                                'song_id',
                                'userId',
                                'level',
                                'sessionId',
                                'location',
                                'userAgent',
                                'month',
                                'year']
    songplays_df.createOrReplaceTempView("songplays_table")

    songplays_table = spark.sql("""
              SELECT songplay_id, 
              timestamp as start_time, 
              artist_id, 
              song_id, 
              userId,
              level,
              sessionId,
              location,
              userAgent,
              month,
              year    
              FROM songplays_table
        """)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-aws-parquet/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
