# Project: Data Lake

## Summary:
The team at Sparkify is constantly evolving and creating new tasks for their Data Engineers. The attached documentation and files will take semi-structured metadata from a myraid of json files that reside in their Data Lake (hosted in S3) and transform them into a set of fact/dimensional tables. This will create a much more streamlined quering process for their analytics team and furthermore create business intelligence for smarted business initatives.

## Files/Functions:
1. dl.cfg:
    - Contains company KEY and SECRET KEY to access the proper AWS S3 bucket for storage of new tables. 
    - This is also used to access Data Lake json files that contain all metadata needs to create tables. This information resides in "s3a://udacity-dend/". 

2. etl.py:
    - Contains the logicall and programmatic process for the entire ETL pipeline from one semi-structured S3 bucket into tables that will reside in a new S3 bucket (myawsbucket20).
    - Functions in python 3 doc:
        <code>
        process_song_data(spark, input_data, output_data):
        """
        Overview:
        - Creates path to song_data and reads in json files iteratively 
        - Creates songs_table and artists_table
        Params: spark(uses SparkSession), input_data(udacity s3 filepath) and output_data(student s3 filepath)
        Returns: None
        """
        </code>
        <code>
        process_log_data(spark, input_data, output_data):
        """
        Overview:
        - Creates path to log_data and reads in json files
        - Filters log_data for only page == NextSong
        - Adds Timestamp and Datetime column to DataFrame
        - Creates users_table, time_table and songplays
        Params: spark(uses SparkSession), input_data(udacity s3 filepath) and output_data(student s3 filepath)
        Returns: None
        """
        </code>
        <code>
        main():
        spark = create_spark_session()
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://myawsbucket20/"

        process_song_data(spark, input_data, output_data)    
        process_log_data(spark, input_data, output_data)
        </code>
        
    - **To Run etl.py:** In terminal window input: python etl.py  PRESS ENTER
        
3. Test.ipynb
    - This Jupyter Notebook contains some code for testing. This will allow to test small batch of data to make sure the ETL pipeline is function properly. 


