# Data Warehouse Build

## Purpose of Migration
Sparkify's data analytics team needs a better way to query their user data that is fast, reliable, scalable and accessible at all of their locations easily. The answer is to create a data warehouse hosted on AWS Redshift. Using Amazon's already robust infrastructure will streamline the warehouse build process and eliminate the expense of on-prem hardware and network hardware. 

## Database Schema
We will be transferring the 3rd normal form databases into a star schema for Redshift. Below are the fact and dimension (dim) tables we'll be using as well as listed columns and data types. This should allow for an easy understanding for business stakeholders as the data grows and becomes more complex. 

**factSongplays:**<br>
1. songplay_key (INTEGER IDENTITY(0,1))
2. start_time (TIMESTAMP NOT NULL) 
3. user_key (INTEGER NOT NULL) 
4. song_key (VARCHAR NOT NULL) 
5. artist_key (VARCHAR NOT NULL) 
6. session_id (INTEGER NOT NULL) 
7. location (VARCHAR)
8. user_agent (VARCHAR)
9. length (FLOAT)

**dimUsers:**<br>
1. user_key (INT PRIMARY KEY)
2. first_name (VARCHAR)
3. last_name (VARCHAR)
4. gender (CHAR(1))
5. level (VARCHAR)

**dimSongs:**<br>
1. song_key (VARCHAR PRIMARY KEY) 
2. title (VARCHAR)
3. artist_name (VARCHAR) 
4. year (SMALLINT)
5. duration (FLOAT)

**dimArtists:**<br>
1. artist_key (VARCHAR PRIMARY KEY) 
2. artist_name (VARCHAR) 
3. location (VARCHAR) 
4. latitude (FLOAT) 
5. longitude (FLOAT)

**dimTime:**<br>
1. start_time (TIMESTAMP PRIMARY KEY) 
2. hour (INTEGER NOT NULL) 
3. day (INTEGER NOT NULL) 
4. week (INTEGER NOT NULL) 
5. month (INTEGER NOT NULL) 
6. year (INTEGER NOT NULL) 
7. weekday (INTEGER NOT NULL)

## ETL Outline and Functions
Firstly, all sql queries needed for INSERT, COPY, CREATE and DROP are all in sql_queries.py. Below are the etl functions used to create the datawhare in Redshift.

**etl.py:**<br>
<code>
def load_staging_tables(cur, conn):
    """
    - Loads staging tables into Redshift using queries in sql_queries
    """ <br>
def insert_tables(cur, conn):
    """
    - Inserts data from staging tables into below tables:
    factSongplays, dimTime, dimSongs, dimArtists, dimUsers
    - Done with query found in sql_queries
    """
</code>

**create_tables.py:**<br>
<code>
def drop_tables(cur, conn):
    """
    Drops tables with queries given in sql_queries.
    """<br>
def create_tables(cur, conn):
    """
    - Creates tables with queries given in sql_queries.
    """
</code>

## Conclusion
Amazon Redshift makes it very easy to query all of our data with their build into query editor in the cloud. This should make data analytics and customer insight more steamlined as the data warehouse is optimized for OLAP and won't be bogging down our customer OLTP systems.  