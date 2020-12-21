# Increasing Sparkify's Anayltical Capabilities

## Intro: 
The anayltics team needs to increase the read performance of their data. As a new startup, they have an immature data structures which increases read time and creates frustrations. I've created an ETL pipeline with Python that organizes the data into a postgresql data warehouse. I've choosen to denormalize into a star schema for great read performance and faster customer insights.

## File respository:
**create_tables.py** - implements create_database, create_tables and drop_tables functions.<br>
**sql_queries.py** - lists out specific SQL queries needed to create and update tables with organized/formatted data.<br>
**etl.py** - the ETL pipeline that extracts current data formatted in JSON and reorganizes in into tables specified in create_tables and below

## Tables created:
**songs** - organizes song information into *(song_id varchar NOT NULL, title varchar, artist_id varchar NOT NULL, year int, duration float)* format. <br>
**artists** - organizes arist informaiton into *(artist_id varchar UNIQUE NOT NULL, name varchar, location varchar, latitude varchar, longitude varchar)* format. <br>
**time** - time information formatted from log files: *(start_time timestamp UNIQUE NOT NULL, hour int, day int, week int, month int, year int, weekday int)*<br>
**users** - user information organized into *(user_id varchar UNIQUE, first_name varchar, last_name varchar, gender text, level varchar)* columns and format.<br>
**songplays** - Fact table formatted *(songplay_id SERIAL PRIMARY KEY, start_time timestamp NOT NULL, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int NOT NULL, location varchar, user_agent varchar, UNIQUE (start_time, user_id, session_id))*

## Interest: 
**What songs do users listen to?**<br>
The analytics teams wants to better understand what their current users interests are so they create a better user experience and provide them with music more suited to there liking.<br>

Ex. Sql query to achieve insights to retrieve simply the song_id and corresponding user_id: *SELECT user_id, song_id FROM songplays*

**OR**

Ex. query actual song names: *SELECT user_id, songs.title FROM songplays JOIN songs ON songplays.song_id = songs.song_id*







