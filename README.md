# Cloud Data Lake
Data Engineering Nanodegree Program - Project 4

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


The objective of this project is to build an ETL pipeline that extracts data from S3, processes it using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow Sparkify's analytics team to continue finding insights in what songs their users are listening to.


## Data
* Songs metadata: collection of JSON files that describes the songs such as title, artist name, duration, year, etc.
* Logs data: collection of JSON files where each file covers the users activities over a given day.


## Methodology
We will load input data (Songs metadata and Logs data) from S3, process the data into analytics tables (a central Fact table and Dimension tables) using Spark, and load them back into S3. This will create a star schema optimized for queries on song play analysis. This includes the following tables:

* Fact table: songplays
* Dimensions tables: songs, artist, users, time

![StarSchema](https://user-images.githubusercontent.com/6285945/76783209-75c9a400-67d7-11ea-8594-9710cae17048.PNG)

The three most important advantages of using Star schema are:

* Denormalized tables
* Simplified queries
* Fast aggregation
