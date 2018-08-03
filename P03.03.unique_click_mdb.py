#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 03/31/2018
# Descr. : Count unique clicks per url by hour and inserting on MariaDB Database
from pyspark import SparkContext, SparkConf
from datetime import datetime
import mysql.connector as mariadb

# Define spark context.
conf = SparkConf().setMaster("local[*]").setAppName("URLCountPython")
#conf = SparkConf().setAppName("ClickCountPython")
sc = SparkContext(conf = conf)

# Function to split lines into variables.
def parse_log_line_w5(line):
    (uuid, timestamp, url, user) = line.strip().split(" ")
    hour = timestamp[0:13]
    # Concatenate hour, url and user, in order to create a key to use in redeucebykey action.
    return (hour + ',' + url + ',' + user, uuid, 1)

# Get all files from a directory 
text_file = sc.textFile("file:///home/centos/inputlab7/*.txt")
#text_file = sc.textFile("inputlab7")

# Create RDD with distinct data.
pairRDD = text_file.map(parse_log_line_w5).distinct()

# Create a new RDD only with Hour and Count.
uniqueRDD = pairRDD.map(lambda x: (x[0],1))

# Sum the same key.
counts = uniqueRDD.reduceByKey(lambda a, b: a + b)

# Save the result on the directory.
counts.coalesce(1).saveAsTextFile("file:///home/centos/outputurl7")

mariadb_connection = mariadb.connect(user='root', password='password', database='HW08')
cursor = mariadb_connection.cursor()

for x in counts.collect():    
    date_hour, url, user = x[0].split(",")
    date_hour = "'"+str(date_hour + ':00:00')+"'"
    date_day  = date_hour[:11]+"'"
    url       = "'"+url+"'"
    user      = "'"+user+"'"
    count_u   = int(x[1])
    cmd = "INSERT INTO btClick_q3 (DATE_HOUR, URL, USER, COUNT_UNIQUE, DATE_DAY) VALUES (" + date_hour + ", " + url + ", " + user + ", " + str(count_u) + ", " + date_day + ")"
    cursor.execute(cmd) 

# Commit and close the Database    
mariadb_connection.commit()
mariadb_connection.close()

