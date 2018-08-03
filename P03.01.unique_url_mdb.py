#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 03/31/2018
# Descr. : Inserting data on MariaDB DataBase

from pyspark import SparkContext, SparkConf
from datetime import datetime
import mysql.connector as mariadb


# Define spark context.
conf = SparkConf().setMaster("local[*]").setAppName("URLCountPython")
#conf = SparkConf().setAppName("URLCountMDB")
sc = SparkContext(conf = conf)

# Function to split lines into variables.
def parse_log_line_w5(line):
    (uuid, timestamp, url, user) = line.strip().split(" ")
    hour = timestamp[0:13]
    return (hour, url, 1)

# Get all files from a directory 
#text_file = sc.textFile("file:///home/michelbenites/inputlab7/*.txt")
#text_file = sc.textFile("file:///home/michelbenites/inputlog/*.txt")
#text_file = sc.textFile("file:///home/centos/inputlog/*.txt")
text_file = sc.textFile("file:///home/centos/inputlab7/*.txt")

#text_file = sc.textFile("inputlab7")

# Create RDD with distinct data.
pairRDD = text_file.map(parse_log_line_w5).distinct()

# Create a new RDD only with Hour and Count.
uniqueRDD = pairRDD.map(lambda x: (x[0],1))

# Sum the same key.
counts = uniqueRDD.reduceByKey(lambda a, b: a + b)

# Save the result on the directory.
#counts.coalesce(1).saveAsTextFile("outputurl7")
counts.coalesce(1).saveAsTextFile("file:///home/centos/outputurl7")

mariadb_connection = mariadb.connect(user='root', password='password', database='HW08')
cursor = mariadb_connection.cursor()


for x in counts.collect():    
    date_hour = "'"+str(x[0] + ':00:00')+"'"
    date_day  = "'"+str(x[0][:10])+"'"
    count_u   = int(x[1])
    cmd = "INSERT INTO btURL_q1 (DATE_HOUR, COUNT_UNIQUE, DATE_DAY) VALUES (" + date_hour + ", " + str(count_u) + ", " + date_day + ")"
    cursor.execute(cmd) 

# Commit and close the Database    
mariadb_connection.commit()
mariadb_connection.close()
