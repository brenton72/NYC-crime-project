from __future__ import print_function

import sys
from operator import add
from csv import reader
from pyspark import SparkContext
import datetime

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    
    header = lines.first() #extract header
    data = lines.filter(lambda x: x != header) 

#Crimes by borough by hour
    def borough_get_hour(rows):
        try:
            hour = int(rows[2][0:2])           
        except ValueError:
            hour = 99     
        return ((rows[13],hour),1)
    rdd = data.map(lambda x: borough_get_hour(x))
    by_boro_hr = rdd.reduceByKey(add).sortByKey()
    by_boro_hr = by_boro_hr.map(lambda x: (str(x[0][0])+','+str(x[0][1])+','+str(x[1])))
    by_boro_hr.saveAsTextFile('by_boro_hr.out')

#Crimes by borough by year
    def borough_get_year(rows):
        try:
            year = int(rows[1][6:])           
        except ValueError:
            year = 99     
        return ((rows[13],year),1)

    rdd = data.map(lambda x: borough_get_year(x))
    by_boro_yr = rdd.reduceByKey(add).sortByKey()
    by_boro_yr = by_boro_yr.map(lambda x: (str(x[0][0])+','+str(x[0][1])+','+str(x[1])))
    by_boro_yr.saveAsTextFile('by_boro_yr.out')

    sc.stop()
