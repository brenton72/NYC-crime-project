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
#update column number
    #col_num=5
#create rdd (CMPLN_NUM, (year, month, day, hour))  where datetime values are from CMPLNT_FR
    def extract_dt(rows):
        try:
            year = int(rows[1][6:])
            day = int(rows[1][3:5])
            month = int(rows[1][0:2])
            hour = int(rows[2][0:2])           
        except ValueError:
            year = 0
            day = 0
            month = 0
            hour = 99     
        return (rows[0],(year,month,day,hour))
    rdd = data.map(lambda x: extract_dt(x))
    
    
#Borough/ymd aggregation
    def get_ymd(rows):
        try:
            month, day, year = (int(x) for x in rows[1].split('/'))
            dt = (year, month, day)
        except ValueError:
            dt = (-9,-9,-9)     
        return ((rows[13],dt),1)
    rdd = data.map(lambda x: get_ymd(x))
    by_boro_ymd = rdd.reduceByKey(add).sortByKey()
    by_boro_ymd.saveAsTextFile('by_boro_ymd.out')
    
    sc.stop()
