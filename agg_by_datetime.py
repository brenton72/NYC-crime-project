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
    
#Different timedate aggregations
    #rdd.saveAsTextFile('rdd_test.out')
    #by year
    by_year = rdd.map(lambda x: (x[1][0],1)).reduceByKey(add).sortBy(lambda x: -x[1])
    by_year.saveAsTextFile('by_year.out')
    
    #by month
    by_month = rdd.map(lambda x: (x[1][1],1)).reduceByKey(add).sortBy(lambda x: -x[1])
    by_month.saveAsTextFile('by_month.out')
        
    #by year-month
    by_ym = rdd.map(lambda x: ('%s-%s' %(x[1][0],x[1][1]),1)).reduceByKey(add).sortByKey()
    by_ym.saveAsTextFile('by_ym.out')
    
    #by year-month-day
    by_ymd = rdd.map(lambda x: ('%s-%s-%s' %(x[1][0],x[1][1],x[1][2]),1)).reduceByKey(add).sortByKey()
    by_ymd.saveAsTextFile('by_ymd.out')
    
    #by hour
    by_hour = rdd.map(lambda x: (x[1][3],1)).reduceByKey(add).sortBy(lambda x: -x[1])
    by_hour.saveAsTextFile('by_hour.out')
    


    sc.stop()
