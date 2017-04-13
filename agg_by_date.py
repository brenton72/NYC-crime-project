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

#create rdd (CMPLNT_NUM, (year, month, day, hour))  where datetime values are from CMPLNT_FR
    def extract_date(rows):
        try:
            year = int(rows[1][6:])
            day = int(rows[1][3:5])
            month = int(rows[1][0:2])
        except ValueError:
            year = 0
            day = 0
            month = 0
        return (rows[0],(year,month,day))

    def extract_hour_min(rows):
        try:
            hour = int(rows[2][0:2])
            minute = int(rows[2][3:5])
        except ValueError:
            hour = 0
            minute = 0
        return (rows[0],(hour, minute))

    rdd = data.map(lambda x: extract_date(x))
    rdd_time = data.map(lambda x: extract_hour_min(x))
    
    by_year = rdd.map(lambda x: (x[1][0],1)).reduceByKey(add).sortBy(lambda x: -x[1])
    by_year = by_year.map(lambda x: (str(x[0])+','+str(x[1])))
    by_year.saveAsTextFile('by_year_only.out')
    
    #by month
    by_month = rdd.map(lambda x: (x[1][1],1)).reduceByKey(add).sortBy(lambda x: -x[1])
    by_month = by_month.map(lambda x: (str(x[0])+','+str(x[1])))
    by_month.saveAsTextFile('by_month_only.out')
        
    #by year-month
    by_ym = rdd.map(lambda x: ('%s-%s' %(x[1][0],x[1][1]),1)).reduceByKey(add).sortByKey()
    by_ym = by_ym.map(lambda x: (x[0], x[0].split('-')[0], x[0].split('-')[1], x[1]))
    by_ym = by_ym.map(lambda x: (str(x[0])+','+str(x[1])+','+str(x[2])+','+str(x[3])))
    by_ym.saveAsTextFile('by_ym_only.out')
    
    #by year-month-day
    by_ymd = rdd.map(lambda x: ('%s-%s-%s' %(x[1][0],x[1][1],x[1][2]),1)).reduceByKey(add).sortByKey()
    by_ymd = by_ymd.map(lambda x: (x[0], x[0].split('-')[0], x[0].split('-')[1], x[0].split('-')[2], x[1]))
    by_ymd = by_ymd.map(lambda x: (str(x[0])+','+str(x[1])+','+str(x[2])+','+str(x[3])+','+str(x[4])))
    by_ymd.saveAsTextFile('by_ymd_only.out')

    #by hour
    by_hour = rdd_time.map(lambda x: (x[1][0],1)).reduceByKey(add).sortBy(lambda x: -x[1])
    by_hour = by_hour.map(lambda x: (str(x[0])+','+str(x[1])))
    by_hour.saveAsTextFile('by_hour_only.out')

    #by hour-minute 
    by_hm = rdd_time.map(lambda x: ('%s-%s' %(x[1][0],x[1][1]),1)).reduceByKey(add).sortByKey()
    by_hm = by_hm.map(lambda x: (x[0], x[0].split('-')[0], x[0].split('-')[1], x[1]))
    by_hm = by_hm.map(lambda x: (str(x[0])+','+str(x[1])+','+str(x[2])+','+str(x[3])))
    by_hm.saveAsTextFile('by_hm_only.out')

    sc.stop()
