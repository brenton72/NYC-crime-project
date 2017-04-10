from __future__ import print_function

import sys
from operator import add
from csv import reader
from pyspark import SparkContext
from datetime import datetime, timedelta
import datetime
import pytz
import time

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
    
#Type/Weekday-Hour aggregation
    def get_wkday_hour(rows):
        try:
            month, day, year = (int(x) for x in rows[1].split('/'))    
            dt = datetime.date(year, month, day)
            wkday = dt.weekday()
        except:
            wkday = -99
        
        try:
            tz = pytz.timezone('US/Eastern')
            month, day, year = (int(x) for x in rows[1].split('/'))
            dt = datetime.datetime(year, month, day)
            dstime = int(bool(tz.dst(dt, is_dst=None)))
        except:
            dstime = -9
        try:
            hour = int(rows[2][0:2])           
        except ValueError:
            hour = 99     
        return ((rows[11],wkday,dstime,hour),1)
    rdd = data.map(lambda x: get_wkday_hour(x))
    by_type_wkday_hr = rdd.reduceByKey(add).sortByKey()
    by_type_wkday_hr.saveAsTextFile('by_type_wkday_hr_dst.out')
    
    sc.stop()
