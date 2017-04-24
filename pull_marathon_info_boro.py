from __future__ import print_function

import sys
from operator import add
from csv import reader
from pyspark import SparkContext
from datetime import datetime, timedelta
import datetime
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
        NYC_Marathon_list = [(11, 1, 2015), (11, 2, 2014), (11, 3, 2013), (11, 2, 2012), (11, 6, 2011),  (11, 7, 2010), (11, 1, 2009), (11, 2, 2008), (11, 4, 2007), (11, 5, 2006)]
        NYC_Half_Marathon_list = [(8, 27, 2006), (8, 5, 2007), (7, 27, 2008), (8, 26, 2009), (3, 21, 2010), (3, 20, 2011), (3, 18, 2012), (3, 17, 2013),(3, 16, 2014), (3, 15, 2015)]
        NYC_Marathons_all = NYC_Marathon_list + NYC_Half_Marathon_list
        NYC_Marathons_dts = [datetime.datetime(x[2],x[0],x[1]) for x in NYC_Marathons_all]
        
        try:
            month, day, year = (int(x) for x in rows[1].split('/'))
            dt = datetime.datetime(year, month, day)
            lwk = dt - timedelta(days=7)
            if dt in NYC_Marathons_dts:
                test_group = 1
            elif lwk in NYC_Marathons_dts:
                test_group = 0
            else:
                dt = datetime.datetime(1000, 1, 1)
                test_group = -9
        except:
            test_group = -99
            dt = datetime.datetime(1000, 1, 1)
        try:
            hour = int(rows[2][0:2])           
        except ValueError:
            hour = 99     
        if dt == datetime.datetime(1000, 1, 1):
            hour = 99
            wkday = -99
        return ((rows[13],dt,test_group),1)
    rdd = data.map(lambda x: get_wkday_hour(x))
    by_type_wkday_hr = rdd.reduceByKey(add).sortByKey()
    by_type_wkday_hr.saveAsTextFile('marathon_info_boro.out')
    sc.stop()
