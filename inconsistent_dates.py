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
    
#Report Date before Complaint From Date    
    def rpt_before_cmplnt_fr(rows):
        try:
            cmplnt_year = int(rows[1][6:])
            cmplnt_day = int(rows[1][3:5])
            cmplnt_month = int(rows[1][0:2])
            cmplnt_fr = datetime.datetime(cmplnt_year,cmplnt_month,cmplnt_day)
            
            year = int(rows[5][6:])
            day = int(rows[5][3:5])
            month = int(rows[5][0:2])
            rpt = datetime.datetime(year,month,day)
            
            if cmplnt_year > 2015 or cmplnt_year <1965:
                flag = 'N'
            elif rpt < cmplnt_fr:
                flag = 'Y'
            else:
                flag = 'N'        
        except ValueError:
            flag = 'N'
            cmplnt_fr = 'N/A'
            rpt = 'N/A'
    
        return (flag,(cmplnt_fr, rpt))            
             
    data1 = data.map(lambda x: rpt_before_cmplnt_fr(x))
    list_rpt_before_cmplnt_fr = data1.filter(lambda x: x[0] == 'Y')
    list_rpt_before_cmplnt_fr.saveAsTextFile('list_rpt_before_cmplnt_fr.out')
    
    sum_rpt_before_cmplnt_fr = data1.map(lambda x: (x[0], 1)).reduceByKey(add)
    sum_rpt_before_cmplnt_fr.saveAsTextFile('sum_rpt_before_cmplnt_fr.out')

#Report Date before Complaint To Date    
    def rpt_before_cmplnt_to(rows):
        try:
            cmplnt_year = int(rows[3][6:])
            cmplnt_day = int(rows[3][3:5])
            cmplnt_month = int(rows[3][0:2])
            cmplnt_to = datetime.datetime(cmplnt_year,cmplnt_month,cmplnt_day)
            
            year = int(rows[5][6:])
            day = int(rows[5][3:5])
            month = int(rows[5][0:2])
            rpt = datetime.datetime(year,month,day)
            
            if cmplnt_year > 2015 or cmplnt_year <1965:
                flag = 'N'
            elif rpt < cmplnt_to:
                flag = 'Y'
            else:
                flag = 'N'        
        except ValueError:
            flag = 'N'
            cmplnt_to = 'N/A'
            rpt = 'N/A'
    
        return (flag,(cmplnt_to, rpt))            
             
    data2 = data.map(lambda x: rpt_before_cmplnt_to(x))
    list_rpt_before_cmplnt_to = data2.filter(lambda x: x[0] == 'Y')
    list_rpt_before_cmplnt_to.saveAsTextFile('list_rpt_before_cmplnt_to.out')
    
    sum_rpt_before_cmplnt_to = data2.map(lambda x: (x[0], 1)).reduceByKey(add)
    sum_rpt_before_cmplnt_to.saveAsTextFile('sum_rpt_before_cmplnt_to.out')
    
#Complaint To Date before Complaint From Date    
    def cmplnt_to_before_cmplnt_fr(rows):
        try:
            cmplnt_to_year = int(rows[3][6:])
            cmplnt_to_day = int(rows[3][3:5])
            cmplnt_to_month = int(rows[3][0:2])
            cmplnt_to_hour = int(rows[3][0:2])
            cmplnt_to_min = int(rows[3][3:5])
            cmplnt_to_sec = int(rows[3][6:8])
            cmplnt_to = datetime.datetime(cmplnt_to_year,cmplnt_to_month,cmplnt_to_day,cmplnt_to_hour,cmplnt_to_min,cmplnt_to_sec)
            
            year = int(rows[1][6:])
            day = int(rows[1][3:5])
            month = int(rows[1][0:2])
            hour = int(rows[1][0:2])
            min = int(rows[1][3:5])
            sec = int(rows[1][6:8])
            cmplnt_fr = datetime.datetime(year,month,day,hour,min,sec)
            
            if cmplnt_to_year > 2015 or cmplnt_to_year <1965 or year > 2015 or year <1965:
                flag = 'N'
            elif cmplnt_to < cmplnt_fr:
                flag = 'Y'
            else:
                flag = 'N'        
        except ValueError:
            flag = 'N'
            cmplnt_to = 'N/A'
            cmplnt_fr = 'N/A'
    
        return (flag,(cmplnt_fr, cmplnt_to))            
             
    data3 = data.map(lambda x: cmplnt_to_before_cmplnt_fr(x))
    list_cmplnt_to_before_cmplnt_fr = data3.filter(lambda x: x[0] == 'Y')
    list_cmplnt_to_before_cmplnt_fr.saveAsTextFile('list_cmplnt_to_before_cmplnt_fr.out')
    
    sum_cmplnt_to_before_cmplnt_fr = data3.map(lambda x: (x[0], 1)).reduceByKey(add)
    sum_cmplnt_to_before_cmplnt_fr.saveAsTextFile('sum_cmplnt_to_before_cmplnt_fr.out')
    
    


    sc.stop()
