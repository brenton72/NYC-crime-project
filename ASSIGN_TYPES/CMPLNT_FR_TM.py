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
    col_num=2
    
    def assign_types(rows, col_num):
#creates rdd with key as col name, values {data_type,semantic_type,valid_ind}
        data_type = 'DATETIME'
        semantic_type = 'HH:MM:SS'
        try:
            hour = int(rows[col_num][0:2])
            min = int(rows[col_num][3:5])
            sec = int(rows[col_num][6:8])
            value = datetime.time(hour,min,sec)
            valid_ind = 'VALID'
            
        except ValueError:
            if rows[col_num] == '':
                valid_ind='NULL'
            else:
                valid_ind = 'INVALID/OUTLIER'
        
        return (header[col_num],(rows[col_num],data_type,semantic_type,valid_ind))            
             
    output = data.map(lambda x: assign_types(x, col_num))
    output.saveAsTextFile('type_%s.out' %(header[col_num]))
    
    #aggregate summary stats
    data_type = output.map(lambda x: ('data_type, %s' %(x[1][1]),1)).reduceByKey(add)
    semantic_type = output.map(lambda x: ('semantic_type, %s' %(x[1][2]),1)).reduceByKey(add)
    valid_ind = output.map(lambda x: ('valid_ind, %s' %(x[1][3]),1)).reduceByKey(add)
    summary = sc.union([data_type, semantic_type, valid_ind]).sortByKey()
    summary.saveAsTextFile('summary_%s.out' %(header[col_num]))
    


    sc.stop()
