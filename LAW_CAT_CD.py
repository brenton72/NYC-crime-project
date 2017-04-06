from __future__ import print_function

import sys
from operator import add
from csv import reader
from pyspark import SparkContext


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    
    header = lines.first() #extract header
    data = lines.filter(lambda x: x != header) 
#update column number
    col_num=11
    
    
    def assign_types(rows, col_num):
#creates rdd with key as col name, values {data_type,semantic_type,valid_ind}
        semantic_type = 'CRIME CATEGORY'
        data_type = 'STR'
        if rows[col_num] == '':
            valid_ind='NULL'
        elif (rows[col_num] == 'FELONY' or rows[col_num] == 'MISDEMEANOR' or rows[col_num] == 'VIOLATION'):
            valid_ind = 'VALID'
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