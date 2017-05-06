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
    
#codes aggregation
    def get_prem_type(rows):
        prem_type  = rows[16]           
        return ((prem_type),1)
    rdd = data.map(lambda x: get_prem_type(x))
    by_prem_type = rdd.reduceByKey(add).sortByKey()
    by_prem_type.saveAsTextFile('by_prem_type.out')
    
    sc.stop()
