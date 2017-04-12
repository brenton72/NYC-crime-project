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
    def get_codes(rows):
        try:
            ky_cd = int(rows[6])           
        except ValueError:
            ky_cd = -99     
        try:
            pd_cd = int(rows[8])
        except ValueError:
            pd_cd = -9
        return ((ky_cd,rows[7],pd_cd),1)
    rdd = data.map(lambda x: get_codes(x))
    by_codes = rdd.reduceByKey(add).sortByKey()
    by_codes.saveAsTextFile('by_codes.out')
    
    sc.stop()
