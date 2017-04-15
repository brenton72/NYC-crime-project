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
    
#codes aggregation
    def get_parks(rows):
        park  = rows[17]
        if park != '':
            park_type = 'All Parks'
        else:
            park_type = park           
        return ((park_type),1)
    rdd = data.map(lambda x: get_parks(x))
    by_parks = rdd.reduceByKey(add).sortByKey()
    by_parks.saveAsTextFile('parks_count.out')
    
    sc.stop()
