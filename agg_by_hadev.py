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
    def get_hadev(rows):
        hadev  = rows[18]
        if hadev != '':
            hadev_type = 'All Public Housing'
        else:
            hadev_type = hadev           
        return ((hadev_type),1)
    rdd = data.map(lambda x: get_hadev(x))
    by_hadev = rdd.reduceByKey(add).sortByKey()
    by_hadev.saveAsTextFile('hadev_count.out')
    
    sc.stop()
