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
    def get_juris(rows):
        juris = rows[12]
        if juris == 'STATN IS RAPID TRANS':
            juris_SI = juris
        else:
            juris_SI = 'Other'           
        boro = rows[13]
        return ((juris_SI,boro),1)
    rdd = data.map(lambda x: get_juris(x))
    by_juris = rdd.reduceByKey(add).sortByKey()
    by_juris.saveAsTextFile('by_juris_SI.out')
    
    sc.stop()
