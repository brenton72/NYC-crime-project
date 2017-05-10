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

    def latlon_get_year(rows):
        try:
            year = int(rows[1][6:])           
        except ValueError:
            year = 99     
        return ((rows[23],year),1)

    rdd = data.map(lambda x: latlon_get_year(x))
    output = rdd.reduceByKey(lambda x, y: x+y).filter(lambda x: x[0][1] > 2009)
    output = output.map(lambda x: (str(x[0][0])+','+str(x[0][1])+','+str(x[1])))
    output.saveAsTextFile('crime_latlon_year.out')
    sc.stop()
