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
    def get_precinct(rows):
        lat_lb = 40.785397
        lat_ub = 40.798709
        lon_lb = -73.892698
        lon_ub = -73.870872
        precinct = rows[14]
        try:
            lat = float(rows[21])
            lon = float(rows[22])
            if (lat > lat_lb and lat < lat_ub):
                if (lon > lon_lb and lon < lon_ub):
                    loc = 'RIKERS ISLAND'
                else:
                    loc = 'Other'
            else:
                loc = 'Other'
        except:
            loc = 'Other (Invalid)'           
        return ((loc,precinct),1)
    rdd = data.map(lambda x: get_precinct(x))
    by_precinct = rdd.reduceByKey(add).sortByKey()
    by_precinct.saveAsTextFile('by_precinct_Rikers.out')
    
    sc.stop()
