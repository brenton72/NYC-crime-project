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
    
    
    def bound_nyc(rows):
        try:
            lat = float(rows[21])
            lon = float(rows[22])
            if lat >= 40.477399 and lat <=40.917577 and lon >= -74.259090 and lon <= -73.700272:
                flag = 'NYC'
            elif lat >= 40.785397 and lat <= 40.798709 and lon >= -73.892698 and lon <= -73.870872:
            	flag = 'RIKERS'
            else:
            	flag = 'NOT NYC'
            
        except ValueError:
            flag = 'N/A'
        
        return (flag, 1)            
             
    nyc = data.map(lambda x: bound_nyc(x)).reduceByKey(add)
    nyc.saveAsTextFile('bound_nyc.out')

    def bound_boro(rows):
        try:
            lat = float(rows[21])
            lon = float(rows[22])
            boro_nm = rows[13]
            #EXCLUDE RIKERS
            if lat >= 40.785397 and lat <= 40.798709 and lon >= -73.892698 and lon <= -73.870872:
            	flag = 'RIKERS'
            else:
				#QUEENS
				if (lat <= 40.489794 or lat >=40.812242 or lon <= -74.042112 or lon >= -73.700272) and boro_nm == 'QUEENS':
					flag = 'INVALID QUEENS'
				#BROOKLYN
				elif (lat <= 40.551042 or lat >= 40.739446 or lon <= -74.056630 or lon >= -73.833365) and boro_nm == 'BROOKLYN':
					flag = 'INVALID BROOKLYN'
				#MANHATTAN
				elif (lat <= 40.680396 or lat >= 40.882214 or lon <= -74.047285 or lon >= -73.907000) and boro_nm == 'MANHATTAN':
					flag = 'INVALID MANHATTAN'
				#BRONX
				elif (lat <= 40.785743 or lat >= 40.917577 or lon <= -73.933808 or lon >= -73.748060) and boro_nm == 'BRONX':
					flag = 'INVALID BRONX'
				#STATEN ISLAND
				elif (lat <= 40.477399 or lat >= 40.651812 or lon <= -74.259090 or lon >= -74.034547) and boro_nm == 'STATEN ISLAND':
					flag = 'INVALID STATEN ISLAND'
				else:
					flag = 'VALID'
        except ValueError:
            flag = 'N/A'
        
        return (flag, 1)            
             
    boro = data.map(lambda x: bound_boro(x)).reduceByKey(add)
    boro.saveAsTextFile('bound_boro.out')



    sc.stop()