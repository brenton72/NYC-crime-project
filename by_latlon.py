from __future__ import print_function

import sys
from operator import add
from csv import reader
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField


if __name__ == "__main__":
	sc = SparkContext()
	sql = SQLContext(sc)
	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))

	header = lines.first() #extract header
	data = lines.filter(lambda x: x != header)

	schema = StructType([StructField('CMPLNT_NUM', IntegerType(), True), \
		StructField('CMPLNT_FR_DT', StringType(), True),\
		StructField('CMPLNT_FR_TM', StringType(), True),\
		StructField('CMPLNT_TO_DT', StringType(), True),\
		StructField('CMPLNT_TO_TM', StringType(), True),\
		StructField('RPT_DT', StringType(), True),\
		StructField('KY_CD', IntegerType(), True),\
		StructField('OFNS_DESC', StringType(), True),\
		StructField('PD_CD', IntegerType(), True),\
		StructField('PD_DESC', StringType(), True),\
		StructField('CRM_ATPT_CPTD_CD', StringType(), True),\
		StructField('LAW_CAT_CD', StringType(), True),\
		StructField('JURIS_DESC', StringType(), True),\
		StructField('BORO_NM', StringType(), True),\
		StructField('ADDR_PCT_CD', IntegerType(), True),\
		StructField('LOC_OF_OCCUR_DESC', StringType(), True),\
		StructField('PREM_TYP_DESC', StringType(), True),\
		StructField('PARKS_NM', StringType(), True),\
		StructField('HADEVELOPT', StringType(), True),\
		StructField('X_COORD_CD', IntegerType(), True),\
		StructField('Y_COORD_CD', IntegerType(), True),\
		StructField('Latitude', DoubleType(), True),\
		StructField('Longitude', DoubleType(), True),\
		StructField('Lat-Long', StringType(), True)])
	df = sql.createDataFrame(data, schema)
        
	#aggregate by Lat-Long
	output = df.groupBy('Lat-Long').count()
	output.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("crime_latlon.out")

	sc.stop()

