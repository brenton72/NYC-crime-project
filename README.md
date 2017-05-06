# NYC-crime-project
Team Name: Data Cops(?) 

Link to Google Doc: https://docs.google.com/document/d/19kTMeooXbNqTtDJFR8NcyXxequzhJDtE2-FuVnQKYkY/edit

Link to Data: https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i

Link to Project Log: https://docs.google.com/document/d/1keVwZa3IIZ67xKU97SWUoY3bT05QjRtw334fHXwtr8Q/edit?ts=58ddb088

Link to NYC Census Tract GeoJson File: http://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/nyct2010/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson

Link to NYC Zip Code GeoJson File:
http://catalog.civicdashboards.com/dataset/11fd957a-8885-42ef-aa49-5c879ec93fac/resource/28377e88-8a50-428f-807c-40ba1f09159b/download/nyc-zip-code-tabulation-areas-polygons.geojson

Type the following lines before running the "agg_by_dow_dst_hr_type.py" program:
- module load python/gnu/3.4.4 
- export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python

To view base/semantic type and validity of each column, see README.md inside folder ASSIGN_TYPES.

All other .py files can be run using "spark-submit "data_path" file.py where "data_path" is the path on HDFS where the crime data is stored.

The .py files will generate the following summary counts:
- agg_by_boro_datetime.py: Aggregates data by borough & hour and borough & year
- agg_by_datetime.py: Aggregates data by various granularities of time (years, months, days, etc.)
- agg_by_date.py: Aggregates data by finer granularities of time than agg_by_datetime.py (hours, minutes, etc.)
- agg_by_dow_dst_hr_type.py: Aggregates data by day-of-week, hour, crime type (Felony, etc.) and DST status
- agg_by_dow_hr_boro.py: Aggregates data by day-of-week, hour and borough
- agg_by_dow_hr_type.py: Aggregates data by day-of-week, hour and crime type (Felony, etc.)
- agg_by_hadev.py: Aggregates data by whether it occurred in a Housing Authority Development Area
- agg_by_juris_boro_MN.py: Aggregates data by whether is fell under Metro North Railroad jurisdiction
- agg_by_juris_boro_SI.py: Aggregates data by whether is fell under Staten Island Rapid Transit jurisdiction
- agg_by_offense_codes.py: Aggregates data by KY_CD and PD_CD offense codes
- agg_by_parks.py: Aggregates data by whether it occured in a park
- agg_by_prec_rikers.py: Aggregates data by precinct and whether it occured in the area of Riker's Island
- agg_by_prem_type.py: Aggregates data by premise type
- agg_by_ymd_boro.py: Aggregates by year, month, day and boro
- by_boro_viol.py: Aggregates by boro and violation type
- by_latlon.py: Aggregates by latitude-longuitude pair
- pull_marathon_boro_hrly.py: Aggregates data by hour, boro, and whether it occurred on a Marathon/Half Marathon day or the week after one
- pull_marathon_info_boro.py: Aggregates data by boro, and whether it occurred on a Marathon/Half Marathon day or the week after one
- pull_marathon_type_hrly.py: Aggregates data by hour, crime type (Felony, etc.), and whether it occurred on a Marathon/Half Marathon day or the week after one
- pull_marathon_info_type.py: Aggregates data by crime type (Felony, etc.), and whether it occurred on a Marathon/Half Marathon day or the week after one
- pull_parade_info_boro.py Aggregates data by boro, and whether it occurred on a Marathon/Half Marathon/St. Patricks Day/Veteran's Day day or the week after one

The following .py files will check the data for inconsistencies:
- inconsistent_dates.py: Checks if report date columns and complaint date columns have temporal inconsistencies
- lat_lon_outliers.py: Checks if latitude-longitude pairs occur outside of the bounding box of their respective boroughs
