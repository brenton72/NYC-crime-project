# NYC-crime-project
Team Name: Data Cops(?) 

Link to Google Doc: https://docs.google.com/document/d/19kTMeooXbNqTtDJFR8NcyXxequzhJDtE2-FuVnQKYkY/edit

Link to Data: https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i

Link to Project Log: https://docs.google.com/document/d/1keVwZa3IIZ67xKU97SWUoY3bT05QjRtw334fHXwtr8Q/edit?ts=58ddb088


Type the following lines before running the "agg_by_dow_dst_hr_type.py" program:
- module load python/gnu/3.4.4 
- export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python

To view base/semantic type and validity of each column, from the folder ASSIGN_TYPES, run:
- chmod 777 submit.sh
- ./submit.sh
- hfs -getmerge type_'column_name'.out type_'column_name'.out
A summary of the validity of each column will be stored and retrieved under summary_'column_name'.out automatically by running submit.sh

All other .py files can be run using "spark-submit "data_path" file.py where "data_path" is the path on hfs where the crime data is stored.

The .py files will generate the following summary counts:
- agg_by_boro_datetime.py: Aggregates data by borough & hour and borough & year
- agg_by_datetime.py: Aggregates data by various granularities of time (years, months, days, etc.)
- agg_by_date.py: Aggregates data by finer granularities of time than agg_by_datetime.py (hours, minutes, etc.)
- agg_by_dow_dst_hr_type.py: Aggregates data by day-of-week, hour, crime type (Felony, etc.) and DST status
- 
