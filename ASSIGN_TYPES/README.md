To view base/semantic type and validity of each column, from this folder (ASSIGN_TYPES):
- run "chmod 777 submit.sh"
- change "/user/tr1312/crime_data.csv" to the data path on HDFS in submit.sh
- run "./submit.sh"
- run "hfs -getmerge type_'column_name'.out type_'column_name'.out"
A summary of the validity of each column will be stored and retrieved under summary_'column_name'.out automatically by running submit.sh
