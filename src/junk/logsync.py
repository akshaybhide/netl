#!/usr/bin/python

###################################################
# Template script for moving log files from S3    #
# into HDFS. Includes copying and processing,     #
# removing redundant/un needed fields and         # 
# recompressing in appropriate format (LZO).      #
# Relies on several python scripts to be present  #
# in working directory. Is expected to run daily, #
# after midnight, to copy yesterday's logs.       #
###################################################

# Log_type argument expected at command line
LOG_TYPE = $1

# Yesterday's logs are default
TARGET_DAY = get_yesterday()

# Get list of files to copy from S3 in HDFS
filelise  = boto_script($LOGTYPE,$TARGET_DAY)
write_to_file(filelist, s3_source_file)

# Write file list to HDFS
hadoop fs -put s3_source_file hdfs://tmp/s3_source_file

# Run distcp with s3 files list as argument
hadoop distcp /tmp/s3_source_file /tmp/data/$LOG_TYPE/$TARGET_DAY/.

# Hadoop processing job
hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-0.20.2-cdh3u1.jar \
-D mapred.job.name=\"Log Sync - $LOG_TYPE\" \
-D SET OUTPUT TO LZO
-jobconf \"stream.recordreporter.compression=gzip\" \
-input /tmp/data/$LOG_TYPE/$TARGET_DAY/*.gz \
-mapper \"./logstripper.py $LOG_TYPE\" \
-reducter \"cat\"
-file logstripper.py

