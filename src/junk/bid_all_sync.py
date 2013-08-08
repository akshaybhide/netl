#!/usr/bin/python

####################################
# Script that syncs yesterdays     #
# logs from S3 to HDFS             #
#                                  #
####################################

import os
import s3pull, s3pull_helper

#Get the appropriate date
yesterday = s3pull_helper.get_yesterday()

# Sync impression logs
#print "Syncing Impression Logs"
#s3pull.sync_activity_logs('imp',yesterday)
# Sync click logs
#print "Syncing Click Logs"
#s3pull.sync_activity_logs('click',yesterday)
# Sync conversion logs
#print "Syncing Conversion Logs"
#s3pull.sync_activity_logs('conversion',yesterday)
# Sync pixel logs
#print "Syncing Pixel Logs"
#s3pull.sync_pixel_logs('pixel',yesterday)

# Sync bid_all logs
# Make file list on HDFS for distributed processing
print "Syncing Bid Logs"
s3pull_helper.bid_all_helper(yesterday)

# Hadoop command that will run the syncing process for bid_all
hadoop_jar = 'hadoop jar /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2-cdh3u1.jar'
opt1 = '-D mapred.reduce.tasks=0'
opt2 = '-D mapred.map.tasks.speculative.execution=false'
opt3 = '-D mapred.task.timeout=1200000000'
opt4 = '-D mapred.job.name="bid_all sync - '+yesterday+'"'
input_arg = '-input /user/mapred/projects/data_management/tmp/bid_all/'+yesterday+'/*.txt'
mapper_arg = '-mapper s3pull.py'
output_arg  = '-output /user/mapred/projects/data_management/tmp/bid_all/'+yesterday+'-sync'
file_arg = '-file s3pull.py'

hadoop_cmd = ' '.join([hadoop_jar,opt1,opt2,opt3,opt4,input_arg,mapper_arg,output_arg,file_arg])
print hadoop_cmd
os.system(hadoop_cmd)

# Index all of these lovely files
hadoop_index = 'hadoop jar /usr/lib/hadoop-0.20/lib/hadoop-lzo-0.4.9.jar '
hadoop_arg = 'com.hadoop.compression.lzo.DistributedLzoIndexer /user/hdfs/data/'
os.system(hadoop_index+hadoop_arg)
