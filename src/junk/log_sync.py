#!/usr/bin/python

###################################################
# Template script for moving log files from S3    #
# into HDFS. Includes copying and processing,     #
# removing redundant/un needed fields and         # 
# compressing in appropriate format (LZO).        #
# Relies on several python scripts to be present  #
# in working directory. Is expected to run daily, #
# after midnight, to copy yesterday's logs.       #
###################################################

import os, sys
import s3pull

# Basic components of Hadoop command for log Synchronization and Stripping
hadoop_jar = 'hadoop com.adnetik.data_management.LogSync'

if __name__ == "__main__":

    logtype = sys.argv[1]
    
    #Get the appropriate date
    if len(sys.argv)==3:
        target_day = sys.argv[2]
    else:
        target_day = s3pull.get_yesterday()
    
    # Copy gzipped logs to HDFS will be deleted later    
    tmp_raw_logs = s3pull.copy_logs(logtype,target_day)
    print "Copied Raw logs from S3 to HDFS", tmp_raw_logs
    
    # Hadoop job configuration
    input_arg = tmp_raw_logs
    output_loc  = '/data/'+logtype+'/'+target_day

    # EXPORT PATH
    export_path = "export HADOOP_CLASSPATH=/mnt/javaclass/adnetik.jar"
    os.system(export_path)
    hadoop_cmd = ' '.join([hadoop_jar,input_arg,output_loc])
    print hadoop_cmd
    os.system(hadoop_cmd)
    print 'Stripped Logs of redundant fields and wrote output to ',output_loc

    # Index the output
    print 'Indexing LZO files'
    hadoop_index = 'hadoop jar /usr/lib/hadoop-0.20/lib/hadoop-lzo-0.4.9.jar '
    class_arg = 'com.hadoop.compression.lzo.DistributedLzoIndexer '
    os.system(hadoop_index+class_arg+output_loc)
    
    # Delete raw logs copied from S3
    print 'Deleting raw log files at ',tmp_raw_logs
    rm_cmd = 'hadoop fs -rmr '+tmp_raw_logs
    #os.system(rm_cmd)


