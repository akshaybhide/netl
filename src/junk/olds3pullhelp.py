#!/usr/bin/python

############################################################
# Helper function for running the cron jobs that sync      #
# S3 data on HDFS. Leverages capability of hadoop by       #
# writing input files to HDFS before executing commands    #
#                                                          #
############################################################

import os,sys
import datetime

def get_yesterday():
    """
    Returns yesterday's timestamp as a string in format YYYY-MM-DD
    """
    now = datetime.datetime.now()
    d1 = datetime.timedelta(days=1)
    yesterday = now-d1
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    return yesterday_str

def fileinputlist(logtype,target_day):
    """
    Get a list of files for a log type for a target day
    from s3 and writes them to a file which gets put on
    HDFS, to be used as input for distcp
    """
    exchanges = ['adbrite','admeld','adnexus','casale','contextweb',
                 'id','nexage','openx','rtb','rubicon','yahoo']                 
    base_hdfs = '/tmp/log_sync/'+logtype+'/'+target_day+'/'
    outfilename = log_type+'-'+target_day+'-s3_locations.txt'    
    for exchange in exchanges:
        output = open(outfilename,'w')
        print >> output, 'bid_all',exchange,target_day 
        output.close()
        hdfs_target = base_hdfs+target_day+'/'+outfilename
        os.system('hadoop fs -put '+outfilename+' '+hdfs_target)
        os.system('rm '+outfilename)

def activity_helper(log_type,target_day):
    """
    """
    base_hdfs = '/user/mapred/projects/data_management/tmp/'
    outfilename = log_type+target_day+'.txt'
    output = open(outfilename,'w')
    print >> output,log_type,target_day 
    output.close()
    hdfs_target = base_hdfs+log_type+'/'+outfilename
    os.system('hadoop fs -put '+outfilename+' '+hdfs_target)
    os.system('rm '+outfilename)

def pixel_helper(log_type,target_day):
    """
    """
    base_hdfs = '/user/mapred/projects/data_management/tmp/pixel/'
    outfilename = log_type+target_day+'.txt'
    output = open(outfilename,'w')
    print >> output,log_type,target_day 
    output.close()
    hdfs_target = base_hdfs+log_type+'/'+outfilename
    print 'Putting',hdfs_target
    os.system('hadoop fs -put '+outfilename+' '+hdfs_target)
    os.system('rm '+outfilename)
    

if __name__ == "__main__":

    log_type = sys.argv[1]
    
    if len(sys.argv)>2:
        target_day = sys.argv[2]
    else:
        target_day = get_yesterday()

    if log_type=='bid_all':
        bid_all_helper(target_day)
    else:
        days = ['2011-07-29','2011-07-30','2011-07-31','2011-08-01',
                '2011-08-02','2011-08-03','2011-08-04','2011-08-05',
                '2011-08-06','2011-08-07','2011-08-08','2011-08-09',
                '2011-08-10','2011-08-11','2011-08-12','2011-08-13',
                '2011-08-14','2011-08-15','2011-08-16','2011-08-17',
                '2011-08-18','2011-08-19','2011-08-20','2011-08-21',
                '2011-08-22','2011-08-23','2011-08-24','2011-08-25',
                '2011-08-26','2011-08-27']

        if log_type=='pixel':
            for day in days:            
                pixel_helper(log_type,day)
        else:
            for day in days:
                activity_helper(log_type,day)
