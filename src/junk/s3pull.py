#!/usr/bin/python

############################################################
# This script writes the s3 file targets for log synching  #
# to HDFS and executes a distcp command to move the actual #
# files to HDFS. This is a preliminary step in the log     #
# process to allow for a faster map-reduce job that strips #
# logs of redundant fields and compresses the output with  #
# LZO.                                                     #
############################################################

import sys,os
import boto,datetime

def fileinputlist(logtype,target_day):
    """
    Get a list of files for a log type for a target day
    from s3 and writes them to a file which is put on
    HDFS, to be used as input for distcp
    """
    exchanges = getExchanges()                 
    outfilename = logtype+'-'+target_day+'-s3_locations.txt'
    hdfs_target = '/tmp/log_sync/'+logtype+'/'+target_day+'/'+outfilename    
    s3_filelocs = open(outfilename,'w')
    s3bucket = s3BucketGrab()
    
    # Get list of target keys, exchange by exchange
    base_loc = 's3n://adnetik-uservervillage/'
    for exchange in exchanges:
        for s3key in s3KeyList(s3bucket,exchange,logtype,target_day):
            print >> s3_filelocs, base_loc+s3key.name
            
    s3_filelocs.close()
    # Move input file list to HDFS
    fileloc_copy = 'hadoop fs -put '+outfilename+' '+hdfs_target
    print "Copying s3 file input list to HDFS", fileloc_copy
    os.system(fileloc_copy)
    # Delete local copy
    os.system('rm '+outfilename)
    
    return hdfs_target

def fileinputlist_exchange(exchange,logtype,target_day):
    """
    Get a list of files for a log type for a target day
    from s3 and writes them to a file which is put on
    HDFS, to be used as input for distcp
    """
    outfilename = exchange+'-'+logtype+'-'+target_day+'-s3_locations.txt'
    hdfs_target = '/tmp/log_sync/'+outfilename    
    s3_filelocs = open(outfilename,'w')
    s3bucket = s3BucketGrab()
    
    # Get list of target keys, exchange by exchange
    base_loc = 's3n://adnetik-uservervillage/'
    for s3key in s3KeyList(s3bucket,exchange,logtype,target_day):
        print >> s3_filelocs, base_loc+s3key.name
            
    s3_filelocs.close()
    # Move input file list to HDFS
    fileloc_copy = 'hadoop fs -put '+outfilename+' '+hdfs_target
    print "Copying s3 file input list to HDFS", fileloc_copy
    os.system(fileloc_copy)
    # Delete local copy
    os.system('rm '+outfilename)
    
    return hdfs_target

def hadoop_distcp(inputfile):
    """
    Run distcp with s3 files list as argument.
    Data is written to the same directory as the inputfile
    """
    base_cmd = 'hadoop distcp -f '
    log_type,day = inputfile.split('/')[3],inputfile.split('/')[4]
    tmp_data_loc = '/'.join(inputfile.split('/')[:-1])
    job_name = '-D mapred.job.name="Distcp - '+log_type+'-'+day+'"'
    hadoop_cmd = base_cmd+inputfile+' '+tmp_data_loc
    print "Copying S3 Data to HDFS", hadoop_cmd
    os.system(hadoop_cmd)

    # Delete the source file list from HDFS
    rm_cmd = "hadoop fs -rm "+inputfile
    os.system(rm_cmd)
    
    return tmp_data_loc
    
def copy_logs(logtype,day):
    """
    Copy the gzipped logs of specified type for the specified
    day across all exchanges to HDFS.
    """
    input_source = fileinputlist(logtype,day)
    tmp_data_loc = hadoop_distcp(input_source)
    
    return tmp_data_loc
    
def getLogTypes():
    """
    Returns a list of all logtypes
    """
    return ['no_bid','bid_all','imp','click','conversion']
    
def getExchanges():
    """
    Returns a list of all exchanges
    """
    exchanges = ['adbrite','admeld','adnexus','casale','contextweb',
                 'id','nexage','openx','rtb','rubicon','yahoo']
    
    return exchanges
    
def s3BucketGrab():
    """
    Gets the adnetik-uservervilage bucket from S3
    """
    s3 = boto.connect_s3('AKIAJQSQ6DAW3LXRD2CA','VMbpYn+36mGuDqMd9mOa/NeoF4tN0+AAuJa+T5TK')
    bucket = s3.get_bucket('adnetik-uservervillage')

    return bucket

def s3KeyList(bucket,exchange,log_type,day):
    """
    Returns a list of files for a given exchange, log type 
    and date in the s3 bucket -adnetik-userver-village
    Exchanges need to be referenced correctly:
    ex. rtb, rubicon, admeld, etc
    Date should be in form YYYY-MM-DD ex. 2011-07-19
    """
    # Bucket in hand, jack and jill can go up the hill to fetch a pail of data
    if log_type=='pixel':
        data_path = 'prod/userver_log/pixel/'+day
    else:
        data_path = '/'.join([exchange,'userver_log',log_type,day])
    
    keylist = [s3key for s3key in bucket.list(data_path) if s3key.name.split('.')[-1]=='gz']

    return keylist

def get_yesterday():
    """
    Returns yesterday's timestamp as a string in format YYYY-MM-DD
    """
    now = datetime.datetime.now()
    d1 = datetime.timedelta(days=1)
    yesterday = now-d1
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    return yesterday_str


    
