#!/usr/bin/python

############################################################
# This script copies log files from S3 and processed them  #
# appropriately before before putting files in HDFS.       #
# There are separate functions for different log types.    #
#                                                          #
############################################################

import sys,os
import boto
import gzip


def getExchanges():
    """
    Returns a list of all exchanges
    """
    exchanges = ['adbrite','admeld','adnexus','casale','contextweb',
                 'id','nexage,''openx','rtb','rubicon','yahoo']
    
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
    data_path = '/'.join([exchange,'userver_log',log_type,day])
    keylist = bucket.list(data_path)

    return keylist

def s3pixelKeyList(bucket,day):
    """
    Returns a list of files for a given exchange, log type 
    and date in the s3 bucket -adnetik-userver-village
    Exchanges need to be referenced correctly:
    ex. rtb, rubicon, admeld, etc
    Date should be in form YYYY-MM-DD ex. 2011-07-19
    """
    # Bucket in hand, jack and jill can go up the hill to fetch a pail of data
    data_path = 'prod/userver_log/pixel/'+day
    keylist = bucket.list(data_path)

    return keylist

def getKey(s3key,keylabel):
    """
    Copies s3 key to local
    """
    local_target = keylabel.split('/')[-1]
    local_copy = open(local_target,'w')
    s3key.get_file(local_copy)
    local_copy.close()
    return local_target

def processFile(sourcefile,targetfile):
    """
    Unzips sourcefile and appends content to targetfile
    then deletes
    """
    filecontents = gzip.open(sourcefile)        
    for fileline in filecontents:
        print >> targetfile,fileline.strip()
    filecontents.close()
    rm_cmd = 'rm -f '+sourcefile
    os.system(rm_cmd)

def writetoHDFS(sourcefile,targetloc):
    """
    Writes the file to HDFS
    """
    put_base = "hadoop fs -put "
    put_cmd = put_base+sourcefile+" "+targetloc
    os.system(put_cmd)
    os.system('rm '+sourcefile) 


def sync_bid_all(exchange,day):
    """
    Gets all the bid_all logs for a single day concatenates
    into a single file, lzo compresses and copies that file to
    HDFS://user/hdfs/data/bid_all/exchange/full-day-log.lzo
    """    
    s3bucket = s3BucketGrab()
    print >> sys.stderr, "reporter:status:Getting list of bid_all log files for %s" % (day)    
    s3keys = s3KeyList(s3bucket,exchange,"bid_all",day)
    s3keylabels = [s3key.name for s3key in s3keys if s3key.name.split('.')[-1]=='gz']
    print >> sys.stderr, "reporter:status:Retrieving files from S3 (%s %s)" % (exchange,day)
    targetfilename = "-".join([exchange,log_type,day])
    targetfilename+=".txt"
    targetfile = open(targetfilename,'w')
    # Retreive Files from S3, concatenating contents to targetfile
    for s3key in s3keys:
        keylabel = s3key.name
        if keylabel.split('.')[-1]!='gz':
            continue
        sourcefile = getKey(s3key,keylabel)
        processFile(sourcefile,targetfile)

    targetfile.close()
    print >> sys.stderr, "reporter:status:Successfully Concatenated log files into %s" % targetfilename
    # Compress targetfile with lzo
    compressedlogs = targetfilename+'.lzo'
    lzo_cmd = 'lzop -o '+compressedlogs+' '+targetfilename
    os.system(lzo_cmd)
    os.system('rm '+targetfilename)
    hdfs_target = base_hdfs_target+"bid_all/"+day+"/"+compressedlogs
    print >> sys.stderr, "reporter:status:Writing %s to HDFS://%s" % (compressedlogs,hdfs_target)
    writetoHDFS(compressedlogs,hdfs_target)


def sync_activity_logs(log_type,day):
    """
    Gets all the logs of a single type for a single day across all exchanges
    and concatenates into a single file, lzo compresses and copies that file to
    HDFS://user/hdfs/data/log_type/full-day-log.lzo    
    """
    s3bucket = s3BucketGrab()
    exchanges = getExchanges()
    print >> sys.stderr, "reporter:status:Getting list of %s log files for %s" % (log_type,day)    
    targetfilename = '-'.join([log_type,day])
    targetfilename+=".txt"
    targetfile = open(targetfilename,'w')
    for exchange in exchanges:
        s3keys = s3KeyList(s3bucket,exchange,log_type,day)
        print >> sys.stderr, "reporter:status:Retrieving files from S3 (%s %s)" % (exchange,day)
        for s3key in s3keys:
            keylabel = s3key.name
            if keylabel.split('.')[-1]!='gz':
                continue
            sourcefile = getKey(s3key,keylabel)
            processFile(sourcefile,targetfile)
            
    targetfile.close()
    print >> sys.stderr, "reporter:status:Successfully Concatenated log files into %s" % targetfilename
    # Compress targetfile with lzo
    compressedlogs = targetfilename+'.lzo'
    lzo_cmd = 'lzop -o '+compressedlogs+' '+targetfilename
    os.system(lzo_cmd)
    os.system('rm '+targetfilename)
    hdfs_target = base_hdfs_target+log_type+"/"+compressedlogs
    print >> sys.stderr, "reporter:status:Writing %s to HDFS://%s" % (compressedlogs,hdfs_target)
    writetoHDFS(compressedlogs,hdfs_target)
    

def sync_pixel_logs(log_type,day):
    """
    Gets all the pixel logs for a single day and concatenates into
    a single file, lzo compresses and copies that file to
    HDFS://user/hdfs/data/pixel/full-day-log.lzo    
    """
    s3bucket = s3BucketGrab()
    exchanges = getExchanges()
    print >> sys.stderr, "reporter:status:Getting list of %s log files for %s" % (log_type,day)    
    targetfilename = '-'.join([log_type,day])
    targetfilename+=".txt"
    targetfile = open(targetfilename,'w')

    s3keys = s3pixelKeyList(s3bucket,day)
    print >> sys.stderr, "reporter:status:Retrieving files from S3 for %s" % (day)    
    for s3key in s3keys:
        keylabel = s3key.name
        if keylabel.split('.')[-1]!='gz':
            continue
        sourcefile = getKey(s3key,keylabel)
        processFile(sourcefile,targetfile)
            
    targetfile.close()
    print >> sys.stderr, "reporter:status:Successfully Concatenated log files into %s" % targetfilename
    # Compress targetfile with lzo
    compressedlogs = targetfilename+'.lzo'
    lzo_cmd = 'lzop -o '+compressedlogs+' '+targetfilename
    os.system(lzo_cmd)
    os.system('rm '+targetfilename)
    hdfs_target = base_hdfs_target+log_type+"/"+compressedlogs
    print >> sys.stderr, "reporter:status:Writing %s to HDFS://%s" % (compressedlogs,hdfs_target)
    writetoHDFS(compressedlogs,hdfs_target)
    

base_hdfs_target = '/user/hdfs/data/'

if __name__ == "__main__":

    if len(sys.argv)!=1:
        print "Usage: echo 'log_type [exchange] YYYY-MM-DD' | ./s3pull.py "
        print "\t exchange should only be specified for bid_all log queries"
        print "\t ex: echo 'bid_all rtb 2011-08-15' | ./s3pull.py "
        print "\t ex: echo 'imp 2011-08-15' | ./s3pull.py "
        print "\t ex: echo 'pixel 2011-08-15' | ./s3pull.py "                
        sys.exit(1)

    # Input should be in the form log_type [exchange] date
    for line in sys.stdin:
        args = line.strip().split()
        log_type = args[0]
        if log_type == 'bid_all':
            exchange,day = args[1],args[2]
            sync_bid_all(exchange,day)
        elif log_type == 'pixel':
            day = args[1]
            sync_pixel_logs(log_type,day)
        else:
            day = args[1]
            sync_activity_logs(log_type,day)
