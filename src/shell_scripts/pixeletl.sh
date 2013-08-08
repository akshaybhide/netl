#!/bin/bash
java -cp /home/armita/jars/adnetik.jar:/home/armita/jars/mysql.jar:/home/armita/jars/hadoop-core.jar  com.digilant.pixel.AggregationWrapper 2013-04-10 0 30 /local/fastetl/pixelfuture-replication/pixel.config thorin-internal-replication.digilant.com>>/local/fastetl/pixelfuture-replication/april10.log
mydate=$(date --date="-0 day" "+%Y-%m-%d %H:%M:%S")
echo $mydate
mysql -h thorin-internal.digilant.com -uarmita -pdata_101? audit -e "replace into audit_report (latest_update) values ()" 

#java -cp /home/armita/jars/adnetik.jar:/home/armita/jars/mysql.jar:/home/armita/jars/hadoop-core.jar  com.digilant.dbh.AggregationWrapper 2013-04-10 0 30 /local/fastetl/dbhfuture/mconfig thorin-internal.digilant.com>>/local/fastetl/dbhfuture/april10.log&

#java -cp /home/armita/jars/adnetik.jar:/home/armita/jars/mysql.jar:/home/armita/jars/hadoop-core.jar  com.digilant.mobile.AggregationWrapper 2013-03-28 0 10 /local/fastetl/mobilefuture/mconfig thorin-internal.digilant.com>>/local/fastetl/mobilefuture/march28.log&


