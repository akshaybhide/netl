#!/bin/bash
FILES=$(ls  /mnt/adnetik/adnetik-uservervillage/admeld/userver_log/imp/2012-11-18/*.gz)

for f in $FILES
do
  #echo " $f "
#  zcat "$f"|cut -f 1,153,154,156
  isthere=$(zcat "$f"|cut -f 1,10,143 |grep 2012-11-18| grep admeld_mobile|grep -c 'Phone')
  echo "$isthere"
#	mysql -u armita -h thorin-internal.adnetik.com --password='data_101?' mobile < "$f"
#	if [ $? -eq 0 ] 
#	then 
#	mv "$f" mobileinsertedfiles/
#	echo "moved $f to ./mobileinsertedfiles" 
#	fi
  # take action on each file. $f store current file name
#  cat $f
done
