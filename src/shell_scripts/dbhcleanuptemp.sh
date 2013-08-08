#!/bin/bash

FILES='/home/armita/dbhinsertedfiles/dbh[0-9]*.txt'

mkdir /home/armita/dbhinsertedfiles/tmp2
for f in $FILES
do
  echo "mysql $f "
	mysql -u armita -h thorin-internal.adnetik.com --password='data_101?' dbh < "$f"
	if [ $? -eq 0 ] 
	then 
	mv "$f" dbhinsertedfiles/
	echo "moved $f to ./dbhinsertedfiles" 
	fi
  # take action on each file. $f store current file name
#  cat $f
done


