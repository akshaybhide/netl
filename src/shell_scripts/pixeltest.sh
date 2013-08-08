#!/bin/bash
FILES=$(grep 2012-11-18 /home/armita/test/current/cleanlist.txt |sort|uniq)

for f in $FILES
do
  #echo " $f "
  #isthere=$(zcat "$f"|cut -f 143,145| grep 'Tablet\|Phone\|Amazon\|HTC_Desire\|_Other\|Galaxy\|iPad\|iPod\|Huawei\|BlackBerry\|Droid' |wc -l)
  isthere=$(zcat "$f"|cut -f 1|grep -c 2012-11-18)
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
