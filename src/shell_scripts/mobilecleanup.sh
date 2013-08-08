#!/bin/bash
mydate=$(date  "+%Y-%m-%d_%H:%M")
FILES='/local/fastetl/mobilebackfill/*/*/mobile_[0-9]*.txt'
FILES2='/local/fastetl/mobilefuture/*/*/mobile_[0-9]*.txt'
#FILES='/home/armita/insertedfiles/[0-9]*.txt'
outfolder="/local/fastetl/mobilebfinsertedfiles/"$mydate
mkdir -p $outfolder
for f in $FILES
do
  echo "mysql $f "
	mysql -u armita -h thorin-internal.adnetik.com --password='data_101?' mobile < "$f"
	if [ $? -eq 0 ] 
	then 
	mv "$f" $outfolder
	echo "moved $f to $outfolder" 
	fi
  # take action on each file. $f store current file name
#  cat $f
done
rm -rf /local/fastetl/mobilebackfill/junk/*
outfolder="/local/fastetl/mobilefutureinsertedfiles/"$mydate
mkdir -p $outfolder

for f in $FILES2
do
  echo "mysql $f "
        mysql -u armita -h thorin-internal.adnetik.com --password='data_101?' mobile < "$f"
        if [ $? -eq 0 ]
        then
        mv "$f" $outfolder
        echo "moved $f to $outfolder"
        fi
  # take action on each file. $f store current file name
#  cat $f
done
rm -rf /local/fastetl/mobilefuture/junk/*
