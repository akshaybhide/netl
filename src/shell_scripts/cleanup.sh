#!/bin/bash
mydate=$(date  "+%Y-%m-%d_%H:%M")
echo $mydate
FILES='/local/fastetl/pixelbackfill-replication/*/*/[0-9]*.txt'
FILES2='/local/fastetl/pixelfuture-replication/*/*/[0-9]*.txt'
#FILES='/home/armita/insertedfiles/[0-9]*.txt'
outfolder="/local/fastetl/pixelbfinsertedfiles-replication/"$mydate
mkdir -p $outfolder
for f in $FILES
do
  echo "mysql $f "
	mysql -u armita -h thorin-internal-replication.digilant.com --password='data_101?' fastpixel < "$f"
	if [ $? -eq 0 ] 
	then 
	mv "$f" $outfolder
	echo "moved $f to " $outfolder
	fi
  # take action on each file. $f store current file name
#  cat $f
done
rm -rf /local/fastetl/pixelbackfill-replication/junk/*
outfolder='/local/fastetl/pixelfutureinsertedfiles-replication/'$mydate
mkdir -p $outfolder

for f in $FILES2
do
  echo "mysql $f "
        mysql -u armita -h thorin-internal-replication.digilant.com --password='data_101?' fastpixel < "$f"
        if [ $? -eq 0 ]
        then
        mv "$f" $outfolder
        echo "moved $f to " $outfolder
        fi
  # take action on each file. $f store current file name
#  cat $f
done

