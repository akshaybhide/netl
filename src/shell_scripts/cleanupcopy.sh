#!/bin/bash

FILES='/home/armita/pixelinsertedfiles/*.txt_dummy'

mkdir /home/armita/pixelinsertedfiles/
for f in $FILES
do
  echo "mysql $f "
	mysql -u armita -h thorin-internal.adnetik.com --password='data_101?' fastpixel < "$f"
	if [ $? -eq 0 ] 
	then 
	mv "$f" pixelinsertedfiles/
	echo "moved $f to ./pixelinsertedfiles" 
	fi
done
