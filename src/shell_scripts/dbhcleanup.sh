#!/bin/bash
mydate=$(date  "+%Y-%m-%d_%H:%M")
FILES='/local/fastetl/dbhbackfill/*/*/dbh[0-9]*.txt'
FILES2='/local/fastetl/dbhfuture/*/*/dbh[0-9]*.txt'
#FILES='/home/armita/insertedfiles/[0-9]*.txt'
mainfolderpast='/local/fastetl/dbhinsertedfilespast/'
outputfolderpast=$mainfolderpast""$mydate
mainfolderfuture='/local/fastetl/dbhinsertedfilesfuture/'
outputfolderfuture=$mainfolderfuture""$mydate
mkdir $mainfolderpast
mkdir $mainfolderfuture
mkdir $outputfolderpast
mkdir $outputfolderfuture
for f in $FILES
do
  echo "mysql $f "
	mysql -u armita -h thorin-internal.adnetik.com --password='data_101?' dbh < "$f"
#	if [ $? -eq 0 ] 
#	then 
	mv "$f" $outputfolderpast
	echo "moved $f to $outputfolderpast" 
#	fi
  # take action on each file. $f store current file name
#  cat $f
done

for f in $FILES2
do
  echo "mysql $f "
        mysql -u armita -h thorin-internal.adnetik.com --password='data_101?' dbh < "$f"
 #       if [ $? -eq 0 ]
 #       then
        mv "$f $outputfolderfuture"
        echo "moved $f to $outputfolderfuture"
  #      fi
  # take action on each file. $f store current file name
#  cat $f
done

        mysql -u armita -h 66.117.49.100 --password='data_101?' dbh < "$f"


FILES='/local/fastetl/dbhforip/*/*/dbh[0-9]*.txt'
FILES2='/local/fastetl/dbhfutureforip/*/*/dbh[0-9]*.txt'
mainfolderpastforip='/local/fastetl/dbhinsertedfilespastforip/'
outputfolderpastforip=$mainfolderpastforip"/"$mydate
mainfolderfutureforip='/local/fastetl/dbhinsertedfilesfutureforip/'
outputfolderfutureforip=$mainfolderfutureforip"/"$mydate
mkdir $mainfolderpastforip
mkdir $mainfolderfutureforip
mkdir $outputfolderpastforip
mkdir $outputfolderfutureforip
for f in $FILES
do
  echo "mysql $f "
        mysql -u armita -h 66.117.49.100 --password='data_101?' dbh < "$f"
   #     if [ $? -eq 0 ]
    #    then
        mv "$f" $outputfolderpastforip
        echo "moved $f to $outputfolderpastforip"
     #   fi
  # take action on each file. $f store current file name
#  cat $f
done

for f in $FILES2
do
  echo "mysql $f "
        mysql -u armita -h 66.117.49.100 --password='data_101?' dbh < "$f"
      #  if [ $? -eq 0 ]
       # then
        mv "$f $outputfolderfutureforip"
        echo "moved $f to $outputfolderfutureforip"
        #fi
  # take action on each file. $f store current file name
#  cat $f
done
rm -rf /local/fastetl/dbhfuture/junk/*
