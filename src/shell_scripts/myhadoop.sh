#!/bin/bash
for (( i=3 ; i<=3 ; i++ ))
do
mydate=$(date --date="-$i day" "+%Y-%m-%d")
echo "running for $mydate"
hadoop  jar ./blukai2.jar com.digilant.blukai.BluKaiDriver  -D mapred.child.java.opts=-Xmx2048M  -libjars /home/armita/jars/adnetik.jar,commons-io-2.4.jar output$mydate $mydate

hadoop fs -cat "output"$mydate/part* > $mydate".txt"
if [ -s $mydate".txt" ]
then 
sed  -i "s/,\s*/,/g" $mydate".txt"
#printf "Unix Time Stamp\t Unique BlueKai User ID \t\t\t BlueKai Category IDs\t BK Campaign ID\t Uniqe Partner User ID\t Media Campaign ID\t Client IP Address\n" > $mydate"_fixed.txt"
rm $mydate"_fixed.txt"
awk '{print($2"\t"$1"\t"$5"\t\t\t"$3"\t\t\t")}' $mydate".txt" >> $mydate"_fixed.txt"
cat  $mydate"_fixed.txt"| tr -d ']'>tmp.txt
cat tmp.txt | tr -d '['>$mydate"_fixed.txt"
#tar -jcvf "AudienceOn-Logdata-1110."$mydate".tar.bz2" $mydate"_fixed.txt" 
bzip2 -cz $mydate"_fixed.txt">"AudienceOn-Logdata-1110."$mydate".bz2"
HOST='upload.bluekai.com'
USER='adnetik_ao'
PASSWD='gJ1i8mX0aEob'
UPFILE="AudienceOn-Logdata-1110."$mydate".bz2"

export SSHPASS=$PASSWD
sshpass -e sftp -oBatchMode=no -b - $USER@$HOST << !
   put $UPFILE
   bye
!
mv "AudienceOn-Logdata-1110."$mydate".bz2" ./bkfolder/
fi
./bkreport.sh "$mydate"
done

#printf "Segment, Count\n" > $mydate".csv"
#concatfiles=
#campaignid=$(hadoop fs -cat "output"$mydate/part*)
#hadoop fs -cat "output"$mydate/part* | cut -f 5 | tr ',' '\n' | tr -d '[' |tr -d ']' |tr -d ' ' |sort |uniq -c |sed 's/^\s\+//'|awk '{print($2,",",$1)}' >> $mydate".csv"
#ftp -n $HOST <<END_SCRIPT
#quote USER $USER
#quote PASS $PASSWD
#put $FILE
#quit
#END_SCRIPT
#exit 0


#daybefore=$(date --date="-3 day" "+%Y-%m-%d")
#hadoop fs -cat "output"$daybefore/part* > $daybefore".txt"
#awk '{print($2,"\t",$1, "\t", $5, "\t", $3)}' $daybefore".txt" > $daybefore"_fixed.txt"
