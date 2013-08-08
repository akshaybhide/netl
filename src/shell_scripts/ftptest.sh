#!/bin/bash
rm"AudienceOn-Logdata-1110.2013-01-26.tar.bz2"
tar -jcvf "AudienceOn-Logdata-1110.2013-01-26.tar.bz2" "2013-01-26_fixed.txt"
FILE="AudienceOn-Logdata-1110.2013-01-26.tar.bz2"
HOST="upload.bluekai.com"
USER="adnetik_ao"
PASSWD="gJ1i8mX0aEob"
ls -s  2013-01-29.txt
if [ -s  2013-01-29.txt ] 
then 
echo "not empty"
else
echo "was empty" 
fi
#export SSHPASS=$PASSWD
#sshpass -e sftp -oBatchMode=no -b - $USER@$HOST << !
#   put $FILE
#   bye
#!
#sftp -n $HOST <<END_SCRIPT
#quote USER $USER
#quote PASS $PASSWD
#quit
#END_SCRIPT
#exit 0



#put $FILE
#daybefore=$(date --date="-3 day" "+%Y-%m-%d")
#hadoop fs -cat "output"$daybefore/part* > $daybefore".txt"
#awk '{print($2,"\t",$1, "\t", $5, "\t", $3)}' $daybefore".txt" > $daybefore"_fixed.txt"
~

