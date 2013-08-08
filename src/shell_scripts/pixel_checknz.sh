#!/bin/bash
# now loop through the above array
#source .bashrc
i=1
mydate=$(date --date="-$i day" "+%Y-%m-%d")
if [ $1 ]
then
  mydate=$1
fi

echo "running for $mydate"
realcount=69181305
realcount=$(zcat /mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/$mydate/*.gz|wc -l)
echo "realcount:"$realcount
query="select sum(hits) from fastpixel.pixel_general where entry_date='"$mydate"'";
echo $query
res=$(nzsql -h 66.117.49.50 -u armita -pw data_101? -Dfastpixel -c "$query")
echo "query res:"$res
b=$(echo $res |grep -o -P "\([0-9]* row\)")
dbcount=$(echo "${res/$b}" |grep -o -P "[0-9]+")
if [ -z "${dbcount}" ]
then 
  echo "not set"
  dbcount=0
fi
echo "dbcount:"$dbcount
if [ $dbcount -eq $realcount ]
then 
echo "were equal"
else
sender="Armita Kaboli<armita@digilant.com>"
receiver="armita@digilant.com,raj.joshi@digilant.com,sekhar.krothapalli@digilant.com"
body="Discrepancy between log files and db content in pixel_general in NETEZZA"
subj="Netezza Pixel discrepancy  for $mydate, real count: $realcount dbcount: $dbcount"
echo $body|mailx -s "$subj" "$receiver"
echo "sent notification email for netezza"
fi

