#!/bin/bash
# now loop through the above array
#source .bashrc
i=1
while true
do
mydate=$(date --date="-$i day" "+%Y-%m-%d")
if [ $1 ]
then
  mydate=$1
fi

echo "running for $mydate"
realcount=69181305
realcount=$(zcat /mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/$mydate/*.gz|wc -l)
echo "realcount:"$realcount
query="select sum(hits) from fastetl.pixel_general where entry_date='"$mydate"'";
echo $query
res=$(nzsql -h 66.117.49.50 -u reporting -pw k4573F7B -Dfastetl -c "$query")
echo "query res:"$res
b=$(echo $res |grep -o -P "\([0-9]* row\)")
dbcount=$(echo "${res/$b}" |grep -o -P "[0-9]+")
if [ -z "${dbcount}" ]
then 
  echo "not set"
  dbcount=0
fi
echo "dbcount:"$dbcount
diff=$((realcount - dbcount))
percent=$(echo "scale=4;$diff*100/$realcount" | bc)
#percent=$((100*diff/realcount))
echo $percent
if [ $dbcount -eq $realcount ]
then 
echo "were equal"
else
#if [ $percent -gt 0.1 ]
sender="Akshay Bhide<akshay.bhide@digilant.com>"
#receiver="akshay.bhide@digilant.com"
receiver="akshay.bhide@digilant.com,raj.joshi@digilant.com,sekhar.krothapalli@digilant.com"
body="Discrepancy between log files and db content in pixel_general in NETEZZA.REAL COUNT : $realcount. NETEZZA DB COUNT: $dbcount. DIFFERENCE is $diff ($percent %)"
#body="This is a test mail"
subj="Netezza Pixel discrepancy for $mydate"
echo $body|mailx -s "$subj" "$receiver"
echo "sent notification email for netezza"
fi
#sleep $((60 * 60 * 24))
sleep 24h
done
