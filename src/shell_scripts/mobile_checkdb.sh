#!/bin/bash
# now loop through the above array
j=0
declare -a arr=("imp" "conversion" "bid_all" "click")
declare -a arr2=("num_impressions" "num_conversions" "num_bids" "num_clicks")
for tp in ${arr[@]}
do
   echo $tp # or do whatever with individual element of the array
i=1
mydate=$(date --date="-$i day" "+%Y-%m-%d")
#mydate="2013-03-24"
echo "running for $mydate"

realcount=$(zcat /mnt/adnetik/adnetik-uservervillage/{admeld,rtb,nexage,openx}/userver_log/$tp/$mydate/*.gz|cut -f 8,10,141,143|grep "admeld_mobile\|google\|nexage\|openx"|grep -c "Tablet\|Phone")
echo $realcount
query="select sum(${arr2[$j]}) from mobile.mobile_hourly where (device_type='Tablet' or device_type='Phone') and entry_date='"$mydate"'";
echo $query
dbcount=$(mysql -h  thorin-internal.digilant.com -uarmita -pdata_101? dbh -e "$query")
dbcount=$(echo ${dbcount/"sum(${arr2[$j]})"/})
echo $dbcount
if [ $dbcount == $realcount ]; then
  echo "were equal"
else
sender="Armita Kaboli<armita@digilant.com>"
receiver="armita@digilant.com,raj.joshi@digilant.com,sekhar.krothapalli@digilant.com"
body="Discrepancy between log files and db content in mobile_hourly"
subj="Mobile discrepancy in $tp for $mydate, real count: $realcount dbcount: $dbcount"
echo $body | mail $receiver -s "$subj" -a"From:$sender"
echo "sent notification email"
fi
j=`expr $j + 1`
done
