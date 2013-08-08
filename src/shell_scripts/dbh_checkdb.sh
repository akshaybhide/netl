#!/bin/bash
declare -a arr=("imp" "conversion" "bid_all" "click")
# now loop through the above array
for tp in ${arr[@]}
do
   echo $tp # or do whatever with individual element of the array
i=1
mydate=$(date --date="-$i day" "+%Y-%m-%d")
#mydate="2013-03-24"
echo "running for $mydate"
realcount=$(zcat /mnt/adnetik/adnetik-uservervillage/dbh/userver_log/$tp/$mydate/*.gz|wc -l)
echo $realcount
query="select count(*) from dbh.dbh_general2 where entry_date='"$mydate"' and type='"$tp"'";
echo $query
dbcount=$(mysql -h  thorin-internal.digilant.com -uarmita -pdata_101? dbh -e "$query")
dbcount=$(echo ${dbcount/count(\*)/})
echo $dbcount
if [ $dbcount == $realcount ]; then
  echo "were equal"
else
sender="Armita Kaboli<armita@digilant.com>"
receiver="armita@digilant.com,raj.joshi@digilant.com,sekhar.krothapalli@digilant.com"
body="Discrepancy between log files and db content in dbh_general2"
subj="DBH discrepancy in $tp for $mydate, real count: $realcount dbcount: $dbcount"
echo $body | mail $receiver -s "$subj" -a"From:$sender"
echo "sent notification email"
fi
done
