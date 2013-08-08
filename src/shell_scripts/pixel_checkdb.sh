#!/bin/bash
# now loop through the above array
i=1
mydate=$(date --date="-$i day" "+%Y-%m-%d")
#mydate="2013-03-24"
echo "running for $mydate"
realcount=$(zcat /mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/$mydate/*.gz|wc -l)
echo $realcount
query="select sum(pixel_count) from fastpixel.pixel_general where entry_date='"$mydate"'";
echo $query
dbcount=$(mysql -h  thorin-internal-replication.digilant.com -uarmita -pdata_101? fastpixel -e "$query")
dbcount=$(echo ${dbcount/sum(pixel_count)/})
echo $dbcount
if [ $dbcount == $realcount ]; then
  echo "were equal"
else
sender="Armita Kaboli<armita@digilant.com>"
receiver="armita@digilant.com,raj.joshi@digilant.com,sekhar.krothapalli@digilant.com"
body="Discrepancy between log files and db content in pixel_general"
subj="Pixel discrepancy  for $mydate, real count: $realcount dbcount: $dbcount"
echo $body | mail $receiver -s "$subj" -a"From:$sender"
echo "sent notification email"
fi
