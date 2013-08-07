#!/bin/bash
# now loop through the above array
#source .bashrc
body=$(printf  "%18s%18s%18s%18s%18s\n" "Date" "ExcName" "LogCount" "DBCount" "Diff")
declare -a exch=("admeld" "admeld_mobile" "admeta" "adnexus" "casale" "contextweb" "dbh" "facebook" "improvedigital" "liveintent" "nexage" "openx" "pubmatic" "rtb" "rubicon" "yahoo")
i=1
for ex in ${exch[@]}
do
   echo "exchange:"$ex # or do whatever with individual element of the array
mydate=$(date --date="-$i day" "+%Y-%m-%d")
#mydate="2013-03-24"
if [ $1 ]
then
  mydate=$1
fi
echo "running for $mydate"
if [ $ex = "rtb" ]
then
  ex2="google"
else
  ex2=$ex
fi
if [ $ex = "admeld_mobile" ]
then 
  realcount=$(zcat /mnt/adnetik/adnetik-uservervillage/admeld/userver_log/imp/$mydate/*.gz|cut -f 10|grep $ex|wc -l)
else
 realcount=$(zcat /mnt/adnetik/adnetik-uservervillage/$ex/userver_log/imp/$mydate/*.gz|wc -l)
fi
echo $realcount
query="select count(event_type) from fastetl.cbicca where entry_date='"$mydate"' and event_type='imp' and ad_exchange='"$ex2"'"
echo $query
res=$(nzsql -h 66.117.49.50 -u armita -pw data_101? -Dfastetl -c "$query")
echo "query res:"$res
b=$(echo $res |grep -o -P "\([0-9]* row\)")
dbcount=$(echo "${res/$b}" |grep -o -P "[0-9]+")
if [ -z "${dbcount}" ]
then
  echo "not set"
  dbcount=0
fi
echo "dbcount:"$dbcount
diff=`expr $realcount - $dbcount`
newline=$(printf "%18s%18s%18s%18s%18s\n" "$mydate" "$ex" "$realcount" "$dbcount" "$diff")
body=$body"\n"$newline
done
sender="Armita Kaboli<armita@digilant.com>"
receiver="armita@digilant.com,raj.joshi@digilant.com,sekhar.krothapalli@digilant.com,aaron.ives@digilant.com,aubrey.jaffer@digilant.com"
subj="Fastetl report"
printf "$body" | mailx -s "$subj" "$receiver"
echo "sent notification email"
