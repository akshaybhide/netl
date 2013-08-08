#!/bin/bash
for (( i=2; i<=2 ; i++ ))
do
mydate=$(date --date="-$i day" "+%Y-%m-%d")
if [ $1 ]
then
  mydate=$1
fi
echo "running for $mydate"
hadoop fs -cat "output"$mydate/part* > $mydate".txt"
if [ -s $mydate".txt" ]
then
sed  -i "s/,\s*/,/g" $mydate".txt"
fi
mysql -h thorin-internal.digilant.com -u armita -pdata_101? fastetl -e "select external_id, price from fastetl.tpp_segments" > tpp_segments
#awk '{print($1)}' $mydate".csv"  
#cat $mydate".csv"

printf "CampaignID, SegmentID, Count\n">$mydate".csv"
awk '{num=split(substr($5, match($5, /\[/)+1, match($5,/\]/)-2), tmp, "\,");for(i=1;i<=num;i++)print(tmp[i],"\t",$3) }' $mydate".txt" |sort |uniq -c |sed 's/^\s\+//'|awk '{print($3,",",$2,"," ,$1)}'>>$mydate".csv"
/bin/rm $mydate"_price.csv"
/bin/rm finance_report.csv
cat $mydate".csv" | while read LINE 
do 
#  echo "LINE:"$LINE
  campid=$(echo $LINE|awk '{print($1)}'|tr -d ',')
  segmentid=$(echo $LINE|awk '{print($3)}'|tr -d ',')
  count=$(echo $LINE|awk '{print($5)}'|tr -d ',')
#  echo "Count:"$count
#  echo $segmentid"=segmentid"
  a=$(echo $segmentid|grep -oh "[0-9]*") #ignoring first line which are the titles: Segment 
#  echo "a is "$a
  if [ $a ]
  then
    #echo "(grep bk_$segmentid$ tpp_segments)"
    g=$(grep "bk_"$segmentid"\s" tpp_segments)
    #echo $g"=g"
    if [ "$g" ];then
      price=$(echo $g | grep -oh "\s[0-9]*.[0-9]*")
      #echo $price"=price"
      #echo $rr" total"
      rr=$(echo "$count * $price"|bc)
      rr=$(echo "scale=4;$rr / 1000"|bc)
    fi
    echo $mydate" , "$campid" , "$segmentid" , "$count" , "$price ", " $rr >> $mydate"_price.csv"
  fi 
done
cp $mydate"_price.csv" finance_report.csv
if [ -s finance_report.csv ]
then
#echo hi
mysqlimport --local --fields-optionally-enclosed-by="\"" --fields-terminated-by=, --lines-terminated-by="\n" -h thorin-internal.digilant.com -uarmita -pdata_101? thirdparty 'finance_report.csv'
fi
if [ $1 ]
then
  break
fi
done
