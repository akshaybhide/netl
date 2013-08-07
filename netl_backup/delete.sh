#!/bin/bash
FILES=$(ls -latr /home/armita/pixel/staging/[0-9]*.txt |grep -o -P "[0-9]+.txt")
totalno=$(ls -latr /home/armita/pixel/staging/[0-9]*.txt |wc -l)
echo "total="$totalno
#echo $FILES
#FILES='/home/armita/insertedfiles/[0-9]*.txt'
count=10
for f in $FILES
do
echo "rm $f"
#echo $totalno
#echo $count
if [ $count -gt $totalno ]; then
 break
fi
rm "/home/armita/pixel/staging/"$f
count=`expr $count + 1`
done
