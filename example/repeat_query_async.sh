#!/bin/bash

wuid_list="wuid_list.txt"
wuid_host_list="wuid_host_list.txt"
repeats=100

query="roxie query --name searchlinks --query name 'Subido, Rebecca' --wait 1 >> $wuid_list"
for (( i=0;i<$repeats;i++)); do eval $query; done

while read wuid_line; do
  wuid=`echo $wuid_line | cut -d':' -f1`
  ecl wait $wuid
  echo "$wuid:" `roxie lookup_workunit_info $wuid | cut -c 3- | rev | cut -c 3- | rev` >> wuid_host_list.txt
done < $wuid_list
