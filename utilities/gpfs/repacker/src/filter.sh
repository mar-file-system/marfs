#!/bin/bash

for i in `cat tmp_packed_log | awk '{print $1}' | sort | uniq`; do
  echo $i 
  packet_count=`grep $i tmp_packed_log | awk '{print $3}' | tail -1`
  found_count=`grep $i tmp_packed_log | wc -l`
  #echo packet_count=$packet_count  found_count=$found_count
  if ((packet_count != found_count)); then
    echo packet_count=$packet_count  found_count=$found_count $i
  else 
    echo "###### packet_count = ${packet_count} found_count ${found_count}######"
  fi
  echo
done
for i in `cat tmp_packed_log | awk '{print $1}' | sort | uniq`; do
  echo "##### OBJECT: ${i} has the following files associated with it:"
  for j in `cat tmp_packed_log | grep $i | awk '{print $2}'`; do
     echo "      #### FILE: ${j}"
  done
done
