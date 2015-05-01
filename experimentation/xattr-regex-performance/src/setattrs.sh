#!/bin/bash

testdirectory="testdirectory"
filecount=10000
addrcount=10
addrstep=10
whichaddr=3
delimiter=","
attrlength=64

pushd $testdirectory

echo -n "setting xattrs..."
for i in $(seq 1 $addrstep $filecount); do

  echo "changing file$i"

  # Generate attribute
  xattr=""
  for j in $(seq 1 $((whichaddr - 1))); do
    newattr=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $attrlength | head
-n 1)
    xattr="$xattr$delimiter$newattr"
  done

  #add attr of zeros
  newattr=$(echo
"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
| tr -dc 'a-zA-Z0-9' | fold -w $attrlength | head -n 1)
  xattr="$xattr$delimiter$newattr"

  testthing=$(seq $((whichaddr + 1)) $addrcount)
  for j in $testthing; do
    newattr=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $attrlength | head
-n 1)
    xattr="$xattr$delimiter$newattr"
  done

  xattr="$xattr$delimiter"
  echo $xattr

  #set attribute
  attr -s testattr -V $xattr file$i &>/dev/null &
  
done

wait
echo "done"

popd
