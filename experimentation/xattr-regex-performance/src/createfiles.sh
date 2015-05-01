#!/bin/bash

testdirectory="testdirectory"
filecount=10000
addrcount=10
addrlength=64
delimiter=","

echo -n "making directory..."
mkdir -p $testdirectory
echo "done"

pushd $testdirectory

echo -n "create files..."
for i in $(seq 1 $filecount); do

  #create file
  touch file$i &
done

wait
echo "done"

echo -n "add xattrs..."
for i in $(seq 1 $filecount); do

  # Generate attribute
  xattr=""
  for j in $(seq 1 $addrcount); do
    newattr=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $addrlength | head
-n 1)
    xattr="$xattr$delimiter$newattr"
  done
  xattr="$xattr$delimiter"

  #set attribute
  attr -s testattr -V $xattr file$i &>/dev/null &
  
done

wait
echo "done"

popd
