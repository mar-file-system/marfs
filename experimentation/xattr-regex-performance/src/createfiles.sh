#!/bin/bash
#
# This script tests the extra cost of using regular expression parsing on sub
# fields of extended attributes (using a GPFS policy engine)
#
#

# VARIABLES --------------------------------------------------------------------
testdirectory="testdirectory" #make this a full path, in a GPFS
filecount=10
attrcount=10
attrlength=64
delimiter=","
attrstep=1
whichattr=3

echo "Starting with the following values:"
echo "Test Directory:                  $testdirectory"
echo "Total File Count:                $filecount"
echo "Attribute Subfield Count:        $attrcount"
echo "Attribute Subfield Length:       $attrlength"
echo "Attribute Delimiter:             $delimiter"
echo "Matching Attribute Step Counter: $attrstep"
echo "Matching Subfield Index:         $whichattr"


# Create Files -----------------------------------------------------------------

# Create directory
echo -n "Creating directory $testdirectory ..."
mkdir -p $testdirectory
echo "done!"

# Enter directory
echo "Entering $testdirectory"
pushd $testdirectory

rm -f *
ls -al
# Create files in parallel
echo -n "Creating $filecount empty files..."
for i in $(seq 1 $filecount); do
  touch file$i &
done
wait
echo "done!"

# Set attributes in parallel
echo  "Adding the large extended attrbute to each file."
echo  "This will take some time..."
for i in $(seq 1 $filecount); do
  echo -n "Setting file $i ..."
  xattr=""
  for j in $(seq 1 $attrcount); do
    newattr=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' \
        | fold -w $attrlength | head -n 1)
    xattr="$xattr$delimiter$newattr"
  done
  xattr="$xattr$delimiter"
  attr -s testattr -V $xattr file$i &>/dev/null &
  echo "done!"
done
wait
echo "done!"

# Set matching attributes ------------------------------------------------------

echo "Setting matching attributes for files"
for i in $(seq 1 $attrstep $filecount); do
  echo -n "Changing file $i ..."
  xattr=""
  for j in $(seq 1 $((whichattr - 1))); do
    newattr=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $attrlength \
        | head -n 1)
    xattr="$xattr$delimiter$newattr"
  done
  newattr=$(printf "%01000d" 0 | tr -dc 'a-zA-Z0-9' | fold -w $attrlength \
      | head -n 1)
  xattr="$xattr$delimiter$newattr"
  testthing=$(seq $((whichattr + 1)) $attrcount)
  for j in $testthing; do
    newattr=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $attrlength \
        | head -n 1)
    xattr="$xattr$delimiter$newattr"
  done
  xattr="$xattr$delimiter"
  attr -s testattr -V $xattr file$i &>/dev/null &
  echo "done!" 
done
wait
echo "done!"

# Exit test directory
echo "Exiting $testdirectory"
popd

sleep 5
time mmapplypolicy $(pwd)/$testdirectory -P noregex.txt
time mmapplypolicy $(pwd)/$testdirectory -P regex.txt
