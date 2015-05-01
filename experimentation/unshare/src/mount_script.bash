#!/bin/bash

# first argument is the GPFS to mount
# second argument is the directories to use for MHDDFS
# third argument is mount point for MHDDFS

echo Mounting GPFS: $1
echo Directories for MHDDFS: $(echo $2 | tr ',' '\n')
echo Mount point for MHDDFS: $3
mmmount $1
for directory in $(echo $2 | tr ',' '\n'); do
  mkdir -p $directory
done
mhddfs $2 $3 -o allow_other

