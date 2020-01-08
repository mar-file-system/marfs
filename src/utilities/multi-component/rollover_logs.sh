#!/bin/bash

function usage {
    echo "$0 <path to log dir symlink>"
    echo "  If path is '-' then create the log dir and symlink from scratch"
    echo "  in the current directory."
}

if [ $# != 1 ] ; then
    usage
    exit 1
fi

if [ $1 = '-' ] ; then
    mkdir degraded.0
    ln -s `realpath degraded.0` degraded
    exit 0
fi
    
real_logdir=`realpath $1`
echo "Old log dir: $real_logdir"

let n=`echo $real_logdir | awk -F. '{print $NF}'`
let n++

mkdir $real_logdir/../degraded.$n
rm $1
ln -s `realpath $real_logdir/../degraded.$n` $1

echo "New log dir: $(realpath $1)"
