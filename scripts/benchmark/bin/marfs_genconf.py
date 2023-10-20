#!/usr/bin/env python3

import argparse
import xml.etree.ElementTree as xml
import os

#initialize command line argument parser
parser = argparse.ArgumentParser()
#optional argument: count of data blocks
parser.add_argument('-n','--dblocks', help='Count of data blocks', type=int, required=True)
#optional argument: count of parity blocks
parser.add_argument('-e', '--pblocks', help='Count of parity blocks', type=int, required=True)
#optional argument: erasure stripe width
parser.add_argument('-p', '--partsize', help='Partsize value', type=int, required=True)
#create parser to access cmdline arguments
args = parser.parse_args()

#get repo environment variable to get example marfs config
repo_root = os.getenv("MARFS_BENCHMARK_REPO_REF")

#parse xml file into tree and identify root
configtree = xml.parse(f'{repo_root}/etc/marfs-config-ex.xml')
configroot = configtree.getroot()

#find respective tags per cmdline arg and replace all instances with user specs
for n in configroot.findall('.//N'):
    n.text = str(args.dblocks)
for e in configroot.findall('.//E'):
    e.text = str(args.pblocks)
for psz in configroot.findall('.//PSZ'):
    psz.text = str(args.partsize)

#write user mods to new config file
confp = os.getenv("MARFS_CONFIG_PATH") 
configtree.write(confp)
