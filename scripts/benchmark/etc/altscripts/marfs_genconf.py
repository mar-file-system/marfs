#!/usr/bin/env python3

import argparse
import xml.etree.ElementTree as xml
import yaml

#initialize command line argument parser
parser = argparse.ArgumentParser()

#optional argument: count of data blocks
parser.add_argument('-d','--dblocks', help='Count of data blocks', type=int, default=4,nargs='?')

#optional argument: count of parity blocks
parser.add_argument('-p', '--pblocks', help='Count of parity blocks', type=int, default=1,nargs='?')

#optional argument: erasure stripe width
parser.add_argument('-s', '--partsize', help='Partsize value', type=int, default=1024,nargs='?')

#create parser to access cmdline arguments
args = parser.parse_args()

#open config file to parse for template and store as string
with open('benchmark-config.yml', 'r') as file:
    config = yaml.safe_load(file)
tmplt_loc = config['tmplt_path']

#parse xml file into tree and identify root
configtree = xml.parse(tmplt_loc)
configroot = configtree.getroot()

#find respective tags per cmdline arg and replace all instances with user specs
for n in configroot.findall('.//N'):
    n.text = str(args.dblocks)
for e in configroot.findall('.//E'):
    e.text = str(args.pblocks)
for psz in configroot.findall('.//PSZ'):
    psz.text = str(args.partsize)

#write user mods to new config file
fname = f'/opt/campaign/install/etc/marfs-config.xml'
configtree.write(fname)
