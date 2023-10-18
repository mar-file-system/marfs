#!/usr/bin/env python3
import pandas as pd
import argparse
from datetime import datetime
import yaml
from pathlib import Path
import subprocess
import statgen

#initialize command line argument parser
parser = argparse.ArgumentParser()
#argument: range of data blocks
parser.add_argument('-d','--dblocks', help='Range and traversal interval of data blocks', type=int, required=True, nargs=3)
#argument: range of parity blocks
parser.add_argument('-p', '--pblocks', help='Range and traversal interval of parity blocks', type=int, required=True, nargs=3)
#argument: erasure stripe width
parser.add_argument('-s', '--partsize', help='Partsize range and multiplier', type=int, required=True, nargs=3)
#flags: determine whether to read, write, or both. set required and mutually exclusive flag argument for user.
rwflags = parser.add_mutually_exclusive_group(required=True)
rwflags.add_argument('-w', '--write', help = 'Perform only write benchmark', action='store_true')
rwflags.add_argument('-rw', '--readwrite', help = 'Perform both read and write benchmark', action ='store_true')
#flag: determine whether to take 4 bytes off partsize to allow for alignment with CRC accounted for
parser.add_argument('-t', '--trim', help = 'Subtract 4 bytes from part size for buffer alignment', action='store_true')

#create parser to access cmdline arguments
args = parser.parse_args()

#pull arguments from parser
#data block vars
dmin = args.dblocks[0]
dmax = args.dblocks[1]
diter = args.dblocks[2]

#parity block vars
pmin = args.pblocks[0]
pmax = args.pblocks[1]
piter = args.dblocks[2]

#erasure stripe width vars
smin = args.partsize[0]
smax = args.partsize[1]
smult = args.partsize[2]

#read and write vars, set by checking flags
r, w = 0, 1
if(args.readwrite):
    r = 1

trim = 0
if(args.trim):
    trim = 1

#open config file for parsing
with open('benchmark-config.yml', 'r') as file:
    config = yaml.safe_load(file)
#pull clush group
clushg = config['clush_g']
#set up variables to directories in source directory path, timestamping the path to where data will be stored
source_path = config['source_path']
read_path = config['read_path']
marfs_path = config['dest_path']
dp = config['data_path']
timeappend = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
data_path = f"{dp}/{timeappend}"
#provide name for archive directory for runtime
archive_path = f"{data_path}/archive"

#pull filesize
fsize = config['file_size']
#Validate filesize config value
try:
    #check if argument is just an integer
    intfs = int(fsize)
#if not, attempt to parse for valid int and string
except ValueError:
    strfs = ""
    i = 0
    #iterate through fsize string
    for char in fsize:
        #find first nondigit character
        if(not char.isdigit()):
            #substring to find string component of fsize
            strfs = fsize[i::]
            #if no integer or integers later in string component, throw error
            if(not strfs.isalpha() or i == 0):
                print("Reconfigure with valid file size. Terminating.")
                quit()
            #otherwise substring is valid, exit for loop
            else:
                break
        i+=1

    #compare string component of fsize to valid filesizes and throw error if not valid filesize
    valid_sizes = ["K","M","G","T","P","E","Z","Y","R","Q","KB","MB","GB","TB","PB","EB","ZB","YB"]
    if not strfs in valid_sizes:
        print("Reconfigure with valid file size. Terminating.")
        quit()

#Set up CSV file and header.
csv = f'{data_path}/run{dmin}-{dmax}d_{pmin}-{pmax}p_{smin}-{smax}s.csv'
homedir = Path.home()
try:
    
    subprocess.check_call(f'./benchmark.sh -a {dmin} -b {dmax} -c {diter} -d {pmin} -e {pmax} -f {piter} -g {smin} -h {smax} -i {smult} -j {clushg} -k {source_path} -l {marfs_path} -m {data_path} -n {read_path} -o {archive_path} -p {fsize} -q {csv} -r {homedir} -s {r} -t {trim}', shell=True)
except subprocess.CalledProcessError:
    print("Problem running bash script. Terminated early")
    quit()

#call function to generate summary statistics and clean up csv
statgen.gen_stats_stripe(csv, timeappend, dmin, dmax, diter, pmin, pmax, piter, smin, smax, smult, fsize, r, data_path)
