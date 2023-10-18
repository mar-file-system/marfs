#!/usr/bin/env python3
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
import benchmark_helper 
import os
from shutil import copy2 as copy
import atexit

#initialize command line argument parser
parser = argparse.ArgumentParser()

#get repo root environment variable
repo_root = os.getenv("MARFS_BENCHMARK_REPO_REF")

#setup command line args
parser.add_argument('-d','--dblocks', help='Fixed number of data blocks', type=int, required=True)
parser.add_argument('-p','--pblocks', help='Fixed number of parity blocks', type=int, required=True)
parser.add_argument('-s','--partsize', help='Fixed partsize', type=int, required=True)
parser.add_argument('-r','--rank', help='Range of ranks to benchmark over', type=int, required=True, nargs=2)
parser.add_argument('-i', '--rankiter', help='Number to increment ranks by while iterating from min to max', type=int, required=False, default=1)
parser.add_argument("-fs", "--filesize", type=str, required=True, help="Test file size. Provide a string in the same format as you would provide to truncate or similar commands (ie, '1T', '2MB', etc.)")
parser.add_argument("-g", "--group", type=str, required=False, default="all", help="group of nodes on a cluster across which clush will run for administrative tasks")
parser.add_argument("-b", "--basepath", type=str, default=f"{repo_root}/data", required=False, help="Absolute path within user's filesystem from which run data and archive directories will be created")
parser.add_argument("-t", "--testfile-root", type=str, default="/dev/shm", required=False, help="Absolute path within user's filesystem from which test file will be created across all nodes for the benchmark run")
parser.add_argument("-wd","--write-dest", type=str, default="/campaign/full-access-subspace", required=False, help="Absolute path which pfcp will write to within the MarFS instance")
parser.add_argument("-rd", "--read-dest", type=str, default="/dev/null", required=False, help="Absolute path within user filesystem that pfcp will write to after reading from MarFS instance")

#flags: determine whether to read, write, or both. set required and mutually exclusive flag argument for user.
rwflags = parser.add_mutually_exclusive_group(required=True)
rwflags.add_argument('-w', '--write', help = 'Perform only write benchmark', action='store_true')
rwflags.add_argument('-rw', '--readwrite', help = 'Perform both read and write benchmark', action ='store_true')

#obtain user input and set relevant variables
args = parser.parse_args()
clushg = args.group
source_path = args.testfile_root
read_path = args.read_dest
marfs_path = args.write_dest
dp = args.basepath

#create parser to access cmdline arguments
args = parser.parse_args()

#get fixed marfs config values
db = args.dblocks
pb = args.pblocks
ps = args.partsize

#get rank values and iteration
minrank = args.rank[0]
maxrank = args.rank[1]
rankiter = args.rankiter

#read and write vars, set by checking flags
r, w = 0, 1
if(args.readwrite):
    r = 1

#setup for directory structure

timeappend = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
data_path = f"{dp}/rankbm_{timeappend}"
#provide name for archive directory for runtime
archive_path = f"{data_path}/archive"

#get filesize
fsize = args.filesize
#validate filesize
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

#get home directory to set up sync file
homedir = Path.home()

#register exit handler with relevant paths to ensure cleanup on exit
atexit.register(benchmark_helper.cleanup, clushg, f"{homedir}/sfile", f"{read_path}/tstfile", f"{marfs_path}/tstfile", f"{dp}/temp_marfs", f"{dp}/temp_pftool")

#set up csv path
csv = f'{data_path}/run{db}d_{pb}p_{ps}s:{minrank}_to_{maxrank}_by_{rankiter}.csv'

#call helper bash script with all relevant cmdline args
try:    
    subprocess.check_call(f'./rank_benchmark.sh -a {db} -b {pb} -c {ps} -d {minrank} -e {maxrank} -f {rankiter} -g {clushg} -h {source_path} -i {marfs_path} -j {data_path} -k {read_path} -l {archive_path} -m {fsize} -n {csv} -o {homedir}/sfile -p {r}', shell=True)
except subprocess.CalledProcessError:
    print("Problem running bash script. Terminated early")
    quit()

#statistics generation for rank benchmarking
benchmark_helper.gen_stats_rank(csv, timeappend, minrank, maxrank, rankiter, db, pb, ps, fsize, r, data_path)

#setup copy csv without backfill to act as backup copy, put in archive directory
copied_csv_path = f"{archive_path}/run{db}d_{pb}p_{ps}s:{minrank}_to_{maxrank}_by_{rankiter}-unbackfilled_data.csv"
copy(csv, copied_csv_path)

#run backfill script with relevant arguments
try:    
    subprocess.run(f'./csv_backfill.py -f {csv} -g {clushg} -b {archive_path} -t {source_path} -wd {marfs_path} -rd {read_path} -r', shell = True)
except subprocess.CalledProcessError:
    print("Problem running backfill script.")
