#!/usr/bin/env python3

import argparse
import pandas as pd
import re
from pathlib import Path
import subprocess
from datetime import datetime
import os
import shutil
import benchmark_helper
import atexit


"""
Method to return bandwidth and seconds for a single pfcp run. Does not modify configurations at all, just performs a single pfcp run and returns output.
Parameter breakdown:
mode: 'r' or 'w' based on if failed trial was a read or write, fsize: size of file to truncate for testing, clushg: clush group to run commands across relevant nodes, marp: path to marfs directory to write to, readp: path to read directory to read into, srcp: location to set up truncated file, sfilep: path to file to sync mtimes with, arxp: path to archive directory to put all pfcp raw outputs for each trial, run_num: run number, used to append to archive textfile to track runs 

"""
def rerun_trial(mode, fsize, clushg, marp, readp, srcp, sfilep, arxp, run_num):

    #remove any existing files and generate data to be written on all compute nodes
    try:
        subprocess.run(f"clush -g {clushg} rm -f {marp}/tstfile", shell=True)
        subprocess.run(f"clush -g {clushg} rm -f {srcp}/tstfile", shell=True) 
        subprocess.run(f"clush -g {clushg} truncate --size={fsize} {srcp}/tstfile", shell=True)

        #timesync all testfiles on nodes based on sync file
        subprocess.run(f"clush -g {clushg} touch -m -r {sfilep} {srcp}/tstfile", shell=True)
    except subprocess.CalledProcessError:
        print(f"Error with pre-pfcp setup. Bandwidth for {db} {pb} {ps} {mode} not recovered.")
        return (-1, -1)
    
    #pfcp command for write, redirecting output to an archive file for the run
    try:
        subprocess.run(f"pfcp {srcp}/tstfile {marp} >> {arxp}/rerun{run_num}-w.txt 2>&1", shell=True)
    except subprocess.CalledProcessError:
        print(f"Failed pfcp. Bandwith for {db} {pb} {ps} {mode} not generated.")
        return (-1, -1)
    
    #if redoing write trial, return bandwidth and seconds from write trial
    if(mode == 'w'):
        #call helper function to pull bandwidth and seconds from archive file path
        bw, secs = pull_bw_secs(f"{arxp}/rerun{run_num}-w.txt")
        return (bw, secs)

   #pfcp command for read
    try:
        subprocess.run(f"pfcp {marp}/tstfile {readp} >> {arxp}/rerun{run_num}-r.txt 2>&1", shell=True)
    except subprocess.CalledProcessError:
        print(f"Failed pfcp. Bandwith for {db} {pb} {ps} {mode} not generated.")
        return (-1, -1)
    
    #if redoing read trial, return bandwidth and seconds from read trial
    if(mode == 'r'):
        #call helper function to pull bandwidth and seconds from archive file path
        bw, secs = pull_bw_secs(f"{arxp}/rerun{run_num}-r.txt")
        return (bw, secs)

#function to parse for bandwidth and seconds given output file
def pull_bw_secs(outputfile):
    #open file for reading and store text contents
    with open(f'{outputfile}', 'r') as of:
       filetxt = of.read()
    
    #search for bandwidth and seconds with regex
    bw = re.search(r'(?<=Data Rate:).*(?=/second)',filetxt)
    secs = re.search(r'(?<=Elapsed Time:).*(?=seconds)',filetxt)
    
    #if bandwidth not found, trial failed
    if(not bw):
        return (-1, -1)
    
    #pull bandwidth from search object, remove all spaces, and translate to GB
    bw = bw.group(0).replace(" ", "")
    bw = benchmark_helper.translate_fs(bw)

    #return found bandwidth and second values
    return (bw, secs.group(0).strip())

#script main body
if __name__ == "__main__":
    
    #variables to define column names
    bw_col = 'bw'
    mode_col = 'mode'
    db_col = 'db'
    pb_col = 'pb'
    ps_col = 'PSZ'
    run_col = 'gl_iters'
    fs_col = 'filesize'
    sec_col = 'secs'
    rank_col = 'rank'

    #get environment variables for config paths and data repo
    marfs_path = os.getenv("MARFS_CONFIG_PATH")
    pftool_path = os.getenv("PFTOOL_CONFIG_PATH")
    repo_bin = os.getenv("MARFS_BENCHMARK_REPO_BIN")
    
    #take various arguments for trial setup
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--filepath', help='Path to CSV to parse for missing bandwidths', required=True)
    parser.add_argument("-g", "--group", type=str, required=False, default="all", help="group of nodes on a cluster across which clush will run for administrative tasks") 
    parser.add_argument("-b", "--basepath", type=str, default=os.path.abspath("."), required=False, help="Absolute path within user's filesystem from which run data and archive directories will be created")
    parser.add_argument("-t", "--testfile-root", type=str, default="/dev/shm", required=False, help="Absolute path within user's filesystem from which test file will be created across all nodes for the benchmark run")
    parser.add_argument("-wd","--write-destination", type=str, default="/campaign/full-access-subspace", required=False, help="Absolute path which pfcp will write to within the MarFS instance")
    parser.add_argument("-rd", "--read-destination", type=str, default="/dev/null", required=False, help="Absolute path within user filesystem that pfcp will write to after reading from MarFS instance")

    #flag to determine whether the trials to be rerun were stripe benchmarks or rank benchmarks
    modeflags = parser.add_mutually_exclusive_group(required=True)
    modeflags.add_argument('-s', '--stripe', help = 'Backfill for stripe benchmark', action='store_true')
    modeflags.add_argument('-r', '--rank', help = 'Backfill for rank benchmark', action ='store_true')

    #pull command line args from parser and store into easy variables
    args = parser.parse_args()
    csvp = args.filepath
    cg = args.group
    rp = args.read_destination
    mp = args.write_destination
    dp = args.basepath
    sp = args.testfile_root

    #setup vars for data directory structure
    timeappend = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    ap = f"{dp}/rerun-{timeappend}/archive"
    inter_csvp=f"{dp}/rerun-{timeappend}/output.csv"

    #create path to eventually store sync file
    homep = Path.home()
    sfile = f"{homep}/sfile"
    
    #register exit handler to cleanup on exit with all relevant files
    atexit.register(benchmark_helper.cleanup, cg, f"{sfile}", f"{rp}/tstfile", f"{mp}/tstfile", f"{dp}/temp_marfs", f"{dp}/temp_pftool") 
    
    #set mode = 0 for rank benchmarking, 1 for stripe benchmarking
    rerun_stripe = 0
    if(args.stripe):
        rerun_stripe = 1

    #read in csv to pandas dataframe
    try:
        csv = pd.read_csv(f'{csvp}')
    except FileNotFoundError:
        print("CSV not found. Exiting.")
        quit()

    
    #pull only trials where bandwidth is -1, indicating failure
    failedtrials = csv.loc[csv[f'{bw_col}'] == -1,[f'{db_col}',f'{pb_col}',f'{ps_col}',f'{mode_col}',f'{fs_col}',f'{run_col}',f'{rank_col}']]
    #get number of failed trials, quit if there are no trials to rerun
    num_ft = failedtrials.shape[0]
    if(num_ft == 0):
        print("No trials to backfill.")
        quit()
    #print future number of reruns to stdout
    print(f"Total number of trials to backfill: {num_ft}")
    
    #create archive data subdirectory
    os.makedirs(ap)   
    
    #create csv file to append intermediate results (in addition to modifying the other csv in place post-run)
    csv_out = open(inter_csvp, "a")
    print(f"{pb_col},{db_col},{ps_col},{bw_col},{fs_col},{mode_col},{run_col},{sec_col},{rank_col}", file=csv_out)

    #create sync file in home directory to sync mtimes for all runs
    try:
        subprocess.run(f"touch {sfile}", shell=True)
    except subprocess.CalledProcessError:
        print("Failed to generate sync file. Exiting.")
        quit()
    
    #copy current config to separate location in data directory to preserve upon termination
    shutil.copy2(marfs_path, f"{dp}/temp_marfs")
    shutil.copy2(pftool_path, f"{dp}/temp_pftool")

    #perform different setup depending on if stripe or rank benchmark is being backfilled
    #if stripe benchmark is being rerun, set ranks in pftool config to match ranks from run
    if(rerun_stripe):
        #pull first value from ranks column in csv, as ranks stays the same over the run
        test_ranks = csv[f'{rank_col}'].iloc[0]

        #set configuration to that number of ranks
        benchmark_helper.set_ranks(test_ranks)
    
    #if rank benchmark is being rerun, set erasure stripe configs to match run
    else:
        #obtain fixed values for db, pb, ps from csv
        test_db = csv[f'{db_col}'].iloc[0]
        test_pb = csv[f'{pb_col}'].iloc[0]
        test_ps = csv[f'{ps_col}'].iloc[0]

        #set marfs config to that scheme with genconf script
        try:
            subprocess.run(f"{repo_bin}/marfs_genconf.py -n {test_db} -e {test_pb} -p {test_ps}", shell=True)
        except subprocess.CalledProcessError:
            print(f"Config could not be generated. Exiting.")
            quit()

    #iterate through all failed trials
    rewrite_count = 0
    for row in failedtrials.itertuples():

        #print progress info
        print(f"Trials run: {rewrite_count}/{num_ft}")
        
        #pull values from csv row to know what to rerun
        db=getattr(row,f'{db_col}')
        pb=getattr(row,f'{pb_col}')
        ps=getattr(row,f'{ps_col}')
        mo=getattr(row,f'{mode_col}')
        fs=getattr(row,f'{fs_col}')
        rn=getattr(row,f'{run_col}')
        rank=getattr(row,f'{rank_col}')
        
        
        #setup for stripe rerun
        if(rerun_stripe):
            #print trial info to stdout
            print(f"Rerunning trial for {db} + {pb}, {ps} partsize, {mo}")
        
            #generate marfs config file for given parameters
            try:
                subprocess.run(f"{repo_bin}/marfs_genconf.py -n {db} -e {pb} -p {ps}", shell=True)
            except subprocess.CalledProcessError:
                print(f"Config could not be generated. Exiting.")
                quit()

        #setup for rank rerun
        else:
            #print trial info to stdout
            print(f"Rerunning trial for {rank}, {mo}")
            
            #generate new pftool config accordingly
            benchmark_helper.set_rank(rank)

        #set bandwidth and number of failed trials to prepare for run
        new_bw = -1
        num_failed = 0
        
        #rerun trial in a loop until it succeeds, printing the number of failed trials
        while(new_bw < 0):
            if(num_failed > 0): print(f"Failed. Reattempting trial for the {num_failed} time.")
            new_bw, new_secs = rerun_trial(mo, fs, cg, mp, rp, sp, sfile, ap, rn) 
            num_failed+=1 
        rewrite_count+=1
        
        #rewrite column values for bw and secs if run was successful
        #edit csv variable to update row with new and correct bandwidth
        csv.loc[(csv[f'{run_col}'] == rn) & (csv[f'{mode_col}'] == mo),f'{bw_col}'] = new_bw
        csv.loc[(csv[f'{run_col}'] == rn) & (csv[f'{mode_col}'] == mo),f'{sec_col}'] = new_secs
        print(f"Bandwidth generated was: {new_bw}") 
        #print row to csv for rerun trials only
        print(f"{pb},{db},{ps},{new_bw},{fs},{mo},{rn},{rank},{new_secs}", file=csv_out)      
        
    #write out csv to original file path
    csv.to_csv(f'{csvp}', index=False)
    
    #close file with only values from rerun
    csv_out.close()

    
