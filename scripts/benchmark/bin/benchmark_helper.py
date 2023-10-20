import pandas as pd
import os
import re
import os.path
import subprocess
import shutil

def parse_bandwidth(output_filepath):
    """
    Takes pfcp output previously written to a file, then parses that file to find bandwidth data in accordance with standard pfcp output format. The pfcp data rate output follows a standard format, so this function hard-codes where the "data rate" location will be within its line and grabs the numeric value accordingly. After getting the bandwidth value, converts from specified unit to GB/s explicitly as necessary. This function handles file opening and closing operations, so the user does not need to open the file before the function call or close it afterward.

    Params: output_filepath (str) -- string representing a path to a valid output file within the user's filesystem.
    Returns: true_bandwidth (float) -- parsed bandwidth value, converted to numeric type and scaled to GB/s. 
    """

    out = open(output_filepath, "r")

    unit_bandwidth = -1.0
    bandwidth_exponent = 0
    elapsed_time = -1.0

    for line in out.readlines():
        
        line_list = line.split() # split on whitespace
        # e.g., "INFO  FOOTER  Data    Rate:   123.456 GB/s" >>> ["INFO", "FOOTER", "Data", "Rate:", "123.456", "GB/s"]

        # Hard code the indices to expect a specific pfcp output format characteristic
        if (len(line_list) >= 4) and (line_list[2] == "Data" and ("Rate" in line_list[3])):
            
            unit_bandwidth = float(line_list[4])  # e.g., from ["123.456", "GB/s"] >>> unit_bandwidth = 123.456

            bandwidth_list = (line_list[5]).split('/')  # splits "GB/s" into ["GB", "s"] and similar
            
            # Parse based on units
            # If units exactly B, then bytes,
            # Else, parse by distinctive suffixes
            if (bandwidth_list[0] == "B"):
                bandwidth_exponent = 3
            elif ("K" in bandwidth_list[0]):  # must be KB
                bandwidth_exponent = 2
            elif ("M" in bandwidth_list[0]):  # must be MB
                bandwidth_exponent = 1
            elif ("G" in bandwidth_list[0]):  # must be GB
                bandwidth_exponent = 0
            elif ("T" in bandwidth_list[0]):  # must be TB
                bandwidth_exponent = -1
            elif ("P" in bandwidth_list[0]):  # must be PB
                bandwidth_exponent = -2
            elif ("E" in bandwidth_list[0]):  # must be EB
                bandwidth_exponent = -3

        if (len(line_list) >= 4) and (line_list[2] == "Elapsed" and ("Time" in line_list[3])):
            elapsed_time = float(line_list[4])

    out.close()

    # divide bandwidth present in file by a power of 1024 to convert to uniform GB/s unit
    true_bandwidth = (unit_bandwidth) / (1024**(bandwidth_exponent))
    return (true_bandwidth, elapsed_time)

"""
Function to generate summary statistics for a stripe benchmarking run. Takes parameters related to the run, including the path to the csv, time of the run, datablock/parity block/partsizd range and iteration values, file size, run mode (0 if no read, 1 if read), and path to output data to.
Parameter information:
csv: path to file, timeappend: string with time information, dmin-smult: integers, fsize: truncated file size string, read flag: 0 or 1, data_path: path to directory to store data
"""
def gen_stats_stripe(csv, timeappend, dmin, dmax, diter, pmin, pmax, piter, smin, smax, smult, fsize, readflag, data_path):
    #initialize variables with csv column names.
    bw_col = 'bw'
    sec_col = 'secs'
    mode_col = 'mode'
    db_col = 'db'
    pb_col = 'pb'
    ps_col = 'PSZ'

    #read csv into pandas dataframe and pull bandwidth and seconds columns for editing and parsing
    sumcsv = pd.read_csv(f'{csv}')
    bws = sumcsv[f'{bw_col}']
    seconds = sumcsv[f'{sec_col}']
    
    #apply lambda function to seconds column to set all missing values to -1
    seconds = seconds.apply(lambda s: -1 if pd.isna(s) else s)

    #write revised seconds and bandwidth to dataframe
    sumcsv[f'{bw_col}'] = bws
    sumcsv[f'{sec_col}'] = seconds
    
    #print stars to terminal
    printStars()
    
    #print header of run summary to stdout and summary file
    printLog(data_path, f"SUMMARY STATS: {timeappend} run.")
    printLog(data_path, f"Data blocks: {dmin}-{dmax}, intervals of {diter}. Parity blocks: {pmin}-{pmax}, intervals of {piter}. Part size: {smin}-{smax}, multiplier of {smult}. Filesize tested: {fsize}.")

    #pull column of write bandwidth data only
    sumcsv_w = sumcsv[sumcsv[f'{mode_col}'] == 'w']
    bws_w = sumcsv_w[f'{bw_col}']

    #exclude and tally any -1 values  to check number of pfcp failures
    failed_write_count = len(bws_w[bws_w == -1])
    bws_w = bws_w[bws_w != -1]

    #pull min, max, average values for write bandwidth
    min_bw_w = bws_w.min()
    max_bw_w = bws_w.max()
    ave_bw_w = round(bws_w.mean(), 2)

    #pull corresponding values of data/parity blocks and partsize for max value
    maxrows_w = sumcsv_w.loc[sumcsv_w[f'{bw_col}']==max_bw_w,[f'{db_col}',f'{pb_col}',f'{ps_col}']]
    minrows_w = sumcsv_w.loc[sumcsv_w[f'{bw_col}']==min_bw_w,[f'{db_col}',f'{pb_col}',f'{ps_col}']]

    #print summary statistics for write trials
    printLog(data_path, f"\nAverage WRITE bandwidth across all successful trials: {ave_bw_w} GB")
    printLog(data_path, f"Number of failed WRITE trials: {failed_write_count}")
    printLog(data_path, f"Maximum WRITE bandwidth of {max_bw_w} GB occurs at: ")
    printLog(data_path, maxrows_w.to_string(index=False))
    printLog(data_path, f"Minimum WRITE bandwidth of {min_bw_w} GB occurs at: ")
    printLog(data_path, minrows_w.to_string(index=False))
    
    #check if read was part of run to print read stats
    if(readflag):
        #pull column of read bandwidth data only  
        sumcsv_r = sumcsv[sumcsv[f'{mode_col}'] == 'r']
        bws_r = sumcsv_r[f'{bw_col}']
        
        #exclude and tally any -1 values  to check number of pfcp failures
        failed_read_count = len(bws_r[bws_r==-1])
        bws_r = bws_r[bws_r != -1]

        #pull min, max, average values for read bandwidth
        min_bw_r = bws_r.min()
        max_bw_r = bws_r.max()
        ave_bw_r = round(bws_r.mean(), 2)

        #pull corresponding values of data/parity blocks and partsize for max value
        maxrows_r = sumcsv_r.loc[sumcsv_r[f'{bw_col}']==max_bw_r,[f'{db_col}',f'{pb_col}',f'{ps_col}']]
        minrows_r = sumcsv_r.loc[sumcsv_r[f'{bw_col}']==min_bw_r,[f'{db_col}',f'{pb_col}',f'{ps_col}']]

        #print summary statistics for read trials
        printLog(data_path, f"\nAverage READ bandwidth across all successful trials: {ave_bw_r} GB")
        printLog(data_path, f"Number of failed READ trials: {failed_read_count}")
        printLog(data_path, f"Maximum READ bandwidth of {max_bw_r} GB occurs at: ")
        printLog(data_path, maxrows_r.to_string(index=False))
        printLog(data_path, f"Minimum READ bandwidth of {min_bw_r} GB occurs at: ")
        printLog(data_path, minrows_r.to_string(index=False))

    #print another star separator
    printStars()
    
    #write out modified CSV with update bandwidth, seconds counts
    sumcsv.to_csv(f'{csv}', index=False)


"""
Function to generate summary statistics for a rank benchmarking run. Takes parameters related to the run, including the path to the csv, time of the run, rank/stripe range and iteration values, file size, run mode (0 if no read, 1 if read), and path to output data to.
Parameter information:
csv: path to file, timeappend: string with time information, rmin-ps: integers, fsize: truncated file size string, read flag: 0 or 1, data_path: path to directory to store data
"""
def gen_stats_rank(csv, timeappend, rmin, rmax, riter, db, pb, ps, fsize, readflag, data_path):
    #initialize variables with csv column names.
    bw_col = 'bw'
    sec_col = 'secs'
    mode_col = 'mode'
    db_col = 'db'
    pb_col = 'pb'
    ps_col = 'PSZ'
    rank_col = 'rank'

    #read csv into pandas dataframe and pull bandwidth and seconds columns for editing and parsing
    sumcsv = pd.read_csv(f'{csv}')
    bws = sumcsv[f'{bw_col}']
    seconds = sumcsv[f'{sec_col}']
    
    #apply function to pulled bandwidth column to convert everything to GB
    bws = bws.apply(translate_fs)

    #apply lambda function to seconds column to set all missing values to -1
    seconds = seconds.apply(lambda s: -1 if pd.isna(s) else s)

    #write revised seconds and bandwidth to dataframe
    sumcsv[f'{bw_col}'] = bws
    sumcsv[f'{sec_col}'] = seconds
    
    printStars()
    
    #print header of run summary
    printLog(data_path, f"SUMMARY STATS: {timeappend} run.")
    printLog(data_path, f"Testing at {db}+{pb}, {ps} PSZ. Filesize tested: {fsize}. Iterating from {rmin} to {rmax} ranks in intervals of {riter}.")

    #pull column of write bandwidth data only
    sumcsv_w = sumcsv[sumcsv[f'{mode_col}'] == 'w']
    bws_w = sumcsv_w[f'{bw_col}']

    #exclude and tally any -1 values  to check number of pfcp failures
    failed_write_count = len(bws_w[bws_w == -1])
    bws_w = bws_w[bws_w != -1]

    #pull min, max, average values for write bandwidth
    min_bw_w = bws_w.min()
    max_bw_w = bws_w.max()
    ave_bw_w = round(bws_w.mean(), 2)

    #pull corresponding values of data/parity blocks and partsize for max value
    maxrows_w = sumcsv_w.loc[sumcsv_w[f'{bw_col}']==max_bw_w,[f'{rank_col}']]
    minrows_w = sumcsv_w.loc[sumcsv_w[f'{bw_col}']==min_bw_w,[f'{rank_col}']]

    #print summary statistics for write trials
    printLog(data_path, f"\nAverage WRITE bandwidth across all successful trials: {ave_bw_w} GB")
    printLog(data_path, f"Number of failed WRITE trials: {failed_write_count}")
    printLog(data_path, f"Maximum WRITE bandwidth of {max_bw_w} GB occurs at: ")
    printLog(data_path, maxrows_w.to_string(index=False))
    printLog(data_path, f"Minimum WRITE bandwidth of {min_bw_w} GB occurs at: ")
    printLog(data_path, minrows_w.to_string(index=False))
    
    #check if read was part of run to print read stats
    if(readflag):
        #pull column of read bandwidth data only  
        sumcsv_r = sumcsv[sumcsv[f'{mode_col}'] == 'r']
        bws_r = sumcsv_r[f'{bw_col}']
        
        #exclude and tally any -1 values  to check number of pfcp failures
        failed_read_count = len(bws_r[bws_r==-1])
        bws_r = bws_r[bws_r != -1]

        #pull min, max, average values for read bandwidth
        min_bw_r = bws_r.min()
        max_bw_r = bws_r.max()
        ave_bw_r = round(bws_r.mean(), 2)

        #pull corresponding values of data/parity blocks and partsize for max value
        maxrows_r = sumcsv_r.loc[sumcsv_r[f'{bw_col}']==max_bw_r,[f'{rank_col}']]
        minrows_r = sumcsv_r.loc[sumcsv_r[f'{bw_col}']==min_bw_r,[f'{rank_col}']]

        #print summary statistics for read trials
        printLog(data_path, f"\nAverage READ bandwidth across all successful trials: {ave_bw_r} GB")
        printLog(data_path, f"Number of failed READ trials: {failed_read_count}")
        printLog(data_path, f"Maximum READ bandwidth of {max_bw_r} GB occurs at: ")
        printLog(data_path, maxrows_r.to_string(index=False))
        printLog(data_path, f"Minimum READ bandwidth of {min_bw_r} GB occurs at: ")
        printLog(data_path, minrows_r.to_string(index=False))

    #print another star separator
    printStars()
    
    #write out modified CSV with update bandwidth, seconds counts
    sumcsv.to_csv(f'{csv}', index=False)

#function to print a row of stars to console.
def printStars():
    #Print separator of terminal width, or if not running in terminal or attribute unavailable, just a lot of stars
    try:
        ter_width = os.get_terminal_size().columns
        print('*' * ter_width)
    except OSError:
        print('*' * 200)

"""
function to print to stdout as well as a summary statistics text file in a given directory, data_path.
parameter breakdown:
data_path: path to directory to create and append to stat file in, *args/**kwargs: values to be printed
"""
def printLog(data_path, *args, **kwargs):
   #print passed arguments to stdout
   print(*args, **kwargs)
   #print passed arguments to summary file in append mode
    with open(f'{data_path}/summary_stats.txt','a') as file:
        print(*args, **kwargs, file=file)

"""
Function to take bandwidth string and convert it into GB, and turn NAN into -1. Designed to be applied to a pandas dataframe colmn, but can be used iteratively on a group of bandwidth strings or a single bandwidth string.
Takes string value to parse into GB bandwidth.
"""

def translate_fs(val):
    #set any null value to -1, as pfcp did not output bandwidth
    if(pd.isna(val)):
        return -1.00
    
    #dictionary to convert filesize to bytes to convert all bandwidth to GB
    size_conv = {'K':10**3,'M':10**6,'G':10**9,'T':10**12,'P':10**15,'E':10**18,'Z':10**21,'Y':10**24,'KB':10**3,'MB':10**6,'GB':10**9,'TB':10**12,'PB':10**15,'EB':10**18,'ZB':10**21,'YB':10**24}

    #parse for string and find multiplier in dictionary based on it
    mult = size_conv.get(''.join([i for i in str(val) if (not i.isdigit() and not i == '.')]))
    #multiply integer parsed from string by the found multiplier
    newval = float(''.join([i for i in str(val) if i.isdigit() or i == '.']))*mult
    #convert to GB and return rounded to 3 decimal places
    return round(newval/(10**9),3)

#Function to get minimum ranks per node configuration from pftool configuration.
def get_ranks():
    #get environment variable with pftool config path
    confp = os.getenv("PFTOOL_CONFIG_PATH")

    #open config file and read in all text
    with open(f'{confp}', 'r') as of:
       conftxt = of.read()
    
    #parse config file text using regular expression to find specific phrase with ranks, return value
    ranks = re.search(r'(?<=min_per_node: ).*', conftxt)
    return ranks.group(0)

"""
Function to set rank configuration within pftool config. Takes parameter of new rank value to set to.
Modifies configuration file directly-- to avoid overwriting, make a copy before and restore post benchmarking.
"""
def set_ranks(new_ranks):
    #get environment variable with pftool config path
    confp = os.getenv("PFTOOL_CONFIG_PATH")

    #open config file and read in all text
    with open(f'{confp}', 'r') as of:
       conftxt = of.read()

    #sub regular expression for ranks per node in the text of the config file
    conftxt = re.sub(r'(?<=min_per_node: ).*', str(new_ranks), conftxt)

    #write config file back out to config path
    with open(f'{confp}', 'w') as of:
       of.write(conftxt)

"""
Function to act as exit handler to clean up after a script run; deletes files used for testing and restores configs. Intended to be registered as an exit handler using the atexit module in various benchmarking scripts, but can be run independently as well. Attempts to check file existence when possible before attempting remove operation.
Parameter breakdown:
clush_g: clush group to run commands across nodes, sync_path: path to file used to sync mtimes in script, tstfile_r/w: stem path to files used for trial run (all files with that prefix will be removed), backup_marfs/pftool_path: paths to the backup locations of the original config files to restore post-run.
"""
def cleanup(clush_g, sync_path, tstfile_r, tstfile_w, backup_marfs_path, backup_pftool_path):
    #check if sync file exists at given location, and if so, perform remove.
    if(os.path.isfile(sync_path)):
        os.remove(sync_path) 
    #check if testfile path exists, and if so, remove. This accounts for the difference in full name but the consistent stem.
    if(tstfile_r):
        subprocess.run(f"clush -g {clush_g} rm -f {tstfile_r}*", shell=True)
    if(tstfile_w):
        subprocess.run(f"clush -g {clush_g} rm -f {tstfile_w}*", shell=True)
    #restore marfs and pftool configs if there is a file at the backup path.
    if(os.path.isfile(backup_marfs_path)):
        #obtain path to config file and copy back into place
        marfs_config_path = os.getenv("MARFS_CONFIG_PATH")
        shutil.copy2(backup_marfs_path, marfs_config_path)
        #remove backup copy
        os.remove(backup_marfs_path)
    if(os.path.isfile(backup_pftool_path)):
        #obtain path to config file and copy back into place
        pftool_config_path = os.getenv("PFTOOL_CONFIG_PATH")
        shutil.copy2(backup_pftool_path, pftool_config_path)
        #remove backup copy
        os.remove(backup_pftool_path)
