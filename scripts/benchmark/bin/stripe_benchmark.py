#!/usr/bin/env python3

import os                       # for os.getenv() and os.path.abspath()
import subprocess               # for the subprocess.Popen() class with related constructor and .poll() methods
import argparse                 # for creating a parser object that can interpret command-line arguments
from datetime import datetime   # for creating timestamps
from shutil import copy2        # for copying files while preserving the original source metadata
import benchmark_helper         # custom helper functions
import atexit                   # to register an exit handler

#### Parsing/administrative tasks begin here ####

# Fetch environment variables previously sourced from setup.sh for convenient absolute path references.
repo_root = os.getenv("MARFS_BENCHMARK_REPO_REF")
repo_bin = os.getenv("MARFS_BENCHMARK_REPO_BIN")

# Instantiate an ArgumentParser object to interpret command-line arguments.
# See Python `argparse` documentation for full information.
#
parser = argparse.ArgumentParser(description='Process arguments to benchmark MarFS: which range for N, which range for E, which range for PSZ.')

# Add various command-line arguments for parser to recognize
parser.add_argument("-n", "--nrange", nargs=2, type=int, required=True, help="iterate from MIN to MAX (inclusive) number of storage/data blocks n", metavar=("MIN", "MAX"))

parser.add_argument("--n-increment", type=int, default=1, required=False, help="Specifies how much to increment N by for each trial")

parser.add_argument("-e", "--erange", nargs=2, type=int, required=True, help="iterate from MIN to MAX (inclusive) number of erasure/parity blocks e", metavar=("MIN", "MAX"))

parser.add_argument("--e-increment", type=int, default=1, required=False, help="Specifies how much to increment E by for each trial")

parser.add_argument("-p", "--prange", nargs=2, type=int, required=True, help="iterate from MIN to MAX (inclusive) part size range in bytes, doubling on each run or increasing by integer multiple otherwise supplied to -m", metavar=("MIN", "MAX"))

parser.add_argument("-m", "--multiple", type=int, default=2, required=False, help="integer multiple to scale part size per trial")

parser.add_argument("-s", "--filesize", type=int, required=True, help="unit size (1 to 1023) of file to create for each run of pfcp used to benchmark the MarFS instance")

parser.add_argument("-u", "--unit", type=str, choices=["K", "M", "G", "T", "P", "E"], required=True, help="byte size magnitude (\"K\"ilobyte, \"M\"egabyte, \"G\"igabyte, \"T\"erabyte, \"P\"etabyte, or \"E\"xabyte) to scale option \"-s\" by", metavar="K/M/G/T/P/E")

parser.add_argument("--mode", type=str, choices=["w", "rw"], required=False, default="rw", help="Select benchmarking mode, either write-only to MarFS instance (\"-w\") or write to MarFS followed by read from MarFS (\"-rw\").", metavar="MODE")

parser.add_argument("-g", "--group", type=str, required=False, default="all", help="group of nodes on a cluster across which clush will run for administrative tasks") 

parser.add_argument("-b", "--basepath", type=str, default=f"{repo_root}/data/", required=False, help="Absolute path within user's filesystem from which run data and archive directories will be created")

parser.add_argument("-t", "--testfile-root", type=str, default="/dev/shm", required=False, help="Absolute path within user's filesystem from which test file will be created across all nodes for the benchmark run")

# Write destination, i.e., destination for transfer *from* user filesystem *to* MarFS
parser.add_argument("-wd","--write-destination", type=str, default="/campaign/full-access-subspace", required=False, help="Absolute path which pfcp will write to within the MarFS instance")

# Read destination, i.e., destination for transfer *from* MarFS to *to* user filesystem
parser.add_argument("-rd", "--read-destination", type=str, default="/dev/null", required=False, help="Absolute path within user filesystem that pfcp will write to after reading from MarFS instance")

parser.add_argument("--nostats", action='store_true', required=False, help="Display summary statistics for the benchmark run once complete")

parser.add_argument("-f", "--nofill", action='store_true', required=False, help="Attempt to remediate any crashes during the benchmarking run by running csv_backfill.py after the sweep initially concludes.")

args = parser.parse_args()  # program will raise warnings/errors here if required args are missing


home_path = os.getenv("HOME")
syncfile = f"{home_path}/syncfile"  # $HOME **must** be shared over NFS; this program proceeds with that assumption. 

# Copy the old $MARFS_CONFIG_PATH to a file in the home directory. 
current_config_path = os.getenv("MARFS_CONFIG_PATH")
home_config_copy = f"{home_path}/current-marfs-config.xml"

# Preserves the original settings. $MARFS_CONFIG_PATH will be modified in place, then overwritten with this original configuration after testing.
copy2(current_config_path, home_config_copy)

atexit.register(benchmark_helper.cleanup, args.group, syncfile, f"{args.read_destination}/testfile", f"{args.write_destination}/testfile", home_config_copy, "")  

# For each argument -n, -e, -p, take the actual loop counter variables as the true min and max of the list.
# This occurs regardless of whether user listed arguments in appropriate actual MIN MAX order or the reverse.
nmin = min(args.nrange)
nmax = max(args.nrange)

emin = min(args.erange)
emax = max(args.erange)

pmin = min(args.prange)
pmax = max(args.prange)

# Concatenates directly, e.g., '1' + 'G' = '1G'. This argument format is convenient for `truncate`.
filesize_str = str(args.filesize) + (args.unit)

# datetime.now() retrieves current system time. Cast to string, split on whitespace, then rejoin with non-whitespace character.
timestamp_list = str(datetime.now()).split(" ")

# timestamp serves essentially as a unique "hash" to refer to benchmarking data.
timestamp = f"{timestamp_list[0]}_{timestamp_list[1]}"

data_root = f"{args.basepath}/data-{timestamp}"

# format, e.g., "./data-2023-07-03_13:19:46.377484/archive"
mkdir_proc = subprocess.Popen(f"mkdir -p {data_root}/archive", shell=True)

while (mkdir_proc.returncode == None):
    mkdir_proc.poll()

if (mkdir_proc.returncode != 0):
    print("ERROR: Failed to create one or more target directories for storing benchmark data")
    exit(1)  # parent needs archive/data directories to exist before writing to them, so exit on error as a best practice.

# Create the syncfile for later mtime reference and synchronization
sync_file_proc = subprocess.Popen(f"touch {syncfile}", shell=True)

while (sync_file_proc.returncode == None):
    sync_file_proc.poll()

testfile = f"{args.testfile_root}/testfile"  # format, e.g., "/dev/shm/testfile"

global_iterations = 0  # a counter variable unused in this script but maintained as a human-readable progress tracker at runtime and for the CSV.

stripewidth = 0  # unused specifically for this script; merely used as a data point for the CSV.


csv_path = f"{data_root}/data-{timestamp}.csv"

csv_out = open(csv_path, "a")

# Output format, in order:
# Stripewidth, parity blocks, data blocks, bandwidth, part size, filesize, global iterations
print("sw,pb,db,bw,PSZ,filesize,gl_iters,mode,secs,rank", file=csv_out)

n_increment = int(args.n_increment)
e_increment = int(args.e_increment)


#### Parsing/administrative tasks end here ####


#### Main loop begins here ####

this_run_ranks = benchmark_helper.get_ranks()

for nblocks in range(nmin, (nmax + 1), n_increment):

    for eblocks in range(emin, (emax + 1), e_increment):

        stripewidth = nblocks + eblocks  # recalculate N+E every time one or the other changes to update the CSV.

        psize = pmin
        while (psize <= pmax):

            # Forces pftool to do a data rewrite on every iteration.
            #
            # subprocess object instantiated with "shell=True" option here and in many instances in this code
            # to provide a format string as a single argument to execute. "shell=True" introduces a shell injection
            # vulnerability; for the original design, however, the shell is controlled sufficiently to prevent injection.
            # It is therefore the user's responsibility to likewise control the shell enough to prevent injection.
            #
            clear_subspace_proc = subprocess.Popen(f"rm -f {args.write_destination}/testfile-*", shell=True)
            
            # prevent race condition by polling for child process to exit before parent proceeds.
            # Popen.returncode field is initally None and set to 0 on success, so an explicit "None" check is done here
            # rather than (while not proc.returncode) and the like.
            #
            while clear_subspace_proc.returncode == None:
                clear_subspace_proc.poll()  

            write_bandwidth = 0
            run_data = ()

            # create new base file handle unique to the current iteration of the current run
            outfile_stem = f"{data_root}/archive/trial-{global_iterations}"

            # launch shell subprocess to ensure that no file at path {testfile} exists. Wait until subprocess exits to continue.
            rm_proc = subprocess.Popen(f"clush -g {args.group} rm -f {testfile}-*", shell=True) 

            while (rm_proc.returncode == None):
                rm_proc.poll()

            # launch shell subprocess to truncate file that will be copied. Wait until subprocess exits to continue parent.
            truncate_proc = subprocess.Popen(f"clush -g {args.group} truncate --size={filesize_str} {testfile}-{global_iterations}", shell=True)             

            while (truncate_proc.returncode == None):
                truncate_proc.poll()  

            # File presence on all nodes is a *requirement* for successful parent execution to continue.
            # If truncate failed in some way, parent cannot proceed and should exit now.
            #
            if (truncate_proc.returncode != 0):
                print("ERROR: clush and/or truncate returned nonzero exit code")
                exit(1)
   
            # spawn child process here to sync the modify times of all {testfile}s across relevant nodes to the modify time of a new file shared over NFS in a home directory.
            # syncing mtime allows all instances of {testfile} to coordinate during pftools checks and pass them smoothly.
            #
            sync_proc = subprocess.Popen(f"clush -g {args.group} touch -m -r {syncfile} {testfile}-{global_iterations}", shell=True)

            # poll child process for return status continuously while child has not returned
            while (sync_proc.returncode == None):
                sync_proc.poll()

            # without synchronization succeeding, the parent process is likely to encounter errors and crashes when it invokes pfcp.
            # at this point, the program would be better off exiting.
            #
            if (sync_proc.returncode != 0):
                print("ERROR: clush and/or touch returned nonzero exit code")
                exit(1)

            # spawn child process to configure MarFS based on the given trial nblocks, eblocks, and psize.
            # then, wait for child process to exit before continuing parent.
            #
            config_proc = subprocess.Popen(f"{repo_bin}/marfs_genconf.py -n {nblocks} -e {eblocks} -p {psize}", shell=True)

            while (config_proc.returncode == None):
                config_proc.poll()

            # parent process cannot proceed with a given trial without the configuration file being successfully modified.
            if (config_proc.returncode != 0):
                print("ERROR: marfs_genconf.py returned nonzero exit code")
                exit(1)
 
            # launch pfcp for the given trial, then wait for the pfcp run to exit before resuming parent execution
            pfcp_write_proc = subprocess.Popen(f"pfcp {testfile}-{global_iterations} {args.write_destination} > {outfile_stem}-w.txt 2>&1", shell=True)

            while (pfcp_write_proc.returncode == None):
                pfcp_write_proc.poll()

            # if pfcp fails in a way that returns a nonzero exit code, the parent process cannot write data for that run, and should print an error before skipping the writing steps for that trial.
            if (pfcp_write_proc.returncode != 0):
                print(f"ERROR: pfcp returned nonzero exit code on write for trial #{global_iterations} (Params N={nblocks}, E={eblocks}, PSZ={psize})")
                print(f"{stripewidth},{eblocks},{nblocks},-1.0,{psize},{filesize_str},{global_iterations},w,-1.0,{this_run_ranks}", file=csv_out)

            if (pfcp_write_proc.returncode == 0):
                # See function definition in `benchmark_helper.py` for documentation
                run_data = benchmark_helper.parse_bandwidth(f"{outfile_stem}-w.txt")
                write_bandwidth = run_data[0]
                elapsed_seconds = run_data[1]

                # print formatted data for write to CSV
                print(f"{stripewidth},{eblocks},{nblocks},{write_bandwidth},{psize},{filesize_str},{global_iterations},w,{elapsed_seconds},{this_run_ranks}", file=csv_out)

                # Create in-progress output visible to user as a check that the program is not hanging
                print(f"Write test #{global_iterations} (N = {nblocks}, E = {eblocks}, PSZ = {psize}) complete.")

            # If user has not opted to skip read benchmarking
            if (args.mode == "rw"):
                read_bandwidth = 0
                elapsed_seconds = 0

                # as with write, launch subprocess for read, then poll it to wait for it to complete.
                pfcp_read_proc = subprocess.Popen(f"pfcp {args.write_destination}/testfile-{global_iterations} {args.read_destination} > {outfile_stem}-r.txt 2>&1", shell=True)

                while (pfcp_read_proc.returncode == None):
                    pfcp_read_proc.poll()

                # as with write, if pfcp returns a nonzero exit code, data is not available to parent, so parent should print an error and skip writing steps.
                if (pfcp_read_proc.returncode != 0):
                    print(f"ERROR: pfcp returned nonzero exit code on read for trial #{global_iterations} (params N={nblocks}, E={eblocks}, PSZ={psize}).")
                    print(f"{stripewidth},{eblocks},{nblocks},-1.0,{psize},{filesize_str},{global_iterations},r,-1.0,{this_run_ranks}", file=csv_out)

                if (pfcp_read_proc.returncode == 0):
                    run_data = benchmark_helper.parse_bandwidth(f"{outfile_stem}-r.txt")
                    read_bandwidth = run_data[0]
                    elapsed_seconds = run_data[1]

                    # print formatted data for read to CSV
                    print(f"{stripewidth},{eblocks},{nblocks},{read_bandwidth},{psize},{filesize_str},{global_iterations},r,{elapsed_seconds},{this_run_ranks}", file=csv_out)

                    # As with write, output a check for user that program is not hanging
                    print(f"Read test #{global_iterations} (N = {nblocks}, E = {eblocks}, PSZ = {psize}) complete.")
            
            global_iterations += 1 
            psize *= (args.multiple)

            # If an improper argument was supplied for args.multiple, increment psize to prevent an infinite loop.
            if (args.multiple <= 1):
                psize += 1


#### Main loop ends here ####

csv_out.close() # close file to reset the file pointer.

# Copy the original MarFS configuration present before testing back to $MARFS_CONFIG_PATH
# Note: current_config_path was previously defined as $MARFS_CONFIG_PATH
#
copy2(home_config_copy, current_config_path) 

# Copy is no longer needed now that it has been restored to $MARFS_CONFIG_PATH, so clean up work.
os.remove(home_config_copy)

copied_csv_path = ""

if (not args.nofill):
    copied_csv_path = f"{data_root}/archive/unfilled-{timestamp}-data.csv"
    copy2(csv_path, copied_csv_path)

if (not (args.nostats and args.nofill)):

    benchmark_helper.gen_stats_stripe(copied_csv_path, timestamp, nmin, nmax, n_increment, emin, emax, e_increment, pmin, pmax, args.multiple, filesize_str, ('r' in args.mode), data_root)

if (not args.nofill):
    
    # Rerun the backfill script (`csv_backfill.py`) until data for all trials has successfully been "patched."
    # csv_backfill.py will run in a single invocation until all trials have been successfully patched with usable (non-sentinel) data.
    # Ranks are fixed for a single invocation of this script, so csv_backfill.py will be invoked with respect to stripes.
    #
    backfill_proc = subprocess.Popen(f"{repo_bin}/csv_backfill.py -f {csv_path} -g {args.group} -b {data_root}/archive -t {args.testfile_root} --write-destination {args.write_destination} --read-destination {args.read_destination} -s", shell=True)
    
    while (backfill_proc.returncode == None):
        backfill_proc.poll()
