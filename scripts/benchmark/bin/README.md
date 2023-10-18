# Script Usage Guide

[[_TOC_]]

## Dependencies

The scripts have the following dependencies:

* Clustershell (`clush`)
* An example MarFS config (`marfs-config-ex.xml`) for `marfs_genconf.py` to parse and write out to the real config file at `$MARFS_CONFIG_PATH`.
* An example pftool config (`pftool-ex.cfg`) for `benchmark_helper.pftool_modify()` to parse and write out to the real pftool config at `$PFTOOL_CONFIG_PATH`.
* existing MarFS dependencies (`OpenMPI`; `ISA-L`, `erasureUtils`, `marfs`, and `pftools` builds; `$MARFS_CONFIG_PATH` and `$PFTOOL_CONFIG_PATH` environment variables set for running user)
* All nodes in the cluster which will run the `pftools` operations must have MPI; separately, all nodes must have a functional MarFS FUSE mount which can be written to. 

DAL and MDAL roots (i.e., data and metadata directories) should be shared spaces through **NFS Version 4.2**. All nodes, whether head/management nodes or compute/worker nodes, should also share over NFS (minimum version 3) a home directory for the user running the script in which the application will create `syncfile`. 

## Mandatory Setup

After cloning this repository, run `source ./setup.sh /path/to/MarFS/config /path/to/pftool/config`. This adds the repo to `$PATH` and defines absolute-path based environment variables for `stripe_benchmark.py`, the pftool config path, and the MarFS config path to reference at runtime. These environment variables are used throughout the script suite.

## `stripe_benchmark.py`

### Description
At a high level, this script aims to repeatedly modify a MarFS configuration file, then copy a file to and from an associated MarFS instance and save bandwidth data for that copying. Separately, that bandwidth data can be read from a .csv file to generate a human-readable heatmap graph identifying bandwidth z-axis values from x-axis erasure stripe width ("N" storage block + "E" erasure/parity block count) and y-axis part size (number of file bytes encoded per erasure stripe, e.g., 1024) values.

This program receives and parses command-line arguments (see `stripe_benchmark.py -h` and § Usage Notes) for relevant parameters like storage block ("N") range, erasure/parity block ("E") range, and part size ("PSZ") range. After parsing arguments, the program performs setup tasks like creating a unique timestamp to identify data, making room for this data at `{args.basepath}/data-{timestamp}/archive`, and creating a shared "syncfile" in the user's home directory to reference mtimes in each run before proceeding to the main loop over the ranges for N, E, and PSZ.  

For each run, writes from the user filesystem to a MarFS instance will always occur first, optionally followed by a read from MarFS to the user filesystem. The program first removes the testfile that will be written to if the file exists within the MarFS instance. This is done to force `pftools` to copy the `testfile` on every iteration; without doing so, once the mtimes of the `testfile` instances are synced with `syncfile`'s mtime, `pftools` is likely to skip the write stage. Next, the program removes the local copies of `testfile` on all nodes that `pfcp` will run on; similarly to clearing the destination, this is done to force a full rewrite. Then, `testfile` is recreated according to the user-supplied filesize arguments; the size of `testfile` is held constant for the lifetime of a single program run. The mtimes of all `testfile` instances across the nodes are synced with the mtime of `~/syncfile` to satisfy `pftools` checks at write time. Otherwise, if mtimes differ between `testfile` instances on the nodes, `pftools` is likely to raise errors and crash individual trials. 

Once synchronization completes, the program calls `marfs_genconf.py` to dynamically build the MarFS configuration XML according to the given run's N, E, and PSZ values. Lastly, the program invokes `pfcp` and reads out average bandwidth data for the trial. The full output is copied to a text file identified with the iteration number, and the bandwidth specifically is written along with other trial parameters to a CSV with all trial data.

This program relies extensively on subprocesses and inter-process communication through the Python `subprocess` module. Apart from `pfcp`, however, most operations like the shell utility and `marfs_genconf.py` invocations happen serially since the parent process depends on all subprocesses to complete and return results in order _before_ the parent resumes executing.

### Usage Notes

In addition to the information provided by running `stripe_benchmark.py -h`, there are specific considerations for some arguments:

`-e MIN MAX`: choose which range to iterate over for erasure blocks E. _Unlike for N_, the range for E is **zero-inclusive**, in which case MarFS will skip erasure coding for `E = 0` trials during testing. This is likely to result in higher bandwidth holding N, PSZ, and filesize constant; however, running with `E = 0` has more _testing_ value than _production_ value since `E = 0` means there is no effort to back up data with erasure coding within the MarFS instance.

`--n-increment=INCREMENT`, `--e-increment=INCREMENT`: choose which values to (separately) increment N and E by for each trial. The user should coordinate their ranges for `-n` and `-e` with respective increments to ensure that they get the data they want, since the script does no checking to coordinate these arguments. For example, running with arguments `... -n 1 10 --n-increment=2 -e 1 4 --e-increment=2 ...` will test over ranges `n = [1, 3, 5, 7, 9]` and `e = [1, 3]`, neither of which have 2-aligned values. If the user wants to test over 2-aligned values, they must set the bounds for N and E acordingly, like running with `-n 2 10` and either `-e 0 4` or `-e 2 4`.

`--mode=MODE`: choose which mode to benchmark in (default="rw"). If the "w" argument is supplied, the script will only invoke `pfcp` between the user filesystem and MarFS once (i.e., **only** to **write**). If "rw" is supplied (or `--mode` is not supplied, reverting to the default), the script will invoke `pfcp` to **write** from the user filesystem to MarFS, **then** invoke `pfcp` to **read** out from MarFS into the user filesystem. The "w" option effectively skips the read stage for all trials in the benchmarking run. 

`-g GROUP`, `--group GROUP`: the group of nodes that will be provided as an argument to the `clush` commands within the script. This script has no visibility into the cluster layout, meaning the script _cannot_ check the argument for validity and **it is therefore the user's responsibility to provide a valid argument** here. By default, `GROUP = all`, meaning that `clush` will run across the `all` group if defined in the user's `clush` configuration. The user must separately make their nodes discoverable to `pftools` in their `pftool.cfg` configuration.

`-t TESTFILE_ROOT`, `--testfile-root TESTFILE_ROOT`: the absolute path within the user's filesystem where a file will be created before being copied through `pfcp`. For performance reasons, it is **highly recommended** that the user either keep the default `/dev/shm/` or supply an argument to another _quickly accessed_ space within the filesystem. Supplying an argument which points to a space like a user home directory can result in **drastic decreases** in bandwidth. 

`-rd READ_DESTINATION`, `--read-destination READ_DESTINATION`: the absolute path within the user filesystem that `pfcp` will be directed to write to after reading from the MarFS instance. The destination _should not_ lie within a MarFS filesystem (i.e., `-rd` and `-wd` **cannot both** be paths within a MarFS instance), though this script does no checking to enforce this.

`-wd WRITE_DESTINATION`, `--write-destination WRITE_DESTINATION`: the absolute path within a MarFS instance that `pfcp` will be directed to write to. The destination _should_ lie within a MarFS instance, though this script does no checking to enforce this. 

### Example Invocations

`stripe_benchmark.py -h`: Print usage information and exit.

`stripe_benchmark.py -n 1 10 -e 1 5 -p 1024 1048576 -s 256 -u G`: benchmarks by iterating from 1 to 10 storage blocks, 1 to 5 erasure/parity blocks, and part sizes from 1024 bytes (1 KB) to 1048576 bytes (1 MB). No `-m` argument was specified, so part size will increase by the default multiple 2 each time. The copied file will be _256_ (-s) _Gigabytes_ (-u). Since no mode was specified, the benchmarking run will perform both reads and writes as is the default.

`stripe_benchmark.py -n 1 10 -e 1 5 -p 1024 524288 -m 4 -s 256 -u G --mode w`: same usage as before, with the exception of part size and mode. The program will begin with a part size of 1024 bytes (1 KB) and multiply by 4 (supplied as `-m`) on each iteration. The maximum part size will be 262,144 bytes (256 KB), however, since the program will reset the part size once it is greater than the provided maximum argument and multiplying 1024 by 4 yields the range `[4096, 16384, 65536, 262144, 1048576]` with 262144 being the greatest value less than or equal to the maximum. Additionally, since "w" was supplied for `--mode`, the benchmarking run will only perform writes.

`stripe_benchmark.py -n 2 10 --n-increment=2 -e 0 4 --e-increment=2 -p 1024 1048576 -s 1 -u T`: benchmarks by iterating from 2 to 10 storage blocks, increasing by 2 each time to produce the range [2, 4, 6, 8, 10]; iterating from 0 to 4 erasure/parity blocks, increasing by 2 each time to produce the range [0, 2, 4]; iterating from a part size of 1024 bytes (1 KB) to 1048576 bytes (1 MB), increasing by integer multiple 2 since no `-m` argument was otherwise specified; and copying a 1T (1 TB) file for each read and write.

## `rank_benchmark.py`

### Description
Script to benchmark MarFS throughput across different `minimum rank per node` values, at a fixed and user-specified erasure scheme. Modifies the MarFS config a single time to fix the values as user-specified, then repeatedly modifies `pftool` config file to update the ranks per node value, iterating through user-specified ranges. Produces a CSV file of data, a text file with summary statistics, and an archive subdirectory with all raw run output. The script will automatically backfill failed trials after generating summary statistics. Restores original config files and removes testing-related files on exit.

The script performs the following operations while iterating through the user-provided rank range:

    1. Updates `pftool` config.
    2. Creates a truncated file in user-provided path in preparation for write to MarFS mount.
    3. Syncs the mtimes for the truncated file to a common sync file in the home directory across all nodes, which allows the upcoming `pfcp` operation to pass internal `pftool` checks.
    4. Perform pfcp operation writing to user-provided MarFS mount and direct output to archive file.
    5. Parse output for bandwidth and seconds, writing all trial info to a row in the output CSV.
    6. If user wants to benchmark read (specified in cmdline flags), perform `pfcp` operation from MarFS mount to user-provided read location and perform same redirect to archive file and parsing for CSV.
    7. Remove the truncated file from the source path and the MarFS path to pass internal `pftool` checks for future trials and avoid directory clutter.

Argument parsing & validation, summary statistic generation, and backfilling are done with the `rank_benchmark.py` script; the script then calls a bash helper script, `rank_benchmark.sh`, to perform the steps detailed above. This is done in order to streamline the program and reduce subprocess overhead. 
 
### Usage

Run with the `-h` flag (i.e. `./rank_benchmark.py -h`) to get a list of possible flags and descriptions.

A list of mandatory arguments:

`-d DBLOCKS, --dblocks DBLOCKS`: Fixed integer value of `N`, or fixed datablock count for erasure scheme.

`-p PBLOCKS, --pblocks PBLOCKS`: Fixed integer value of `E`, or fixed parity block count for erasure scheme.

`-s PARTSIZE, --partsize PARTSIZE`: Fixed integer value of `PSZ`, or fixed partsize for erasure scheme.

`-r RANK RANK, --rank RANK RANK`: Integer values for starting and ending points of rank iteration, as in range of ranks to test during benchmark run.

`-w` or `-rw`: Pass in one of the two flags to determine whether to just benchmark writes (`-w`) or reads and writes (`-rw`).

A list of optional arguments:

`-g GROUP`, `--group GROUP`: Default `all`. The group of nodes that will be provided as an argument to the `clush` commands within the script. The user must separately make their nodes discoverable to `pftools` in their `pftool.cfg` configuration.

`-t TESTFILE_ROOT`, `--testfile-root TESTFILE_ROOT`: Default `/dev/shm`. The absolute path within the user's filesystem where a file will be created before being copied through `pfcp`. For performance reasons, this should be a very rapidly accessible space within the filesystem. 

`-rd READ_DESTINATION`, `--read-destination READ_DESTINATION`: Default `/dev/null`. The absolute path within the user filesystem that `pfcp` will be directed to read to from the MarFS instance. The destination _should not_ lie within a MarFS instance, though this script does no checking to enforce this.

`-wd WRITE_DESTINATION`, `--write-destination WRITE_DESTINATION`: Default `/campaign/full-access-subspace`. The absolute path within a MarFS instance that `pfcp` will be directed to write to. The destination _should_ lie within a MarFS instance, though this script does no checking to enforce this.

`-i RANKITER`, `--rankiter RANKITER`: Default `1`. Value to increment rank values by during iteration from min provided rank to max provided rank.

Finally, note that `rank_benchmark.sh` can be a standalone script, and can be run separately by looking through the `optarg` flags and passing them directly to the program accordingly, although no arg validation is performed. (However, bandwidth will not be converted to GB floats; to account for this, the appropriate helper function in `benchmark_helper.py`, `translate_fs`, should be applied separately to the CSV before graphing.)

## Other Utilities

### `marfs_genconf.py`

This script takes 3 mandatory arguments (N, E, PSZ) and modifies a template file, in `etc/marfs-config-ex.xml`, then writing that modified template directly to the MarFS config file path, specified in the environment variable `$MARFS_CONFIG_PATH`.

### `csv_backfill.py`

This script takes command line arguments of a benchmark CSV in addition to suite-typical arguments like read and write paths (full list can be accessed with the `-h` flag), and overwrites failed trials in the CSV by rerunning them. An intermediate CSV with just the rerun data is also produced, as well as an archival folder of raw run output.

This script is called directly from both `rank_benchmark.py` and `stripe_benchmark.py`, but can be called independently on any CSV benchmark output.

### `benchmark_helper.py`

This is a set of utility functions called from various benchmarking scripts. Documentation is provided in-file, but below is a brief overview of the functions:

* `parse_bandwidth` and `translate_fs`: helper parsing functions to get bandwidth in GB.
* `gen_stats_stripe` and `gen_stats_rank`: function to generate summary stats for a stripe benchmarking run or rank benchmarking run, respectively.
* `printStars` and `printLog`: helper functions for the statistic generation functions.
* `get_ranks` and `set_ranks`: functions to get rank configuration from pftool config or modify the pftool config per user specs, respectively.
* `cleanup`: function to put back configs and remove test-related files, registered as an exit handler with the `atexit` module in any benchmarking script.
