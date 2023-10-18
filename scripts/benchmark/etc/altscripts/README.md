# script overviews

## marfs\_benchmark.py

marfs-benchmark.py is a script that tests bandwidth for writing to a MarFS mount configured per commandline user input.

### usage

marfs-benchmark.py requires the following arguments for each run, passed in through command line flags:
* `-d:` range of data blocks to iterate over (i.e. the "n" in an "n+e" stripe). Takes **3** integer arguments-- minimum and maximum data block values to iterate through and an increment value-- and the script will iterate in increments of the provided value from min to max, inclusive.
* `-p:` range of parity blocks to iterate over (i.e. the "e" in an "n+e" stripe). Takes **3** integer arguments-- minimum and maximum parity block values to iterate through and an increment value-- and the script will iterate in increments of the provided value from min to max, inclusive.
* `-s:` range of part sizes to iterate over (i.e. the height of a stripe, or the size of each block). Takes **3** integer arguments-- minimum part size value, maximum part size value, and a multiplier to control iteration (i.e. loop from min to max by multiplying by provided value). If multiplier is set to 1, the script will iterate in single increments from min to max, inclusive.  
* `-w` or `-rw`: flag to determine whether script benchmarks only write bandwidth or both read and write bandwidth. Only one of these flags is accepted. 
* `-t`: **optional** flag; when flag is set, 4 bytes are subtracted from partsize at each iteration to align partsize with marFS buffer given the 4-byte CRC tacked on to each block.

Run with `-h` or `--help` flag to see the full list on the command line (i.e. ` ./marfs-benchmark.py -h `  , `./marfs-benchmark.py --help`).

In addition, the following arguments must be provided in the script configuration file, `benchmark-config.yml`, as they are expected to stay relatively consistent between runs:
* `data_path:` path to directory to write benchmark output data. Output will be a csv file and created subdirectory of archive text files containing raw run data in the `data_path` directory.
* `source_path:` path to directory to create test file that will act as pfcp source. Should be a non-marFS location, and for optimal performance, a location that allows for efficient access such as the provided /dev/shm.
* `read_path:` path to where a file read from marFS path should be stored. For benchmarking purposes, /dev/null is a good option.
* `tmplt_path:` path to file to use as template config file that is edited per iteration.
* `dest_path:` path to directory to copy test file to. Should be within the marFS mount.
* `file_size:` size of test file to use in pfcp invocation. For pfcp reasons, this should be kept above 15GB.
* `clush_g:` name of clush group containing all nodes that will be used for pfcp operation.

A call to the script would look like:

``` 
./marfs_benchmark.py -d 2 10 2 -p 5 8 1 -s 1024 4096 4 
```

A sample config file is provided and should be updated based on user or cluster considerations.

### output

The script will produce the following output in a timestamped subdirectory created in the `data_path` directory:
* a CSV file containing one row per run with the following information for the iteration: # datablocks, # parity blocks, partsize, bandwidth, seconds, filesize, run #, and whether it was a read or write operation.
* an archive subdirectory containing raw output from each pfcp invocation in a separate textfile, including anything sent to stderr.
* a txt file containing summary statistics for the run: min, max, and average bandwidth for read and write separately, number of failed trials for read and write separately, and the run start timestamp and run configurations.

The following output is also sent to stdout:
* output from each pfcp invocation.
* summary statistics for the benchmark after all trials are concluded.

### dependencies

To run marfs-benchmark.py, the following supporting files are required (and are provided in the repo):
* `benchmark-config.yml:` script configuration file containing user/cluster information as detailed above.
* `benchmark.sh:` bash script called from marfs-benchmark.py. If desired benchmark.sh can be run independently; the top of the file contains the set of ~15 arguments that would need to be passed in manually. No validation is performed on these arguments.
* `marfs_genconf.py:` script to generate marFS config file, called from `benchmark.sh`.
* `statgen.py:` contains utility functions for printing summary statistics and performing csv cleanup, imported by `marfs-benchmark.py`.

The following should be set up on the cluster:
* A marFS mount and separate FUSE instances across all the nodes.
* All marFS dependencies.
* Clustershell (a.k.a. clush).
* python3.
* Shared home directory across nodes.
