/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

use clap::Parser;
use std::collections::BTreeMap;
use std::fs;
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime};

// path components to leaves of marfs data tree
const PATH_SEGS: &[&str] = &[
    "pod",
    "block",
    "cap",
    "scat"
];

// use BTreeMap to remove duplicates and sort on keys
// do not manually allocate - use parse_user_thresholds
type Thresholds = BTreeMap<u8, u64>;

/**
 * Select how old files are allowed to be given system
 * utilization and thresholds
 *
 * Example:
 *     thresholds:
 *         10 -> 60
 *         20 -> 1
 *
 * If the utilization is less than or equal to 10%, files older than
 * 60 seconds should be flushed. If the utilization is greater than
 * 10% and less than or equal to 20%, files older than 1 second should
 * be flushed.
 *
 * TODO: Change to BTreeMap::upper_bound once btree_cursors is merged.
 *
 * @param thresholds   mapping of thresholds to file age limits
 * @param utilization  system utilization
 * @return file age limit in seconds
 */
fn util2age(thresholds: &Thresholds, utilization: u8) -> u64 {
    // return the age associated with the first threshold
    // that is greater than or equal to the utilization
    for (threshold, age) in thresholds.iter() {
        if *threshold >= utilization {
            return *age;
        }
    }

    // this line does double duty as a compiler silencer
    // and as an invalid utilization value check
    panic!("Error: Utilization percentage not found");
}

/**
 * Process files under <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+/
 * if (reftime - file.atime) > age, print the file's path
 *
 * @param path        <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+/
 * @param reftime     a timestamp to compare atimes with
 * @param thresholds  mapping of thresholds to file age limits
 * @return number of leaf files processed
 */
fn process_leaf(path: PathBuf, reftime: Arc<SystemTime>, thresholds: Arc<Thresholds>) -> u64 {
    let entries = match fs::read_dir(&path) {
        Ok(list)   => list,
        Err(error) => {
            eprintln!("Warning: Could not read_dir {}: {}", path.display(), error);
            return 0;
        },
    };

    // get the leaf's utilization
    let util = unsafe {
        use errno::errno;
        use libc;
        use std::ffi::CString;
        use std::mem;

        let path_cstr = CString::new(path.display().to_string()).unwrap();
        let mut vfs_st: libc::statvfs = mem::zeroed();

        if libc::statvfs(path_cstr.as_ptr(), &mut vfs_st as *mut libc::statvfs) < 0 {
            println!("Warning: Getting utilization for {} failed: {}", path.display(), errno());
            return 0;
        }

        (100 - vfs_st.f_bfree * 100 / vfs_st.f_blocks) as u8
    };

    // figure out the file age limit
    let age = util2age(&thresholds, util);

    let mut count = 0;

    // loop through leaf directory and find files older than the limit
    for entry_res in entries {
        let entry = match entry_res {
            Ok(entry) => entry,
            Err(error) => {
                eprintln!("Warning: Could not get entry: {}", error);
                continue;
            },
        };

        if let Ok(entry_type) = entry.file_type() {
            let child = entry.path();
            if entry_type.is_file() {
                if let Ok(st) = child.metadata() {
                    if let Ok(atime) = st.accessed() {
                        if let Ok(dur) = reftime.duration_since(atime) {
                            // older than allowed file age - print path for flushing
                            if dur.as_secs() > age {
                                println!("{}", child.display());
                                count += 1;
                            }
                        }
                    }
                }
            } else {
                eprintln!("Warning: {} is not a file", child.display());
            }
        }
    }

    return count;
}

/**
 * Find directories matching <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+
 *
 * @param path        <DAL root> and lower
 * @param level       which path segment is currently being processed
 * @param reftime     a timestamp to compare atimes with
 * @param thresholds  mapping of thresholds to file age limits
 * @param tx          push thread handles here
 * @return number of paths that had the expected path segment pattern
 */
fn process_non_leaf(path: PathBuf, level: usize,
                    reftime: Arc<SystemTime>,
                    thresholds: Arc<Thresholds>,
                    tx: mpsc::Sender<thread::JoinHandle<u64>>) -> u64 {
    if level == 4 {
        // panic on > 4?
        let _ = tx.send(thread::spawn(move || {
            process_leaf(path, reftime, thresholds)
        }));
        return 0;
    }

    let entries = match fs::read_dir(&path) {
        Ok(list)   => list,
        Err(error) => {
            eprintln!("Warning: Could not read_dir {}: {}", path.display(), error);
            return 0;
        },
    };

    let expected = PATH_SEGS[level];
    let len = expected.len();

    let mut count = 0;

    // find paths that match current marfs path segment
    for entry_res in entries {
        let entry = match entry_res {
            Ok(entry)  => entry,
            Err(error) => {
                eprintln!("Warning: Could not get entry: {}", error);
                continue;
            },
        };

        let child = entry.path();

        if child.is_dir() == false {
            continue;
        }

        // make sure current basename has expected path segment
        if let Some(basename) = child.file_name() {
            if len < basename.len() {
                if let Some(basename_str) = basename.to_str() {
                    if &basename_str[0..len] != expected {
                        continue;
                    }

                    if let Err(_) = &basename_str[len..].parse::<u32>() {
                        continue;
                    }

                    let tx_clone = tx.clone();
                    let reftime_arc = reftime.clone();
                    let thresholds_arc = thresholds.clone();

                    let _ = tx.send(thread::spawn(move || {
                        process_non_leaf(child, level + 1, reftime_arc, thresholds_arc, tx_clone)
                    }));

                    count += 1;
                }
            }
        }
    }

    count
}

/**
 * Recurse down to <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+
 * and find files that are older than the provided age
 *
 * @param dal_root    <DAL root>
 * @param reftime     a timestamp to compare atimes with
 * @param thresholds  mapping of thresholds to file age limits
 */
fn print_flushable_in_dal(dal_root: &PathBuf, reftime: &SystemTime, thresholds: &Thresholds) {
    let (tx, rx) = mpsc::channel();

    let tx_clone = tx.clone();
    let path = dal_root.clone();
    let reftime_arc = Arc::new(*reftime);
    let thresholds_arc = Arc::new(thresholds.clone());

    let _ = tx.send(std::thread::spawn(move || {
        process_non_leaf(path, 0, reftime_arc, thresholds_arc, tx_clone)
    }));

    mem::drop(tx);

    while let Ok(thread) = rx.recv() {
        let _ = thread.join();
    }
}

/**
 * Convert <utilization>,<age> strings from the commandline
 * to integers and insert them into a map.
 *
 * Utilization is an integer representing utilization percentage.
 * An integer is required because rust does not have an Ord trait
 * defined for f32 and f64. https://stackoverflow.com/a/69117941/341683
 *
 * Age is integer number of seconds since Jan 1, 1970 00:00:00 UTC.
 *
 * Example:
 *
 *     <this program> ... <other args> ... 10,60 20,1
 *
 * @param args   a vector of strings parsed by clap
 * @param delim  separator between utilization and age
 * @return a mapping of utilizations to file age limits
 */
fn parse_user_thresholds(args: &Vec<String>, delim: char) -> Thresholds {
    let mut thresholds = Thresholds::from([
        (0, u64::MAX), // if utilization is at 0%, don't flush anything
        (100, 0),      // if utilization is at 100%, flush everything
    ]);

    for arg in args {
        match arg.split_once(delim) {
            Some((util_str, age_str)) => {
                let util = match util_str.parse::<u8>() {
                    Ok(val)    => val,
                    Err(error) => panic!("Error: Bad utilization string: '{}': {}", util_str, error),
                };

                if util > 100 {
                    panic!("Error: Utilization can be between 0% and 100%. Got '{}'", util);
                }

                let age = match age_str.parse::<u64>() {
                    Ok(val)    => val,
                    Err(error) => panic!("Error: Bad age string: '{}': {}", age_str, error),
                };

                thresholds.insert(util, age);
            },
            None => panic!("Error: Bad <utilization>,<age> string: '{}'", arg),
        }
    }

    // check for monotonically decreasing file ages
    let mut prev = thresholds.first_key_value();
    for (utilization, age) in thresholds.iter().skip(1) {
        if age >= prev.unwrap().1 {
            panic!("Error: File age must be strictly monotonically decreasing. Found {},{} -> {},{}",
                   prev.unwrap().0, prev.unwrap().1, utilization, age);
        }

        prev = Some((&utilization, &age))
    }

    thresholds
}

#[derive(Parser, Debug)]
#[command()]
struct Args {
    #[arg(help="DAL root path")]
    root: PathBuf,

    #[arg(help="Reference Timestamp (Seconds Since Epoch)")]
    reftime: u64,

    #[arg(help="Comma separated utilization percentage (integer) and age (integer seconds) thresholds")]
    thresholds: Vec<String>,
}

fn main() {
    let args = Args::parse();

    // get reference timestamp
    let reftime = SystemTime::UNIX_EPOCH + Duration::from_secs(args.reftime);

    // convert user input to a map
    let thresholds = parse_user_thresholds(&args.thresholds, ',');

    // find files older than age
    print_flushable_in_dal(&args.root, &reftime, &thresholds);
}

#[cfg(test)]
mod tests;
