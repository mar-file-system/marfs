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

use clap::{Args, Parser};
use errno;
use regex::Regex;
use rusqlite::{Connection, OpenFlags};
use std::cmp::min;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::time::{Duration, SystemTime};
use threadpool::ThreadPool;

mod config;

#[derive(Debug, Args, Clone)]
#[group(required=true, multiple=true)]
struct Ops {
    #[arg(long, help="Print leaf files to flush")]
    flush: bool,

    #[arg(long, help="Print leaf files to push")]
    push: bool,
}

#[derive(Clone)]
struct FlushPush {
    config: config::Config,
    ops: Ops,
    must_match: Option<Regex>,
    force: bool,
}

// list of paths to write to output files
#[derive(Clone)]
struct Output {
    push:  mpsc::Sender<PathBuf>,
    flush: mpsc::Sender<PathBuf>,
}

// path components to leaves of marfs data tree
static PATH_SEGS: &[&str] = &[
    "pod",
    "block",
    "cap",
    "scat"
];

// //////////////////////////////////////
// Constants for database containing files that have been pushed
//
// If a file is selected for pushing, log it here as though it has
// been pushed so that it will not show up again next time this is run
//
// If a file is selected for flushing, it will be removed from the
// database
static PUSHDB_NAME:  &str = "push.db";

// pattern for blacklisting this file
static PUSHDB_REGEX: &str = "^push\\.db$";

/**
 * Open and/or create the PUSH database
 * Tables are set up if they don't exist
 *
 * @param path    <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+/
 * @return a handle to the PUSH database
 */
fn open_pushdb(path: &PathBuf) -> Result<Connection, String> {
    let mut dbname = path.clone();
    dbname.push(PUSHDB_NAME);

    match Connection::open_with_flags(&dbname,
                                      OpenFlags::SQLITE_OPEN_CREATE |
                                      OpenFlags::SQLITE_OPEN_READ_WRITE) {
        Ok(conn) => {
            // these can't really fail
            let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS push (name TEXT PRIMARY KEY, mtime int64);");
            let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS mtime (oldest int64);");
            Ok(conn)
        },
        Err(msg) => Err(format!("Could not open PUSH database {}: {}",
                                dbname.display(), msg)),
    }
}
// //////////////////////////////////////

/**
 * Get the filesystem utilization of the provided path
 * using statvfs, rounded down to the nearest integer.
 *
 * @param path    <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+/
 * @return utilization
 */
fn get_utilization(path: &PathBuf) -> Result<u8, errno::Errno> {
    // get the leaf's utilization
    unsafe {
        use libc;
        use std::ffi::CString;
        use std::mem;

        let path_cstr = CString::new(path.display().to_string()).unwrap();
        let mut vfs_st: libc::statvfs = mem::zeroed();

        if libc::statvfs(path_cstr.as_ptr(), &mut vfs_st as *mut libc::statvfs) < 0 {
            return Err(errno::errno());
        }

        Ok((100 - vfs_st.f_bfree * 100 / vfs_st.f_blocks) as u8)
    }
}

/**
 * Process files under <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+/
 *
 * if this file's basename is allowed
 *     if FLUSH
 *         if (reftime - file.mtime) > age
 *             print the file's path and delete it from the PUSH db
 *
 *     if file was not flushed, record min(mtime)
 *
 *     if PUSH
 *         insert the path into the PUSH db and print the file's path
 *
 * @param path    <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+/
 * @param fp      flush/push config
 * @param output  send flush and push tx channel endpoints
 */
fn process_leaf(path: PathBuf, fp: Arc<FlushPush>, output: Output) {
    // open/create the PUSH database
    let pushdb = match open_pushdb(&path) {
        Ok(conn) => conn,
        Err(msg) => { eprintln!("Error: {}", msg); return; },
    };

    // get previously existing oldest mtime, if one exists
    //
    // if a previous mtime was not found, default to UNIX_EPOCH: new
    // mtimes must come after UNIX_EPOCH, so every file will pass the
    // mtime check
    let prev_oldest_mtime = SystemTime::UNIX_EPOCH +
        match pushdb.query_row::<u64, _, _>("SELECT oldest FROM mtime;", [], |row| row.get(0)) {
            Ok(val) => Duration::from_secs(val),
            Err(_)  => Duration::from_secs(0),
        };

    // get the leaf's utilization
    let util = match get_utilization(&path) {
        Ok(val)  => val,
        Err(msg) => { eprintln!("Warning: Getting utilization for {} failed: {}", path.display(), msg); return; },
    };

    // figure out the file age limit
    // get here to not do repeated searches
    let max_age = fp.config.util2age(util);

    // use a time that all files should have been created before
    // assumes all system clocks are not off by more than a year
    let future_mtime = SystemTime::now() + Duration::from_secs(60 * 60 * 24 * 365);
    let mut curr_oldest_mtime = future_mtime.clone();

    let entries = match fs::read_dir(&path) {
        Ok(list) => list,
        Err(msg) => { eprintln!("Error: Could not read_dir {}: {}", path.display(), msg); return; },
    };

    // loop through leaf directory and find files older than the limit
    for entry_res in entries {
        let entry = match entry_res {
            Ok(entry) => entry,
            Err(msg)  => { eprintln!("Warning: Could not get entry: {}", msg); continue; },
        };

        let child = entry.path();

        if let Ok(entry_type) = entry.file_type() {
            // only expecting files under leaf directories
            if !entry_type.is_file() {
                eprintln!("Warning: {} is not a file. Skipping.", child.display());
                continue;
            }
        } else {
            eprintln!("Warning: Could not get type of {}. Skipping.", child.display());
            continue;
        }

        if let Ok(st) = child.metadata() {
            if let Ok(mtime) = st.modified() {
                if let Ok(file_age) = fp.config.file_age(mtime) {
                    let basename = child.file_name().unwrap().to_str().unwrap();

                    // basename must match in order to be processed
                    if let Some(whitelist) = &fp.must_match {
                        if !whitelist.is_match(&basename) {
                            continue;
                        }
                    }

                    // do not process blacklisted files
                    if fp.config.is_blacklisted(&basename) {
                        continue;
                    }

                    // If the FLUSH flag was specified, target object
                    // components will be selected if their mtime
                    // value is older than the value corresponding to
                    // the current FS fullness percentage, as
                    // specified in the config file.
                    //
                    // Do actual FLUSH later in order to track mtime
                    //
                    // Can mtime < prev_oldest_mtime happen?
                    let flush = fp.ops.flush &&
                        (fp.force || ((mtime >= prev_oldest_mtime) && (file_age.as_secs() > max_age)));

                    // keep track of the oldest mtime value amongst
                    // all encountered files, excluding those for
                    // which a FLUSH op is targeted, regardless of
                    // whether they match any white/blacklist object
                    // regex.
                    if !flush && (mtime < curr_oldest_mtime) {
                        curr_oldest_mtime = mtime;
                    }

                    // process FLUSH here
                    if flush {
                        let _ = output.flush.send(child.clone());
                        let _ = pushdb.execute(&format!("DELETE FROM push WHERE name == '{}';", basename), []);
                        continue; // flushed files are done being processed
                    }

                    if !fp.ops.push {
                        continue;
                    }

                    let msecs = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();

                    // if the path is not already in the PUSH db, queue it for printing
                    if let Ok(_) = pushdb.execute(&format!("INSERT INTO push (name, mtime) VALUES ('{}', {});",
                                                           basename, msecs), []) {
                        let _ = output.push.send(child.clone());
                        continue; // done
                    }

                    // file is already listed - check the timestamp
                    match pushdb.query_row::<u64, _, _>(&format!("SELECT mtime FROM push WHERE name == '{}';",
                                                                 basename), [], |row| row.get(0)) {
                        Ok(old_mtime) => {
                            let old_mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(old_mtime);
                            if old_mtime < mtime {
                                // attempt to update PUSH db
                                if let Err(msg) = pushdb.execute(&format!("UPDATE push SET mtime = {} WHERE name == '{}';",
                                                                          msecs, basename), []) {
                                    eprintln!("Error: Could not update mtime of previously pushed file {}: {}",
                                              child.display(), msg);
                                }

                                // add to PUSH file no matter what
                                let _ = output.push.send(child.clone());
                            } else {
                                // add to PUSH file anyways
                                if fp.force {
                                    let _ = output.push.send(child.clone());
                                }
                            }
                        },
                        Err(msg) => {
                            eprintln!("Warning: Could not get existing mtime for {}, so pushing again: {}",
                                      child.display(), msg);
                            let _ = output.push.send(child.clone());
                        },
                    };
                }
            }
        }
    }

    // if the oldest mtime was updated (this directory has at least 1 file), write it to PUSH db
    if (curr_oldest_mtime > prev_oldest_mtime) &&
        (curr_oldest_mtime != future_mtime) {
        let msecs = curr_oldest_mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();

        // remove all rows just in case there's more than 1 row
        let _ = pushdb.execute_batch("DELETE FROM mtime;");

        // insert the new oldest mtime
        let _ = pushdb.execute(&format!("INSERT INTO mtime (oldest) VALUES ({});", msecs), []);
    }
}

/**
 * Find directories matching <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+
 *
 * @param path        <DAL root> and lower
 * @param level       which path segment is currently being processed
 * @param fp          flush/push config
 * @param output      send flush and push tx channel endpoints
 * @param threadpool  the threadpool that is processing child work
 */
fn process_non_leaf(path: PathBuf, level: usize,
                    fp: Arc<FlushPush>,
                    output: Output,
                    threadpool: ThreadPool) {
    if level == 4 {
        // panic on > 4?
        threadpool.execute(move || {
            process_leaf(path, fp, output);
        });
        return;
    }

    let entries = match fs::read_dir(&path) {
        Ok(list) => list,
        Err(msg) => {
            eprintln!("Warning: Could not read_dir {}: {}", path.display(), msg);
            return;
        },
    };

    let expected = PATH_SEGS[level];
    let len = expected.len();

    // find paths that match current marfs path segment
    for entry_res in entries {
        let entry = match entry_res {
            Ok(entry) => entry,
            Err(msg)  => {
                eprintln!("Warning: Could not get entry: {}", msg);
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

                    let fp_clone = fp.clone();
                    let threadpool_clone = threadpool.clone();
                    let output_clone = output.clone();

                    threadpool.execute(move ||{
                        process_non_leaf(child, level + 1,
                                         fp_clone,
                                         output_clone,
                                         threadpool_clone);
                    });
                }
            }
        }
    }
}

/**
 * Recurse down to <DAL root>/pod[0-9]+/block[0-9]+/cap[0-9]+/scat[0-9]+
 * and find files that are older than the provided age
 *
 * @param dal_root  <DAL root>
 * @param fp        flush/push config
 * @param output    send flush and push tx channel endpoints
 * @param nthreads  the number of the threads that should be used to process this DAL root
 */
fn print_flushable_in_dal(dal_root: &PathBuf,
                          fp: FlushPush,
                          output: Output,
                          nthreads: usize) {
    let path = dal_root.clone();
    let fp_arc = Arc::new(fp);

    // no need to clone output here

    let pool = ThreadPool::new(nthreads);
    let pool_clone = pool.clone();

    pool.execute(move || {
        process_non_leaf(path, 0, fp_arc, output, pool_clone);
    });

    // Can't parallelize writes during processing:
    //   - need to drop original tx, but if write threads interrupt
    //     walk threads, they might complete before walk threads
    //
    //   - write threads would sit on threads for the entire lifetime
    //     of the threadpool, blocking any work queued behind them,
    //     preventing completion
    //     - this threadpool doesn't steal work
    //
    //   - spawning (nthreads - 2) walk threads and then spawning 2
    //     more threads for writing does not seem correct
    //
    //   - spawning nthreads walk threads and then spawning 2 more
    //     threads for writing does not seem correct either
    //
    //   - not writing in walk threads in order to use locking of
    //     mpsc
    //
    //   - threading rx clones/references at the end of each leaf to
    //     write paths in parallel doesn't make sense
    //     - just pass around file handles and locks
    //

    pool.join();
}

/**
 * Iterate through rx channel endpoints and write the paths to the
 * appropriate files.
 *
 * @param output_dir    directory to place files at
 * @param flush         the rx channel endpoint containing flush paths
 * @param push          the rx channel endpoint containing push paths
 * @param nthreads      number of threads allowed to use to write output
 */
fn write_outputs(output_dir: PathBuf, ops: Ops,
                 flush: mpsc::Receiver<PathBuf>,
                 push:  mpsc::Receiver<PathBuf>,
                 nthreads: usize) {
    fn write_paths(pool: &ThreadPool,
                   output_dir: PathBuf,
                   name: &str,
                   paths: mpsc::Receiver<PathBuf>) {
        let name_str = String::from(name);
        pool.execute(move || {
            let mut output_path = output_dir;
            output_path.push(name_str);
            let mut output_file = match fs::File::create(output_path.clone()) {
                Ok(file) => file,
                Err(msg) => panic!("Error: Could not open flush file {}: {}", output_path.display(), msg),
            };

            for path in paths {
                let _ = output_file.write_all((path.display().to_string() + "\n").as_bytes());
            }
        });
    }

    let nfiles = if ops.flush { 1 } else { 0 } + if ops.push { 1 } else { 0 };
    let pool = ThreadPool::new(min(nthreads, nfiles));
    if ops.flush {
        write_paths(&pool, output_dir.clone(), "flush", flush);
    }
    if ops.push {
        write_paths(&pool, output_dir.clone(), "push",  push);
    }
    pool.join();
}

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    #[arg(help="DAL root path")]
    dal_root: PathBuf,

    #[arg(help="Path to config file")]
    config_file: PathBuf,

    #[arg(help="Output Directory")]
    output_dir: PathBuf,

    #[clap(flatten)]
    ops: Ops,

    #[arg(short, long, value_name="regexp")]
    #[arg(help="Regex pattern which objects must match in order to be eligible for any operation")]
    must_match: Option<Regex>,

    #[arg(short, long, default_value="1", help="Thread Count")]
    nthreads: usize,

    #[arg(short, long, help="Force operation even if condition is not met")]
    force: bool,

    #[arg(short, long, help="Overwrite config file reftime")]
    reftime: Option<u64>,

    #[arg(short, long, help="Input path is a leaf dir")]
    leaf: bool
}

fn main() {
    let cli = Cli::parse();

    let mut fp = { FlushPush {
        config:     config::Config::from_pathbuf(cli.config_file.clone()),
        ops:        cli.ops.clone(),
        must_match: cli.must_match,
        force:      cli.force,
    } };

    // never process PUSH db
    fp.config.add_to_blacklist(PUSHDB_REGEX);

    // overwrite reftime found in config file with command line argument
    if let Some(reftime) = cli.reftime {
        fp.config.set_reftime(reftime);
    }

    // output channels for each type of operation
    let (flush_tx, flush_rx) = mpsc::channel();
    let (push_tx,  push_rx)  = mpsc::channel();

    // threads write to tx
    let output = Output {
        flush: flush_tx,
        push:  push_tx,
    };

    if cli.leaf {
        process_leaf(cli.dal_root, Arc::new(fp), output);
    } else {
        print_flushable_in_dal(&cli.dal_root, fp, output, cli.nthreads);
    }

    // read from rx
    write_outputs(cli.output_dir, cli.ops, flush_rx, push_rx, cli.nthreads);
}

#[cfg(test)]
mod tests;
