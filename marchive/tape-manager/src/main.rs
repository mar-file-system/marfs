// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

///
/// This utility is meant to perform management 'tasks' for the orchestration of Marchive data objects
/// stored on tape media.
///
/// The utility is intended to be run on Marchive storage nodes ( separate instance per-node ) via a
/// systemd unit wrapper.  It is assumed that only a single utility instance will be active on a
/// host at a time ( systemd service unit wrapper should enforce this ).
///
/// Behavior of the utility is controlled via a TOML config file, passed as a '-c <ConfigFilePath>'
/// argument to the program at statup.
/// See 'example-config.toml' for details.
///
/// The utility does not itself perform any direct tape-interaction.  Instead, it is designed to
/// take in 'taskfiles' ( lists of objects associated with some operation ) produced by client
/// programs, reformat those taskfiles for ingest by arbitrary tape management program(s),
/// check for conflicts between various tasks targeting the same object,
/// launch and track the status of tape management program instances,
/// recognize when the original client program has completed its associated work,
/// and finally to release the associated tracking state for any tasks from that client.
///
/// Taskfiles transition between various filesystem paths, indicating their overall status:
///   GENERATING -- Location for clients to perform initial output of taskfiles.
///                 This utility ignores this location, so as to avoid parsing an incomplete
///                 taskfile output.
///   INPUT      -- Location scanned by this utility for new taskfile inputs
///   PROCESSING -- Location where taskfiles are stored during parsing / conflict checks / and
///                 throughout the runtime of the associated tape management program(s).
///   OUTPUT     -- Location where taskfiles are moved to upon completion of associated tape
///                 management programs.
///                 There are two subpaths within this location, one for successful tasks and
///                 another for failed tasks.  Any failure of any associated tape management
///                 program results in the entire task being marked as 'failed'.
///                 Note also that 'successful' tasks may have entries omitted from their
///                 taskfile if that object operation was 'overridden' by another client.
///                 See the TOML config file for details on this.
///   COMPLETE   -- NOT a location, but rather an assumed state indicated once a taksfile is
///                 deleted from the OUTPUT location.
///                 At this point, the associated client is assumed to have completed.  This
///                 utility will then clear state associated with the task, allowing any
///                 subsequent conflicting / overriding tasks to run.
///

/// parsing / validation / representation of TOML config file
mod config;
/// depth-first-search capability for filesystem trees
mod dfs;
/// definition of various regex / printing / pathname formatters
mod format;
/// tracker for all task state and associated procs
mod runner;

use clap::Parser;
use config::Config;
use dfs::DFS;
use format::{ProcessingPath, ProcessingPathElement};
use runner::Runner;
use std::{
    env, fs,
    io::ErrorKind,
    path::PathBuf,
    sync::{Arc, OnceLock, RwLock},
    thread::sleep,
    time::{Duration, Instant, SystemTime},
};
use tokio;

pub static PROGRAM_CONFIG: OnceLock<Config> = OnceLock::new();

#[derive(Parser, Debug)]
struct Args {
    /// Path to the program config file.
    #[arg(short, long)]
    config_path: String,

    /// Hostname associated with this program. Determines which per-host config params to utilize.
    #[arg(short = 'H', long)]
    hostname: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let hostname: String = args.hostname.unwrap_or_else(||
        env::var("HOSTNAME").expect("hostname should either be passed as '-H <hostname>' or specified in the HOSTNAME environment variable")
    );
    println!("/// Startup ///");
    let check_signals = Arc::new(RwLock::new((false, false)));
    let set_signals = Arc::clone(&check_signals);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        set_signals.write().unwrap().0 = true;
        tokio::signal::ctrl_c().await.unwrap();
        set_signals.write().unwrap().1 = true;
    });

    println!("/// Reading in config file content ///");
    if PROGRAM_CONFIG
        .set(dbg!(Config::new(&args.config_path, hostname)))
        .is_err()
    {
        panic!("failed to initialize PROGRAM_CONFIG OnceLock");
    }
    let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref

    // cleanup anything pre-existing in the processing subdir
    processing_tree_cleanup(config);

    // prepare state tracking
    let mut runner = Runner::new();
    let mut proc_limit: u32 = config.task_parallelism;
    let mut drain_now = false;
    let mut abort = false;
    let mut last_status = Instant::now() - config.status_frequency; // force immediate status report
    let mut last_scan = Instant::now() - config.scan_frequency; // force immediate input scan
    let mut last_filter = Instant::now();
    let mut last_poll = Instant::now();
    let mut last_check = Instant::now();
    let mut last_cleanup = Instant::now();

    // main logic loop
    println!("/// Ready for inputs ///");
    while !abort {
        // check for caught signal
        if !drain_now {
            drain_now = check_signals.read().unwrap().0;
            if drain_now {
                println!("/// SIGINT received -- Draining tasks ///");
            }
        } else {
            abort = check_signals.read().unwrap().1;
            if abort {
                println!("/// SIGINT received AGAIN -- ABORTING ///");
            }
        }

        // give a status report
        if last_status.elapsed() > config.status_frequency {
            runner.report();
            last_status = Instant::now();
        }

        // perform cleanup
        if last_cleanup.elapsed() > config.cleanup_frequency {
            println!("Performing cleanup...");
            let cleanstart = Instant::now();
            output_tree_cleanup(config);
            println!(
                "...complete in {}",
                format::duration_to_string(&cleanstart.elapsed())
            );
            last_cleanup = Instant::now();
        }

        // scan for input files, if not draining or overloaded
        let mut newtasks = false;
        if !drain_now && !runner.is_overloaded() && last_scan.elapsed() > config.scan_frequency {
            newtasks = runner.scan_for_inputs();
            last_scan = Instant::now();
        }

        if runner.has_active_tasks() {
            // progress active tasks

            if newtasks || drain_now || last_filter.elapsed() > config.filter_frequency {
                runner.filter_tasks();
                last_filter = Instant::now();
            }

            runner.launch_tasks(&mut proc_limit);

            if drain_now || last_poll.elapsed() > config.poll_frequency {
                runner.poll_tasks(&mut proc_limit);
                last_poll = Instant::now();
            }

            assert!(proc_limit <= config.task_parallelism);

            if drain_now || last_check.elapsed() > config.check_frequency {
                if runner.check_tasks() {
                    // retrigger filter
                    last_filter = Instant::now() - config.filter_frequency;
                }
                last_check = Instant::now();
            }
        } else {
            // no active tasks
            assert!(proc_limit == config.task_parallelism);
            if drain_now {
                break;
            } // fully drained
        }

        // avoid pointlessly tight update loop
        sleep(Duration::from_millis(100));
    }

    println!("/// Terminating ///");
}

/// Clean up any files / dirs in the processing location from previous runs
fn processing_tree_cleanup(config: &Config) {
    let mut dfs = match DFS::new(&config.processing_subdir) {
        Err(e) => {
            if e.kind() != ErrorKind::NotFound {
                eprintln!(
                    "ERROR: Failed to begin scan of processing subdir {:?}: {e}",
                    &config.processing_subdir
                );
            }
            return;
        }
        Ok(dfs) => dfs,
    };
    // find our designated processing subdir
    while let Some(entry) = dfs.next() {
        let (entry, etype) = match entry {
            Err(e) => {
                eprintln!("ERROR: During cleanup of processing tree: {e}");
                continue;
            }
            Ok(ent) => ent,
        };
        let entpath = entry.path();
        match ProcessingPath::try_from(entpath.as_ref()) {
            Ok(pp) => {
                assert_eq!(pp.process(), None);
                assert_eq!(pp.timestamp(), None);
                assert!(matches!(
                    pp,
                    ProcessingPath {
                        element: ProcessingPathElement::IntermediateDir,
                        ..
                    }
                ));
                if etype.is_dir() {
                    dfs.focus_search();
                }
                break;
            }
            Err(e) => {
                if etype.is_dir() {
                    dfs.omit_search();
                } else {
                    eprintln!("ERROR: Encountered unrecognized path in processing tree during cleanup: {e}");
                }
            }
        }
    }
    // use reversed dfs for cleanup
    for value in dfs.rev() {
        let Ok((entry, etype)) = value else {
            eprintln!(
                "ERROR: During cleanup of processing tree: {}",
                value.err().unwrap()
            );
            continue;
        };
        let entpath = entry.path();
        let ppath = match ProcessingPath::try_from(entpath.as_ref()) {
            Err(e) => {
                eprintln!(
                    "ERROR: Encountered unrecognized path in processing tree during cleanup: {e}"
                );
                continue;
            }
            Ok(p) => p,
        };
        match ppath.element {
            ProcessingPathElement::IntermediateDir => {
                if let Err(e) = fs::remove_dir(&entpath) {
                    eprintln!(
                        "ERROR: Failed to cleanup processing intermediate dir {entpath:?}: {e}"
                    );
                }
            }
            ProcessingPathElement::OriginalTaskfile(subpath) => {
                if etype.is_file() {
                    let mut rnametgt = PathBuf::from(&config.output_failure_subdir);
                    rnametgt.push(subpath);
                    if let Some(rnameparent) = rnametgt.parent() {
                        match fs::create_dir_all(rnameparent) {
                            Err(e) => eprintln!(
                                "ERROR: Failed to create rename target dir(s) {rnameparent:?}: {e}"
                            ),
                            Ok(_) => (),
                        }
                    }
                    if let Err(e) = fs::rename(&entpath, &rnametgt) {
                        eprintln!(
                            "ERROR: Failed to rename {entpath:?} to failure path {rnametgt:?}: {e}"
                        );
                    } else {
                        println!("Renamed encountered taskfile {entpath:?} to failure location {rnametgt:?}")
                    }
                } else if let Err(e) = fs::remove_dir(&entpath) {
                    eprintln!(
                        "ERROR: Failed to cleanup processing intermediate dir {entpath:?}: {e}"
                    );
                }
            }
            ProcessingPathElement::FilteredTaskfile => {
                if let Err(e) = fs::remove_file(&entpath) {
                    eprintln!("ERROR: Failed to cleanup filtered taskfile {entpath:?}: {e}");
                }
            }
            ProcessingPathElement::SubTaskDir(_) => {
                if let Err(e) = fs::remove_dir(&entpath) {
                    eprintln!("ERROR: Failed to cleanup SubTask dir {entpath:?}: {e}");
                }
            }
            ProcessingPathElement::SubTaskInput(_) => {
                if let Err(e) = fs::remove_file(&entpath) {
                    eprintln!("ERROR: Failed to cleanup SubTask input file {entpath:?}: {e}");
                }
            }
            ProcessingPathElement::SubTaskOutput(_) => {
                if let Err(e) = fs::remove_file(&entpath) {
                    eprintln!("ERROR: Failed to cleanup SubTask output file {entpath:?}: {e}");
                }
            }
        }
    }
}

/// Cleanup any old files / dirs in output locations
fn output_tree_cleanup(config: &Config) {
    for cleantgt in [&config.output_success_subdir, &config.output_failure_subdir] {
        let output_dfs = match DFS::new(cleantgt) {
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    eprintln!("ERROR: Failed to open {:?} for cleanup: {e}", cleantgt);
                }
                continue;
            }
            Ok(dfs) => dfs,
        };
        for value in output_dfs {
            let Ok((entry, etype)) = value else {
                eprintln!(
                    "ERROR: During traversal of output tree: {}",
                    value.err().unwrap()
                );
                continue;
            };
            let epath = entry.path();
            let tgtmeta = match fs::metadata(&epath) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("ERROR: Failed to stat {entry:?} for cleanup: {e}");
                    continue;
                }
            };
            let atime = match tgtmeta.accessed() {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("ERROR: Failed to get {entry:?} mtime for cleanup: {e}");
                    continue;
                }
            };
            let age = SystemTime::now()
                .duration_since(atime)
                .unwrap_or(Duration::ZERO);
            if age < config.cleanup_timeout {
                continue;
            }
            if etype.is_file() {
                if let Err(e) = fs::remove_file(&epath) {
                    eprintln!("ERROR: Failed to remove {epath:?} file during cleanup: {e}");
                } else {
                    println!(
                        "Removed {epath:?} after {}",
                        format::duration_to_string(&age)
                    );
                }
            } else if let Err(e) = fs::remove_dir(&epath) {
                match e.kind() {
                    ErrorKind::DirectoryNotEmpty | ErrorKind::NotFound => (),
                    _ => eprintln!("ERROR: Failed to remove {epath:?} dir during cleanup: {e}"),
                }
            }
        }
    }
}
