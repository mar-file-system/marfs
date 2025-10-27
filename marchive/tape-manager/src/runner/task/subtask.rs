// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

use crate::{
    config::ConfigTask,
    format::{ProcessingPath, ProcessingPathElement},
    PROGRAM_CONFIG,
};
use chrono::{DateTime, Local};
use std::{
    collections::HashMap,
    fmt, fs,
    io::{ErrorKind, LineWriter},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Instant, SystemTime},
};

/// Encapsulate state of child procs and clean up after them
pub struct SubTask {
    pub pid: u32,
    pub timestamp: SystemTime,
    pub count: usize,
    pub cmdvals: HashMap<String, String>,
    pub child: TaskChild,
    pub started: Instant,
    pub taskdef: Arc<ConfigTask>,
}
pub enum TaskChild {
    None,
    Child(Child),
    Complete(i32),
}

impl fmt::Display for SubTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&format!(
            "Subtask{} of \"{}\" Task@{:?}",
            self.count,
            &self.taskdef.name,
            DateTime::<Local>::from(self.timestamp)
        ))
    }
}

// cleanup logic for SubTask
impl Drop for SubTask {
    /// Ensure we kill any child process ( if running ) and cleanup its temporary taskfile input
    fn drop(&mut self) {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
                                                    // kill our child proc, if it exists
        if let TaskChild::Child(ref mut child) = self.child {
            match child.kill() {
                Ok(_) => eprintln!("WARNING: {self} forcibly killed child process"),
                Err(error) => {
                    panic!("FATAL ERROR: {self} failed to send SIGKILL to child process: {error}")
                }
            }
        }
        // clean up our output file
        let mut ppath = ProcessingPath::new(
            Some(self.pid),
            Some(self.timestamp),
            ProcessingPathElement::SubTaskOutput(self.count),
        );
        let outputpath = PathBuf::from(&ppath);
        if !matches!(&self.child, TaskChild::Complete(0)) {
            let rnamepath = match outputpath.strip_prefix(&config.processing_subdir) {
                Ok(path) => { let mut p = PathBuf::from(&config.logged_failure_subdir); p.push(path); p }
                Err(_) => panic!(
                    "FATAL ERROR: {self} failed to trim input subdir path \"{}\" from output target {outputpath:?}",
                    &config.input_subdir,
                ),
            };
            if let Some(rnamedir) = rnamepath.parent() {
                if let Err(error) = fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(rnamedir)
                {
                    match error.kind() {
                        ErrorKind::AlreadyExists => (), // ignore EEXIST
                        _ => {
                            eprintln!(
                                "ERROR: {self} failed to create failure output dir location \
                            {rnamedir:?} for {outputpath:?}: {error}"
                            );
                        }
                    }
                }
            }
            if let Err(error) = fs::rename(&outputpath, &rnamepath) {
                eprintln!("ERROR: {self} failed to rename taskfile {outputpath:?} to output location {rnamepath:?}: {error}");
            }
        } else {
            if let Err(error) = fs::remove_file(&outputpath) {
                eprintln!(
                    "ERROR: {self} failed to cleanup temporary file {outputpath:?} after child process \
                    termination: {error}"
                );
            }
        }
        // clean up our input file
        ppath.element = ProcessingPathElement::SubTaskInput(self.count);
        let inputpath = PathBuf::from(&ppath);
        if let Err(error) = fs::remove_file(&inputpath) {
            eprintln!(
                "ERROR: {self} failed to cleanup temporary file {inputpath:?} after child process termination: {error}"
            );
        }
        // clean up SubTask subdir
        ppath.element = ProcessingPathElement::SubTaskDir(self.count);
        let dirpath = PathBuf::from(&ppath);
        if let Err(error) = fs::remove_dir(&dirpath) {
            eprintln!(
                "ERROR: {self} failed to cleanup temporary directory {dirpath:?} after child process termination: {error}"
            );
        }
    }
}

// general implementation for SubTask
impl SubTask {
    /// Instantiate a SubTask based on the given parent taskfile path, count of pre-existing
    /// sibling SubTasks, and Config references
    pub fn new(
        cmdvals: HashMap<String, String>,
        parent_pid: u32,
        parent_timestamp: SystemTime,
        subtask_count: usize,
        taskdef: &Arc<ConfigTask>,
    ) -> SubTask {
        SubTask {
            pid: parent_pid,
            timestamp: parent_timestamp,
            count: subtask_count,
            cmdvals,
            child: TaskChild::None,
            started: Instant::now(),
            taskdef: Arc::clone(taskdef),
        }
    }

    /// Produce a LineWriter for the temporary input taskfile of the given SubTask,
    /// appending to any pre-existing file
    pub fn get_writer(&self) -> Result<LineWriter<fs::File>, String> {
        match &self.child {
            TaskChild::None => (),
            _ => return Err("should not be written to after child proc launch".to_string()),
        }
        let mut ppath = ProcessingPath::new(
            Some(self.pid),
            Some(self.timestamp),
            ProcessingPathElement::SubTaskDir(self.count),
        );
        let dirpath = PathBuf::from(&ppath);
        match fs::DirBuilder::new().mode(0o700).create(&dirpath) {
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => (), // ignore EEXIST
                _ => return Err(format!("failed to create sub-task subdir {dirpath:?}: {e}")),
            },
            Ok(()) => (),
        };
        ppath.element = ProcessingPathElement::SubTaskInput(self.count);
        let inputpath = PathBuf::from(&ppath);
        let inputfile = match fs::OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(&inputpath)
        {
            Err(error) => {
                return Err(format!(
                    "failed to open sub-task input file {inputpath:?}: {error}"
                ))
            }
            Ok(file) => file,
        };
        Ok(LineWriter::new(inputfile))
    }

    /// Launch the command associated with this SubTask
    pub fn launch(&mut self) -> Result<(), String> {
        match &self.child {
            TaskChild::None => (),
            _ => return Err("already launched previously".to_string()),
        }
        // temporarily insert our 'file' value for command formating
        let mut ppath = ProcessingPath::new(
            Some(self.pid),
            Some(self.timestamp),
            ProcessingPathElement::SubTaskInput(self.count),
        );
        if let Some(val) = self
            .cmdvals
            .insert(String::from("file"), String::from(&ppath))
        {
            panic!("FATAL ERROR: {self} cmdvals table held pre-existing 'file' target: \"{val}\"");
        }
        let commandline = super::format_line(&self.taskdef.command, &self.cmdvals);
        let _ = self.cmdvals.remove("file"); // revert our 'file' addition
        let commandline = match commandline {
            Err(error) => return Err(format!("failed to format command string: {error}")),
            Ok(cmd) => cmd,
        };
        // construct a bash wrapper for this command
        let mut command = Command::new("bash");
        command.arg("-c");
        command.arg(&commandline);
        // open our output file
        ppath.element = ProcessingPathElement::SubTaskOutput(self.count);
        let outpath = PathBuf::from(&ppath);
        let outfile = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&outpath)
        {
            Err(e) => {
                return Err(format!(
                    "failed to create process output file {outpath:?}: {e}"
                ))
            }
            Ok(f) => f,
        };
        let errfile = match outfile.try_clone() {
            Err(e) => {
                return Err(format!(
                    "failed to clone process output file {outpath:?}: {e}"
                ))
            }
            Ok(f) => f,
        };
        let start_time = Instant::now();
        let child = match command
            .stdin(Stdio::null())
            .stdout(outfile)
            .stderr(errfile)
            .spawn()
        {
            Err(error) => {
                return Err(format!(
                    "failed to launch command \"{commandline}\": {error}"
                ))
            }
            Ok(c) => c,
        };
        self.child = TaskChild::Child(child);
        self.started = start_time;
        Ok(())
    }

    /// Poll the status of this SubTask's running command
    pub fn poll(&mut self) -> &TaskChild {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
        let selfdisplay = format!("{self}"); // preformat our display String to avoid simultaneous ref conflict with 'child'
        let child = match self.child {
            TaskChild::Child(ref mut c) => c,
            TaskChild::Complete(_) => return &self.child,
            _ => panic!("FATAL ERROR: {self} not yet launched!"),
        };
        let mut killproc = false;
        match child.try_wait() {
            Ok(Some(status)) => {
                self.child = match status.code() {
                    Some(code) => TaskChild::Complete(code),
                    None => TaskChild::Complete(-1),
                };
                return &self.child;
            }
            Ok(None) => (),
            Err(e) => eprintln!("could not query child proc status: {e}"),
        }
        if let Some(timeout) = self.taskdef.timeout {
            let mut since_update = self.started.elapsed(); // if we haven't seen an update, use started time instead
            let ppath = ProcessingPath::new(
                Some(self.pid),
                Some(self.timestamp),
                ProcessingPathElement::SubTaskOutput(self.count),
            );
            let outpath = PathBuf::from(&ppath);
            if let Ok(md) = fs::metadata(&outpath) {
                if let Ok(t) = md.modified() {
                    if let Ok(d) = SystemTime::now().duration_since(t) {
                        since_update = d;
                    }
                }
            }
            if since_update >= timeout {
                eprintln!(
                    "ERROR: {selfdisplay} exceeded timeout after running for {} without new output",
                    crate::runner::duration_to_string(&since_update)
                );
                killproc = true;
            }
        }
        if !killproc && self.started.elapsed() >= config.hard_timeout {
            eprintln!(
                "ERROR: {selfdisplay} exceeded hard timeout after running for {}",
                crate::runner::duration_to_string(&self.started.elapsed())
            );
            killproc = true;
        }
        if killproc {
            match child.kill() {
                Ok(_) => {
                    self.child = TaskChild::Complete(-1);
                }
                Err(error) => {
                    panic!("FATAL ERROR: {selfdisplay} failed to SIGKILL child process: {error}")
                }
            }
        }
        &self.child
    }
}
