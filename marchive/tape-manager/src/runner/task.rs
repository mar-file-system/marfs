// Copyright 2015. T//ad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

mod subtask;

use super::objtable::{LookupError, ObjLocation, ObjTable, Object};
use crate::{
    config::ConfigTask,
    format::{ProcessingPath, ProcessingPathElement, BRACED_NAME_REGEX},
    PROGRAM_CONFIG,
};
use regex::Match;
use std::{
    cell::RefCell,
    collections::HashMap,
    fs,
    io::{BufRead, BufReader, ErrorKind, LineWriter, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    process,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use subtask::SubTask;

/// Encapsulates the handling of a particular taskfile from the config input subdir
pub struct Task<S> {
    itask: InnerTask,
    status: PhantomData<S>,
}
/// 'inner' Task def to make cleanup simpler ( see Drop implementation )
struct InnerTask {
    taskfile: PathBuf,
    currentpath: PathBuf,
    timestamp: SystemTime,
    taskdef: Arc<ConfigTask>,
    objtable: Rc<RefCell<ObjTable>>,
    parsedoffset: usize,
    pathvals: HashMap<String, String>,
    subtasks: Vec<SubTask>,
    times: Times,
    cleanup: InnerTaskCleanup,
}
/// Defines how to handle a taskfile during cleanup of an InnerTask def
///     Success => rename below config.output_success_subdir
///     Failure => rename below config.output_failure_subdir
///     Ignore  => do not perform a rename
///                ( taskfile should not be handled by this host's instance )
enum InnerTaskCleanup {
    Delete,
    Failure,
    Ignore,
}
/// Tracking of various important events in a Task lifetime
#[derive(Clone)]
pub struct Times {
    pub detected: Option<Instant>,
    pub filtered: Option<Instant>,
    pub started: Option<Instant>,
    pub completed: Option<Instant>,
    pub cleaned: Option<Instant>,
}
// All possible 'status' values for a Task
pub struct TaskGrabbed;
pub struct TaskFiltered;
pub struct TaskRunning;
pub struct TaskComplete;
/// Returned by most Task methods, this defines a value as either being unchanged
/// (returning ownership of the original type), transformed (providing ownership
/// of a new Task subtype), or an error description string.
pub enum TaskResult<U, T> {
    Unchanged(U),
    Transformed(T),
    Err(String),
}

// Cleanup logic for InnerTask
impl Drop for InnerTask {
    /// Remove any ObjTable entries associated with this Task and either rename the original taskfile to
    /// the appropriate output location, delete it, or ignore it
    fn drop(&mut self) {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
        if self.parsedoffset > 0 {
            // revert any ObjTable entries
            self.objtable
                .borrow_mut()
                .remove(&self.taskdef, self.timestamp);
            // possibly cleanup filtered taskfile ( if not already renamed )
            let filterpath = PathBuf::from(&ProcessingPath::new(
                Some(process::id()),
                Some(self.timestamp),
                ProcessingPathElement::FilteredTaskfile,
            ));
            if let Err(error) = fs::remove_file(&filterpath) {
                if !matches!(error.kind(), ErrorKind::NotFound) {
                    eprintln!("ERROR: Failed to cleanup filtered taskfile {filterpath:?}: {error}")
                }
            }
        }
        // cleanup our taskfile
        let currentpath = &self.currentpath;
        match &self.cleanup {
            InnerTaskCleanup::Ignore => return, // early exit to avoid parent dir cleanup
            InnerTaskCleanup::Failure => {
                let mut rnamepath = PathBuf::from(&config.output_failure_subdir);
                rnamepath.push(&self.taskfile);
                if let Some(rnamedir) = rnamepath.parent() {
                    if let Err(error) = fs::create_dir_all(rnamedir) {
                        match error.kind() {
                            ErrorKind::AlreadyExists => (), // ignore EEXIST
                            _ => {
                                eprintln!(
                                    "ERROR: Failed to create output dir location {rnamedir:?} for taskfile {currentpath:?}: {error}"
                                ); // almost certainly means rename will fail... but we'll attempt anyhow
                            }
                        }
                    }
                }
                if let Err(error) = fs::rename(currentpath, &rnamepath) {
                    eprintln!("ERROR: Failed to rename taskfile {currentpath:?} to output location {rnamepath:?}: {error}");
                } else {
                    println!("Taskfile {currentpath:?} has been renamed to output location {rnamepath:?}");
                }
            }
            InnerTaskCleanup::Delete => {
                if let Err(error) = fs::remove_file(currentpath) {
                    eprintln!("ERROR: Failed to remove {currentpath:?}: {error}")
                }
            }
        };
        // drop all subtasks early, so they cleanup subtrees
        let _ = self.subtasks.drain(..);
        // cleanup intermediate processing dirs
        let mut ppath = ProcessingPath::new(
            Some(process::id()),
            Some(self.timestamp),
            ProcessingPathElement::IntermediateDir,
        );
        let cleanup_root = PathBuf::from(&ppath);
        if cleanup_root.is_dir() {
            ppath.element = ProcessingPathElement::OriginalTaskfile(&self.taskfile);
            let cleanup_path = PathBuf::from(&ppath);
            let mut cleanup_tgt: &Path = &cleanup_path;
            while &cleanup_root != cleanup_tgt {
                cleanup_tgt = cleanup_tgt.parent().unwrap_or_else(|| {
                    // NOTE: intentially strips file name suffix prior to first rmdir
                    eprintln!(
                        "ERROR: Failed to identify parent dir of cleanup target: {cleanup_tgt:?}"
                    );
                    &cleanup_root
                });
                if let Err(error) = fs::remove_dir(&cleanup_tgt) {
                    if error.kind() != ErrorKind::NotFound {
                        eprintln!(
                            "ERROR: Failed to cleanup Task processing subdir {cleanup_tgt:?}: {error}"
                        );
                    }
                }
            }
        }
    }
}

impl<S> Task<S> {
    pub fn times(&self) -> &Times {
        &self.itask.times
    }
}

impl Task<TaskGrabbed> {
    /// Create a new Task, associated with a new taskfile, a Config reference, and an ObjTable
    pub fn new(currentpath: PathBuf, objtable: Rc<RefCell<ObjTable>>) -> Option<Self> {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
        let Ok(taskfile) = currentpath.strip_prefix(&config.input_subdir) else {
            eprintln!(
                "ERROR: Encountered task file {currentpath:?} does not appear to \
                be within the designated input subdir {}",
                &config.input_subdir
            );
            return None;
        };
        let mut itask = InnerTask {
            taskfile: taskfile.to_path_buf(),
            currentpath,
            taskdef: Arc::clone(&config.tasks[0]), // ugly stand-in value until we can fill it properly
            timestamp: SystemTime::now(),
            objtable,
            parsedoffset: 0,
            pathvals: HashMap::new(),
            subtasks: Vec::new(),
            times: Times {
                detected: Some(Instant::now()),
                filtered: None,
                started: None,
                completed: None,
                cleaned: None,
            },
            cleanup: InnerTaskCleanup::Failure, // failed until proven otherwise
        };
        let taskfile = &itask.taskfile; // shorthand ref to new buffer location
                                        // attempt to capture all values from the task file path
        let Some(pathcaptures) = config.task_path_regex.captures(taskfile.to_str().unwrap()) else {
            eprintln!(
                "ERROR: Encountered task file {taskfile:?} does not match \
                      expected path formatting!"
            );
            return None; // note that itask.drop() will be called here
        };
        // make sure this program instance should even be interacting with this file
        if !value_within_restriction(taskfile, "pod", &config.pods, &pathcaptures.name("pod"))
            || !value_within_restriction(
                taskfile,
                "block",
                &config.blocks,
                &pathcaptures.name("block"),
            )
            || !value_within_restriction(taskfile, "cap", &config.caps, &pathcaptures.name("cap"))
            || !value_within_restriction(
                taskfile,
                "scatter",
                &config.scatters,
                &pathcaptures.name("scatter"),
            )
        {
            itask.cleanup = InnerTaskCleanup::Ignore; // 'defuse' our destructor
            return None;
        }
        // find a matching task defintion in the Config
        let Some(taskname) = pathcaptures.name("task") else {
            eprintln!(
                "ERROR: Encountered task file {taskfile:?} does not contain a \
                        parsable '{{task}}' value!"
            );
            return None;
        };
        let taskname = taskname.as_str();
        let Some(taskdef) = config.tasks.iter().filter(|t| t.name == taskname).next() else {
            eprintln!(
                "ERROR: Failed to map task file {taskfile:?} ( '{{task}}' == \
                       \"{taskname}\" ) to any known task type!"
            );
            return None;
        };
        itask.taskdef = Arc::clone(taskdef);
        // establish a HashMap of all relevant path captures
        for value in &config.task_path_values {
            let Some(capture) = pathcaptures.name(&value) else {
                eprintln!(
                    "ERROR: Encountered task file {taskfile:?} does not contain a \
                           parsable '{{{value}}}' value!"
                );
                return None;
            };
            if let Some(_) = itask
                .pathvals
                .insert(value.clone(), String::from(capture.as_str()))
            {
                eprintln!(
                    "ERROR: Encountered task file {taskfile:?} contains repeat def \
                           of a '{{{value}}}' value!"
                );
                return None;
            }
        }
        // create a destination dir(s) for the taskfile within our processing location
        let origts = itask.timestamp;
        while itask.timestamp.duration_since(origts).unwrap() < Duration::from_millis(100) {
            // on taskfile timestamp collision, reattempt up to 100 times
            let ppath = ProcessingPath::new(
                Some(process::id()),
                Some(itask.timestamp),
                ProcessingPathElement::IntermediateDir,
            );
            let tsdirpath = PathBuf::from(&ppath);
            if let Some(tsparent) = tsdirpath.parent() {
                if let Err(error) = fs::create_dir_all(tsparent) {
                    if !matches!(error.kind(), ErrorKind::AlreadyExists) {
                        eprintln!(
                            "ERROR: Failed to create processing parent path {tsparent:?}: {error}"
                        );
                        return None;
                    }
                }
            }
            match fs::create_dir(&tsdirpath) {
                Ok(()) => break,
                Err(e) => {
                    match e.kind() {
                        ErrorKind::AlreadyExists => itask.timestamp += Duration::from_millis(1), // reattempt w/ timestamp 1ms ahead
                        _ => {
                            eprintln!("ERROR: Failed to create processing timestamp path {tsdirpath:?}: {e}");
                            return None;
                        }
                    }
                }
            }
        }
        let ppath = ProcessingPath::new(
            Some(process::id()),
            Some(itask.timestamp),
            ProcessingPathElement::OriginalTaskfile(taskfile.as_ref()),
        );
        let tgtpath = PathBuf::from(&ppath);
        if let Some(tgtdir) = tgtpath.parent() {
            if let Err(error) = fs::create_dir_all(tgtdir) {
                match error.kind() {
                    ErrorKind::AlreadyExists => (), // ignore EEXIST
                    _ => {
                        eprintln!(
                            "ERROR: Failed to create processing dir location {tgtdir:?}: {error}"
                        );
                        return None;
                    }
                }
            }
        }
        // actually grab the taskfile, moving it into our processing location
        if let Err(error) = fs::rename(&itask.currentpath, &tgtpath) {
            eprintln!(
                "ERROR: Failed to rename taskfile {:?} to processing location \
                      {tgtpath:?}: {error}",
                &itask.currentpath
            );
            return None;
        }
        println!(
            "New taskfile {:?} has been renamed to processing location {tgtpath:?}",
            &itask.currentpath
        );
        itask.currentpath = tgtpath; // update active path
                                     // finally, return an instantiantion of our Task
        Some(Task {
            itask,
            status: PhantomData,
        })
    }

    /// Parse the full content of this Task's taskfile and produce temporary input files for all
    /// associated SubTasks
    /// This function will produce TaskResult::Unchanged() if it recieves a 'Wait' result from its
    /// associated ObjTable.  A repeated invocation will reattempt file parsing.
    pub fn filter(mut self) -> TaskResult<Task<TaskGrabbed>, Task<TaskFiltered>> {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
                                                    // establish value tables and populate what we can from pathvals
        let mut cmdvals: HashMap<String, String> = HashMap::new();
        let mut filevals: HashMap<String, String> = HashMap::new();
        for key in &self.itask.taskdef.command_values {
            let Some(pval) = self.itask.pathvals.get(key) else {
                continue;
            };
            if let Some(oldval) = cmdvals.insert(String::from(key), String::from(pval)) {
                eprintln!(
                    "WARNING: Pre-existing {key}->{oldval} mapping \
                            found in command values of {} task ( bizarre edge case )",
                    &self.itask.taskdef.name
                );
            }
        }
        for key in &self.itask.taskdef.file_format_values {
            let Some(pval) = self.itask.pathvals.get(key) else {
                continue;
            };
            if let Some(oldval) = filevals.insert(String::from(key), String::from(pval)) {
                eprintln!(
                    "WARNING: Pre-existing {key}->{oldval} mapping \
                            found in file values of {} task ( bizarre edge case )",
                    &self.itask.taskdef.name
                );
            }
        }
        // open the taskfile for reading
        let sourcefile = match fs::File::open(&self.itask.currentpath) {
            Err(error) => {
                return TaskResult::Err(format!(
                    "ERROR: Failed to open taskfile {:?}: {}",
                    &self.itask.currentpath, error
                ))
            }
            Ok(file) => file,
        };
        let sourcereader = BufReader::new(sourcefile);
        // open filtered taskfile for writing
        let ppath = ProcessingPath::new(
            Some(process::id()),
            Some(self.itask.timestamp),
            ProcessingPathElement::FilteredTaskfile,
        );
        let filterpath = PathBuf::from(&ppath);
        let filterfile = match fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&filterpath)
        {
            Err(error) => {
                return TaskResult::Err(format!(
                    "failed to open filtered taskfile output {filterpath:?}: {error}"
                ))
            }
            Ok(file) => file,
        };
        let mut filterwriter = LineWriter::new(filterfile);
        // open writers for all SubTasks
        let mut writers: Vec<LineWriter<fs::File>> = Vec::new();
        for stask in &self.itask.subtasks {
            writers.push(match stask.get_writer() {
                Err(error) => return TaskResult::Err(format!("ERROR: {error}")),
                Ok(writer) => writer,
            });
        }
        // parse and reformat each line from the source
        for (index, sourceline) in sourcereader.lines().enumerate() {
            // check for read error
            let sourceline = match sourceline {
                Err(error) => {
                    return TaskResult::Err(format!(
                        "ERROR: Failed to read line from task file \
                                {:?}: {error}",
                        &self.itask.currentpath
                    ))
                }
                Ok(line) => line,
            };
            // skip previously processed lines
            if index < self.itask.parsedoffset {
                continue;
            }
            // apply our content regex
            let linecaptures = match config.task_content_regex.captures(&sourceline) {
                None => {
                    return TaskResult::Err(format!(
                        "ERROR: Task file {:?} does not match expected content \
                            formatting!\n\
                            Offending line: {sourceline}",
                        &self.itask.currentpath
                    ))
                }
                Some(caps) => caps,
            };
            // populate parsed values
            for key in &config.task_content_values {
                if self.itask.taskdef.command_values.iter().any(|k| k == key) {
                    let _ = cmdvals.insert(
                        String::from(key),
                        String::from(linecaptures.name(&key).unwrap().as_str()),
                    );
                }
                if self
                    .itask
                    .taskdef
                    .file_format_values
                    .iter()
                    .any(|k| k == key)
                {
                    let _ = filevals.insert(
                        String::from(key),
                        String::from(linecaptures.name(&key).unwrap().as_str()),
                    );
                }
            }
            // identify our object details  TODO this is horrible
            let location = ObjLocation {
                pod: {
                    if config.task_content_values.iter().any(|v| v == "pod") {
                        linecaptures["pod"].parse().unwrap()
                    } else {
                        match self.itask.pathvals.get("pod") {
                            Some(value) => value.parse().unwrap(),
                            None => 0,
                        }
                    }
                },
                block: {
                    if config.task_content_values.iter().any(|v| v == "block") {
                        linecaptures["block"].parse().unwrap()
                    } else {
                        match self.itask.pathvals.get("block") {
                            Some(value) => value.parse().unwrap(),
                            None => 0,
                        }
                    }
                },
                cap: {
                    if config.task_content_values.iter().any(|v| v == "cap") {
                        linecaptures["cap"].parse().unwrap()
                    } else {
                        match self.itask.pathvals.get("cap") {
                            Some(value) => value.parse().unwrap(),
                            None => 0,
                        }
                    }
                },
                scatter: {
                    if config.task_content_values.iter().any(|v| v == "scatter") {
                        linecaptures["scatter"].parse().unwrap()
                    } else {
                        match self.itask.pathvals.get("scatter") {
                            Some(value) => value.parse().unwrap(),
                            None => 0,
                        }
                    }
                },
            };
            let object = Object {
                name: String::from(&linecaptures["object"]),
                location,
            };
            // attempt to insert a record for this op
            let insertres = self.itask.objtable.borrow_mut().insert(
                object,
                &self.itask.taskdef,
                self.itask.timestamp,
            );
            if let Err(error) = insertres {
                match error {
                    LookupError::Wait => {
                        // we must exit this op and reattempt later
                        return TaskResult::Unchanged(self);
                    }
                    LookupError::Skip => {
                        // note the skip and continue on
                        continue;
                    }
                    LookupError::Conflict(error) => {
                        // absolute worst case, we must abort
                        return TaskResult::Err(format!(
                            "ERROR: Task conflict detected with new {} ops: {error}",
                            &self.itask.taskdef.name,
                        ));
                    }
                }
            }
            self.itask.parsedoffset = index + 1; // note successful addition to objtable
                                                 // write the accepted line to our filtered taskfile output
            if let Err(error) = writeln!(filterwriter, "{sourceline}") {
                return TaskResult::Err(format!(
                    "ERROR: Failed to write to filtered taskfile {filterpath:?}: {error}"
                ));
            }
            // format our output and command strings
            let outputline = match format_line(&self.itask.taskdef.file_format, &filevals) {
                Err(error) => {
                    return TaskResult::Err(format!(
                        "ERROR: Failed to filter taskfile {} line {sourceline}: {error}",
                        &self.itask.taskdef.name
                    ))
                }
                Ok(res) => res,
            };
            // find / create a SubTask + writer associated with this command
            let (subtask, writer) = match self
                .itask
                .subtasks
                .iter()
                .position(|st| st.cmdvals == cmdvals)
            {
                Some(pos) => (
                    self.itask.subtasks.get(pos).unwrap(),
                    writers.get_mut(pos).unwrap(),
                ),
                None => {
                    let stcount = self.itask.subtasks.len();
                    self.itask.subtasks.push(SubTask::new(
                        cmdvals.clone(),
                        process::id(),
                        self.itask.timestamp,
                        stcount,
                        &self.itask.taskdef,
                    ));
                    let st = self.itask.subtasks.get(stcount).unwrap();
                    writers.push(match st.get_writer() {
                        Err(error) => return TaskResult::Err(format!("ERROR: {error}")),
                        Ok(writer) => writer,
                    });
                    (st, writers.get_mut(stcount).unwrap())
                }
            };
            if let Err(error) = writeln!(writer, "{outputline}") {
                // output our reformatted line
                return TaskResult::Err(
                    format!(
                        "ERROR: Failed to write reformatted line from taskfile {:?} for {subtask}: {error}",
                        &self.itask.currentpath
                    )
                );
            }
        } // end of read-write loop
        self.itask.times.filtered = Some(Instant::now());
        TaskResult::Transformed(Task {
            itask: self.itask,
            status: PhantomData,
        })
    }
}

impl Task<TaskFiltered> {
    /// Launch command procs for all SubTasks, respecting (and updating) the provided limit on
    /// total count of processes
    pub fn launch(
        mut self,
        proc_limit: &mut u32,
    ) -> TaskResult<Task<TaskFiltered>, Task<TaskRunning>> {
        if self.itask.subtasks.is_empty() {
            // NOTE empty taskfile will result in empty subtask list,
            // effectively a 'fallthrough' case where we launch nothing and
            // assume success
            self.itask.times.started = Some(Instant::now());
        }
        for subtask in self.itask.subtasks.iter_mut() {
            if *proc_limit == 0 {
                return TaskResult::Unchanged(self);
            }
            match subtask.child {
                subtask::TaskChild::Complete(_) => continue,
                subtask::TaskChild::Child(_) => match subtask.poll() {
                    subtask::TaskChild::Complete(_) => *proc_limit += 1, // *increase* proc limit to reflect child termination
                    _ => (),
                },
                subtask::TaskChild::None => match subtask.launch() {
                    Ok(_) => {
                        *proc_limit -= 1;
                        if self.itask.times.started.is_none() {
                            self.itask.times.started = Some(Instant::now());
                        }
                    }
                    Err(e) => {
                        return TaskResult::Err(format!(
                            "ERROR: Failed to launch SubTask process: {e}"
                        ))
                    }
                },
            }
        }
        TaskResult::Transformed(Task {
            itask: self.itask,
            status: PhantomData,
        })
    }
}

impl Task<TaskRunning> {
    /// Check the status of all running SubTask processes, incrementing the provided process limit
    /// as processes terminate
    pub fn poll(
        mut self,
        proc_limit: &mut u32,
    ) -> TaskResult<Task<TaskRunning>, Task<TaskComplete>> {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
        let mut anyrunning = false;
        for subtask in self.itask.subtasks.iter_mut() {
            match &subtask.child {
                subtask::TaskChild::None => panic!(
                    "FATAL ERROR: {subtask} has no child, despite being in a 'running' state"
                ),
                subtask::TaskChild::Child(_) => match subtask.poll() {
                    subtask::TaskChild::Complete(_) => *proc_limit += 1, // *increase* proc limit to reflect child termination
                    _ => anyrunning = true,
                },
                subtask::TaskChild::Complete(_) => (),
            }
        }
        if anyrunning {
            return TaskResult::Unchanged(self);
        }
        if self.itask.subtasks.iter().any(|st| match &st.child {
            subtask::TaskChild::Complete(retval) => *retval != 0,
            _ => true, // should never happen, but might as well return an error if it does
        }) {
            return TaskResult::Err(format!(
                "ERROR: Detected failed child proc(s) for {} task on file {:?}",
                &self.itask.taskdef.name, &self.itask.taskfile,
            ));
        }
        // rename to success path
        let ppath = ProcessingPath::new(
            Some(process::id()),
            Some(self.itask.timestamp),
            ProcessingPathElement::FilteredTaskfile,
        );
        let filterpath = PathBuf::from(&ppath);
        let mut successpath = PathBuf::from(&config.output_success_subdir);
        successpath.push(&self.itask.taskfile);
        if let Some(tgtdir) = successpath.parent() {
            if let Err(error) = fs::create_dir_all(tgtdir) {
                match error.kind() {
                    ErrorKind::AlreadyExists => (), // ignore EEXIST
                    _ => {
                        return TaskResult::Err(format!(
                            "ERROR: Failed to create output dir location {tgtdir:?} for filtered taskfile {filterpath:?}: {error}"
                        ));
                    }
                }
            }
        }
        if let Err(error) = fs::rename(&filterpath, &successpath) {
            return TaskResult::Err(format!("ERROR: Failed to rename filtered taskfile {filterpath:?} to output location {successpath:?}: {error}"));
        } else {
            println!("Filtered taskfile {filterpath:?} has been renamed to output location {successpath:?}");
        }
        // record completion time
        self.itask.times.completed = Some(Instant::now());
        // prepare our destructor to delete, instead of rename
        self.itask.cleanup = InnerTaskCleanup::Delete;
        TaskResult::Transformed(Task {
            itask: self.itask,
            status: PhantomData,
        })
    }
}

impl Task<TaskComplete> {
    pub fn check(mut self) -> TaskResult<Task<TaskComplete>, (String, Times)> {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
        let mut successpath = PathBuf::from(&config.output_success_subdir);
        successpath.push(&self.itask.taskfile);
        match fs::metadata(&successpath) {
            Err(error) => match error.kind() {
                ErrorKind::NotFound => println!(
                    // this is what we want, someone cleaned up the output link
                    "Noted completion of client for taskfile {successpath:?}"
                ),
                _ => {
                    return TaskResult::Err(format!(
                        "ERROR: Failed to retrieve metadata from taskfile {successpath:?}: {error}",
                    ))
                }
            },
            Ok(_) => return TaskResult::Unchanged(self),
        };
        // record cleanup time
        self.itask.times.cleaned = Some(Instant::now());
        TaskResult::Transformed((
            String::from(&self.itask.taskdef.name),
            self.itask.times.clone(),
        ))
    }
}

/// Internal helper function: Intended to verify that a given taskfile's regex-matched value falls
/// within the range which this program instance has been set to operate on.
fn value_within_restriction(
    taskfile: &Path,
    valuename: &str,
    restriction: &Option<Vec<u32>>,
    value: &Option<Match<'_>>,
) -> bool {
    if let Some(restriction) = restriction {
        let Some(value) = value else {
            eprintln!(
                "ERROR: Encountered task file {taskfile:?} \
                       does not contain a recognizable '{{{valuename}}}' value!"
            );
            return false;
        };
        let value: u32 = match value.as_str().parse() {
            Err(error) => {
                eprintln!(
                    "ERROR: Failed to parse '{{{valuename}}}' value from {taskfile:?}: {error}"
                );
                return false;
            }
            Ok(val) => val,
        };
        if !restriction.contains(&value) {
            return false; // ignore this file
        }
    }
    true
}

/// Internal helper function: Intended to format an output String by replacing '{braced-name}'
/// entries with replacement values present in the given HashMap
fn format_line(format: &str, values: &HashMap<String, String>) -> Result<String, String> {
    // check for valid key mappings
    for key in BRACED_NAME_REGEX.captures_iter(format) {
        if !values.contains_key(&key["name"]) {
            return Err(format!(
                "no value associated with {} could be found",
                &key["name"]
            ));
        }
    }
    Ok(String::from(
        BRACED_NAME_REGEX.replace_all(format, |captures: &regex::Captures| {
            values.get(&captures["name"]).unwrap()
        }),
    ))
}
