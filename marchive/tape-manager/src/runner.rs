// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

/// tracking of operations targeting specific objects for conflict detection
mod objtable;
/// manipulation of taskfiles and tracking of associated procs
mod task;

use crate::{
    dfs::DFS,
    format::{duration_to_string, ProcessingPath, ProcessingPathElement},
    runner::task::TaskComplete,
    PROGRAM_CONFIG,
};
use objtable::ObjTable;
use std::{
    cell::RefCell,
    fmt, fs,
    io::ErrorKind,
    ops::Div,
    path::PathBuf,
    process,
    rc::Rc,
    time::{Duration, Instant},
};
use task::{Task, TaskResult};

/// Maximum number of tasks to track at one time
const TASKCOUNT: i32 = 4096;

/// Identifies task files, launches associated commands, tracks command status, and checks for client termination
pub struct Runner {
    started: Instant,
    status: RunnerStatus,
    times: RunnerTimes,
    objtable: Rc<RefCell<ObjTable>>,
    new_tasks: Vec<Task<task::TaskGrabbed>>,
    filtered_tasks: Vec<Task<task::TaskFiltered>>,
    running_tasks: Vec<Task<task::TaskRunning>>,
    resting_tasks: Vec<Task<TaskComplete>>,
}

impl Drop for Runner {
    /// cleanup processing path locations associated with this program instance + hostname
    fn drop(&mut self) {
        let ppath = ProcessingPath::new(
            Some(process::id()),
            None,
            ProcessingPathElement::IntermediateDir,
        );
        let cleanpath = PathBuf::from(&ppath);
        if let Err(error) = fs::remove_dir(&cleanpath) {
            if error.kind() != ErrorKind::NotFound {
                eprintln!("ERROR: Failed to cleanup processing dir {cleanpath:?}: {error}");
            }
        }
        let ppath = ProcessingPath::new(None, None, ProcessingPathElement::IntermediateDir);
        let cleanpath = PathBuf::from(&ppath);
        if let Err(error) = fs::remove_dir(&cleanpath) {
            if error.kind() != ErrorKind::NotFound {
                eprintln!("ERROR: Failed to cleanup processing dir {cleanpath:?}: {error}");
            }
        }
    }
}

impl fmt::Display for Runner {
    /// print details on overall status / operation times / running tasks / task times
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match &self.status {
            RunnerStatus::Idle(since) => {
                format!("Idle for {}", &duration_to_string(&since.elapsed()))
            }
            RunnerStatus::Working(since) => {
                format!("Working for {}", &duration_to_string(&since.elapsed()))
            }
            RunnerStatus::Busy(since) => {
                format!("Busy for {}", &duration_to_string(&since.elapsed()))
            }
            RunnerStatus::Overloaded(since) => {
                format!("Overloaded for {}", &duration_to_string(&since.elapsed()))
            }
        };
        f.pad("")?;
        write!(
            f,
            "Running for {}\n",
            &duration_to_string(&self.started.elapsed())
        )?;
        f.pad("")?;
        write!(f, "{status}\n")?;
        if self.has_active_tasks() {
            f.pad("")?;
            write!(f, "Active Tasks:\n")?;
            if !self.new_tasks.is_empty() {
                f.pad("")?;
                write!(f, "{:>4}{} New\n", "", self.new_tasks.len())?;
            }
            if !self.filtered_tasks.is_empty() {
                f.pad("")?;
                write!(f, "{:>4}{} Filtered\n", "", self.filtered_tasks.len())?;
            }
            if !self.running_tasks.is_empty() {
                f.pad("")?;
                write!(f, "{:>4}{} Running\n", "", self.running_tasks.len())?;
            }
            if !self.resting_tasks.is_empty() {
                f.pad("")?;
                write!(f, "{:>4}{} Resting\n", "", self.resting_tasks.len())?;
            }
        }
        if self.times.scan.0 != 0
            || self.times.filter.0 != 0
            || self.times.launch.0 != 0
            || self.times.poll.0 != 0
            || !self.times.tasks.is_empty()
        {
            f.pad("")?;
            write!(f, "Times:\n")?;
            f.pad("")?;
            write!(f, "{:>4}", &self.times)?;
        }
        Ok(())
    }
}

/// Overall status of the runner
pub enum RunnerStatus {
    /// No active tasks since this time
    Idle(Instant),
    /// Some active tasks since this time
    Working(Instant),
    /// Maximum active tasks since this time
    Busy(Instant),
    /// Maximum total tasks since this time
    Overloaded(Instant),
}

/// Tracker of time statistics for a Runner
struct RunnerTimes {
    scan: (u32, OpTimes),
    filter: (u32, OpTimes),
    launch: (u32, OpTimes),
    poll: (u32, OpTimes),
    check: (u32, OpTimes),
    tasks: Vec<(String, u32, TaskTimes)>,
}
impl Default for RunnerTimes {
    /// default to no task / op times yet recorded
    fn default() -> Self {
        RunnerTimes {
            scan: (0, Default::default()),
            filter: (0, Default::default()),
            launch: (0, Default::default()),
            poll: (0, Default::default()),
            check: (0, Default::default()),
            tasks: Vec::new(),
        }
    }
}
impl fmt::Display for RunnerTimes {
    /// print time info for any recorded operations
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let task_times = if self.tasks.is_empty() {
            "".to_string()
        } else {
            let mut task_times = "Completed Tasks:\n".to_string();
            for (name, count, times) in &self.tasks {
                task_times.push_str(&format!("        {count} \"{name}\" Tasks:\n{times:>12}\n"));
            }
            let _ = task_times.pop(); // throw out trailing newline
            task_times
        };
        if self.scan.0 != 0 {
            f.pad("")?;
            write!(f, "{} Scans: {}\n", &self.scan.0, &self.scan.1)?;
        }
        if self.filter.0 != 0 {
            f.pad("")?;
            write!(f, "{} Filters: {}\n", &self.filter.0, &self.filter.1)?;
        }
        if self.launch.0 != 0 {
            f.pad("")?;
            write!(f, "{} Launches: {}\n", &self.launch.0, &self.launch.1)?;
        }
        if self.poll.0 != 0 {
            f.pad("")?;
            write!(f, "{} Polls: {}\n", &self.poll.0, &self.poll.1)?;
        }
        if self.check.0 != 0 {
            f.pad("")?;
            write!(f, "{} Checks: {}\n", &self.check.0, &self.check.1)?;
        }
        f.pad("")?;
        write!(f, "{task_times}")
    }
}
impl RunnerTimes {
    /// record details from the lifetime of a completed task
    fn record_task(&mut self, task_name: String, newtimes: &task::Times) {
        let (_, count, times) = match self
            .tasks
            .iter()
            .position(|(name, _, _)| name == &task_name)
        {
            Some(index) => self.tasks.get_mut(index).unwrap(),
            None => {
                let index = self.tasks.len();
                self.tasks.push((task_name, 0, Default::default()));
                self.tasks.get_mut(index).unwrap()
            }
        };
        let mut countdup = *count;
        times.waiting.update(
            &mut countdup,
            newtimes
                .started
                .unwrap()
                .duration_since(newtimes.detected.unwrap()),
        );
        let mut countdup = *count;
        times.running.update(
            &mut countdup,
            newtimes
                .completed
                .unwrap()
                .duration_since(newtimes.started.unwrap()),
        );
        times.resting.update(
            count,
            newtimes
                .cleaned
                .unwrap()
                .duration_since(newtimes.completed.unwrap()),
        );
    }
}

/// Tracker of times we care about for Task variants
struct TaskTimes {
    /// time spent between detection and startup of proc(s)
    waiting: OpTimes,
    /// time spent between startup of proc(s) and completion of final proc
    running: OpTimes,
    /// time spent between completion of final proc and completion of client ( deletion of output file )
    resting: OpTimes,
}
impl Default for TaskTimes {
    fn default() -> Self {
        TaskTimes {
            waiting: Default::default(),
            running: Default::default(),
            resting: Default::default(),
        }
    }
}
impl fmt::Display for TaskTimes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("")?;
        write!(f, "Waiting: {}\n", &self.waiting)?;
        f.pad("")?;
        write!(f, "Running: {}\n", &self.running)?;
        f.pad("")?;
        write!(f, "Resting: {}", &self.resting)
    }
}

/// Time statistics for a specific operation
struct OpTimes {
    longest: Duration,
    shortest: Duration,
    average: Duration,
}
impl Default for OpTimes {
    fn default() -> Self {
        OpTimes {
            longest: Duration::ZERO,
            shortest: Duration::ZERO,
            average: Duration::ZERO,
        }
    }
}
impl fmt::Display for OpTimes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("")?;
        write!(
            f,
            "{{ average: {}, longest: {}, shortest: {} }}",
            duration_to_string(&self.average),
            duration_to_string(&self.longest),
            duration_to_string(&self.shortest),
        )
    }
}
impl OpTimes {
    /// update value based on a new duration for this op
    fn update(&mut self, op_count: &mut u32, op_duration: Duration) {
        *op_count = op_count.saturating_add(1);
        if op_duration > self.longest {
            self.longest = op_duration;
        }
        if self.shortest.is_zero() || op_duration < self.shortest {
            self.shortest = op_duration;
        }
        self.average = self
            .average
            .saturating_mul(*op_count - 1)
            .saturating_add(op_duration)
            .div(*op_count);
    }
}

impl Runner {
    /// Create a new Runner, associated with the given Config reference
    pub fn new() -> Runner {
        // create expected subdirs
        let ppath = ProcessingPath::new(
            Some(process::id()),
            None,
            ProcessingPathElement::IntermediateDir,
        );
        let dirpath = PathBuf::from(&ppath);
        if let Err(error) = fs::create_dir_all(&dirpath) {
            if error.kind() != ErrorKind::AlreadyExists {
                eprintln!("ERROR: Failed to create processing location {dirpath:?}: {error}");
            }
        }
        Runner {
            started: Instant::now(),
            status: RunnerStatus::Idle(Instant::now()),
            times: Default::default(),
            objtable: Rc::new(RefCell::new(ObjTable::new())),
            new_tasks: Vec::new(),
            filtered_tasks: Vec::new(),
            running_tasks: Vec::new(),
            resting_tasks: Vec::new(),
        }
    }

    /// Print runner status and reset state tracking
    pub fn report(&mut self) {
        println!("\n{self}"); // see Display impls
        self.times = Default::default();
    }

    /// Checks if the Runner is in an Overloaded state ( task count limit reached )
    pub fn is_overloaded(&self) -> bool {
        matches!(&self.status, RunnerStatus::Overloaded(_))
    }

    /// Checks if the Runner has any active tasks ( running or preparing to run )
    pub fn has_active_tasks(&self) -> bool {
        !self.new_tasks.is_empty()
            || !self.filtered_tasks.is_empty()
            || !self.running_tasks.is_empty()
            || !self.resting_tasks.is_empty()
    }

    /// Perform depth-first search of our input_subdir for new Tasks
    pub fn scan_for_inputs(&mut self) -> bool {
        let start = Instant::now();
        let result = self._internal_scan_for_inputs();
        self.times
            .scan
            .1
            .update(&mut self.times.scan.0, start.elapsed());
        result
    }
    // Actual logical implementation of the scan_for_inputs op
    //  Separating them in this manner just makes timing start -> end simpler
    fn _internal_scan_for_inputs(&mut self) -> bool {
        // calculate how many tasks we can safely track
        let mut availtasks: u32 = match TASKCOUNT
            - (self.new_tasks.len()
                + self.filtered_tasks.len()
                + self.running_tasks.len()
                + self.resting_tasks.len()) as i32
        {
            value @ ..0 => panic!(
                "FATAL ERROR: Number of tracked tasks exceeds task limit of {} by {}",
                TASKCOUNT,
                value.abs(),
            ),
            0 => return false,
            value @ 1.. => value.try_into().unwrap(),
        };
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
                                                    // begin a DFS of our input subdir
        let dfs = match DFS::new(&config.input_subdir) {
            Ok(d) => d,
            Err(error) => {
                eprintln!(
                    "ERROR: Failed to open taskfile input subdir \"{}\": {error}",
                    &config.input_subdir
                );
                return false;
            }
        };
        // perform depth first search, looking for files
        let mut foundfiles = false;
        for result in dfs.filter(|r| r.is_err() || r.as_ref().unwrap().1.is_file()) {
            let entry = match result {
                Err(e) => {
                    eprintln!("ERROR: Failure during input tree scanning: {e}");
                    continue;
                }
                Ok((e, _)) => e,
            };
            // attempt to construct associated Task
            let Some(task) = Task::new(entry.path(), Rc::clone(&self.objtable)) else {
                continue;
            };
            foundfiles = true;
            availtasks -= 1;
            if matches!(&self.status, RunnerStatus::Idle(_)) {
                self.status = RunnerStatus::Working(task.times().detected.unwrap());
            }
            if availtasks == 0 && !matches!(&self.status, RunnerStatus::Overloaded(_)) {
                self.status = RunnerStatus::Overloaded(task.times().detected.unwrap());
            }
            self.new_tasks.push(task);
            if availtasks == 0 {
                return foundfiles;
            }
        }
        foundfiles
    }

    /// Filter any newly identified Tasks, or those for which we are waiting on conflicts
    pub fn filter_tasks(&mut self) -> () {
        let mut new_tasks = Vec::new();
        for task in self.new_tasks.drain(..) {
            let start = Instant::now();
            let result = task.filter();
            self.times
                .filter
                .1
                .update(&mut self.times.filter.0, start.elapsed());
            match result {
                TaskResult::Unchanged(t) => new_tasks.push(t),
                TaskResult::Transformed(t) => self.filtered_tasks.push(t),
                TaskResult::Err(e) => eprintln!("{e}"),
            }
        }
        self.new_tasks = new_tasks;
    }

    /// Launch procs associated with any filtered Tasks
    pub fn launch_tasks(&mut self, proc_limit: &mut u32) -> () {
        let mut filtered_tasks = Vec::new();
        for task in self.filtered_tasks.drain(..) {
            let start = Instant::now();
            let result = task.launch(proc_limit);
            self.times
                .launch
                .1
                .update(&mut self.times.launch.0, start.elapsed());
            match result {
                TaskResult::Unchanged(t) => filtered_tasks.push(t),
                TaskResult::Transformed(t) => self.running_tasks.push(t),
                TaskResult::Err(e) => eprintln!("ERROR: Failed to launch task: {e}"),
            }
        }
        if *proc_limit == 0 {
            self.status = match self.status {
                RunnerStatus::Idle(_) | RunnerStatus::Working(_) => {
                    RunnerStatus::Busy(Instant::now())
                }
                RunnerStatus::Busy(t) => RunnerStatus::Busy(t),
                RunnerStatus::Overloaded(t) => RunnerStatus::Overloaded(t),
            }
        }
        self.filtered_tasks = filtered_tasks;
    }

    /// Check the status of all running Tasks
    pub fn poll_tasks(&mut self, proc_limit: &mut u32) -> () {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
        let mut running_tasks = Vec::new();
        for task in self.running_tasks.drain(..) {
            let start = Instant::now();
            let result = task.poll(proc_limit);
            self.times
                .poll
                .1
                .update(&mut self.times.poll.0, start.elapsed());
            match result {
                TaskResult::Unchanged(t) => running_tasks.push(t),
                TaskResult::Transformed(t) => self.resting_tasks.push(t),
                TaskResult::Err(e) => {
                    if *proc_limit == config.task_parallelism
                        && self.new_tasks.is_empty()
                        && self.filtered_tasks.is_empty()
                        && self.resting_tasks.is_empty()
                        && !matches!(&self.status, RunnerStatus::Idle(_))
                    {
                        self.status = RunnerStatus::Idle(Instant::now());
                    } else if matches!(
                        &self.status,
                        RunnerStatus::Busy(_) | RunnerStatus::Overloaded(_)
                    ) {
                        self.status = RunnerStatus::Working(Instant::now());
                    }
                    eprintln!("{e}");
                }
            }
        }
        self.running_tasks = running_tasks;
    }

    /// Check the status of all running Tasks and increment proc_limit as they complete
    pub fn check_tasks(&mut self) -> bool {
        let mut resting_tasks = Vec::new();
        let mut taskcleaned = None;
        for task in self.resting_tasks.drain(..) {
            let start = Instant::now();
            let result = task.check();
            self.times
                .check
                .1
                .update(&mut self.times.check.0, start.elapsed());
            match result {
                TaskResult::Unchanged(t) => resting_tasks.push(t),
                TaskResult::Transformed((taskname, times)) => {
                    self.times.record_task(taskname, &times);
                    taskcleaned = times.cleaned;
                }
                TaskResult::Err(e) => {
                    taskcleaned = Some(Instant::now());
                    eprintln!("{e}");
                }
            }
        }
        if let Some(cleaned) = taskcleaned {
            if self.new_tasks.is_empty()
                && self.filtered_tasks.is_empty()
                && self.running_tasks.is_empty()
                && resting_tasks.is_empty()
                && !matches!(&self.status, RunnerStatus::Idle(_))
            {
                self.status = RunnerStatus::Idle(cleaned);
            } else if matches!(
                &self.status,
                RunnerStatus::Busy(_) | RunnerStatus::Overloaded(_)
            ) {
                self.status = RunnerStatus::Working(cleaned);
            }
        }
        self.resting_tasks = resting_tasks;
        taskcleaned.is_some()
    }
}
