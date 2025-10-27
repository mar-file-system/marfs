// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

mod parsing;

use crate::format::{duration_from_string, BRACED_NAME_REGEX, ESCAPED_BRACED_NAME_REGEX};
use regex::{self, Regex};
use std::{env, sync::Arc, time::Duration};

pub const NUMERIC_VALUES: [&str; 4] = ["pod", "block", "cap", "scatter"];
pub const REQUIRED_PATH_VALUES: [&str; 1] = ["task"];
const REQUIRED_PATH_VALUE_COUNT: usize = REQUIRED_PATH_VALUES.len();

/// Representation of the program's TOML config file
///
/// WARNING: Be aware that Config::new() performs a env::set_current_dir() to the root path
///          specified in the config file.  This is for convenience of subpath interaction
///          throughout the program.
///          However, this does mean we can not support multiple Config instances in a sensible
///          manner.
#[derive(Debug)]
pub struct Config {
    // Options
    pub status_frequency: Duration,
    pub cleanup_frequency: Duration,
    pub scan_frequency: Duration,
    pub filter_frequency: Duration,
    pub poll_frequency: Duration,
    pub check_frequency: Duration,
    pub task_parallelism: u32,
    pub hard_timeout: Duration,
    pub cleanup_timeout: Duration,
    // 'root' omitted, since we chdir at construction
    pub input_subdir: String,
    pub processing_subdir: String,
    pub output_success_subdir: String,
    pub output_failure_subdir: String,
    pub logged_failure_subdir: String,
    // Task path / content parsing info
    pub task_path_values: Vec<String>,
    pub task_path_regex: Regex,
    pub task_content_values: Vec<String>,
    pub task_content_regex: Regex,
    // Host-specific values
    pub host: String,
    pub pods: Option<Vec<u32>>,
    pub blocks: Option<Vec<u32>>,
    pub caps: Option<Vec<u32>>,
    pub scatters: Option<Vec<u32>>,
    // Tasks
    pub tasks: Vec<Arc<ConfigTask>>,
}
/// Per-Task config values
#[derive(Debug)]
pub struct ConfigTask {
    pub name: String,
    pub command: String,
    pub command_values: Vec<String>,
    pub file_format: String,
    pub file_format_values: Vec<String>,
    pub overrides: Vec<String>,
    pub conflicts: Vec<String>,
    pub timeout: Option<Duration>,
}

impl Config {
    /// Initialize a new Config structure, based on the content of the given TOML file and hostname string
    pub fn new(filepath: &str, hostname: String) -> Config {
        Config::from((parsing::ParsedConfig::new(filepath), hostname))
    }
}

impl From<(parsing::ParsedConfig, String)> for Config {
    /// Translates a (ParsedConfig, Hostname: String) tuple to a Config struct
    fn from(confhost: (parsing::ParsedConfig, String)) -> Self {
        let (pconfig, hostname) = confhost;
        // only incorporate info for the host we're running on
        let Some(hostdef) = pconfig.hosts.iter().find(|host| host.name == hostname) else {
            panic!("Failed to identify config host definition for this node: \"{hostname}\"");
        };
        // translate task file path string into a regex we can use to capture values
        // from file paths
        let mut panic = false; // flag to panic, but try to report all cases first
        let reformat = &ESCAPED_BRACED_NAME_REGEX;
        let task_path_regex = format!("^{}$", regex::escape(&pconfig.options.task_file_path));
        let mut path_keys: Vec<String> = Vec::new(); // track encountered key names
        let mut requiredvals: [bool; REQUIRED_PATH_VALUE_COUNT] = [false];
        for key in reformat.captures_iter(&task_path_regex) {
            match &key["name"] {
                "file" => {
                    eprintln!("ERROR: Job file path specifies reserved 'file' value!");
                    panic = true;
                }
                name if REQUIRED_PATH_VALUES.contains(&name) => {
                    requiredvals[REQUIRED_PATH_VALUES
                        .iter()
                        .position(|n| n == &name)
                        .unwrap()] = true;
                }
                "_" => continue, // ignore special '_' name
                name => {
                    if path_keys.iter().any(|value| value == name) {
                        eprintln!(
                            "ERROR: Job file path contains duplicate \"{name}\" \
                                  value"
                        );
                        panic = true;
                    }
                    path_keys.push(String::from(&key["name"]));
                }
            }
        }
        let task_path_regex = reformat.replace_all(
            &task_path_regex,
            |captures: &regex::Captures| match &captures["name"] {
                name if NUMERIC_VALUES.contains(&name) => {
                    format!(r"(?<{}>\d+)", name)
                }
                "_" => format!(r"[^/\s]+"),
                name => format!(r"(?<{}>[^/\s]+)", name),
            },
        );
        let task_path_regex = Regex::new(&task_path_regex).unwrap();
        // ...same for our task file content string
        let task_content_regex = format!("^{}$", regex::escape(&pconfig.options.task_file_content));
        let mut content_keys: Vec<String> = Vec::new(); // track encountered key names
        for key in reformat.captures_iter(&task_content_regex) {
            match &key["name"] {
                "file" => panic!("Job file content specifies reserved 'file' value!"),
                "_" => continue, // ignore special '_' name
                name => {
                    if path_keys.iter().any(|value| value == name) {
                        panic!("Job file content contains duplicate \"{name}\" value");
                    }
                    content_keys.push(String::from(&key["name"]));
                }
            }
        }
        let task_content_regex = reformat.replace_all(
            &task_content_regex,
            |captures: &regex::Captures| match &captures["name"] {
                name if NUMERIC_VALUES.contains(&name) => {
                    format!(r"(?<{}>\d+)", name)
                }
                "_" => format!(r"[^/\s]+"),
                name => format!(r"(?<{}>[^/\s]+)", name),
            },
        );
        let task_content_regex = Regex::new(&task_content_regex).unwrap();
        // our task file path MUST provide enough info for this instance to identify
        // which files our program instance is responsible for and which task types
        // they correspond to
        for (position, _) in requiredvals.iter().enumerate().filter(|(_, have)| !*have) {
            eprintln!(
                "ERROR: No '{{{}}}' value could be identified in task file paths.",
                REQUIRED_PATH_VALUES[position]
            );
            panic = true;
        }
        if hostdef.pods.is_some() && !path_keys.iter().any(|key| key == "pod") {
            eprintln!(
                "ERROR: This instance is restriced to certain 'pod' values, \
                        but no such value is provided in task file paths."
            );
            panic = true;
        }
        if hostdef.blocks.is_some() && !path_keys.iter().any(|key| key == "block") {
            eprintln!(
                "ERROR: This instance is restriced to certain 'block' values, \
                        but no such value is provided in task file paths."
            );
            panic = true;
        }
        if hostdef.caps.is_some() && !path_keys.iter().any(|key| key == "cap") {
            eprintln!(
                "ERROR: This instance is restriced to certain 'cap' values, \
                        but no such value is provided in task file paths."
            );
            panic = true;
        }
        if hostdef.scatters.is_some() && !path_keys.iter().any(|key| key == "scatter") {
            eprintln!(
                "ERROR: This instance is restriced to certain 'scatter' values, \
                        but no such value is provided in task file paths."
            );
            panic = true;
        }
        if panic {
            panic!("Cannot safely identify task files to operate on");
        }
        // construct Vec of ConfigTasks
        let revalue = &BRACED_NAME_REGEX;
        let mut tasks: Vec<Arc<ConfigTask>> = Vec::new();
        for task in &pconfig.tasks {
            // first, verify that the task file format only requests keys we expect
            let mut havefile = false;
            let mut command_values: Vec<String> = Vec::new();
            for key in revalue.captures_iter(&task.command) {
                if &key["name"] == "file" {
                    havefile = true;
                    continue; // skip special 'file' key
                }
                command_values.push(String::from(&key["name"]));
                if !path_keys.iter().any(|value| value == &key["name"]) {
                    if content_keys.iter().any(|value| value == &key["name"]) {
                        eprintln!(
                            "WARNING: Task \"{}\" requires \"{}\" value \
                                    which is only provided in file content.  \
                                    This will result in task workload fragmentation.",
                            task.name, &key["name"]
                        );
                    } else {
                        eprintln!(
                            "ERROR: Task \"{}\" requires \"{}\" value \
                                    which could not be identified!",
                            task.name, &key["name"]
                        );
                        panic = true;
                    }
                }
            }
            if !havefile {
                eprintln!(
                    "ERROR: Task \"{}\" does not reference '{{file}}' value \
                    in command string",
                    task.name
                );
                panic = true;
            }
            let mut file_format_values: Vec<String> = Vec::new();
            for key in revalue.captures_iter(&task.file_format) {
                file_format_values.push(String::from(&key["name"]));
                if !content_keys.iter().any(|value| value == &key["name"])
                    && !path_keys.iter().any(|value| value == &key["name"])
                {
                    eprintln!(
                        "ERROR: Task \"{}\" requires \"{}\" value \
                                    which could not be identified!",
                        task.name, &key["name"]
                    );
                    panic = true;
                }
            }
            // when instantiating, translate task conflict / override names to indicies
            tasks.push(Arc::new(ConfigTask {
                name: task.name.clone(),
                command: task.command.clone(),
                command_values: command_values,
                file_format: task.file_format.clone(),
                file_format_values: file_format_values,
                overrides: match &task.overrides {
                    None => Vec::new(),
                    Some(list) => list.iter().map(|s| String::from(s)).collect(),
                },
                conflicts: match &task.conflicts {
                    None => Vec::new(),
                    Some(list) => list.iter().map(|s| String::from(s)).collect(),
                },
                timeout: match &task.timeout {
                    None => None,
                    Some(timeout) => match duration_from_string(timeout) {
                        Ok(d) => Some(d),
                        Err(e) => {
                            eprintln!(
                                "ERROR: Failed to parse timout value for \"{}\" task: {e}",
                                task.name
                            );
                            panic = true;
                            None
                        }
                    },
                },
            }));
        }
        if panic {
            panic!("Some task definitions were invalid");
        }
        // attempt to chdir to the Config.root
        // NOTE using CWD means this interface can't support multiple configs
        // non-issue for now
        if let Err(error) = env::set_current_dir(&pconfig.options.root) {
            panic!(
                "Failed to chdir into Config Root Path \"{}\": {error}",
                &pconfig.options.root
            );
        }
        // finally, instantiate our config struct
        Config {
            status_frequency: duration_from_string(&pconfig.options.status_frequency)
                .unwrap_or_else(|e| panic!("failed to parse status frequency value: {e}")),
            cleanup_frequency: duration_from_string(&pconfig.options.cleanup_frequency)
                .unwrap_or_else(|e| panic!("failed to parse cleanup frequency value: {e}")),
            scan_frequency: duration_from_string(&pconfig.options.scan_frequency)
                .unwrap_or_else(|e| panic!("failed to parse scan frequency value: {e}")),
            filter_frequency: duration_from_string(&pconfig.options.filter_frequency)
                .unwrap_or_else(|e| panic!("failed to parse filter frequency value: {e}")),
            poll_frequency: duration_from_string(&pconfig.options.poll_frequency)
                .unwrap_or_else(|e| panic!("failed to parse poll frequency value: {e}")),
            check_frequency: duration_from_string(&pconfig.options.check_frequency)
                .unwrap_or_else(|e| panic!("failed to parse check frequency value: {e}")),
            task_parallelism: pconfig.options.task_parallelism,
            hard_timeout: duration_from_string(&pconfig.options.hard_timeout)
                .unwrap_or_else(|e| panic!("failed to parse hard timeout value: {e}")),
            cleanup_timeout: duration_from_string(&pconfig.options.cleanup_timeout)
                .unwrap_or_else(|e| panic!("failed to parse cleanup timeout value: {e}")),
            input_subdir: pconfig.options.input_subdir,
            processing_subdir: pconfig.options.processing_subdir,
            output_success_subdir: pconfig.options.output_success_subdir,
            output_failure_subdir: pconfig.options.output_failure_subdir,
            logged_failure_subdir: pconfig.options.logged_failure_subdir,
            task_path_values: path_keys,
            task_path_regex: task_path_regex,
            task_content_values: content_keys,
            task_content_regex: task_content_regex,
            host: hostname,
            pods: hostdef.pods.clone(),
            blocks: hostdef.blocks.clone(),
            caps: hostdef.caps.clone(),
            scatters: hostdef.caps.clone(),
            tasks: tasks,
        }
    }
}
