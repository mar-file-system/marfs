// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

use serde::Deserialize;
use std::fs;

/// Representation of the raw config TOML structure
/// Used for initial parsing, before translation to a more generalized Config
#[derive(Debug, Deserialize)]
pub struct ParsedConfig {
    pub options: ConfigOptions,
    pub hosts: Vec<ConfigHost>,
    pub tasks: Vec<ConfigTask>,
}

/// [options] subsection values
#[derive(Debug, Deserialize)]
pub struct ConfigOptions {
    pub status_frequency: toml::value::Datetime,
    pub cleanup_frequency: toml::value::Datetime,
    pub scan_frequency: toml::value::Datetime,
    pub filter_frequency: toml::value::Datetime,
    pub poll_frequency: toml::value::Datetime,
    pub check_frequency: toml::value::Datetime,
    pub task_parallelism: u32,
    pub hard_timeout: toml::value::Datetime,
    pub cleanup_timeout: toml::value::Datetime,
    pub root: String,
    pub input_subdir: String,
    pub processing_subdir: String,
    pub output_success_subdir: String,
    pub output_failure_subdir: String,
    pub logged_failure_subdir: String,
    pub task_file_path: String,
    pub task_file_content: String,
}

/// [hosts] subsection(s) values
#[derive(Debug, Deserialize)]
pub struct ConfigHost {
    pub name: String,
    pub pods: Option<Vec<u32>>,
    pub blocks: Option<Vec<u32>>,
    pub caps: Option<Vec<u32>>,
    pub scatters: Option<Vec<u32>>,
}

/// [tasks] subsection(s) values
#[derive(Debug, Deserialize)]
pub struct ConfigTask {
    pub name: String,
    pub command: String,
    pub file_format: String,
    pub overrides: Option<Vec<String>>,
    pub conflicts: Option<Vec<String>>,
    pub timeout: Option<toml::value::Datetime>,
}

impl ParsedConfig {
    /// Initialize a new ParsedConfig structure, based on the content of the given TOML file.
    pub fn new(filepath: &str) -> ParsedConfig {
        let filecontent = match fs::read_to_string(filepath) {
            Ok(content) => content,
            Err(error) => {
                panic!("Failed to read file at specified path: \"{filepath}\"\n{error}");
            }
        };
        let config: ParsedConfig = match toml::from_str(&filecontent) {
            Ok(config) => config,
            Err(error) => {
                panic!("Failed to parse config file \"{filepath}\":\n{error}");
            }
        };
        // catch invalid task defs and warn of some extraneous values
        let mut invalid = false;
        for task in &config.tasks {
            if let Some(timeout) = task.timeout {
                if let Some(date) = timeout.date {
                    eprintln!( "WARNING: Task \"{}\" contained extraneous 'date' information ({date}) in timeout value", task.name );
                }
                if let Some(offset) = timeout.offset {
                    eprintln!( "WARNING: Task \"{}\" contained extraneous 'offset' information ({offset}) in timeout value", task.name );
                }
            }
            // check for duplicate task names
            if config.tasks.iter().filter(|t| t.name == task.name).count() > 1 {
                eprintln!(
                    "ERROR: Multiple tasks share the same \"{}\" name",
                    task.name
                );
                invalid = true;
            }
            // check for 'overrides' / 'conflicts' specifying non-existent tasks
            for tgtlist in [&task.overrides, &task.conflicts] {
                let Some(tgts) = tgtlist else { continue };
                for tgt in tgts {
                    if let None = config.tasks.iter().find(|t| t.name == *tgt) {
                        eprintln!(
                            "ERROR: Task \"{}\" specifies \"{tgt}\" as a target, but no task has that name!",
                            task.name
                        );
                        invalid = true;
                    }
                }
            }
        }
        if invalid {
            panic!("Config file \"{filepath}\" is invalid");
        }
        config
    }
}
