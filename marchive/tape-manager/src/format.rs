// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

use crate::PROGRAM_CONFIG;
use chrono::{DateTime, Local};
use regex::Regex;
use std::{
    convert::TryFrom,
    error::Error,
    path::{self, PathBuf},
    sync::LazyLock,
    time::{Duration, SystemTime},
};

/// Static regex for parsing of time duration strings
static DURATION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^((?<days>\d+)[dD])?((?<hours>\d+)[hH])?((?<minutes>\d+)[mM])?((?<seconds>\d+)(\.(?<fractional_seconds>\d+))?[sS]?)?$").unwrap()
});
/// Static regex for replacement of '{...}' strings containing some name value
pub static BRACED_NAME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{(?<name>\w+)\}").unwrap());
/// Static regex for replacement of '{...}' strings containing some name value
///   Should only be used following regex::escape() of the containing string
pub static ESCAPED_BRACED_NAME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\\\{(?<name>\w+)\\\}").unwrap());

/// Format a duration reference as a human-readable String
pub fn duration_to_string(duration: &Duration) -> String {
    let totalsecs = duration.as_secs();
    if totalsecs == 0 {
        return format!("{}ms", duration.as_millis());
    }
    let days = totalsecs / (60 * 60 * 24);
    let mut sec = totalsecs % (60 * 60 * 24);
    let hrs = sec / (60 * 60);
    sec %= 60 * 60;
    let min = sec / 60;
    sec %= 60;
    match (days, hrs, min, sec) {
        (0, 0, 0, s) => format!("{s}s"),
        (0, 0, m, s) => format!("{m}m:{s:0>2}s"),
        (0, h, m, s) => format!("{h}h:{m:0>2}m:{s:0>2}s"),
        (d, h, m, s) => format!("{d}d:{h:0>2}h:{m:0>2}m:{s:0>2}s"),
    }
}

/// Parse a human-readable &str into a Duration type
pub fn duration_from_string(string: &str) -> Result<Duration, Box<dyn Error>> {
    let captures = DURATION_REGEX
        .captures(string)
        .ok_or(regex::Error::Syntax(String::from(
            "invalid duration format",
        )))?;
    let mut sec = 0;
    if let Some(days) = captures.name("days") {
        sec += days.as_str().parse::<u64>()? * 60 * 60 * 24;
    }
    if let Some(hours) = captures.name("hours") {
        sec += hours.as_str().parse::<u64>()? * 60 * 60;
    }
    if let Some(minutes) = captures.name("minutes") {
        sec += minutes.as_str().parse::<u64>()? * 60;
    }
    if let Some(seconds) = captures.name("seconds") {
        sec += seconds.as_str().parse::<u64>()?;
    }
    let nanos = match captures.name("fractional_seconds") {
        Some(fs) => {
            let mut fs = fs.as_str();
            let diff: i32 = 9 - fs.chars().count() as i32;
            if diff < 0 {
                fs = &fs[..fs.char_indices().nth(9).unwrap().0];
            }
            fs.parse::<u32>()?
                * if diff.is_positive() {
                    10_u32.pow(diff as u32)
                } else {
                    1
                }
        }
        None => 0,
    };
    Ok(Duration::new(sec, nanos))
}

/// Logical representation of a location ( file / dir ) within the processing subtree.
///   Always represents a path specific to the 'host' value in PROGRAM_CONFIG
///     ( a program instance should never touch a processing subpath associated with another host ).
///   'process' is expected to be the pid ( process::id() ) of the utility instance
///   'timestamp' is a value unique to a Task instance
///   'element' specifies the exact target file / dir
///             this field is public, for convenience of reusing an instance to generate multiple PathBufs
///             if both 'process' + 'timestamp' are specified, any variant of this field is valid
///             if either are omitted, this is expected to be PorcessingPathElement::IntermediateDir
pub struct ProcessingPath<'path> {
    process: Option<u32>,
    timestamp: Option<SystemTime>,
    pub element: ProcessingPathElement<'path>,
}

pub enum ProcessingPathElement<'path> {
    IntermediateDir,
    OriginalTaskfile(&'path path::Path),
    FilteredTaskfile,
    SubTaskDir(usize),
    SubTaskInput(usize),
    SubTaskOutput(usize),
}

impl<'p> ProcessingPath<'p> {
    /// construct a new ProcessingPath
    /// If either process or timestamp is None, element must be a ProcessingPathElement::IntermediateDir variant
    /// If process is None, timestamp must be as well ( all Task-specific paths must be associated with a process instance )
    /// Violation of either of the above contraints results in a panic
    pub fn new(
        process: Option<u32>,
        timestamp: Option<SystemTime>,
        element: ProcessingPathElement<'p>,
    ) -> Self {
        match (&process, &timestamp, &element) {
            (None,None,ProcessingPathElement::IntermediateDir) => (),
            (Some(_),None,ProcessingPathElement::IntermediateDir) => (),
            (Some(_),Some(_),_) => (),
            (None,Some(_),_) => panic!(
                "cannot specify 'timestamp' while omitting 'process' number"
            ),
            (_,None,_) => panic!(
                "cannot specify a non-itermediate dir element without also specifying 'process' + 'timestamp'"
            ),
        };
        ProcessingPath {
            process,
            timestamp,
            element,
        }
    }

    /// 'getter' for process value
    pub fn process(&self) -> Option<u32> {
        self.process
    }

    /// 'getter' for timestamp value
    pub fn timestamp(&self) -> Option<SystemTime> {
        self.timestamp
    }
}

impl<'p> TryFrom<&'p path::Path> for ProcessingPath<'p> {
    type Error = String;

    /// attempt to parse a given Path into a ProcessingPath representation
    fn try_from(path: &'p path::Path) -> Result<ProcessingPath<'p>, String> {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
                                                    // strip processing subdir prefix
        let path = match path.strip_prefix(&config.processing_subdir) {
            Err(_) => return Err("not within processing subdir".into()),
            Ok(s) => s,
        };
        // strip host prefix
        let path = match path.strip_prefix(&config.host) {
            Err(_) => return Err("not within per-host subdir".into()),
            Ok(s) => s,
        };
        // stip+parse PID prefix
        let Some(path::Component::Normal(piddir)) = path.components().next() else {
            return Ok(ProcessingPath {
                process: None,
                timestamp: None,
                element: ProcessingPathElement::IntermediateDir,
            });
        };
        let Some(piddir) = piddir.to_str() else {
            return Err("invalid UTF-8 encoding".into());
        };
        let Some(pidstr) = piddir.strip_prefix("PID") else {
            return Err("'PID' subdir prefix not found".into());
        };
        let process = Some(match pidstr.parse() {
            Ok(pid) => pid,
            Err(e) => return Err(format!("process ID could not be parsed: {e}")),
        });
        let path = path.strip_prefix(piddir).unwrap();
        // strip+parse timestamp prefix
        let Some(path::Component::Normal(timestampstr)) = path.components().next() else {
            return Ok(ProcessingPath {
                process,
                timestamp: None,
                element: ProcessingPathElement::IntermediateDir,
            });
        };
        let Some(timestampstr) = timestampstr.to_str() else {
            return Err("invalid UTF-8 encoding".into());
        };
        let Ok(timestamp) = timestampstr.parse::<DateTime<Local>>() else {
            return Err(format!(
                "failed to parse timestamp value \"{timestampstr}\""
            ));
        };
        let timestamp = Some(SystemTime::from(timestamp));
        let path = path.strip_prefix(timestampstr).unwrap();
        // identify element type
        if path.starts_with("original") {
            Ok(ProcessingPath {
                process,
                timestamp,
                element: ProcessingPathElement::OriginalTaskfile(
                    path.strip_prefix("original").unwrap(),
                ),
            })
        } else if path.starts_with("filtered") && path.components().count() == 1 {
            Ok(ProcessingPath {
                process,
                timestamp,
                element: ProcessingPathElement::FilteredTaskfile,
            })
        } else {
            let mut pathcomps = path.components();
            let Some(path::Component::Normal(comp)) = pathcomps.next() else {
                return Ok(ProcessingPath {
                    process,
                    timestamp,
                    element: ProcessingPathElement::IntermediateDir,
                });
            };
            let Some(comp) = comp.to_str() else {
                return Err("invalid UTF-8 encoding".into());
            };
            let Some(comp) = comp.strip_prefix("subtask.") else {
                return Err(
                    format!("unrecognized path element in place of 'original'/'filtered'/'subtask.*': {comp}")
                );
            };
            let Ok(stnum) = comp.parse() else {
                return Err(format!("unparsable subtask number: {comp}"));
            };
            match pathcomps.next() {
                None => Ok(ProcessingPath {
                    process,
                    timestamp,
                    element: ProcessingPathElement::SubTaskDir(stnum),
                }),
                Some(comp) => {
                    if let Some(_) = pathcomps.next() {
                        return Err(format!(
                            "unexpectedly deep subtask subpath: \"{}\"",
                            path.to_str().unwrap()
                        ));
                    }
                    let Some(comp) = comp.as_os_str().to_str() else {
                        return Err("invalid UTF-8 encoding".into());
                    };
                    match comp {
                        "input" => Ok(ProcessingPath {
                            process,
                            timestamp,
                            element: ProcessingPathElement::SubTaskInput(stnum),
                        }),
                        "output" => Ok(ProcessingPath {
                            process,
                            timestamp,
                            element: ProcessingPathElement::SubTaskOutput(stnum),
                        }),
                        _ => return Err(format!("unrecognized subtask element: \"{comp}\"")),
                    }
                }
            }
        }
    }
}

impl<'p> From<&ProcessingPath<'p>> for path::PathBuf {
    /// Produce a PathBuf representation of a Processing Path
    /// Panics if the ProcessingPath is in a state unrepresentable by ProcessingPath::new()
    fn from(ppath: &ProcessingPath<'p>) -> path::PathBuf {
        let config = PROGRAM_CONFIG.get().unwrap(); // convenience ref
        match (&ppath.process, &ppath.timestamp, &ppath.element) {
            (None,None,ProcessingPathElement::IntermediateDir) => path::PathBuf::from(&format!("{}/{}", &config.processing_subdir, &config.host)),
            (Some(pid),None,ProcessingPathElement::IntermediateDir) => path::PathBuf::from(
                &format!("{}/{}/PID{pid}", &config.processing_subdir, &config.host)
            ),
            (Some(pid),Some(time),_) => {
                let mut pbuf = path::PathBuf::from(
                    &format!(
                        "{}/{}/PID{pid}/{:?}",
                        &config.processing_subdir,
                        &config.host,
                        DateTime::<Local>::from(*time)
                    )
                );
                match ppath.element {
                    ProcessingPathElement::IntermediateDir => (),
                    ProcessingPathElement::OriginalTaskfile(path) => { pbuf.push("original"); pbuf.push(path); }
                    ProcessingPathElement::FilteredTaskfile => pbuf.push("filtered"),
                    ProcessingPathElement::SubTaskDir(num) => pbuf.push(&format!("subtask.{num}")),
                    ProcessingPathElement::SubTaskInput(num) => pbuf.push(&format!("subtask.{num}/input")),
                    ProcessingPathElement::SubTaskOutput(num) => pbuf.push(&format!("subtask.{num}/output")),
                };
                pbuf
            }
            (None,Some(_),_) => panic!(
                "ProcessingPath specifies 'timestamp' while omitting 'process' number"
            ),
            (_,None,_) => panic!(
                "ProcessingPath specifies a non-itermediate dir element without also specifying 'process' + 'timestamp'"
            ),
        }
    }
}

impl<'p> From<&ProcessingPath<'p>> for String {
    /// Convenience wrapper around PathBuf::from::<&ProcessingPath>(), to produce a String instead
    /// Panics if the ProcessingPath references any non-UTF-8 text
    fn from(ppath: &ProcessingPath<'p>) -> String {
        // just the dumbest possible implementation
        PathBuf::from(ppath).into_os_string().into_string().unwrap()
    }
}
