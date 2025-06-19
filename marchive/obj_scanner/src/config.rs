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


use regex::Regex;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, SystemTimeError};

/**
 * This struct represents the contents of a config file.
 *
 * The config file contains key-value pairs separated by the ':' character.
 * Key-value pairs may appear in any order.
 * Duplicate key-value pairs may appear. How they are handled depends on the key.
 */
#[derive(Clone)]
pub struct Config {
    // Reference Timestamp (Seconds Since Unix Epoch)
    reftime: SystemTime,

    // Mapping from system utilization to allowed file age
    // using BTreeMap to remove duplicates and sort on keys
    thresholds: BTreeMap<u8, u64>,

    // Regular expressions for basenames to ignore
    blacklist: Vec<Regex>,
}

impl Config {
    const LABEL_DELIM:     char = ':';
    const THRESHOLD_DELIM: char = ',';

    fn new() -> Config {
        Self {
            reftime: SystemTime::UNIX_EPOCH,
            thresholds: BTreeMap::from([
                (0, u64::MAX),  // if utilization is at 0%, don't flush anything
                (100, 0),       // if utilization is at 100%, flush everything
            ]),
            blacklist: Vec::new(),
        }
    }

    pub fn from_pathbuf(path: PathBuf) -> Config {
        let mut config = Config::new();

        let file = match File::open(&path) {
            Ok(f)    => f,
            Err(msg) => panic!("Error: Could not open flush config file {}: {}",
                                 path.display(), msg),
        };

        // parse file
        // labels can appear in any order
        // processing function determines if the new value is overwrites the old value or is aggregated
        for line_res in BufReader::new(file).lines() {
            let line = match line_res {
                Ok(line) => line,
                Err(msg) => panic!("Error: Could not read line from {}: {}",
                                     path.display(), msg),
            };

            config.process_line(&line);
        }

        config.verify_thresholds();

        config
    }

    fn process_line(&mut self, line: &str) {
        if line.len() == 0 {
            return;
        }

        if line.chars().nth(0) == Some('#') {
            return;
        }

        match line.split_once(Self::LABEL_DELIM) {
            Some((label, value)) => {
                match label.trim() {
                    "reftime"   => self.set_reftime_str(value),
                    "threshold" => self.add_to_threshold(value),
                    "blacklist" => self.add_to_blacklist(value),
                    _           => panic!("Error: Unknown config label: {}", label),
                };

            },
            None => panic!("Error: Did not find separator in line: {}", line),
        };
    }

    pub fn set_reftime(&mut self, reftime: u64) {
        self.reftime = SystemTime::UNIX_EPOCH + Duration::from_secs(reftime);
    }

    fn set_reftime_str(&mut self, time_str: &str) {
        match time_str.trim().parse::<u64>() {
            Ok(reftime) => self.set_reftime(reftime),
            Err(msg)    => panic!("Error: Could not convert {} into a timestamp: {}",
                                  time_str, msg),
        };
    }

    /**
     * Convert threshold strings from the config file to integers and
     * insert them into a map.
     *
     * Format:
     *     threshold: <utilization>,<age>
     *
     * Utilization is an integer representing utilization percentage.
     * An integer is required because rust does not have an Ord trait
     * defined for f32 and f64. https://stackoverflow.com/a/69117941/341683
     *
     * Age is integer number of seconds since Jan 1, 1970 00:00:00 UTC.
     *
     * Examples:
     *
     *     threshold: 10,60
     *     threshold:20, 1
     */
    fn add_to_threshold(&mut self, pair_str: &str) {
        match pair_str.trim().split_once(Self::THRESHOLD_DELIM) {
            Some((util_str, age_str)) => {
                let util = match util_str.trim().parse::<u8>() {
                    Ok(val)  => val,
                    Err(msg) => panic!("Error: Bad utilization string: '{}': {}", util_str, msg),
                };

                if util > 100 {
                    panic!("Error: Utilization can be between 0% and 100%. Got '{}'", util);
                }

                let age = match age_str.trim().parse::<u64>() {
                    Ok(val)  => val,
                    Err(msg) => panic!("Error: Bad age string: '{}': {}", age_str, msg),
                };

                self.thresholds.insert(util, age);
            },
            None => panic!("Error: Bad <utilization>,<age> string: '{}'", pair_str),
        }
    }

    pub fn add_to_blacklist(&mut self, regex: &str) {
        match Regex::new(regex.trim()) {
            Ok(re)   => self.blacklist.push(re),
            Err(msg) => panic!("Error: Bad regex pattern: {}: {}", regex, msg),
        };
    }

    /**
     * Check for monotonically decreasing file ages.
     *
     * Call this function after the entire config file has been
     * processed
     */
    fn verify_thresholds(&self) {
        let mut prev = self.thresholds.first_key_value();
        for (utilization, age) in self.thresholds.iter().skip(1) {
            if age >= prev.unwrap().1 {
                panic!("Error: File age must be strictly monotonically decreasing. Found {},{} -> {},{}",
                       prev.unwrap().0, prev.unwrap().1, utilization, age);
            }

            prev = Some((&utilization, &age))
        }
    }

    // this will error if the input is later than the reftime
    // (duration is negative) - propogate the error to let the
    // caller handle it
    pub fn file_age(&self, timestamp: SystemTime) -> Result<Duration, SystemTimeError> {
        self.reftime.duration_since(timestamp)
    }

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
     * @param utilization  system utilization
     * @return file age limit in seconds
     */
    pub fn util2age(&self, utilization: u8) -> u64 {
        // return the age associated with the first threshold
        // that is greater than or equal to the utilization
        for (threshold, age) in self.thresholds.iter() {
            if *threshold >= utilization {
                return *age;
            }
        }

        // this line does double duty as a compiler silencer
        // and as an invalid utilization value check
        panic!("Error: Utilization percentage not found");
    }

    pub fn is_blacklisted(&self, file_name: &str) -> bool {
        for regex in &self.blacklist {
            if regex.is_match(file_name) {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use tempfile::NamedTempFile;

    #[test]
    #[should_panic(expected = "Error: Could not convert a into a timestamp: invalid digit found in string")]
    fn set_reftime_str_bad() {
        let mut config = Config::new();
        config.set_reftime_str("a");
    }

    #[test]
    fn set_reftime() {
        let mut config = Config::new();
        config.set_reftime(0);

        assert_eq!(config.reftime, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn newer_reftime() {
        let mut config = Config::new();
        config.set_reftime(0);

        assert_eq!(config.file_age(SystemTime::now()).is_err(), true);
    }

    #[test]
    fn user_threshold_good_single() {
        let mut config = Config::new();
        config.add_to_threshold("1,1");

        assert_eq!(config.thresholds.len(), 3);
        assert_eq!(config.thresholds.get(&1), Some(&1));
    }

    #[test]
    fn user_threshold_good_multiple() {
        let mut config = Config::new();
        config.add_to_threshold("1,2");
        config.add_to_threshold("2,1");

        assert_eq!(config.thresholds.len(), 4);
        assert_eq!(config.thresholds.get(&1), Some(&2));
        assert_eq!(config.thresholds.get(&2), Some(&1));
    }

    #[test]
    fn user_threshold_good_repeat() {
        let mut config = Config::new();
        config.add_to_threshold("0,1");

        assert_eq!(config.thresholds.len(), 2);
        assert_eq!(config.thresholds.get(&0), Some(&1));
    }

    #[test]
    #[should_panic(expected="Error: Bad <utilization>,<age> string: ''")]
    fn user_threshold_empty() {
        let mut config = Config::new();
        config.add_to_threshold("");
    }

    #[test]
    #[should_panic(expected="Error: Bad age string: '': cannot parse integer from empty string")]
    fn user_threshold_digit_empty() {
        let mut config = Config::new();
        config.add_to_threshold("1,");
    }

    #[test]
    #[should_panic(expected="Error: Bad utilization string: '': cannot parse integer from empty string")]
    fn user_threshold_empty_digit() {
        let mut config = Config::new();
        config.add_to_threshold(",1");
    }

    #[test]
    #[should_panic(expected="Error: Bad utilization string: 'a': invalid digit found in string")]
    fn user_threshold_alpha_empty() {
        let mut config = Config::new();
        config.add_to_threshold("a,");
    }

    #[test]
    #[should_panic(expected="Error: Bad utilization string: '': cannot parse integer from empty string")]
    fn user_threshold_empty_alpha() {
        let mut config = Config::new();
        config.add_to_threshold(",a");
    }

    #[test]
    #[should_panic(expected="Error: Utilization can be between 0% and 100%. Got '200'")]
    fn user_threshold_too_big() {
        let mut config = Config::new();
        config.add_to_threshold("200,");
    }

    #[test]
    #[should_panic(expected="Error: File age must be strictly monotonically decreasing. Found 10,1 -> 20,1")]
    fn user_threshold_same_ages() {
        let mut config = Config::new();
        config.add_to_threshold("10,1");
        config.add_to_threshold("20,1");
        config.verify_thresholds();
    }

    #[test]
    #[should_panic(expected="Error: File age must be strictly monotonically decreasing. Found 10,1 -> 20,2")]
    fn user_threshold_increasing_ages() {
        let mut config = Config::new();
        config.add_to_threshold("10,1");
        config.add_to_threshold("20,2");
        config.verify_thresholds();
    }

    #[test]
    #[should_panic(expected = "Error: Utilization percentage not found")]
    fn util2age_gt_100() {
        let config = Config::new();
        let _ = config.util2age(200);
    }

    #[test]
    #[should_panic(expected = "Error: Bad regex pattern: \\: regex parse error:")]
    fn bad_regex() {
        let mut config = Config::new();
        config.add_to_blacklist("\\");
    }

    #[test]
    fn no_blacklist() {
        let config = Config::new();

        assert_eq!(config.is_blacklisted(""),         false);
        assert_eq!(config.is_blacklisted("test"),     false);
        assert_eq!(config.is_blacklisted("match"),    false);
        assert_eq!(config.is_blacklisted("matching"), false);
    }

    #[test]
    fn blacklist() {
        let mut config = Config::new();
        config.add_to_blacklist("^match.+$");

        assert_eq!(config.is_blacklisted(""),         false);
        assert_eq!(config.is_blacklisted("test"),     false);
        assert_eq!(config.is_blacklisted("match"),    false);
        assert_eq!(config.is_blacklisted("matching"), true);
    }

    #[test]
    #[should_panic(expected = "Error: Did not find separator in line: bad label")]
    fn missing_separator() {
        let mut config = Config::new();
        config.process_line("bad label");
    }

    #[test]
    #[should_panic(expected = "Error: Unknown config label: bad label")]
    fn bad_label() {
        let mut config = Config::new();
        config.process_line("bad label: ");
    }

    #[test]
    #[should_panic(expected = "Error: Could not open flush config file")]
    fn nonexistant_config() {
        let file = NamedTempFile::new().unwrap();
        let path = PathBuf::from(file.path());
        let _ = file.close();

        let _ = Config::from_pathbuf(path);
    }

    #[test]
    fn empty_config() {
        let config = Config::new();

        assert_eq!(config.thresholds.len(), 2);
        assert_eq!(config.blacklist.len(),  0);
    }

    #[test]
    fn example_config() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("example.config");

        let config = Config::from_pathbuf(path);

        assert_eq!(config.reftime, SystemTime::UNIX_EPOCH);

        assert_eq!(config.thresholds.len(), 11);

        // get raw values
        assert_eq!(config.thresholds.get(&000), Some(&u64::MAX));
        assert_eq!(config.thresholds.get(&010), Some(&90));
        assert_eq!(config.thresholds.get(&020), Some(&80));
        assert_eq!(config.thresholds.get(&030), Some(&70));
        assert_eq!(config.thresholds.get(&040), Some(&60));
        assert_eq!(config.thresholds.get(&050), Some(&50));
        assert_eq!(config.thresholds.get(&060), Some(&40));
        assert_eq!(config.thresholds.get(&070), Some(&30));
        assert_eq!(config.thresholds.get(&080), Some(&20));
        assert_eq!(config.thresholds.get(&090), Some(&10));
        assert_eq!(config.thresholds.get(&100), Some(&00));

        // get file age given utilization
        assert_eq!(config.util2age(00), u64::MAX);
        assert_eq!(config.util2age(05), 90);
        assert_eq!(config.util2age(15), 80);
        assert_eq!(config.util2age(25), 70);
        assert_eq!(config.util2age(35), 60);
        assert_eq!(config.util2age(45), 50);
        assert_eq!(config.util2age(55), 40);
        assert_eq!(config.util2age(65), 30);
        assert_eq!(config.util2age(75), 20);
        assert_eq!(config.util2age(85), 10);
        assert_eq!(config.util2age(95), 00);

        assert_eq!(config.blacklist.len(), 1);
    }
}
