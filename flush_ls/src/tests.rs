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

#[cfg(test)]
mod tests {
    use crate::*;
    use tempfile::{TempDir, tempdir};

    fn setup_dirs() -> (TempDir, PathBuf) {
        // DAL root
        let root = tempdir().unwrap();

        // create intermediate directories
        let mut path = PathBuf::from(root.path().to_path_buf().to_owned());

        for path_seg in PATH_SEGS {
            let numbered = PathBuf::from(String::from(*path_seg) + "0");
            path = path.join(numbered);
            let _ = fs::create_dir(&path).unwrap();
        }

        // return root to prevent destructor call
        (root, path)
    }

    fn setup_file(path: &PathBuf, name: &str, atime: SystemTime) {
        // create pod/block/cap/scat/*
        let mut filename = path.to_owned();
        filename = filename.join(name);

        let utime = fs::FileTimes::new().set_accessed(atime);

        let file = fs::File::create(&filename).unwrap();
        file.set_times(utime).unwrap();
    }

    #[test]
    fn process_leaf_good() {
        let reftime = SystemTime::now();
        let (_root, path) = setup_dirs();

        // create 2 files
        setup_file(&path, "0", reftime - Duration::from_secs(1));
        setup_file(&path, "1", reftime - Duration::from_secs(24 * 60 * 60));

        // find files older than 2 seconds
        {
            let thresholds = Thresholds::from([(100, 2)]);
            assert_eq!(process_leaf(path.clone(), Arc::new(reftime), Arc::new(thresholds)), 1);
        }

        // find files older than 0 seconds
        {
            let thresholds = Thresholds::from([(100, 0)]);
            assert_eq!(process_leaf(path.clone(), Arc::new(reftime), Arc::new(thresholds)), 2);
        }
    }

    #[test]
    fn process_leaf_file() {
        let root = tempdir().unwrap();

        // process_leaf should take in a directory path, not a file path
        let mut path = PathBuf::from(root.path().to_path_buf().to_owned());
        path.push("file");
        let _ = fs::File::create(&path).unwrap();

        let thresholds = Thresholds::from([(100, 0)]);
        assert_eq!(process_leaf(path.clone(), Arc::new(SystemTime::now()), Arc::new(thresholds)), 0);
    }

    #[test]
    fn process_leaf_dir() {
        let root = tempdir().unwrap();

        // path should only have files under it
        let mut path = PathBuf::from(root.path().to_path_buf().to_owned());
        path.push("dir");
        let _ = fs::create_dir(&path).unwrap();
        path.pop();

        let thresholds = Thresholds::from([(100, 0)]);
        assert_eq!(process_leaf(path.clone(), Arc::new(SystemTime::now()), Arc::new(thresholds)), 0);
    }

    #[test]
    fn process_non_leaf_bad_dir() {
        let root = tempdir().unwrap();
        let (tx, _) = mpsc::channel();

        // the provided path does not exist
        let mut path = PathBuf::from(root.path().to_path_buf().to_owned());
        path.push("non-existant-path");

        let children = process_non_leaf(path, 0, Arc::new(SystemTime::now()), Arc::new(Thresholds::new()), tx);
        assert_eq!(children, 0);
    }

    #[test]
    fn process_non_leaf_bad_dir_name() {
        let root = tempdir().unwrap();
        let (tx, _) = mpsc::channel();
        let level = 0;

        // subdirectory does exist, but is not an expected path segment
        let mut path = PathBuf::from(root.path().to_path_buf().to_owned());
        path.push(String::from("a") + PATH_SEGS[level]);
        let _ = fs::create_dir(&path).unwrap();
        path.pop();

        let children = process_non_leaf(path, level, Arc::new(SystemTime::now()), Arc::new(Thresholds::new()), tx);
        assert_eq!(children, 0);
    }

    #[test]
    fn process_non_leaf_partial_match() {
        let root = tempdir().unwrap();
        let (tx, _) = mpsc::channel();
        let level = 0;

        // subdirectory prefix matches, but suffix is not an integer
        let mut path = PathBuf::from(root.path().to_path_buf().to_owned());
        path.push(String::from(PATH_SEGS[level]) + "a");
        let _ = fs::create_dir(&path).unwrap();
        path.pop();

        let children = process_non_leaf(path, level, Arc::new(SystemTime::now()), Arc::new(Thresholds::new()), tx);
        assert_eq!(children, 0);
    }

    #[test]
    fn process_non_leaf_file() {
        let root = tempdir().unwrap();
        let (tx, _) = mpsc::channel();

        // there should only be subdirectories, not files
        let mut path = PathBuf::from(root.path().to_path_buf().to_owned());
        path.push("file");
        let _ = fs::File::create(&path).unwrap();
        path.pop();

        let children = process_non_leaf(path, 0, Arc::new(SystemTime::now()), Arc::new(Thresholds::new()), tx);
        assert_eq!(children, 0);
    }

    #[test]
    fn print_flushable_in_dal_good() {
        let (root, _) = setup_dirs();
        let thresholds = parse_user_thresholds(&vec!["0,2".to_string()], ',');
        print_flushable_in_dal(&root.path().to_path_buf(), &SystemTime::UNIX_EPOCH, &thresholds);
    }

    #[test]
    fn user_threshold_good_single() {
        let args = vec![
            "1,1".to_string(),
        ];

        let thresholds = parse_user_thresholds(&args, ',');
        assert_eq!(thresholds.len(), 3);
        assert_eq!(thresholds.get(&1), Some(&1));
    }

    #[test]
    fn user_threshold_good_multiple() {
        let args = vec![
            "1,2".to_string(),
            "2,1".to_string(),
        ];

        let thresholds = parse_user_thresholds(&args, ',');
        assert_eq!(thresholds.len(), 4);
        assert_eq!(thresholds.get(&1), Some(&2));
        assert_eq!(thresholds.get(&2), Some(&1));
    }

    #[test]
    fn user_threshold_good_repeat() {
        let args = vec![
            "0,1".to_string(), // overwrite's default 0% utilization
        ];

        let thresholds = parse_user_thresholds(&args, ',');
        assert_eq!(thresholds.len(), 2);
        assert_eq!(thresholds.get(&0), Some(&1));
    }

    #[test]
    #[should_panic(expected="Error: Bad <utilization>,<age> string: ''")]
    fn user_threshold_empty() {
        parse_user_thresholds(&vec!["".to_string()], ',');
    }

    #[test]
    #[should_panic(expected="Error: Bad age string: '': cannot parse integer from empty string")]
    fn user_threshold_digit_empty() {
        parse_user_thresholds(&vec!["1,".to_string()], ',');
    }

    #[test]
    #[should_panic(expected="Error: Bad utilization string: '': cannot parse integer from empty string")]
    fn user_threshold_empty_digit() {
        parse_user_thresholds(&vec![",1".to_string()], ',');
    }

    #[test]
    #[should_panic(expected="Error: Bad utilization string: 'a': invalid digit found in string")]
    fn user_threshold_alpha_empty() {
        parse_user_thresholds(&vec!["a,".to_string()], ',');
    }

    #[test]
    #[should_panic(expected="Error: Bad utilization string: '': cannot parse integer from empty string")]
    fn user_threshold_empty_alpha() {
        parse_user_thresholds(&vec![",a".to_string()], ',');
    }

    #[test]
    #[should_panic(expected="Error: Utilization can be between 0% and 100%. Got '200'")]
    fn user_threshold_too_big() {
        parse_user_thresholds(&vec!["200,".to_string()], ',');
    }

    #[test]
    #[should_panic(expected="Error: File age must be strictly monotonically decreasing. Found 10,1 -> 20,1")]
    fn user_threshold_same_ages() {
        parse_user_thresholds(&vec!["10,1".to_string(), "20,1".to_string()], ',');
    }

    #[test]
    #[should_panic(expected="Error: File age must be strictly monotonically decreasing. Found 10,1 -> 20,2")]
    fn user_threshold_increasing_ages() {
        parse_user_thresholds(&vec!["10,1".to_string(), "20,2".to_string()], ',');
    }

    #[test]
    fn util2age_good() {
        // low  utilization -> flush older  files
        // high utilization -> flush recent files
        let args = vec![
            "10,90".to_string(),
            "20,80".to_string(),
            "30,70".to_string(),
            "40,60".to_string(),
            "50,50".to_string(),
            "60,40".to_string(),
            "70,30".to_string(),
            "80,20".to_string(),
            "90,10".to_string(),
        ];

        let thresholds = parse_user_thresholds(&args, ',');
        assert_eq!(thresholds.len(), 11);

        assert_eq!(util2age(&thresholds, 05), 90);
        assert_eq!(util2age(&thresholds, 15), 80);
        assert_eq!(util2age(&thresholds, 25), 70);
        assert_eq!(util2age(&thresholds, 35), 60);
        assert_eq!(util2age(&thresholds, 45), 50);
        assert_eq!(util2age(&thresholds, 55), 40);
        assert_eq!(util2age(&thresholds, 65), 30);
        assert_eq!(util2age(&thresholds, 75), 20);
        assert_eq!(util2age(&thresholds, 85), 10);
        assert_eq!(util2age(&thresholds, 95), 00);
    }

    #[test]
    fn util2age_empty() {
        let thresholds = parse_user_thresholds(&vec![], ',');
        assert_eq!(thresholds.len(), 2);
        util2age(&thresholds, 0);
    }

    #[test]
    #[should_panic(expected = "Error: Utilization percentage not found")]
    fn util2age_gt_100() {
        let thresholds = parse_user_thresholds(&vec![], ',');
        assert_eq!(thresholds.len(), 2);
        util2age(&thresholds, 200);
    }
}
