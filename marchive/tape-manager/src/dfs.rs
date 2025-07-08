// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

use std::{collections::VecDeque, convert::AsRef, fs, io, path};

/// Iterator over entries / types encountered during a FS tree depth-first search
pub struct DFS {
    readdirs: VecDeque<fs::ReadDir>,
    buffer: VecDeque<io::Result<(fs::DirEntry, fs::FileType)>>,
}

impl DFS {
    /// Constructor for a DFS based on a given search root path
    /// An Err() result may be produced from the initial fs::read_dir()
    pub fn new<P: AsRef<path::Path> + ?Sized>(path: &P) -> io::Result<Self> {
        let inputdir = fs::read_dir(path)?;
        Ok(DFS {
            readdirs: VecDeque::from([inputdir]),
            buffer: VecDeque::new(),
        })
    }

    /// Limits the DFS to traversing only below the most recently encountered dir
    /// NOTE: No effect after calling next_back() even once
    /// Example:
    ///     let dfs = DFS::new("./root-path").unwrap();
    ///     dfs.next().unwrap(); // "./root-path/subdir"
    ///     dfs.limit_search(); // all further entries will be limited to those
    ///                         // within "./root-path/subdir"
    ///                         // Entries such as "./root-path/file" or
    ///                         // "./root-path/otherdir/path" would be
    ///                         // omitted
    pub fn focus_search(&mut self) {
        let onlyreaddir = match self.readdirs.pop_back() {
            None => return,
            Some(r) => r,
        };
        self.readdirs = VecDeque::from([onlyreaddir]);
    }

    /// Prevents the DFS from traversing any further within the most recently encountered dir
    /// NOTE: No effect after calling next_back() even once
    /// Example:
    ///     let dfs = DFS::new("./root-path").unwrap();
    ///     dfs.next().unwrap(); // "./root-path/subdir"
    ///     dfs.omit_search();  // all further entries within "./root-path/subdir"
    ///                         // will be omitted
    ///                         // Entries such as "./root-path/file" or
    ///                         // "./root-path/otherdir/path" would remain
    pub fn omit_search(&mut self) {
        let _ = self.readdirs.pop_back();
    }
}

impl Iterator for DFS {
    type Item = io::Result<(fs::DirEntry, fs::FileType)>;

    /// Iterates through results of the depth-first search
    /// An Err() result does not necessarily indicate the end of the search,
    /// just that the search will not continue with / descend into the problem
    /// directory.
    fn next(&mut self) -> Option<io::Result<(fs::DirEntry, fs::FileType)>> {
        // check for buffered entries
        if let Some(value) = self.buffer.pop_front() {
            return Some(value);
        }
        // otherwise actually perform readdirs
        while let Some(mut reader) = self.readdirs.pop_back() {
            if let Some(entry) = reader.next() {
                let entry = match entry {
                    Err(e) => return Some(Err(e)), // abandon reading this dir
                    Ok(e) => e,
                };
                self.readdirs.push_back(reader); // continue with current reader later
                let etype = match entry.file_type() {
                    Err(e) => return Some(Err(e)),
                    Ok(t) => t,
                };
                if etype.is_dir() {
                    let subreader = match fs::read_dir(&entry.path()) {
                        Ok(reader) => reader,
                        // NOTE error here unfortunately means we omit this entry,
                        // despite having full details
                        Err(e) => return Some(Err(e)),
                    };
                    self.readdirs.push_back(subreader); // new search target
                }
                return Some(Ok((entry, etype)));
            }
        }
        None
    }
}

/// Here for convenience, but is an incredibly memory-inefficient approach.
/// Huge FS trees may get your proc OOM-killed.
/// Also has side effects, such as preventing use of omit/focus_search() once called.
impl DoubleEndedIterator for DFS {
    fn next_back(&mut self) -> std::option::Option<<Self as Iterator>::Item> {
        // just buffer literally everything
        if !self.readdirs.is_empty() {
            assert!(self.buffer.is_empty());
            self.buffer = self.collect();
        }
        self.buffer.pop_back()
    }
}
