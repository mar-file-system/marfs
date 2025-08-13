/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

use crate::*;
use std::mem;
use tempfile::{TempDir, tempdir};

fn setup_dal() -> (TempDir, PathBuf) {
    // DAL root
    let root = tempdir().unwrap();

    // create intermediate directories
    let mut leaf = PathBuf::from(root.path().to_path_buf().to_owned());

    for path_seg in PATH_SEGS {
        leaf.push(String::from(*path_seg) + "0");
        let _ = fs::create_dir(&leaf);
    }

    // return root to prevent destructor call
    (root, leaf)
}

fn setup_file(path: &PathBuf, name: &str, mtime: u64) {
    // create pod/block/cap/scat/*
    let mut filename = path.clone();
    filename = filename.join(name);

    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(mtime);
    let utime = fs::FileTimes::new().set_modified(timestamp);

    let file = fs::File::create(&filename).unwrap();
    let _ = file.set_times(utime);
}

fn setup_config(flush: bool, push: bool, force: bool) -> FlushPush {
    let mut config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    config_path.push("example.config");

    let mut fp = FlushPush {
        config: config::Config::from_pathbuf(config_path),
        ops: Ops {
            flush: flush,
            push: push,
        },
        must_match: Some(Regex::new(".*").unwrap()),
        force: force,
    };

    fp.config.set_reftime(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
    fp.config.add_to_blacklist(PUSHDB_REGEX);

    fp
}

fn setup_channels() -> (Output, (mpsc::Receiver<PathBuf>, mpsc::Receiver<PathBuf>)) {
    let (flush_tx, flush_rx) = mpsc::channel::<PathBuf>();
    let (push_tx,  push_rx)  = mpsc::channel::<PathBuf>();

    (Output {
        flush: flush_tx,
        push: push_tx,
    }, (flush_rx, push_rx))
}

fn get_u64(path: &PathBuf, sql: &str) -> Result<u64, rusqlite::Error> {
    let mut dbname = path.clone();
    dbname.push(PUSHDB_NAME);

    let conn = Connection::open_with_flags(&dbname,
                                           OpenFlags::SQLITE_OPEN_CREATE |
                                           OpenFlags::SQLITE_OPEN_READ_WRITE)?;

    conn.query_row::<u64, _, _>(sql, [], |row| row.get(0))
}

#[test]
fn process_leaf_correctness() {
    let (_root, leaf) = setup_dal();
    let (tx, _rx) = setup_channels();

    // push on empty leaf
    {
        let fp = Arc::new(setup_config(false, true, false));

        // empty leaf
        {
            let mut count = 0;

            for _ in fs::read_dir(&leaf).unwrap() {
                count += 1;
            }

            assert_eq!(count, 0);
        }

        // getting prev mtime fails because this is an empty leaf
        process_leaf(leaf.clone(), fp.clone(), tx.clone());
        assert!(!get_u64(&leaf, "SELECT oldest FROM mtime;").is_ok(), "Should not have gotten an mtime");

        // PUSHDB has been created
        {
            let mut count = 0;

            for _ in fs::read_dir(&leaf).unwrap() {
                count += 1;
            }

            assert_eq!(count, 1);
        }
    }

    // add a file to establish an mtime
    static FILENAME: &str = "old_file";
    static FIRST_MTIME: u64 = 1000;
    setup_file(&leaf, FILENAME, FIRST_MTIME);

    // push leaf
    {
        let fp = Arc::new(setup_config(false, true, false));

        // getting prev mtime fails (again) because the previous run did not set it
        process_leaf(leaf.clone(), fp.clone(), tx.clone());

        // mtime will be set this time
        assert_eq!(get_u64(&leaf, "SELECT oldest FROM mtime;").unwrap(), FIRST_MTIME);

        // file will be pushed because it was not selected for flush
        assert_eq!(get_u64(&leaf, "SELECT COUNT(*) FROM push;").unwrap(), 1);

        // getting prev mtime should succeed
        process_leaf(leaf.clone(), fp.clone(), tx.clone());

        // force push file that has already been pushed
        let fp = Arc::new(setup_config(false, true, true));
        process_leaf(leaf.clone(), fp.clone(), tx.clone());
        assert_eq!(get_u64(&leaf, "SELECT COUNT(*) FROM push;").unwrap(), 1);
    }

    // //////////////////////////////////////////////////

    // update mtime or else process_leaf() will
    // think this file has already been flushed
    static SECOND_MTIME: u64 = 1000000;
    setup_file(&leaf, FILENAME, SECOND_MTIME);

    // push leaf
    {
        let fp = Arc::new(setup_config(false, true, false));

        // will update row in PUSH db
        process_leaf(leaf.clone(), fp.clone(), tx.clone());

        // prev oldest mtime changed
        assert_eq!(get_u64(&leaf, "SELECT oldest FROM mtime;").unwrap(), SECOND_MTIME);

        // still only 1 file in table
        assert_eq!(get_u64(&leaf, "SELECT COUNT(*) FROM push;").unwrap(), 1);
    }

    // flush leaf
    {
        let fp = Arc::new(setup_config(true, false, false));

        // will delete row from PUSH db
        process_leaf(leaf.clone(), fp.clone(), tx.clone());

        // prev oldest mtime not changed
        assert_eq!(get_u64(&leaf, "SELECT oldest FROM mtime;").unwrap(), SECOND_MTIME);

        // file is no longer scheduled for flushing (because it has been flushed)
        assert_eq!(get_u64(&leaf, "SELECT COUNT(*) FROM push;").unwrap(), 0);
    }
}

#[test]
fn process_non_leaf_empty() {
    let (root, leaf) = setup_dal();
    let fp = setup_config(false, false, false);
    let (tx, _rx) = setup_channels();

    let dal_root = PathBuf::from(root.path().to_path_buf().to_owned());
    print_flushable_in_dal(&dal_root, fp.clone(), tx.clone(), 1);

    let mut bad_root = leaf.clone();
    bad_root.push("non-existant");
    print_flushable_in_dal(&bad_root, fp.clone(), tx.clone(), 1);
}

#[test]
fn readonly_pushdb() {
    let dir = tempdir().unwrap();
    let mut path = PathBuf::from(dir.path().to_path_buf().to_owned());
    path.push("ro");

    let _ = fs::OpenOptions::new().read(true).write(false).create(true).open(&path);

    assert!(!open_pushdb(&path).is_ok(), "open_pushdb should not have succeeded");
}

#[test]
fn write_output_files() {
    let dir = tempdir().unwrap();
    let path = PathBuf::from(dir.path().to_path_buf().to_owned());

    static FLUSH: &str = "flush";
    static PUSH:  &str = "push";

    let ops = Ops {
        flush: true,
        push:  true,
    };

    let flush_contents = FLUSH.to_string();
    let (flush_tx, flush_rx) = mpsc::channel::<PathBuf>();
    let _ = flush_tx.send(PathBuf::from(&flush_contents));
    mem::drop(flush_tx);

    let push_contents = PUSH.to_string();
    let (push_tx,  push_rx)  = mpsc::channel::<PathBuf>();
    let _ = push_tx.send(PathBuf::from(&push_contents));
    mem::drop(push_tx);

    write_outputs(path.clone(), ops, flush_rx, push_rx, 1);

    {
        let mut filename = path.clone();
        filename.push(FLUSH);
        let contents = fs::read_to_string(filename).unwrap();
        assert_eq!(contents, flush_contents + "\n");
    }

    {
        let mut filename = path.clone();
        filename.push(PUSH);
        let contents = fs::read_to_string(filename).unwrap();
        assert_eq!(contents, push_contents + "\n");
    }
}
