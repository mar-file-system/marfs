// Copyright 2015. Triad National Security, LLC. All rights reserved.
//
// Full details and licensing terms can be found in the License file in the main development branch
// of the repository.
//
// MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.

use crate::config::ConfigTask;
use std::{collections::HashMap, fmt, sync::Arc, time::SystemTime};

/// Tracks active operations and associated object targets
/// Upon addition of a new operation, identifies any conflicts / overrides
pub struct ObjTable {
    map: HashMap<Object, Vec<(Arc<ConfigTask>, Vec<SystemTime>)>>,
}

/// Representation of an object target
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Object {
    pub name: String,
    pub location: ObjLocation,
}
impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&format!(
            "Obj{{ \"{}\"@{{Pod{},Block{},Cap{},Scat{}}} }}",
            &self.name,
            &self.location.pod,
            &self.location.block,
            &self.location.cap,
            &self.location.scatter
        ))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjLocation {
    pub pod: u32,
    pub block: u32,
    pub cap: u32,
    pub scatter: u32,
}

/// Error return from attempted operation insert()
///   Conflict -- A conflicting Task is running against this object
///               Something has gone wrong.  The requesting Task should abort.
///   Skip -- An overriding Task is running against this object
///           This operation should be omitted by the requesting Task.
///   Wait -- A Task is running against this object which the requesting operation
///           would normally conflict with and/or override.
///           The requesting Task must wait until completion of the problem Task.
pub enum LookupError {
    Conflict(String),
    Skip,
    Wait,
}

/// Drop implementation purely for sanity checking at shutdown, as it *should* be impossible for the map to hold entries at that time
impl Drop for ObjTable {
    fn drop(&mut self) {
        if !self.map.is_empty() {
            eprintln!("FATAL ERROR: ObjectTable contains active entries at time of DROP!\n                Full dump follows:");
            for (obj, tasklist) in self.map.iter() {
                eprintln!("   {obj:?}  ->  {tasklist:?}");
            }
        }
    }
}

impl ObjTable {
    /// Construct a new ObjTable instance
    pub fn new() -> Self {
        ObjTable {
            map: HashMap::new(),
        }
    }

    /// Insert a new object operation associated with a Task timestamp + ConfigTask definition, checking for conflicts / overrides
    pub fn insert(
        &mut self,
        object: Object,
        task_def: &Arc<ConfigTask>,
        task_timestamp: SystemTime,
    ) -> Result<(), LookupError> {
        let objstr = format!("{object}"); // for error+debug reporting, after we give up ownership
                                          // look up / create object entry
        let tasklist = self.map.entry(object).or_insert(Vec::new());
        // find any conflicts / overrides / pre-existing matches
        let mut lookupres: Result<(), LookupError> = Ok(());
        let mut matchstamps: Option<&mut Vec<SystemTime>> = None;
        for (elemtask, timestamps) in tasklist.iter_mut() {
            // identify any matching op
            if elemtask.name == task_def.name {
                matchstamps = Some(timestamps);
            }
            // if the new task would override / conflict with a running task, we must wait
            if task_def.overrides.contains(&elemtask.name)
                || task_def.conflicts.contains(&elemtask.name)
            {
                lookupres = match lookupres {
                    Ok(()) => Err(LookupError::Wait), // only return 'wait' if nothing else has happened
                    oldres => oldres,
                }
            }
            // if the new task is overriden by a running task, we should skip it
            if elemtask.overrides.contains(&task_def.name) {
                lookupres = match lookupres {
                    Ok(()) | Err(LookupError::Wait) => Err(LookupError::Skip), // don't override a Conflict
                    oldres => oldres,
                }
            }
            // if the new task conflicts with a running task, we must abort
            if elemtask.conflicts.contains(&task_def.name) {
                lookupres = Err(LookupError::Conflict(format!(
                    "conflicting {} task on {objstr}",
                    &elemtask.name
                )));
            }
        }
        if let Err(_) = lookupres {
            // break early if we've hit some error
            return lookupres;
        }
        #[cfg(debug_assertions)]
        println!("ADDING {} of {objstr}", &task_def.name);
        if let Some(timestamps) = matchstamps {
            // found a match
            timestamps.push(task_timestamp);
        } else {
            // object entry, but no matching task
            tasklist.push((Arc::clone(task_def), vec![task_timestamp]));
        }
        lookupres
    }

    /// Remove all entries associated with a given Task, identified by ConfigTask name + timestamp value
    pub fn remove(&mut self, task_def: &Arc<ConfigTask>, task_timestamp: SystemTime) {
        // identify which HashMap entries to keep
        #[allow(unused_variables)] // for 'object' debug printing
        self.map.retain(|object, tasklist| {
            // identify which Task-type entries to keep, associated with an object
            tasklist.retain_mut(|(ref tdef, tstamps)| {
                if tdef.name != task_def.name {
                    return true;
                } // keep all non-matching op types
                  // identify which specific Task timestamp values to keep, associated with an op type
                tstamps.retain(|time| {
                    // keep only non-matching timestamp values
                    #[cfg(debug_assertions)]
                    if time == &task_timestamp {
                        println!("REMOVING {} of {object}", &task_def.name);
                    }
                    time != &task_timestamp
                });
                !tstamps.is_empty()
            });
            !tasklist.is_empty()
        });
    }
}
