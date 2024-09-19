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

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#ifndef __MUSTANG_THREADING_H__
#define __MUSTANG_THREADING_H__

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <config/config.h>
#include <mdal/mdal.h>
#include "hashtable.h"
#include "task_queue.h"

extern size_t id_cache_capacity;

/**
 * Open the file at `path` using the context of the current MarFS position
 * `current_position` and the associated MDAL `current_mdal`. Then, query the 
 * string representation of the file's FTAG by getting the xattr with key 
 * "FTAG_NAME" and return the result (if valid and matching the expected 
 * length from an initial check) as a heap-allocated string. Close the file 
 * before returning.
 *
 * Returns: pointer to a valid FTAG string representation on success, or NULL 
 * on failure.
 *
 * NOTE: This function may return NULL and potentially set errno in any of the
 * following cases:
 * - The file open call using current_mdal->open() fails.
 * - The first extended attribute query using current_mdal->fgetxattr() fails
 *   (errno set to ENOATTR, which is reported as ENODATA if the system does not
 *   define ENOATTR).
 * - The calloc() call to get heap space for the FTAG string representation 
 *   fails (errno set to ENOMEM).
 * - The length of the string value returned in the second 
 *   current_mdal->fgetxattr() call does not match the expected length as 
 *   returned from the first current_mdal->fgetxattr() call (errno set to 
 *   ESTALE).
 */
char* get_ftag(marfs_position* current_position, MDAL current_mdal, char* path);

/**
 * Using a task's parameters, 'traverse' the file that the current setting 
 * of `task_position` corresponds to. Actually, a file is a 'leaf', and 
 * therefore not 'traversed'. Hence, the logic of this routine looks up
 * the object id(s) of the file, and places them in the output_table.
 *
 * NOTE: This function always returns, including on failure. Failures are 
 * always logged to the logfile passed as a program argument since all 
 * build settings at least log errors.
 */
void traverse_file(marfs_config* base_config, marfs_position* task_position, char* file_name, hashtable* output_table, pthread_mutex_t* table_lock, task_queue* pool_queue);

/**
 * Using a task's parameters, traverse the directory that the current setting 
 * of `task_position` corresponds to, reading all directory entries and acting
 * accordingly. If an entry corresponds to a regular file, get its FTAG and its
 * object ID(s), then store the ID(s). If an entry corresponds to a directory, 
 * create a new task for that directory bundled with appropriate state for a 
 * worker thread to complete.
 *
 * NOTE: This function always returns, including on failure. Failures are 
 * always logged to the logfile passed as a program argument since all 
 * build settings at least log errors.
 */
void traverse_dir(marfs_config* base_config, marfs_position* task_position, char* file_name, hashtable* output_table, pthread_mutex_t* table_lock, task_queue* pool_queue);

/**
 * Using a task's parameters, check the namespace that the current setting of
 * `task_position` corresponds to for subspaces. Create new tasks, bundled with
 * appropriate state, for any subspaces (i.e., nested namespaces) for worker 
 * threads to complete. Then, treat this namespace as a directory and check for
 * "standard" (i.e., regular file and directory) contents by calling 
 * traverse_dir().
 *
 * NOTE: Like traverse_dir(), this function always returns, including on 
 * failure. Failures are always logged to the relevant logfile since all build
 * settings for MUSTANG log errors.
 */
void traverse_ns(marfs_config* base_config, marfs_position* task_position, char* file_name, hashtable* output_table, pthread_mutex_t* table_lock, task_queue* pool_queue);

#endif
