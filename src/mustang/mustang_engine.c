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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <limits.h>
#include <time.h>
#include <sys/time.h>
#include <api/marfs.h>
#include <config/config.h>
#include <datastream/datastream.h>

#include "mustang_logging.h"

#ifdef DEBUG_MUSTANG
#define DEBUG DEBUG_MUSTANG
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif

#define LOG_PREFIX "mustang_engine"
#include <logging/logging.h>

#include "hashtable.h"
#include "mustang_threading.h"
#include "task_queue.h"

// Maximum hashtable capacity: 2^24
#define HC_MAX (1L << 24)

extern void* thread_launcher(void* args);

size_t id_cache_capacity;

/**
 * This private routine encapulates the logic used to generete the thread
 * tasks from path arguments given on the command line. While the logic itself
 * could be part of the body of main(), having a separate routine helps
 * focus on the creating of tasks.
 * @param the_arg : the commandline argument to process
 * @param marfs_position* start_position : the Start Position of directory in MarFS file system.
 *                                         Used to create the Task Position
 * @param hashtable* output_table : Hash Table holding Object Ids of scanned files - 
 *                                  passed to the tasks
 * @param pthread_mutex_t* hashtable_lock : Lock to use when accessing output_table - 
 *                                          passed to the tasks
 * @param task_queue* queue : Queue of available threads - passed to the tasks. 
 */
void push_pathargs2queue(char *the_arg, marfs_config* config, marfs_position* start_position, hashtable* output_table, pthread_mutex_t* hashtable_lock, task_queue* queue) {
    struct stat arg_statbuf;
    int statcode = stat(the_arg, &arg_statbuf);
    // Need to hold a "mutable" path for config_traverse() to modify as needed
    char* next_basepath;
    // Also need to hold a mutable file. This is just the filename - no parent/path
    char* next_file = NULL;

    // If stat itself failed, nothing can be assumed about the path argument, so skip
    if (statcode) {
        LOG(LOG_ERR, "Failed to stat path arg \"%s\" (%s)--skipping to next\n", the_arg, strerror(errno));
        return;
    }

    // Safe check here---MarFS will forward S_IFDIR and for the only valid task
    // "targets" (directories, namespaces, and files alike)
    if ((arg_statbuf.st_mode & S_IFMT) == S_IFDIR) {
        next_basepath = strdup(the_arg);
    } else if ((arg_statbuf.st_mode & S_IFMT) == S_IFREG) {
        char *d, *f, *junkp = strdup(the_arg);
	char *s = strrchr(junkp,'/');

	if (s == (junkp + strlen(junkp))) {
            LOG(LOG_WARNING, "Path arg \"%s\" is a file, with a trailing '/'. Will not be processed --skipping to next\n", the_arg);
	    return;
        } else
            *s = '\0';
	// extracting the base filename, since MarFS positions are based on directories
	d = junkp;
        f = s+1;
        next_basepath = strdup(d);
        next_file = strdup(f);

        free(junkp);
	LOG(LOG_DEBUG, "Path arg \"%s\" is a file. (Dir: \"%s\", File: \"%s\")\n", the_arg, next_basepath, next_file);
    } else {
        LOG(LOG_WARNING, "Path arg \"%s\" does not target a file, directory or namespace--skipping to next\n", the_arg);
        return;
    }

    marfs_position* new_task_position = calloc(1, sizeof(marfs_position));

    if (config_duplicateposition(start_position, new_task_position)) {
        LOG(LOG_ERR, "Failed to duplicate parent position to new task!\n");
	free(next_basepath);
	if(next_file) free(next_file);
        free(new_task_position);
        return;
    }

    int new_task_depth = config_traverse(config, new_task_position, &next_basepath, 0);

    if (new_task_depth < 0) {
        LOG(LOG_ERR, "Failed to traverse (got depth: %d)\n", new_task_depth);
        free(next_basepath);
	if(next_file) free(next_file);
        config_abandonposition(new_task_position);
        free(new_task_position);
        return;
    }

    if (config_fortifyposition(new_task_position)) {
        LOG(LOG_ERR, "Failed to fortify new_task position after new_task traverse!\n");
        free(next_basepath);
	if(next_file) free(next_file);
        config_abandonposition(new_task_position);
        free(new_task_position);
        return;
    }

    // If new depth > 0 (guaranteed by previous logic), the target is a 
    // directory, so "place" new task within directory.
    if (new_task_depth != 0) {
        MDAL task_mdal = new_task_position->ns->prepo->metascheme.mdal;
        MDAL_DHANDLE task_dirhandle = task_mdal->opendir(new_task_position->ctxt, next_basepath);

        if (task_dirhandle == NULL) {
            LOG(LOG_ERR, "Failed to open target directory \"%s\" (%s)\n", next_basepath, strerror(errno));
            free(next_basepath);
	    if(next_file) free(next_file);
            config_abandonposition(new_task_position);
            free(new_task_position);
            return;
        }

        // NOTE: mdal->chdir() calls destroy their second argument---hence
        // why callers of directory tasks (traverse_dir()) have to reopen
        // the same directory to be able to call readdir().
        if (task_mdal->chdir(new_task_position->ctxt, task_dirhandle)) {
            LOG(LOG_ERR, "Failed to chdir into target directory \"%s\" (%s)\n", next_basepath, strerror(errno));
            free(next_basepath);
	    if(next_file) free(next_file);
            config_abandonposition(new_task_position);
            free(new_task_position);
            return;
        }
    }

    // Tell the new task where it is (namespace or not) by recording its
    // depth in its state.
    new_task_position->depth = new_task_depth; 

    // If this is a file in the root namespace, force the task to be for a file
    // This hack is needed, since retrieving the object ID(s) of a single file
    // is NOT a recursive operation.
    if (!new_task_depth && next_file) new_task_depth++;

    mustang_task* top_task;

    switch (new_task_depth) {
        case 0:
            // Namespace case. Enqueue a new namespace traversal task.
            top_task = task_init(config, new_task_position, strdup(next_basepath), next_file, output_table, hashtable_lock, queue, &traverse_ns);
            task_enqueue(queue, top_task);
            LOG(LOG_DEBUG, "Created top-level namespace traversal task at basepath: \"%s\"\n", next_basepath);
            break;
        default:
            // "Regular" (directory or file) case. Enqueue a new directory traversal task.
	    if (next_file) {
                top_task = task_init(config, new_task_position, strdup(next_basepath), next_file, output_table, hashtable_lock, queue, &traverse_file);
                LOG(LOG_DEBUG,"Created task to get Object ID(s) for file \"%s\"\n", next_file);
	    } else {
                top_task = task_init(config, new_task_position, strdup(next_basepath), next_file, output_table, hashtable_lock, queue, &traverse_dir);
                LOG(LOG_DEBUG, "Created top-level directory traversal task at basepath: \"%s\"\n", next_basepath);
	    }
            task_enqueue(queue, top_task);
            break;
    }

    // traverse_file() will take care disposing of new_file
    free(next_basepath);

}

/**
 * This private routine encapulates the logic used to generete the thread
 * tasks from object ID arguments given on the command line. While the logic itself
 * could be part of the body of main(), having a separate routine helps
 * focus on the creating of tasks.
 * 
 * Like push_pathargs2queue(), this routine adds initial tasks to the task queue.
 * However these tasks will only run in the namespaces designated by the object IDs.
 * @param the_arg : the commandline argument to process
 * @param marfs_position* start_position : the Start Position of directory in MarFS file system.
 *                                         Used to create the Task Position
 * @param hashtable* output_table : Hash Table holding the file paths of files that are in
 *                                  object(s) passed to the tasks
 * @param pthread_mutex_t* hashtable_lock : Lock to use when accessing output_table - 
 *                                          passed to the tasks
 * @param task_queue* queue : Queue of available threads - passed to the tasks. 
 */
void push_objargs2queue(char* the_arg, marfs_config* config, marfs_position* start_position, hashtable* output_table, pthread_mutex_t* hashtable_lock, task_queue* queue) {
    char* the_objid = strdup(the_arg);//holds the object ID of the files to look for in the namespace
    char nsrootbuf[PATH_MAX];         //buffer to hold the namespace root path found in the object ID
    char nsmntbuf[PATH_MAX];          //buffer to hold the actual MarFS user mount point
    size_t nsrootlen = ftag_nspath(the_objid, nsrootbuf, PATH_MAX);//the length of the namespace root path string

    // If namespace string is bigger than maxpath, then something wierd is happening.
    // If it is zero then there was a problem parsing it out of the object ID
    if (!nsrootlen || (nsrootlen >= PATH_MAX)) {
        LOG(LOG_ERR, "Failed to extract namespace path from object ID  arg \"%s\" (path length = %d)--skipping to next\n", the_arg, nsrootlen);
        if (the_objid) free(the_objid);
        return;
    }
    snprintf(nsmntbuf, PATH_MAX, "%s%s", config->mountpoint, nsrootbuf);

    // Once we have the namespace root, need to postion ourselves in the user metadata tree
    marfs_position* new_task_position = calloc(1, sizeof(marfs_position));

    if (config_duplicateposition(start_position, new_task_position)) {
        LOG(LOG_ERR, "Failed to duplicate parent position to new task--skipping to next\n");
        if (the_objid) free(the_objid);
        free(new_task_position);
        return;
    }

    char* nsroot = strdup(nsmntbuf);//needs to be a maluable pointer to use later...
    int new_task_depth = config_traverse(config, new_task_position, &nsroot, 0);

    if (new_task_depth < 0) {
        LOG(LOG_ERR, "Failed to traverse (got depth: %d)--skipping to next\n", new_task_depth);
        if (the_objid) free(the_objid);
        free(nsroot);
        config_abandonposition(new_task_position);
        free(new_task_position);
        return;
    }
    // If new depth != 0, then the namespace root must not designate a valid namespace
    if (new_task_depth > 0) {
        LOG(LOG_ERR, "\"%s\" is NOT a valid namespace root (got depth: %d)--skipping to next\n", nsroot, new_task_depth);
        if (the_objid) free(the_objid);
        free(nsroot);
        config_abandonposition(new_task_position);
        free(new_task_position);
        return;
    }
    // ... fortify the new position
    if (config_fortifyposition(new_task_position)) {
        LOG(LOG_ERR, "Failed to fortify new_task position after new_task traverse!\n");
        if (the_objid) free(the_objid);
        free(nsroot);
        config_abandonposition(new_task_position);
        free(new_task_position);
        return;
    }

    // Tell the new task where it is (at the top of the namespace) by recording its
    // depth in its state.
    new_task_position->depth = new_task_depth;

    // Now add the task to traverse the namespace, looking for files in the object to
    // the task queue
    mustang_task* top_task = task_init(config, new_task_position, strdup(nsmntbuf), the_objid, output_table, hashtable_lock, queue, &traverse_objns);
    task_enqueue(queue, top_task);
    LOG(LOG_DEBUG, "Created top-level namespace traversal task at namespace path: \"%s\" looking for files in object \"%s\"\n", nsmntbuf, the_objid);

    
    return;
}

// Argument definition string for getopt(). The command line arguments are as 
// follows:
//    -t <max_threads>		Maximum number of threads used by this process. This
//                              is the maximum size if the thread pool.
//    -q <max_tasks>		Maximum number of tasks in a given threads task
//                      	queue
//    -c <max_cache_entries>	Maximum number of entries for a thread's Object ID
//                              cache
//    -h <power_of_2>		Used to set the size of the Object ID hash table
//                              (e.g. 17 -> 2^17 -> table size = 131072)
//    -l <log file>		Log file for this process. Should be a full path.
//    -o <output file>		Output file for the generated Object ID list. This is 
//                              actually a prefix. Should be a full path.
#define GETOPT_STR ":c:hH:l:o:q:t:"
#define USAGE "Usage: mustang -t [max threads] -H [hashtable capacity exponent] -c [cache capacity] -q [task queue size] -o [output file] -l [log file] [paths, ...]\n"

int main(int argc, char** argv) {
    int opt;                         // holds current command line arg from getopt()
    size_t max_threads = 2;          // Maximum # of threads
    size_t queue_capacity = SIZE_MAX;// Maximum size of task queue
    long fetched_id_cache_capacity = 16;//Maximum # of Object ID cache entries
    long hashtable_capacity = 131072;// Maxium size of the Object ID hash table
    FILE* output_ptr = NULL;         // Stream Ptr for output file

    char* invalid = NULL;            // temporary pointer used in numeric conversion
    long hashtable_exp = 0L;         // temporary variable used in argument parsing
    int patharg_idx;                 // holds the arument index of the first path argument

    struct timeval ts;               // timestamp structure
    struct tm ts_local;              // holds local time
    char ts_buf[128];                // buffer to hold formatted timestamp

    errno = 0; // to guarantee an initially successful context and avoid "false positive" errno settings (errno not guaranteed to be initialized)

	// put ':' in the starting of the 
	// string so that program can 
	//distinguish between '?' and ':' 
    while((opt = getopt(argc, argv, GETOPT_STR)) > 0) { 
          switch(opt) { 
             case 't':            // cmdline args are strings. Conversion to numeric is required 
                       invalid = NULL;
                       max_threads = (size_t) strtol(optarg, &invalid, 10);
                       if ((errno == EINVAL) || (*invalid != '\0')) {
                           fprintf(stderr,"Bad max threads argument \"%s\" received. Please specify a nonnegative integer (i.e. > 0), then try again.\n", optarg);
                           if (output_ptr) fclose(output_ptr);
                           return 1;
                       }
                       break; 
             case 'l':            // If stderr not being used for logging, redirect stdout and stderr to specified file (redirection is default behavior)
                       if (strncmp(optarg, "stderr", strlen("stderr")) != 0) {
                           int log_fd = open(optarg, O_WRONLY | O_CREAT | O_APPEND, 0644);

                           if (dup2(log_fd, STDERR_FILENO) == -1) 
                               fprintf(stderr, "Failed to redirect stderr to file \"%s\"! (%s)\n", optarg, strerror(errno));
                           close(log_fd);
                       }
                       break; 
             case 'q':          // Allow -1 as a sentinel for "unlimited" capacity. Conversion to numeric is required
                       invalid = NULL;
                       if (strncmp(optarg, "-1", strlen(optarg)) == 0) {
                           fprintf(stderr,"Using SIZE_MAX as queue capacity (effectively unlimited).\n");
                       } else {
                           queue_capacity = (size_t) strtol(argv[2], &invalid, 10);
                           if ((errno == EINVAL) || (*invalid != '\0')) {
                               fprintf(stderr,"Bad task queue capacity argument \"%s\" received. Please specify a nonnegative integer (i.e., > 0), then try again.\n", optarg);
                               if (output_ptr) fclose(output_ptr);
                               return 1;
                           }
                       }
                       break; 
             case 'c':          // per-thread object ID cache capacity. Conversion to numeric is required

                       fetched_id_cache_capacity = strtol(optarg, &invalid, 10);
                       if ((fetched_id_cache_capacity <= 0) || (errno == EINVAL) || (*invalid != '\0')) {
                           fprintf(stderr,"Bad cache capacity argument \"%s\" received. Please specify a nonnegative integer (i.e. > 0), then try again.\n", optarg);
                           if (output_ptr) fclose(output_ptr);
                           return 1;
                       }
                       break; 
             case 'H':        // value used to compute hashtable capacity. Conversion to numeric is required
                       invalid = NULL;
                       hashtable_exp = strtol(optarg, &invalid, 10);
                       if ((hashtable_exp < 2) || (((size_t) hashtable_exp) > 24) || 
                           (errno == EINVAL) || (*invalid != '\0')) {
                           fprintf(stderr, "Bad hashtable capacity exponent \"%s\" received. Please specify a positive integer between 2 and 24, then try again.\n", optarg);
                           if (output_ptr) fclose(output_ptr);
                           return 1;
                       }
		       hashtable_capacity = (1L << hashtable_exp);
                       break; 
             case 'o':          // open the output file for writing
                       output_ptr = fopen(optarg, "w");
                       if (output_ptr == NULL) {
                           fprintf(stderr, "Failed to open file \"%s\" for writing to output (%s)\n", optarg, strerror(errno));
                           return 1;
                       }
                       break; 
             case 'h':
                       fprintf(stderr,USAGE);
                       return 0; 
             case ':': 
                       fprintf(stderr, "Command option %c needs a value\n",optopt); 
                       fprintf(stderr,USAGE);
                       if (output_ptr) fclose(output_ptr);
		       return 1;
             case '?': 
                       fprintf(stderr, "Unknown option: %c\n", optopt); 
                       fprintf(stderr,USAGE);
                       if (output_ptr) fclose(output_ptr);
		       return 1;
          } 
    } 
    patharg_idx = optind;            // value of optind can get funky later in the program. Caputure it now.
	
    // Verify commandline calues

    // Double check that an output file was specified on the Commandline
    if (!output_ptr) {
        LOG(LOG_ERR, "No output file (-o <output file>) was provided. Exiting ...\n");
	return 1;
    }

    // Check to see that we have paths to scan
    if (patharg_idx >= argc) {
        LOG(LOG_ERR, "No MarFS paths to scan. Exiting ...\n");
        if (output_ptr) fclose(output_ptr);
	return 1;
    }

    // Set the global Object ID Cache, which is treated as a constant by the worker threads,
    // to the successfully parsed value.
    id_cache_capacity = (size_t) fetched_id_cache_capacity; 
    if (id_cache_capacity > 1024) {
        LOG(LOG_WARNING, "Provided cache capacity argument will result in large per-thread data structures, which may overwhelm the heap.\n");
    }

    if (max_threads > 32768) {
        LOG(LOG_WARNING, "Using extremely large number of threads %zu. This may overwhelm system limits such as those set in /proc/sys/kernel/threads-max or /proc/sys/vm/max_map_count.\n", max_threads);
    }

    if (queue_capacity < max_threads) {
        LOG(LOG_WARNING, "Task queue capacity is less than maximum number of threads (i.e., thread pool size), which will limit concurrency by not taking full advantage of the thread pool.\n");
        LOG(LOG_WARNING, "Consider passing a task queue capacity argument that is greater than or equal to the maximum number of threads so that all threads have the chance to dequeue at least one task.\n");
    }

    if (hashtable_capacity < 256) {
        LOG(LOG_WARNING, "Received very small hashtable capacity argument \"%s\". Separate chaining will handle this, but may slow the program run unnecessarily.\n", argv[3]);
    }

    // Prepare to scan
    gettimeofday(&ts, NULL);
    localtime_r(&ts.tv_sec,&ts_local);
    strftime(ts_buf, sizeof(ts_buf), "%d %b %Y %T",&ts_local);
    LOG(LOG_INFO, "*** Starting scan at %s.%ld\n",ts_buf,ts.tv_usec);

    // Begin state initialization
    hashtable* output_table = hashtable_init((size_t) hashtable_capacity);
    
    if ((output_table == NULL) || (errno == ENOMEM)) {
        LOG(LOG_ERR, "Failed to initialize hashtable (%s)\n", strerror(errno));
        fclose(output_ptr);
        return 1;
    }

    pthread_mutex_t ht_lock = PTHREAD_MUTEX_INITIALIZER;

    // Other tools like `marfs-verifyconf` rely on this environment variable.
    // If this is not set, the user has "bigger" problems and needs to check
    // their MarFS installation/instance.
    char* config_path = getenv("MARFS_CONFIG_PATH");

    if (config_path == NULL) {
        LOG(LOG_ERR, "MARFS_CONFIG_PATH not set in environment--please set and try again.\n");
        return 1;
    }

    task_queue* queue = task_queue_init((size_t) queue_capacity);

    if (queue == NULL) {
        LOG(LOG_ERR, "Failed to initialize task queue! (%s)\n", strerror(errno));
        return 1;
    }

    pthread_mutex_t erasure_lock = PTHREAD_MUTEX_INITIALIZER;

    // Worker threads will get `parent_config`, but treat it as read-only
    marfs_config* parent_config = config_init(config_path, &erasure_lock);    
    marfs_position parent_position = { .ns = NULL, .depth = 0, .ctxt = NULL };

    if (config_establishposition(&parent_position, parent_config)) {
        LOG(LOG_ERR, "Failed to establish marfs_position!\n");
        return 1;
    }

    if (config_fortifyposition(&parent_position)) {
        LOG(LOG_ERR, "Failed to fortify position with MDAL_CTXT!\n");
        config_abandonposition(&parent_position);
        return 1;
    }

    // Attempt to reduce threads' stack size from 8 MiB (default, but 
    // excessively large for this application) to 32 KiB, which is the minimum
    // amount of stack space that threads need to successfully perform their 
    // work.
    pthread_attr_t pooled_attr_template;
    pthread_attr_t* attr_ptr = &pooled_attr_template;
    
    if (pthread_attr_init(attr_ptr)) {
        LOG(LOG_ERR, "Failed to initialize attributes for pooled threads!\n");
        LOG(LOG_WARNING, "This means that stack size cannot be reduced from the default, which may overwhelm system resources unexpectedly.\n");
        attr_ptr = NULL; // Cause just the default attributes to be used in the pthread_create() call
    }

    if (pthread_attr_setstacksize(&pooled_attr_template, 2 * PTHREAD_STACK_MIN)) {
        LOG(LOG_ERR, "Failed to set stack size for pooled threads! (%s)\n", strerror(errno));
        LOG(LOG_WARNING, "This means that threads will proceed with the default stack size, which is unnecessarily large for this application and which may overwhelm system resources unexpectedly.\n");
        attr_ptr = NULL; // Cause the default attributs to be used in pthread_create()
    }

    // Malloc used here since initialization not important (pthread_ts will be changed with pthread_create())
    pthread_t* worker_pool = (pthread_t*) malloc(max_threads * sizeof(pthread_t));

    if (worker_pool == NULL) {
        LOG(LOG_ERR, "Failed to allocate memory for worker pool! (%s)\n", strerror(errno));
        return 1;
    }

    for (size_t i = 0; i < max_threads; i += 1) {
        // Attribute is a safe bet: either reduced stack size or default since attr_ptr was set to NULL on prior errors
        int create_errorcode = pthread_create(&(worker_pool[i]), attr_ptr, &thread_launcher, queue);
        
        // In the new thread pool design, failure to create threads is an even more critical error.
        // If a thread could not be created, the user probably hit something like a system-enforced
        // threading limit, and should try again.
        if (create_errorcode) {
            LOG(LOG_ERR, "Failed to create thread! (%s)\n", strerror(create_errorcode));
            LOG(LOG_ERR, "HINT: Try running mustang again with a lower max threads argument.\n");
            return 1;
        }
    }

    // Parse each path argument, check them for validity, and pass along initial tasks
    for (; patharg_idx < argc; patharg_idx++){	 
        LOG(LOG_INFO, "Processing arg \"%s\"\n", argv[patharg_idx]);
        // If the arg has a "|" in it, then it is an object ID, not a path. 
        // Need to initilize the queue/task differently
        if (strchr(argv[patharg_idx],'|'))
	    push_objargs2queue(argv[patharg_idx], parent_config, &parent_position, output_table, &ht_lock, queue);
        else
	    push_pathargs2queue(argv[patharg_idx], parent_config, &parent_position, output_table, &ht_lock, queue);
    }

    pthread_mutex_lock(queue->lock);

    // Put the parent to sleep until worker threads have:
    // 1. Taken all tasks out of the queue (and, therefore, queue->size > 0)
    // 2. Finished all tasks, including having them return in error (threads 
    //    decrement queue->todos after they finish a task, not after they 
    //    remove it from the queue)
    while ((queue->todos > 0) || (queue->size > 0)) {
        pthread_cond_wait(queue->manager_cv, queue->lock);
    }

    pthread_mutex_unlock(queue->lock); 

    // Once there are no tasks left in the queue to do, send workers sentinel (all-NULL) tasks so that they know to exit.
    for (size_t i = 0; i < max_threads; i += 1) {
        mustang_task* sentinel = task_init(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
        task_enqueue(queue, sentinel);
    }

    // Threads should have exited by this point, so join them.
    for (size_t i = 0; i < max_threads; i += 1) {
        if (pthread_join(worker_pool[i], NULL)) {
            LOG(LOG_ERR, "Failed to join thread %0lx! (%s)\n", worker_pool[i], strerror(errno));
        }
    }

    /**
     * If destroying the task queue fails (which will set errno to EBUSY for a 
     * valid queue), something has gone seriously wrong in the synchronization
     * between the manager and worker threads.
     *
     * Logically, this should not be able to happen since all worker threads 
     * (and, therefore, all other users of the task queue) have been joined; 
     * however, there is logging code here "just in case" since the problem is 
     * sufficiently severe.
     */
    if (task_queue_destroy(queue)) {
        LOG(LOG_ERR, "Failed to destroy task queue! (%s)\n", strerror(errno));
        LOG(LOG_WARNING, "This is a critical application error meaning thread-safety measures have failed. You are strongly advised to disregard the output of this run and attempt another invocation.\n");
    }

    free(worker_pool);

    pthread_mutex_lock(&ht_lock);
    // hashtable_dump() returns the result of fclose(), which can set errno
    if (hashtable_dump(output_table, output_ptr)) {
        LOG(LOG_WARNING, "Failed to close hashtable output file pointer! (%s)\n", strerror(errno));
    }
    pthread_mutex_unlock(&ht_lock);

    // If attr could be initialized (and was therefore kept non-NULL), then destroy the attributes
    if (attr_ptr != NULL) {
        pthread_attr_destroy(attr_ptr);
    }

    // Clean up hashtable and associated lock state
    hashtable_destroy(output_table);
    pthread_mutex_destroy(&ht_lock);

    if (config_abandonposition(&parent_position)) {
        LOG(LOG_WARNING, "Failed to abandon parent position!\n");
    }

    if (config_term(parent_config)) {
        LOG(LOG_WARNING, "Failed to terminate parent config!\n");
    }

    pthread_mutex_destroy(&erasure_lock);

    gettimeofday(&ts, NULL);
    localtime_r(&ts.tv_sec,&ts_local);
    strftime(ts_buf, sizeof(ts_buf), "%d %b %Y %T",&ts_local);
    LOG(LOG_INFO, "*** End scan at %s.%ld\n",ts_buf,ts.tv_usec);
    return 0;
}
