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

/**
 * This is the utility used by admins to rebuild degraded objects in
 * multi-component storage.
 */

#include <pwd.h>
#include <fnmatch.h>
#include <mpi.h>
#include <signal.h>

// we absolutely must have multi-component enabled to build this
// program.  This ensures that the necessary symbols are exposed from
// dal.h
#include "common.h"
#include "dal.h" // MC_MAX_PATH_LEN, MC_MAX_LOG_LEN, & MC_DEGRADED_LOG_FORMAT
#include "marfs_configuration.h"
#include "marfs_base.h" // MARFS_MAX_HOST_SIZE

#include "erasure.h"

#define QUEUE_MAX 256
#define PROGRESS_DELAY 100 //number of objects to be rebuilt before printing a progress indicator
#define MPI_STATS_CHANNEL 145
#define MPI_ERR_CHANNEL 141
#define MPI_SUC_CHANNEL 142
#define MPI_INT_CHANNEL 143
#define VRB_FPRINTF(...)   if(verbose) { fprintf(__VA_ARGS__); }


struct object_file {
  char path[MC_MAX_PATH_LEN];
  int n;
  int e;
  int start_block;
  char repo_name[MARFS_MAX_HOST_SIZE];
  int pod;
  int cap;
  int error_pattern;
};

typedef int*** repo_stats_t;

typedef struct stat_list {
  struct stat_list  *next;
  char name[MARFS_MAX_HOST_SIZE];
  repo_stats_t repo_stats;
} stat_list_t;

// global flag indicating a dry run.
int   dry_run;
int   verbose;
int   total_procs;
FILE *error_log;
FILE *success_log;

// global rebuild statistics table.
struct rebuild_stats {
  int              rebuild_failures;
  int              rebuild_successes;
  int              total_objects;
  int              intact_objects;
  int              incomplete_objects;
  // the repo list should only be touched by the main thread. so
  // don't do any locking in record_failure_stats().
  stat_list_t     *repo_list;
} stats;

void init_repo_stats(stat_list_t *repo, MC_Config *config) {
  repo->repo_stats = malloc(config->num_pods * sizeof(int*));
  int i;
  for(i = 0; i < config->num_pods; i++) {
    repo->repo_stats[i] = malloc((config->n + config->e) * sizeof(int*));
    if(repo->repo_stats[i] == NULL) {
      perror("malloc()");
      exit(-1);
    }
    int j;
    for(j = 0; j < config->n + config->e; j++) {
      repo->repo_stats[i][j] = calloc(config->num_cap, sizeof(int));
      if(repo->repo_stats[i][j] == NULL) {
        perror("malloc()");
        exit(-1);
      }
    }
  }
}

// looks up the repo stats field for the repo name. If not repo with
// that name is present in the repo list a new repo stats structure is
// allocated and initialized.
repo_stats_t lookup_repo_stats(const char *repo_name,
                                     MC_Config *repo_config) {
  stat_list_t *stat_list = stats.repo_list;
  if(stat_list == NULL) {
    stats.repo_list = calloc(1, sizeof(stat_list_t));
    if(stats.repo_list == NULL) {
      perror("calloc()");
      exit(-1);
    }
    strncpy(stats.repo_list->name, repo_name, MARFS_MAX_HOST_SIZE);
    init_repo_stats(stats.repo_list, repo_config);
    return stats.repo_list->repo_stats;
  }

  do {
    if(!strcmp(stat_list->name, repo_name)) return stat_list->repo_stats;
    else if(stat_list->next == NULL) {
      stat_list->next = calloc(1, sizeof(stat_list_t));
      if(stat_list->next == NULL) {
        perror("calloc()");
        exit(-1);
      }
      strncpy(stat_list->next->name, repo_name, MARFS_MAX_HOST_SIZE);
      init_repo_stats(stat_list->next, repo_config);
      return stat_list->next->repo_stats;
    }
    else
      stat_list = stat_list->next;
  } while(stat_list->next);

  return NULL; // Should never get here.
}

void record_failure_stats(struct object_file *object) {
  // find the repo in the repo list.
  // if it does not exist, create a new add it to the list.
  MarFS_Repo *repo   = find_repo_by_name(object->repo_name);
  DAL        *dal    = repo->dal;
  MC_Config  *config = (MC_Config*)dal->global_state;

  repo_stats_t repo_stats = lookup_repo_stats(object->repo_name, config);

  int pod = object->pod;
  int cap = object->cap;

  unsigned int mask = 0x01;
  int i;
  for(i = 0; i < object->n + object->e; i++) {
    if(object->error_pattern & (mask << i)) {
      repo_stats[pod][i][cap]++;
    }
  }
}

void print_repo_stats(const char *repo_name, repo_stats_t stats) {
  MarFS_Repo *repo   = find_repo_by_name(repo_name);
  DAL        *dal    = repo->dal;
  MC_Config  *config = (MC_Config*)dal->global_state;

  printf("repo: %s\n", repo_name);
  
  int pod, block, cap;
  int had_error = 0;
  for(pod = 0; pod < config->num_pods; pod++) {
    printf("pod %d\n", pod);
    for(block = 0; block < config->n + config->e; block++) {
      if(had_error) printf("\n");
      had_error = 0;
      for(cap = 0; cap < config->num_cap; cap++) {
        if(stats[pod][block][cap]) {
          printf("\tpod:%d, block:%d, cap:%d, errors:%d\n",
                 pod, block, cap, stats[pod][block][cap]);
          had_error = 1;
        }
      }
    }
  }
  printf("\n");
}

void print_stats() {
  stat_list_t *stat_list = stats.repo_list;
  while(stat_list) {
    print_repo_stats(stat_list->name, stat_list->repo_stats);
    stat_list = stat_list->next;
  }
  if ( dry_run ) {
    printf("==== Dry-Run Summary ====\n");
    printf("objects examined:         %*d\n", 10, stats.total_objects);
    printf("incomplete objects:       %*d\n", 10, stats.incomplete_objects);
    printf("objects with all blocks:  %*d\n", 10, stats.intact_objects);
    printf("objects needing rebuild:  %*d\n", 10, stats.rebuild_successes);
    printf("unrecoverable objects:    %*d\n", 10, stats.rebuild_failures);
    printf("PLEASE NOTE : this was a dry-run only, no object rebuilds were performed!\n" );
  }
  else {
    printf("==== Rebuild Summary ====\n");
    printf("objects examined:   %*d\n", 10, stats.total_objects);
    printf("incomplete objects: %*d\n", 10, stats.incomplete_objects);
    printf("intact objects:     %*d\n", 10, stats.intact_objects);
    printf("rebuilt objects:    %*d\n", 10, stats.rebuild_successes);
    printf("failed rebuilds:    %*d\n", 10, stats.rebuild_failures);
  }
}

typedef struct ht_entry {
  struct ht_entry *next;
  char *key;
} ht_entry_t;

typedef struct hash_table {
  unsigned int size;
  ht_entry_t **table;
} hash_table_t;

hash_table_t rebuilt_objects;

/**
 * Compute the hash of key.
 */
unsigned long polyhash(const char *string) {
  const int salt = 33;
  char c;
  unsigned long hash = *string++;
  while((c = *string++))
    hash = salt * hash + c;
  return hash;
}

/**
 * Initialize the hash table.
 *
 * @param ht    a pointer to the hash_table_t to initialize
 * @param size  the size of the hash table
 *
 * @return NULL if initialization failed. Otherwise non-NULL.
 */
void *ht_init(hash_table_t *ht, unsigned int size) {
  ht->table = calloc(size, sizeof (ht_entry_t));
  ht->size = size;
  return ht->table;
}

/**
 * Lookup a key in the hash table.
 *
 * @param ht  the table to search
 * @param key the key to search for.
 *
 * @return 1 if the key was found. 0 if not.
 */
int ht_lookup(hash_table_t *ht, const char* key) {
  unsigned long hash = polyhash(key);
  ht_entry_t *entry = ht->table[hash % ht->size];
  while(entry) {
    if(!strcmp(entry->key, key)) {
      return 1;
    }
    entry = entry->next;
  }
  return 0;
}

/**
 * Insert an entry into the hash table.
 *
 * @param ht  the hash table to insert in
 * @param key the key to insert
 */
void ht_insert(hash_table_t *ht, const char* key) {
  ht_entry_t *entry = calloc(1, sizeof (ht_entry_t));
  unsigned long hash = polyhash(key);
  if(entry == NULL) {
    perror("calloc()");
    exit(-1);
  }
  entry->key = strdup(key);
  if(entry->key == NULL) {
    perror("strdup()");
    exit(-1);
  }

  if(!ht->table[hash % ht->size]) {
    ht->table[hash % ht->size] = entry;
  }
  else if(!strcmp(ht->table[hash % ht->size]->key, key)) {
    return;
  }
  else {
    ht_entry_t *e = ht->table[hash % ht->size];
    while(e->next) {
      if(!strcmp(e->key, key)) {
        return;
      }
      e = e->next;
    }
    e->next = entry;
  }
}

void usage(const char *program) {
  printf("Usage:\n");
  printf("To rebuild objects logged as degraded when read use the following\n");
  printf("%s [-t <num threads>] [-H <hash table size>] <degraded log dir> ...\n\n", program);
  printf("  <degraded log dir> should be the path to the directory where the\n"
         "                     MC DAL has logged objects it thinks are degraded.\n"
         "                     You may specify one or more log dirs.\n\n"
         "  <hash table size>  Size of the hash table used to track which\n"
         "                     objects have been rebuilt. A larger hash table\n"
         "                     may provide increased performance when\n"
         "                     rebuilding a large number of objects.\n"
         "                     Defaults to 1024.\n");
  printf("\nTo run a rebuild of an entire component use the following flags\n"
         "%s [-t <num threads>] [-s <start>:<end>] [-R] [-p <pod>] [-b <good block>] [-c <capacity-unit>] -r <repo name>\n"
         "  <good block>       The number of a block to be used as a reference\n"
         "                     point for the rebuild\n"
         "  <capacity unit>    The capacity unit to rebuild.\n"
         "  <pod>              The pod where the capacity unit is mounted.\n"
         "  <repo name>        The name of the repo in the marfs config.\n"
         "  <start>:<end>      This defines the inclusive range of scatter \n"
         "                     directories to be scanned and possibly rebuilt.\n"
         "                     The default is all scatters defined for a repo.\n"
         "  WARNING : Unless explicitly specified via -p/-b/-c respectively, the \n"
         "    pod/block/capacity-unit to be rebuilt are chosen randomly!\n",
         program);
  printf("\nNOTE : This program defaults to a dry-run, in which no rebuilds \n"
         "  will actually be performed!  Use \"-R\" to rebuild and write out parts.\n"
         "  This applies to both modes.\n");
  printf("The -t option applies to both modes and is used to specify a number \n"
         "  of threads to use for the rebuild. (default = 16)\n");
  printf("To run the rebuilder as some other user (ie. storageadmin) use the \n"
         "  -u <username> option\n");
  printf("The -o <filename> flag may be used to specify a file to which \n"
         "  rebuild successes should be logged\n");
  printf("The -e <filename> flag may be used to specify a file to which \n"
         "  rebuild failures should be logged\n");
}

/**
 * Rebuild the object described by `object'. If the .start_block field
 * is greater than or equal to zero, attempt to rebuild using the
 * information projeded in the struct (n, e, and start_block).
 * Otherwise allow libne to infer the correct values for these fields.
 *
 * @return -1 on failure, 0 on success.
 */
int rebuild_object(struct object_file object) {

  ne_handle object_handle;

  errno = 0;
  if(object.start_block < 0) { // component-based rebuild
    object_handle = ne_open(object.path, NE_REBUILD|NE_NOINFO);
  }
  else { // log-based rebuild
    object_handle = ne_open(object.path, NE_REBUILD, object.start_block,
                            object.n, object.e);
    if(object_handle == NULL) {
      errno = 0;
      object_handle = ne_open(object.path, NE_REBUILD|NE_NOINFO);
    }
  }

  if(object_handle == NULL) {
    if( errno == ENOENT ) { //ignore the failure to open an incomplete object
      VRB_FPRINTF( stdout, "INFO: skipping unfinished object %s\n", object.path );
      return -2; //signal an incomplete object
    }
    fprintf(stderr, "ERROR: cannot rebuild %s. ne_open() failed: %s.\n",
            object.path, strerror(errno));
    return -1;
  }

  if(dry_run) { return ne_close( object_handle ); }

  int rebuild_result = ne_rebuild(object_handle);
  if(rebuild_result < 0) {
    fprintf(stderr, "ERROR: cannot rebuild %s. ne_rebuild() failed: %s.\n",
            object.path, strerror(errno));
    ne_close(object_handle); //close and ignore any errors
    return -1;
  }
  else {
    int error = ne_close(object_handle);
    if(error < 0) {
      fprintf(stderr, "object %s could not be closed: %s\n",
              object.path, strerror(errno));
      // A failure of a close here means a failure to chown()/rename()
      // If that happens, something is terribly wrong.
      exit(-1);
    }
  }

  return rebuild_result;
}

/**
 * This is the queue used in multi-threaded mode to distribute work to
 * rebuilder threads.
 */
struct rebuild_queue {
  pthread_mutex_t    queue_lock;
  pthread_cond_t     queue_empty;
  pthread_cond_t     queue_full;
  struct object_file items[QUEUE_MAX];
  int                num_items;
  int                work_done; // flag to indicate the producer is done.
  int                queue_head;
  int                queue_tail;
  int                num_consumers;
  char               abort; // flag to indicate an early abort of work
  pthread_t         *consumer_threads;
} rebuild_queue;

/**
 * Add an object to the rebuild queue.
 */
void queue_rebuild(struct object_file object) {
  if(pthread_mutex_lock(&rebuild_queue.queue_lock)) {
    fprintf(stderr, "producer failed to acquire queue lock.\n");
    exit(-1); // a bit unceremonious
  }

  while( !rebuild_queue.abort  &&  rebuild_queue.num_items == QUEUE_MAX)
    pthread_cond_wait(&rebuild_queue.queue_full, &rebuild_queue.queue_lock);

  if( !rebuild_queue.abort ) {
    rebuild_queue.items[rebuild_queue.queue_tail] = object;
    rebuild_queue.num_items++;
    rebuild_queue.queue_tail = (rebuild_queue.queue_tail + 1) % QUEUE_MAX;
    
    pthread_cond_signal(&rebuild_queue.queue_empty);
  }

  pthread_mutex_unlock(&rebuild_queue.queue_lock);
  return;
}

/**
 * Signal the consumer threads and wait for them to stop.
 */
void stop_workers() {
  pthread_mutex_lock(&rebuild_queue.queue_lock);
  rebuild_queue.work_done = 1;
  pthread_cond_broadcast(&rebuild_queue.queue_empty);
  pthread_mutex_unlock(&rebuild_queue.queue_lock);

  int i;
  for(i = 0; i < rebuild_queue.num_consumers; i++) {
    pthread_join(rebuild_queue.consumer_threads[i], NULL);
  }
}


void abort_rebuild( int signo ) {
  fprintf( stderr, "EARLY ABORT REQUESTED\nSignalling threads to stop..." );
  rebuild_queue.abort = 1;
  fprintf( stderr, "done\n" );
}


/**
 * The start function for the consumer threads that actually perform
 * the rebuild work.
 *
 * TODO: Check error codes in return value from pthred_ functions
 */
void *rebuilder(void *arg) {
  char buf[MC_MAX_LOG_LEN];

  if(pthread_mutex_lock(&rebuild_queue.queue_lock)) {
    fprintf(stderr, "consumer failed to acquire queue lock.\n");
    return NULL;
  }

  while(1) {
    // wait until the condition is met.
    while(!rebuild_queue.num_items && !rebuild_queue.work_done)
      pthread_cond_wait(&rebuild_queue.queue_empty, &rebuild_queue.queue_lock);
    
    if( rebuild_queue.abort  ||  ( rebuild_queue.num_items == 0 && rebuild_queue.work_done ) ) {
      if( rebuild_queue.abort ) { pthread_cond_broadcast( &rebuild_queue.queue_full ); }
      pthread_mutex_unlock(&rebuild_queue.queue_lock);
      return NULL;
    }
    else if(rebuild_queue.num_items > 0) {
      // pick up an object.
      struct object_file object = rebuild_queue.items[rebuild_queue.queue_head];
      rebuild_queue.queue_head = (rebuild_queue.queue_head + 1) % QUEUE_MAX;

      //printf( "GOT: PATH-%s | N-%d | E-%d | ST-%d | RES-%d | REPO-%s | POD-%d | CAP-%d\n", object.path, object.n, object.e, object.start_block, 0, object.repo_name, object.pod, object.cap );

      rebuild_queue.num_items--;

      // update statistics here so they are protected by queue locking.
      stats.total_objects++;
      if( !verbose  &&  stats.total_objects % PROGRESS_DELAY == 0 ) { printf( "." ); fflush( stdout ); }

      if( !(stats.total_objects % 100) ) {
        VRB_FPRINTF( stdout, "INFO: total rebuilds completed: %d\n", stats.total_objects );
      }

      // signal the producer to wake up, in case it was sleeping, and
      // put more work in the queue.
      pthread_cond_signal(&rebuild_queue.queue_full);

      // release the lock so other consumers can run and rebuild the
      // object.
      pthread_mutex_unlock(&rebuild_queue.queue_lock);

      int rebuild_result = rebuild_object(object);

      // reacquire the lock.
      if(pthread_mutex_lock(&rebuild_queue.queue_lock)) {
        fprintf(stderr, "consumer failed to acquire queue lock.\n");
        return NULL;
      }

      if ( rebuild_result == -2 ) {
        stats.incomplete_objects++;
      }
      else if( rebuild_result < 0 ) {
        stats.rebuild_failures++;
        if( error_log != NULL ) {
          if( total_procs == 1 ) { //if this is the only process, just wite to the log
            fprintf( error_log, MC_DEGRADED_LOG_FORMAT, 
              object.path, object.n, object.e,
              object.start_block, rebuild_result,
              object.repo_name, object.pod, object.cap);
          }
          else { //otherwise, transmit to the master
            snprintf(buf, MC_MAX_LOG_LEN, MC_DEGRADED_LOG_FORMAT,
              object.path, object.n, object.e,
              object.start_block, rebuild_result,
              object.repo_name, object.pod, object.cap);
            MPI_Send( &buf[0], MC_MAX_LOG_LEN, MPI_BYTE, 0, MPI_ERR_CHANNEL, MPI_COMM_WORLD );
          }
        }
      }
      else if(rebuild_result > 0) {
        stats.rebuild_successes++;
        if( success_log != NULL ) {
          if( total_procs == 1 ) {
            fprintf( success_log, MC_DEGRADED_LOG_FORMAT, 
              object.path, object.n, object.e,
              object.start_block, rebuild_result,
              object.repo_name, object.pod, object.cap);
          }
          else {
            snprintf(buf, MC_MAX_LOG_LEN, MC_DEGRADED_LOG_FORMAT,
              object.path, object.n, object.e,
              object.start_block, rebuild_result,
              object.repo_name, object.pod, object.cap);
            MPI_Send( &buf[0], MC_MAX_LOG_LEN, MPI_BYTE, 0, MPI_SUC_CHANNEL, MPI_COMM_WORLD );
          }
        }
      }
      else {
        stats.intact_objects++;
      }
    }
  }
}

/**
 * Start the consumer threads that will do the actual rebuild work.
 */
void start_threads(int num_threads) {

  rebuild_queue.consumer_threads = calloc(num_threads, sizeof(pthread_t));
  if(rebuild_queue.consumer_threads == NULL) {
    perror("start_threads: calloc()");
    exit(-1);
  }
  
  pthread_mutex_init(&rebuild_queue.queue_lock, NULL);
  pthread_cond_init(&rebuild_queue.queue_empty, NULL);
  pthread_cond_init(&rebuild_queue.queue_full, NULL);

  rebuild_queue.num_items     = 0;
  rebuild_queue.work_done     = 0;
  rebuild_queue.queue_head    = 0;
  rebuild_queue.queue_tail    = 0;
  rebuild_queue.abort         = 0;
  rebuild_queue.num_consumers = num_threads;

  struct sigaction action;
  action.sa_handler = abort_rebuild;
  action.sa_flags = 0;
  sigemptyset( &action.sa_mask );

  if( sigaction( SIGINT, &action, NULL ) ) {
    fprintf( stderr, "ERROR: failed to set signal handler for SIGINT -- %s\n", strerror( errno ) );
    exit ( -1 ); //terminate while it's still safe to do so
  }

  if( sigaction( SIGUSR1, &action, NULL ) ) {
    fprintf( stderr, "ERROR: failed to set signal handler for SIGUSR1 -- %s\n", strerror( errno ) );
    exit ( -1 ); //terminate while it's still safe to do so
  }

  int i;
  for(i = 0; i < num_threads; i++) {
    pthread_create(&rebuild_queue.consumer_threads[i], NULL, rebuilder, NULL);
  }
}

int nextobject(FILE *log, struct object_file *object) {
  char *line = NULL;
  size_t len = 0;
  ssize_t line_len = getline(&line, &len, log);
  if(line_len == -1) {
    return -1;
  }

  int matches = sscanf(line, MC_DEGRADED_LOG_FORMAT,
                       object->path, &object->n, &object->e,
                       &object->start_block, &object->error_pattern,
                       object->repo_name, &object->pod, &object->cap);

  if(matches != 8) {
    fprintf(stderr, "failed to parse log entry\n");
    return -1;
  }

  free(line);
  return 0;
}

void process_log_subdir(const char *subdir_path) {

  DIR *dir = opendir(subdir_path);
  if(!dir) {
    fprintf(stderr, "Could not open subdirectory %s: %s\n",
            subdir_path, strerror(errno));
    return;
  }

  struct dirent *scatter_dir;
  while((scatter_dir = readdir(dir))) {
    if(scatter_dir->d_name[0] == '.') {
      continue; // skip '.' and '..'
    }

    char scatter_path[PATH_MAX];
    snprintf(scatter_path, PATH_MAX, "%s/%s", subdir_path, scatter_dir->d_name);
    DIR *scatter = opendir(scatter_path);

    struct dirent *log_file_dent;
    while((log_file_dent = readdir(scatter))) {
      if(log_file_dent->d_name[0] == '.') {
        continue;
      }

      int fd = openat(dirfd(scatter), log_file_dent->d_name, O_RDONLY);
      if(fd == -1) {
        perror("openat()");
        exit(-1);
      }
      FILE *degraded_object_file = fdopen(fd, "r");
      if(degraded_object_file == NULL) {
        perror("fdopen()");
        exit(-1);
      }

      struct object_file object;
      while(nextobject(degraded_object_file, &object) != -1) {
        if(ht_lookup(&rebuilt_objects, object.path))
          continue;

        // always add the object to the list of rebuilt objects.
        // Even if we fail to rebuild, we don't want to repeatedly
        // attempt to rebuild.
        ht_insert(&rebuilt_objects, object.path);
        // only recording failure stats for log rebuilds.
        record_failure_stats(&object);

        if( rebuild_queue.abort )
          break;

        queue_rebuild(object);
      }

      fclose(degraded_object_file);
      
      if( rebuild_queue.abort )
         break;
    }
    closedir(scatter);
    if( rebuild_queue.abort )
      break;
  }
  closedir(dir);
}

int rebuild_component(MarFS_Repo_Ptr repo,
                       int pod, int good_block, int cap,
                       int *scatter_range) {
  const char *path_template = repo->host;
  int         scatter;
  int         failed = 0;

  for(scatter = scatter_range[0];
      scatter <= scatter_range[1];
      scatter++) {
    char ne_path[MC_MAX_PATH_LEN];
    char scatter_path[MC_MAX_PATH_LEN];

    snprintf(ne_path, MC_MAX_PATH_LEN, path_template, pod, "%d", cap, scatter);
    snprintf(scatter_path, MC_MAX_PATH_LEN, ne_path, good_block);

    struct stat statbuf;
    if(stat(scatter_path, &statbuf) == -1) {
      fprintf( stderr, "CRITICAL ERROR: could not stat scatter dir %s: %s\n", scatter_path, strerror(errno) );
      failed++;
      continue;
    }

    DIR *scatter_dir = opendir(scatter_path);
    if(scatter_dir == NULL) {
      fprintf(stderr, "CRITICAL ERROR: could not open scatter dir %s: %s\n",
              scatter_path, strerror(errno));
      failed++;
      continue;
    }

    if(verbose) {
      printf("INFO: rebuilding scatter%d\n", scatter);
    }
    
    struct dirent *obj_dent;
    while((obj_dent = readdir(scatter_dir)) != NULL) {
      if(obj_dent->d_name[0] == '.')
        continue;

      struct object_file object;
      snprintf(object.path, MC_MAX_PATH_LEN, "%s/%s",
               ne_path, obj_dent->d_name);
      snprintf(object.repo_name, MARFS_MAX_HOST_SIZE, "%s",
               repo->name);
      object.start_block = object.n = object.e = -1;
      object.pod = pod;
      object.cap = cap;

      // skip files that are not complete objects
      if(!fnmatch("*" REBUILD_SFX, object.path, 0) ||
         !fnmatch("*" META_SFX, object.path, 0)    ||
         !fnmatch("*" WRITE_SFX, object.path, 0)) {
         continue;
      }

      queue_rebuild(object);

      if( rebuild_queue.abort ) {
        break;
      }
    }
    closedir(scatter_dir);
    if( rebuild_queue.abort ) {
      break;
    }
  }
  return failed;
}


int rand_less_than( int max ) {
  if ( max <= 0 ) { return -1; }
  unsigned long bin_width = ( (unsigned long)(RAND_MAX) + 1 ) / max;
  unsigned long rand_max = RAND_MAX - ( ( (unsigned long)(RAND_MAX) + 1) % bin_width );

  unsigned long rand;
  do {
    rand = random();
  }
  while ( rand > rand_max );

  return (int)(rand / bin_width);
}


int main(int argc, char **argv) {

  unsigned int   ht_size           = 1024;
  int            component_rebuild = 0;
  char          *repo_name         = NULL;
  int            threads           = 16;
  int            pod, block, cap;
  int            opt;
  int            scatter_range[2];
  int            range_given = 0;
  struct passwd *pw = NULL;
  pod = block = cap = -1;
  verbose = 0; dry_run = 1; //default to dry-run
  error_log = NULL;
  success_log = NULL;
  char *s_ptr=NULL;
  char *e_ptr=NULL;

  while((opt = getopt(argc, argv, "hH:c:p:b:Rr:t:vs:o:e:u:")) != -1) {
    switch (opt) {
    case 'h':
      usage(argv[0]);
      exit(0);
      break;
    case 'H':
      ht_size = strtol(optarg, NULL, 10);
      break;
    case 'c':
      cap = strtol(optarg, NULL, 10);
      break;
    case 'p':
      pod = strtol(optarg, NULL, 10);
      break;
    case 'b':
      block = strtol(optarg, NULL, 10);
      break;
    case 'R':
      dry_run = 0;
      break;
    case 'r':
      component_rebuild = 1;
      repo_name = optarg;
      break;
    case 't':
      threads = strtol(optarg, NULL, 10);
      if(threads < 1) {
        fprintf(stderr, "Invalid number of threads. Defaulting to 1\n");
        threads = 1;
      }
      break;
    case 'v':
      verbose = 1;
      break;
    case 'o':
      s_ptr=optarg;
      break;
    case 'e':
      e_ptr=optarg;
      break;
    case 's':
    {
      char *start, *end;
      end = strdup(optarg);
      if(end == NULL) {
        perror("strdup()");
        exit(-1);
      }

      start = strsep(&end, ":");
      if(end == NULL || start == NULL ) {
        printf("Bad scatter range format."
               "The correct format is \"<start>:<end>\n");
        usage(argv[0]);
        exit(-1);
      }
      scatter_range[0] = strtol(start, NULL, 10);
      scatter_range[1] = strtol(end, NULL, 10);
      //swap start and end if listed in reverse
      if( scatter_range[0] > scatter_range[1] ) {
        int tmp = scatter_range[0];
        scatter_range[0] = scatter_range[1];
        scatter_range[1] = tmp;
      }
      range_given = 1;
      free(start);
      break;
    }
    case 'u':
      if((pw = getpwnam(optarg)) == NULL) {
        fprintf( stderr, "ERROR: failed to retireve uid for the user \"%s\"\n", optarg );
        exit(-1);
      }
      break;
    default:
      usage(argv[0]);
      exit(-1);
    }
  }

  // load the marfs config.
  if(read_configuration()) {
    fprintf( stderr, "ERROR: failed to read marfs configuration\n" );
    exit(-1);
  }
  if(validate_configuration()) {
    fprintf(stderr, "ERROR: failed to validate marfs configuration\n");
    exit(-1);
  }

  if ( pw != NULL ) {
    if( error_log != NULL  &&  chown( e_ptr, pw->pw_uid, pw->pw_gid ) ) {
        printf( "ERROR: failed to chown() error log\n" );
    }
    if( success_log != NULL  &&  chown( s_ptr, pw->pw_uid, pw->pw_gid ) ) {
        printf( "ERROR: failed to chown() success log\n" );
    }

    if(setgid(pw->pw_gid) != 0) {
      fprintf( stderr, "ERROR: failed to setgid to specified user: %s\n", strerror(errno) );
      exit(-1);
    }
    if(setuid(pw->pw_uid) != 0) {
      fprintf( stderr, "ERROR: failed to setuid to specified user: %s\n", strerror(errno) );
      exit(-1);
    }
  }

  stats.rebuild_failures      = 0;
  stats.rebuild_successes     = 0;
  stats.intact_objects        = 0;
  stats.incomplete_objects    = 0;
  stats.total_objects         = 0;
  stats.repo_list             = NULL;

  ht_init(&rebuilt_objects, ht_size);

  int ret_stat = 0;
  int num_procs = 1;
  int proc_rank = 0;

  // Initialize the MPI environment
  MPI_Init(NULL, NULL);

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
  total_procs = num_procs;

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);

  if( proc_rank == 0  &&  dry_run )
    fprintf(stdout, "INFO: this is a dry-run, no actual rebuilds will be performed (run with -R to change this)\n");

  MPI_Comm proc_com;

  // Get the name of the processor
  char hostname[250];
  int name_len;
  MPI_Get_processor_name( hostname, &name_len);

  // open success and error logs as required
  if( s_ptr != NULL ) {
    if( proc_rank == 0 ) {
      success_log = fopen(s_ptr, "a");
      if( success_log == NULL ) {
        fprintf( stderr, "failed to open log file \"%s\"\n", s_ptr );
        MPI_Abort( MPI_COMM_WORLD, errno );
      }
    }
    else {
      success_log = stdout; //just to indicate to workers that they should be logging
    }
  }
  if( e_ptr != NULL ) {
    if( proc_rank == 0 ) {
      if( s_ptr != NULL  &&  ! strncmp( s_ptr, e_ptr, MC_MAX_PATH_LEN ) ) {
        //if the logs are the same file, use the same stream
        error_log = success_log;
      }
      else {
        error_log = fopen(e_ptr, "a");
        if( error_log == NULL ) {
          fprintf( stderr, "failed to open log file \"%s\"\n", e_ptr );
          MPI_Abort( MPI_COMM_WORLD, errno );
        }
      }
    }
    else {
      error_log = stderr; //just to indicate to workers that they should be logging
    }
  }


  if(component_rebuild) {

    MarFS_Repo *repo = find_repo_by_name(repo_name);

    if( !repo ) { 
      if ( proc_rank == 0 )
         fprintf( stderr, "ERROR: failed to retrieve repo"
           " by name \"%s\", check your config file\n", repo_name );
      MPI_Finalize();
      return -1; 
    }   

    DAL        *dal    = repo->dal;
    MC_Config  *config = (MC_Config*)dal->global_state;

    if ( pod == -1 || block == -1 || cap == -1 ) {
      int data[3];
      if ( proc_rank == 0 ) {
        srand( time(0) );
        if ( pod == -1 ) {
          data[0] = rand_less_than( config->num_pods );
          printf( "INFO: using random pod - %d\n", data[0] );
        }
        else {
          data[0] = pod;
        }
        if ( block == -1 ) {
          data[1] = rand_less_than( (config->n + config->e) );
          printf( "INFO: using random block - %d\n", data[1] );
        }
        else {
          data[1] = block;
        }
        if ( cap == -1 ) {
          data[2] = rand_less_than( config->num_cap );
          printf( "INFO: using random capacity unit - %d\n", data[2] );
        }
        else {
          data[2] = cap;
        }
      }
      // synchronize pod,cap,block amongst procs
      MPI_Bcast( &data[0], 3, MPI_INT, 0, MPI_COMM_WORLD );
      pod = data[0];
      block = data[1];
      cap = data[2];
    }

    //verify scatter ranges are sane
    if( range_given ) {
      if( config->scatter_width - 1 < scatter_range[1] ) {
        scatter_range[1] = config->scatter_width - 1;
        if( proc_rank == 0 )
          fprintf( stderr, "WARNING: upper scatter range exceeds range defined by repo,"
            " lowering upper bound to %d\n", scatter_range[1] );
      }
      if( scatter_range[0] < 0 ) {
        scatter_range[0] = 0;
        if( proc_rank == 0 )
          fprintf( stderr, "WARNING: lower scatter range is negative,"
            " reseting lower bound to %d\n", scatter_range[0] );
      }
    }

    // This process is a worker
    if( proc_rank != 0  ||  num_procs == 1 ) {
      MPI_Comm_split( MPI_COMM_WORLD, 1, proc_rank, &proc_com );

      char oneproc = 1;
      if( num_procs > 1 ) {
        num_procs--;
        proc_rank--;
        oneproc = 0;
      }
      else {
        printf( "INFO: Running with %d threads\n", threads );
        if ( ! verbose ) {
          printf( "Rebuilding Objects..." );
          fflush( stdout );
        }
      }

      int scatter_width = config->scatter_width;
      if ( range_given ) {
        scatter_width = scatter_range[1] - scatter_range[0] + 1;
      }
      else {
        scatter_range[0] = 0;
        scatter_range[1] = scatter_width - 1;
      }

      int remainder = scatter_width % num_procs;
      int rwidth = scatter_width / num_procs;

      scatter_range[0] += ( proc_rank * rwidth ) + (( proc_rank < remainder ) ? proc_rank : remainder);

      if ( proc_rank < remainder ) {
        rwidth++;
      }

      scatter_range[1] = scatter_range[0] + rwidth - 1;

      int turn;
      for ( turn = 0; turn < proc_rank; turn++ ) {
        MPI_Barrier( proc_com ); //wait for earlier ranks to print their messages
      }

      // Print off a message
      if ( scatter_range[1] < scatter_range[0] ) {
        // if there are more procs that scatters, procs with no work will have a backwards range
        VRB_FPRINTF( stdout, "INFO: process %d out of %d workers ( Host = %s, Repo = %s, Scatter_Width = %d, Range = [ No work to be done! ] )\n",
           proc_rank+1, num_procs, hostname, repo->name, config->scatter_width );
      }
      else {
        VRB_FPRINTF( stdout, "INFO: process %d out of %d workers ( Host = %s, Repo = %s, Scatter_Width = %d, Range = [%d,%d] )\n",
           proc_rank+1, num_procs, hostname, repo->name, config->scatter_width, scatter_range[0], scatter_range[1] );
      }

      struct timespec tspec;
      tspec.tv_nsec = 50000000; //wait 5 hundredths of a second, to allow message to travel
      tspec.tv_sec = 0;
      nanosleep( &tspec, NULL );

      for ( ; turn < num_procs; turn++ ) {
        MPI_Barrier( proc_com ); //wait for later ranks to print their messages
      }

      tspec.tv_nsec = 0;
      tspec.tv_sec = 1;
      nanosleep( &tspec, NULL ); //wait for a single second, to allow messages to be noticed
      int skipped_scatters = 0;

      // only do work if there is some to be done
      if ( scatter_range[1] >= scatter_range[0] ) {
        VRB_FPRINTF( stdout, "INFO: worker %d starting\n", proc_rank+1 );
        start_threads(threads);
        skipped_scatters = rebuild_component( repo, pod, block, cap, scatter_range );
        stop_workers();
      }
      else {
        fprintf( stderr, "WARNING: worker %d terminating early as it has nothing to do!\n", proc_rank+1 );
      }

      if( !oneproc ) {
        int statbuf[7];
        statbuf[0] = proc_rank;
        statbuf[1] = stats.total_objects;
        statbuf[2] = stats.intact_objects;
        statbuf[3] = stats.rebuild_successes;
        statbuf[4] = stats.rebuild_failures;
        statbuf[5] = stats.incomplete_objects;
        statbuf[6] = skipped_scatters;
        if( MPI_Send( &statbuf[0], 7, MPI_INT, 0, MPI_STATS_CHANNEL, MPI_COMM_WORLD ) != MPI_SUCCESS ) {
           fprintf( stderr, "ERROR: worker process %d failed to transmit its term state to the master\n", proc_rank+1 );
        }

      }
      else {
        if ( error_log != NULL ) {
          fclose( error_log );
          fprintf( stdout, "INFO: error log \"%s\" resides on host '%s'\n", e_ptr, hostname );
        }
        if ( success_log != NULL ) {
          fclose( success_log );
          fprintf( stdout, "INFO: success log \"%s\" resides on host '%s'\n", s_ptr, hostname );
        }
        fflush( stderr );
        if( ! verbose ) { printf( "rebuilds completed\n" ); }
        printf( "Rebuild of ( Pod %d, Block %d, Cap %d )\n", pod, block, cap );
        print_stats();

        if ( skipped_scatters ) {
          fprintf( stderr, "CRITICAL ERROR: %d scatter dir(s) could not be accessed (see above messages)\n", skipped_scatters );
        }
      }
  
    }
    // This process is the master/output-aggregator
    else {
      struct sigaction action;
      action.sa_handler = SIG_IGN;
      action.sa_flags = 0;
      sigemptyset( &action.sa_mask );

      if( sigaction( SIGINT, &action, NULL ) ) {
        fprintf( stderr, "ERROR: failed to set signal handler for SIGINT -- %s\n", strerror( errno ) );
        exit ( -1 ); //terminate while it's still safe to do so
      }

      if( sigaction( SIGUSR1, &action, NULL ) ) {
        fprintf( stderr, "ERROR: failed to set signal handler for SIGUSR1 -- %s\n", strerror( errno ) );
        exit ( -1 ); //terminate while it's still safe to do so
      }

      printf( "INFO: Running with %d threads per worker\n", threads );

      if( ! verbose ) {
        printf( "Rebuilding Objects..." );
        fflush( stdout );
      }

      // call comm_split, but don't bother adding the master to a new communicator
      MPI_Comm_split( MPI_COMM_WORLD, MPI_UNDEFINED, proc_rank, &proc_com );
      int terminated = 1;
      int skipped_scatters = 0;
      int err_procs[num_procs - 1];
      int err_proc_count = 0;

      int buf[7];
      char err_log[MC_MAX_LOG_LEN];
      char suc_log[MC_MAX_LOG_LEN];
      int res_flag;
      MPI_Status status;
      MPI_Request stats_request = MPI_REQUEST_NULL;
      MPI_Request err_request = MPI_REQUEST_NULL;
      MPI_Request suc_request = MPI_REQUEST_NULL;
      while ( terminated < num_procs ) {
        // if we are logging it, check for rebuild error output
        if ( error_log != NULL ) { 
          if( err_request == MPI_REQUEST_NULL )
            MPI_Irecv( &err_log[0], MC_MAX_LOG_LEN, MPI_BYTE, MPI_ANY_SOURCE, MPI_ERR_CHANNEL, MPI_COMM_WORLD, &err_request );

          MPI_Test( &err_request, &res_flag, &status );
          if( res_flag ) {
            fprintf( error_log, "%s", err_log );
          }
        }

        // if we are logging it, check for rebuild success output
        if ( success_log != NULL ) { 
          if( suc_request == MPI_REQUEST_NULL )
            MPI_Irecv( &suc_log[0], MC_MAX_LOG_LEN, MPI_BYTE, MPI_ANY_SOURCE, MPI_SUC_CHANNEL, MPI_COMM_WORLD, &suc_request );

          MPI_Test( &suc_request, &res_flag, &status );
          if( res_flag ) {
            fprintf( success_log, "%s", suc_log );
          }
        }

        // open a new non-blocking recieve request for final worker status
        if ( stats_request == MPI_REQUEST_NULL )
          MPI_Irecv( &buf[0], 7, MPI_INT, MPI_ANY_SOURCE, MPI_STATS_CHANNEL, MPI_COMM_WORLD, &stats_request );

        MPI_Test( &stats_request, &res_flag, &status );
        if( res_flag ) {
          // if we recieved termination stats, tally them and increment the count of finished workers
          stats.total_objects += buf[1];
          stats.intact_objects += buf[2];
          stats.rebuild_successes += buf[3];
          stats.rebuild_failures += buf[4];
          stats.incomplete_objects += buf[5];
          skipped_scatters += buf[6];
          if ( buf[5] ) {
            err_procs[err_proc_count] = buf[0]+1;
            err_proc_count++; //count the number of procs that hit an error
          }
          terminated++;
          VRB_FPRINTF( stdout, "INFO: master received stats from worker %d\n", buf[0]+1 );
        }

      }

      // if requests are still open for either errors or successes, close them
      if (  err_request != MPI_REQUEST_NULL ) {
        MPI_Cancel( &err_request );
        MPI_Request_free( &err_request );
      }
      if ( suc_request != MPI_REQUEST_NULL ) {
        MPI_Cancel( &suc_request );
        MPI_Request_free( &suc_request );
      }

      if ( error_log != NULL ) {
        fclose( error_log );
        fprintf( stdout, "INFO: error log \"%s\" resides on host '%s'\n", e_ptr, hostname );
      }
      if ( success_log != NULL ) {
        fclose( success_log );
        fprintf( stdout, "INFO: success log \"%s\" resides on host '%s'\n", s_ptr, hostname );
      }
      fflush( stderr );
      if( ! verbose ) { printf( "rebuilds completed\n" ); }
      sleep( 1 ); //wait for previous outputs to be displayed
      printf( "Rebuild of ( Pod %d, Block %d, Cap %d )\n", pod, block, cap );
      print_stats();
      if ( skipped_scatters ) { //if any scatters were skipped, print error messages
        sleep( 1 ); //wait for previous outputs to be displayed
        ret_stat = -1;
        fprintf( stderr, "%s: CRITICAL ERROR: %d scatter dir(s) could not be accessed (see above messages)\n", argv[0], skipped_scatters );
        if ( err_proc_count == num_procs - 1 ) {
          fprintf( stderr, "%s: CRITICAL ERROR: All workers encountered errors that resulted in skipped scatter directories\n", argv[0] );
        }
        else {
          fprintf( stderr, "%s: CRITICAL ERROR: The following workers failed to access all scatter dirs in their range :", argv[0] );
          for( terminated = 0; terminated < err_proc_count; terminated++ ) {
            fprintf( stderr, " %d", err_procs[terminated] );
          }
          fprintf( stderr, "\n" );
        }
      }

    }

    MPI_Barrier( MPI_COMM_WORLD );
  }
  else { // Rebuilding from logs
    // running multiple procs off of logs is not supported!
    if ( proc_rank > 0 ) {
      fprintf( stderr, "%s: running multiple procs for log rebuilds is not supported! (rank %d terminating early)\n", argv[0], proc_rank );
      MPI_Comm_split( MPI_COMM_WORLD, MPI_UNDEFINED, proc_rank, &proc_com );
      MPI_Finalize();
      return 0;
    }

    // this is used as a pseudo-barrier, only allowing this process to continue after all others have printed their error
    MPI_Comm_split( MPI_COMM_WORLD, 1, proc_rank, &proc_com );
    printf( "WARNING: This is a dry-run, no actual rebuilds will be performed!\n" );

    if(optind >= argc) {
      fprintf( stderr, "%s: too few arguments\n", argv[0] );
      usage(argv[0]);
      errno=EINVAL;
      MPI_Finalize();
      exit(-1);
    }

    if( ! verbose ) {
      printf( "Rebuilding Objects..." );
      fflush( stdout );
    }

    start_threads(threads);
    int index;
    for(index = optind; index < argc; index++) {

      DIR *log_dir = opendir(argv[index]);
      if(!log_dir) {
        fprintf(stderr, "ERROR: could not open log dir %s: %s\n",
                argv[index], strerror(errno));
        MPI_Finalize();
        exit(-1);
      }

      struct dirent *log_dirent;
      while((log_dirent = readdir(log_dir))) {
        if(log_dirent->d_name[0] == '.') {
          continue; // easy way to avoid reading '.' and '..'
        }

        char log_subdir[PATH_MAX];
        snprintf(log_subdir, PATH_MAX, "%s/%s", argv[index],
                 log_dirent->d_name);

        if( rebuild_queue.abort )
          break;

        process_log_subdir(log_subdir);
      }

      closedir(log_dir);
    }

    stop_workers();
    if( ! verbose ) { printf( "rebuilds completed\n" ); }
    print_stats();
  }

  // Finalize the MPI environment.
  MPI_Finalize();

  return ret_stat;
}
