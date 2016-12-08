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

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fnmatch.h>

// we absolutely must have multi-component enabled to build this
// program.  This ensures that the necessary symbols are exposed from
// dal.h
#include "common.h"
#include "dal.h" // MC_MAX_PATH_LEN & MC_DEGRADED_LOG_FORMAT
#include "marfs_configuration.h"
#include "marfs_base.h" // MARFS_MAX_HOST_SIZE

#include "erasure.h"

#define QUEUE_MAX 256

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

struct rebuild_stats {
  pthread_mutex_t  global_stats_lock;
  int              rebuild_failures;
  int              rebuild_successes;
  int              total_objects;
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
  printf("==== Rebuild Summary ====\n");
  printf("total rebuilds:  %*d\n", 10, stats.total_objects);
  printf("rebuilt objects: %*d\n", 10, stats.rebuild_successes);
  printf("failed rebuilds: %*d\n", 10, stats.rebuild_failures);
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
  printf("%s [-H <hash table size>] <degraded log dir> ...\n\n", program);
  printf("  <degraded log dir> should be the path to the directory where the\n"
         "                     MC DAL has logged objects it thinks are degraded.\n"
         "                     You may specify one or more log dirs.\n\n"
         "  <hash table size>  Size of the hash table used to track which\n"
         "                     objects have been rebuilt. A larger hash table\n"
         "                     may provide increased performance when\n"
         "                     rebuilding a large number of objects.\n"
         "                     Defaults to 1024.\n");
  printf("\nTo run a rebuild of an entire component use the following flags\n"
         "%s -c <capacity unit> -b <good block> -p <pod> -r <repo name>\n"
         "  <good block>       The number of a block to be used as a reference\n"
         "                     point for the rebuild\n"
         "  <capacity unit>    The capacity unit to rebuild.\n"
         "  <pod>              The pod where the capacity unit is mounted.\n"
         "  <repo name>        The name of the repo in the marfs config.\n"
         "  Note: All four flags are required to do a component rebuild.\n",
         program);
  printf("\nThe -t option applies to both modes and is used to specify a "
         "number of threads to use for the rebuild. Defaults to one.\n");

}

/**
 * Rebuild the object described by `object'. If the .start_block field
 * is greater than or equal to zero, attempt to rebuild using the
 * information projeded in the struct (n, e, and start_block).
 * Otherwise allow libne to infer the correct values for these fields.
 */
void rebuild_object(struct object_file object) {

  ne_handle object_handle;

  stats.total_objects++;

  if(object.start_block < 0) {
    object_handle = ne_open(object.path, NE_REBUILD|NE_NOINFO);
  }
  else {
    object_handle = ne_open(object.path, NE_REBUILD, object.start_block,
                            object.n, object.e);
  }

  if(object_handle == NULL) {
    fprintf(stderr, "ERROR: cannot rebuild %s. ne_open() failed: %s.\n",
            object.path, strerror(errno));
    stats.rebuild_failures++;
    return;
  }

  int rebuild_result = ne_rebuild(object_handle);
  if(rebuild_result == 0) {
    stats.rebuild_successes++;
  }
  else {
    fprintf(stderr, "ERROR: cannot rebuild %s. ne_rebuild() failed: %s.\n",
            object.path, strerror(errno));
    stats.rebuild_failures++;
    return;
  }

  int error = ne_close(object_handle);
  if(error < 0) {
    perror("ne_close()");
    fprintf(stderr, "object %s could not be closed: %s\n",
            object.path, strerror(errno));
    // We should never fail to close an object. If we do, then
    // something is terribly wrong.
    exit(-1);
  }
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

  while(rebuild_queue.num_items == QUEUE_MAX)
    pthread_cond_wait(&rebuild_queue.queue_full, &rebuild_queue.queue_lock);

  rebuild_queue.items[rebuild_queue.queue_tail] = object;
  rebuild_queue.num_items++;
  rebuild_queue.queue_tail = (rebuild_queue.queue_tail + 1) % QUEUE_MAX;

  pthread_cond_signal(&rebuild_queue.queue_empty);

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

/**
 * The start function for the consumer threads that actually perform
 * the rebuild work.
 *
 * TODO: Check error codes in return value from pthred_ functions
 */
void *rebuilder(void *arg) {

  while(1) {
    if(pthread_mutex_lock(&rebuild_queue.queue_lock)) {
      fprintf(stderr, "consumer failed to acquire queue lock.\n");
      return NULL;
    }
    // wait until the condition is met.
    while(!rebuild_queue.num_items && !rebuild_queue.work_done)
      pthread_cond_wait(&rebuild_queue.queue_empty, &rebuild_queue.queue_lock);
    
    if(rebuild_queue.num_items == 0 && rebuild_queue.work_done) {
      pthread_mutex_unlock(&rebuild_queue.queue_lock);
      return NULL;
    }
    else if(rebuild_queue.num_items > 0) {
      // pick up an object.
      struct object_file object = rebuild_queue.items[rebuild_queue.queue_head];
      rebuild_queue.queue_head = (rebuild_queue.queue_head + 1) % QUEUE_MAX;

      rebuild_queue.num_items--;

      // signal the producer to wake up, in case it was sleeping, and
      // put more work in the queue.
      pthread_cond_signal(&rebuild_queue.queue_full);

      // release the lock so other consumers can run and rebuild the
      // object.
      pthread_mutex_unlock(&rebuild_queue.queue_lock);

      rebuild_object(object);
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
  rebuild_queue.num_consumers = num_threads;

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

        queue_rebuild(object);
      }

      fclose(degraded_object_file);
    }
    closedir(scatter);
  }
  closedir(dir);
}

void rebuild_component(const char *repo_name,
                       int pod, int good_block, int cap) {
  MarFS_Repo *repo          = find_repo_by_name(repo_name);
  if(! repo ) {
    fprintf(stderr, "could not find repo %s. Please check your config.\n",
            repo_name);
    exit(-1);
  }
  const char *path_template = repo->host;
  int         scatter;

  for(scatter = 0; ; scatter++) {
    char ne_path[MC_MAX_PATH_LEN];
    char scatter_path[MC_MAX_PATH_LEN];

    snprintf(ne_path, MC_MAX_PATH_LEN, path_template, pod, "%d", cap, scatter);
    snprintf(scatter_path, MC_MAX_PATH_LEN, ne_path, good_block);

    struct stat statbuf;
    if(stat(scatter_path, &statbuf) == -1) {
      break;
    }

    DIR *scatter_dir = opendir(scatter_path);
    if(scatter_dir == NULL) {
      fprintf(stderr, "could not open scatter dir %s: %s\n",
              scatter_path, strerror(errno));
      exit(-1);
    }
    struct dirent *obj_dent;
    while((obj_dent = readdir(scatter_dir)) != NULL) {
      if(obj_dent->d_name[0] == '.')
        continue;

      struct object_file object;
      snprintf(object.path, MC_MAX_PATH_LEN, "%s/%s",
               ne_path, obj_dent->d_name);
      object.start_block = object.n = object.e = -1;

      // check that the path does not have a suffix matching
      // REBUILD_SFX or META_SFX
      if(!fnmatch("*" REBUILD_SFX, object.path, 0) ||
         !fnmatch("*" META_SFX, object.path, 0)) {
         continue;
      }

      queue_rebuild(object);
    }
    closedir(scatter_dir);
  }
}

int main(int argc, char **argv) {

  unsigned int  ht_size           = 1024;
  int           component_rebuild = 0;
  char         *repo_name         = NULL;
  int           threads           = 1;
  int           pod, block, cap;
  int           opt;
  pod = block = cap = -1;
  while((opt = getopt(argc, argv, "hH:c:p:b:r:t:")) != -1) {
    switch (opt) {
    case 'h':
      usage(argv[0]);
      exit(0);
      break;
    case 'H':
      ht_size = strtol(optarg, NULL, 10);
      break;
    case 'c':
      component_rebuild = 1;
      cap = strtol(optarg, NULL, 10);
      break;
    case 'p':
      pod = strtol(optarg, NULL, 10);
      break;
    case 'b':
      block = strtol(optarg, NULL, 10);
      break;
    case 'r':
      repo_name = optarg;
      break;
    case 't':
      threads = strtol(optarg, NULL, 10);
      if(threads < 1) {
        fprintf(stderr, "Invalid number of threads. Defaulting to 1\n");
        threads = 1;
      }
      break;
    default:
      usage(argv[0]);
      exit(-1);
    }
  }

  // load the marfs config.
  if(read_configuration()) {
    fprintf(stderr, "failed to read marfs configuration\n");
    exit(-1);
  }
  if(validate_configuration()) {
    fprintf(stderr, "failed to validate marfs configuration\n");
    exit(-1);
  }


  stats.rebuild_failures  = 0;
  stats.rebuild_successes = 0;
  stats.total_objects     = 0;
  stats.repo_list         = NULL;

  ht_init(&rebuilt_objects, ht_size);
  start_threads(threads);

  if(component_rebuild) {
    if(pod == -1 || block == -1 || cap == -1) {
      fprintf(stderr, "Please specify all options -c -r -p and -b\n");
      usage(argv[0]);
      exit(-1);
    }
    rebuild_component(repo_name, pod, block, cap);
  }
  else {
    if(optind >= argc) {
      fprintf(stderr, "Too few arguments\n");
      usage(argv[0]);
      exit(-1);
    }
    int index;
    for(index = optind; index < argc; index++) {

      DIR *log_dir = opendir(argv[index]);
      if(!log_dir) {
        fprintf(stderr, "Could not open log dir %s: %s\n",
                argv[index], strerror(errno));
        exit(-1);
      }

      struct dirent *log_dirent;
      while((log_dirent = readdir(log_dir))) {
        if(log_dirent->d_name[0] == '.') {
          continue; // easy way to avoid reading '.' and '..'
        }

        char log_subdir[PATH_MAX];
        snprintf(log_subdir, PATH_MAX, "%s/%s", argv[index], log_dirent->d_name);

        process_log_subdir(log_subdir);
      }

      closedir(log_dir);
    }
  }

  stop_workers();

  print_stats();

  return 0;
}
