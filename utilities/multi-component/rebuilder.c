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
 * This is a simple first cut at an offline rebuild utility for
 * multi-component storage that reads all the files int the directory
 * specified by the user on the command line as degraded object log
 * files. For each object in the files it attempts to rebuild them.
 *
 * Later versions may benefit from linking libmarfs and reading the
 * repo/DAL config.
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

// we absolutely must have multi-component enabled to build this
// program.  This ensures that the necessary symbols are exposed from
// dal.h
#include "dal.h" // MC_MAX_PATH_LEN & MC_DEGRADED_LOG_FORMAT

#include "erasure.h"

struct object_file {
  char path[MC_MAX_PATH_LEN];
  unsigned int n;
  unsigned int e;
  unsigned int start_block;
  int error_pattern;
};

struct rebuild_stats {
  int rebuild_failures;
  int rebuild_successes;
  int total_objects;
} stats;

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
  printf("%s [-H <hash table size>] <degraded log dir> ...\n\n", program);
  printf("  <degraded log dir> should be the path to the directory where the\n"
         "                     MC DAL has logged objects it thinks are degraded.\n"
         "                     You may specify one or more log dirs.\n\n"
         "  <hash table size>  Size of the hash table used to track which\n"
         "                     objects have been rebuilt. A larger hash table\n"
         "                     may provide increased performance when\n"
         "                     rebuilding a large number of objects.\n"
         "                     Defaults to 1024.\n");
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
                       &object->start_block, &object->error_pattern);

  if(matches != 5) {
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

        stats.total_objects++;

        // always add the object to the list of rebuilt objects.
        // Even if we fail to rebuild, we don't want to repeatedly
        // attempt to rebuild.
        ht_insert(&rebuilt_objects, object.path);

        ne_handle object_handle = ne_open(object.path, NE_REBUILD,
                                          object.start_block,
                                          object.n, object.e);
        if(object_handle == NULL) {
          fprintf(stderr, "ERROR: cannot rebuild %s "
                  "(start: %d, n: %d, e: %d). ne_open() failed: %s.\n",
                  object.path, object.start_block, object.n, object.e,
                  strerror(errno));
          stats.rebuild_failures++;
          continue;
        }

        int rebuild_result = ne_rebuild(object_handle);
        if(rebuild_result == 0) {
          stats.rebuild_successes++;
        }
        else {
          stats.rebuild_failures++;
          fprintf(stderr, "ERROR: cannot rebuild %s (start: %d, n: %d, e: %d)."
                  " ne_rebuild() failed: %s.\n", object.path,
                  object.start_block, object.n, object.e, strerror(errno));
        }

        int error = ne_close(object_handle);
        if(error < 0) {
          perror("ne_close()");
          fprintf(stderr, "object %s could not be closed "
                  "(start: %d, n: %d, e: %d)\n", object.path,
                  object.start_block, object.n, object.e);
          exit(-1);
        }
      }

      fclose(degraded_object_file);
    }
    closedir(scatter);
  }
  closedir(dir);
}

int main(int argc, char **argv) {

  unsigned int ht_size = 1024;
  int opt;
  while((opt = getopt(argc, argv, "H:")) != -1) {
    switch (opt) {
    case 'H' :
      ht_size = strtol(optarg, NULL, 10);
      break;
    default:
      usage(argv[0]);
      exit(-1);
    }
  }

  if(optind >= argc) {
    fprintf(stderr, "Expected at least one path argument.\n");
    usage(argv[0]);
    exit(-1);
  }

  stats.rebuild_failures  = 0;
  stats.rebuild_successes = 0;
  stats.total_objects     = 0;

  ht_init(&rebuilt_objects, ht_size);

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
  // For now we print this every time. Should maybe add a cl option to
  // make the program print a report or not.
  printf("==== Rebuild Summary ====\n");
  printf("Number of objects:   %*d\n", 10, stats.total_objects);
  printf("Successful rebuilds: %*d\n", 10, stats.rebuild_successes);
  printf("Failed rebuilds:     %*d\n", 10, stats.rebuild_failures);

  return 0;
}
