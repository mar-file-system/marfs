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
};

void usage(const char *program) {
  printf("Usage:\n");
  printf("%s <path/to/log/file>\n", program);
  printf("\t<path/to/log/file> should be the path to the file where the MC DAL\n"
         "\t                   has logged objects it thinks are degraded.\n");
}

int nextobject(FILE *log, struct object_file *object) {
  char *line = NULL;
  size_t len = 0;
  ssize_t line_len = getline(&line, &len, log);
  if(line_len == -1) {
    return -1;
  }
  
  int matches = sscanf(line, MC_DEGRADED_LOG_FORMAT,
         &object->path, &object->n, &object->e, &object->start_block);

  if(matches != 4) {
    fprintf(stderr, "failed to parse log entry\n");
    return -1;
  }

  free(line);
  return 0;
}

int main(int argc, char **argv) {

  if(argc != 2) {
    usage(argv[0]);
    exit(1);
  }

  struct rebuild_stats {
    int rebuild_failures;
    int rebuild_successes;
    int total_objects;
  } stats;

  stats.rebuild_failures  = 0;
  stats.rebuild_successes = 0;
  stats.total_objects     = 0;
  
  DIR *log_dir = opendir(argv[1]);
  if(!log_dir) {
    perror("Could not open log dir");
    exit(-1);
  }

  struct dirent *log_dirent;
  while((log_dirent = readdir(log_dir))) {
    if (log_dirent == NULL) {
      if (errno == EBADF) {
        perror("could not read from log dir");
        exit(-1);
      }
      break; // we're done.
    }
    else if(log_dirent->d_name[0] == '.') {
      continue; // easy way to avoid reading '.' and '..'
    }

    char log_file_path[PATH_MAX];
    snprintf(log_file_path, PATH_MAX, "%s/%s", argv[1], log_dirent->d_name);

    // To preserve concurrent operation with marfs processes that may be
    // logging to the file, and may be holding a file descriptor open in
    // DAL, we immediately apped a marker to the file indicating where
    // the rebuild will stop. We then seed back from that marker until we
    // reach either the begining of the file, or another marker.
#if DEBUG
      printf("opening log file %s\n", log_file_path);
#endif
    
    FILE *degraded_object_file = fopen(log_file_path, "r");
    if(degraded_object_file == NULL) {
      perror("fopen()");
      exit(-1);
    }
    struct object_file object;
    while(nextobject(degraded_object_file, &object) != -1) {
      stats.total_objects++;
      ne_handle object_handle = ne_open(object.path, NE_REBUILD,
                                        object.start_block, object.n, object.e);
      if(object_handle == NULL) {
        perror("ne_open()");
        // XXX: Do we want to give up here, or just ignore the error and
        //      continue rebuilding other objects?
        fprintf(stderr, "object %s could not be opened "
                "(start: %d, n: %d, e: %d)\n", object.path,
                object.start_block, object.n, object.e);
        exit(-1);
      }
      
      int rebuild_result = ne_rebuild(object_handle);
      if(rebuild_result == 0) {
        stats.rebuild_successes++;
      }
      else {
        stats.rebuild_failures++;
        fprintf(stderr, "Failed to rebuild %s\n", object.path);
      }
      
      int error = ne_close(object_handle);
      if(error < 0) {
        perror("ne_close()");
        fprintf(stderr, "object %s could not be closed "
                "(start: %d, n: %d, e: %d)\n", object.path,
                object.start_block, object.n, object.e);
        exit(-1); // XXX: see note above.
      }
    }

    fclose(degraded_object_file);
  }
  
  closedir(log_dir);

  // For now we print this every time. Should maybe add a cl option to
  // make the program print a report or not.
  printf("==== Rebuild Summary ====\n");
  printf("Number of objects:   %*d\n", 10, stats.total_objects);
  printf("Successful rebuilds: %*d\n", 10, stats.rebuild_successes);
  printf("Failed rebuilds:     %*d\n", 10, stats.rebuild_failures);
  
  return 0;
}