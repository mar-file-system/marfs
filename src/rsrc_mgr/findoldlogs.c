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

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "hash/hash.h"
#include "rsrc_mgr/findoldlogs.h"
#include "rsrc_mgr/loginfo.h"
#include "rsrc_mgr/parse_program_args.h"

#define OLDLOG_PREALLOC 16  // pre-allocate space for 16 logfiles in the oldlogs hash table (double from there, as needed)

static void close_dirlist(DIR **dirlist) {
    for(int i = 3; i > 0; i--) {
        closedir(dirlist[i - 1]);
        dirlist[i - 1] = NULL; // just to be explicit about clearing this value
    }
}

/**
 * Populate the current rank's oldlogs HASH_TABLE
 * @param rmanstate* rman : State of this rank
 * @param const char* scanroot : Logroot, below which to scan
 * @return int : Zero on success, or -1 on failure
 */
int findoldlogs(rmanstate* rman, const char* scanroot, time_t skipthresh) {
   // construct a HASH_TABLE to hold old logfile references
   HASH_NODE* lognodelist = calloc(rman->nscount, sizeof(struct hash_node_struct));
   size_t pindex = 0;
   while (pindex < rman->nscount) {
      lognodelist[pindex].name = strdup((rman->nslist[pindex])->idstr);
      // node weight should be pre-zeroed by calloc()
      loginfo* newinfo = calloc(1, sizeof(loginfo));
      lognodelist[pindex].content = newinfo;
      newinfo->nsindex = pindex;
      // logcount and requests are pre-zeroed by calloc()
      pindex++;
   }

   if ((rman->oldlogs = hash_init(lognodelist, rman->nscount, 1)) == NULL) {
      LOG(LOG_ERR, "Failed to initialize old logfile hash table\n");
      while (pindex > 0) {
          pindex--;
          free(lognodelist[pindex].name);
          free(lognodelist[pindex].content);
      }
      free(lognodelist);
      return -1;
   }

   // NOTE -- Once the table is initialized, this func won't bother to free it on error.
   //         That is now the caller's responsibility.
   DIR*  dirlist[3] = {0};
   workrequest request = {
      .type = RLOG_WORK,
      .nsindex = 0,
      .refdist = 0,
      .iteration = {0},
      .ranknum = 0
   };

   // open the 'scanroot'
   dirlist[0] = opendir(scanroot);
   if (dirlist[0] == NULL) {
      LOG(LOG_ERR, "Failed to open logging root \"%s\" for scanning\n", scanroot);
      return -1;
   }
   size_t logcount = 0; // per iteration log count
   size_t logdepth = 1;
   char itercheck = 0;
   loginfo* linfo = NULL; // for caching a target location for new log request info
   while (logdepth) {
      if (logdepth == 1) { // identify the iteration to tgt
         if (rman->execprevroot) {
            // check for potential exit
            if (itercheck) {
               LOG(LOG_INFO, "Preparing to exit, after completing scan of prevexec iteration: \"%s/%s\"\n", scanroot, rman->iteration);
               request.iteration[0] = '\0';
               logdepth--;
               continue;
            }

            // note a pass over this section, so we never repeat
            itercheck = 1;

            // only targeting a matching iteration
            if (snprintf(request.iteration, ITERATION_STRING_LEN, "%s", rman->iteration) < 1) {
               LOG(LOG_ERR, "Failed to populate request with running iteration \"%s\"\n", rman->iteration);
               close_dirlist(dirlist);
               return -1;
            }
         }
         else {
            errno = 0; /* readdir(3) */

            // scan through the root dir, looking for other iterations
            struct dirent* entry;
            while ((entry = readdir(dirlist[0]))) {
               // ignore '.'-prefixed entries
               if (strncmp(entry->d_name, ".", 1) == 0) {
                   continue;
               }

               // ignore the running iteration name, as that will cause all kinds of problems
               if (strncmp(entry->d_name, rman->iteration, ITERATION_STRING_LEN) == 0) {
                  LOG(LOG_INFO, "Skipping active iteration path: \"%s\"\n", rman->iteration);
                  continue;
               }

               // all other entries are assumed to be valid iteration targets
               if (snprintf(request.iteration, ITERATION_STRING_LEN, "%s", entry->d_name) >= ITERATION_STRING_LEN) {
                  LOG(LOG_ERR, "Failed to populate request string for old iteration: \"%s\"\n", entry->d_name);
                  close_dirlist(dirlist);
                  return -1;
               }

               break;
            }

            if (errno) {
               const int err = errno;
               LOG(LOG_ERR, "Failed to read log root dir: \"%s\" (%s)\n", scanroot, strerror(err));
               close_dirlist(dirlist);
               return -1;
            }

            if (entry == NULL) {
               // no iterations remain, we are done
               closedir(dirlist[0]);
               dirlist[0] = NULL; // just to be explicit about clearing this value
               request.iteration[0] = '\0';
               logdepth--;
               LOG(LOG_INFO, "Preparing to exit, after completing scan of previous iterations under \"%s\"\n", scanroot);
               continue;
            }
         }

         // open the dir of the matching iteration
         int newfd = openat(dirfd(dirlist[0]), request.iteration, O_DIRECTORY | O_RDONLY, 0);
         if (newfd < 0) {
            const int err = errno;
            LOG(LOG_ERR, "Failed to open log root for iteration: \"%s\" (%s)\n", request.iteration, strerror(err));
            close_dirlist(dirlist);
            return -1;
         }

         // check if program args are compatible with this run
         if (!rman->execprevroot) {
            struct stat stval; // for checking the age of previous iteration files
            int sumfd = openat(newfd, SUMMARY_FILENAME, O_RDONLY, 0);
            if (sumfd < 0) {
               const int err = errno;
               LOG(LOG_WARNING, "Failed to open summary log for old iteration: \"%s\" (%s)\n",
                    request.iteration, strerror(err));

               // stat the root of this iteration
               if (fstat(newfd, &stval)) {
                  const int err = errno;
                  LOG(LOG_ERR, "Failed to stat root of old iteration: \"%s\" (%s)\n",
                                request.iteration, strerror(err));
                  close(newfd);
                  close_dirlist(dirlist);
                  return -1;
               }

               // check if this iteration is old enough for us to feel comfortable running
               if (stval.st_mtime > skipthresh) {
                  fprintf(stderr, "ERROR: Found recent iteration with possible conflicting operations (no summary logfile): \"%s\"\n",
                                   request.iteration);
                  close(newfd);
                  close_dirlist(dirlist);
                  return -1;
               }

               // otherwise, assume we can safely skip over this iteration dir
               printf("Skipping over previous iteration lacking a summary logfile: \"%s\"\n", request.iteration);
               continue;
            }

            // stat the summary logfile
            if (fstat(sumfd, &stval)) {
               const int err = errno;
               LOG(LOG_ERR, "Failed to stat summary logfile of old iteration: \"%s\" (%s)\n",
                             request.iteration, strerror(err));
               close(sumfd);
               close(newfd);
               close_dirlist(dirlist);
               return -1;
            }

            // for runs actually modifying FS content (non-quota-only runs),
            //    check if this iteration is old enough for us to feel comfortable running
            if ((rman->gstate.thresh.gcthreshold  ||  rman->gstate.thresh.repackthreshold  ||
                   rman->gstate.thresh.rebuildthreshold  ||  rman->gstate.thresh.cleanupthreshold)
                  &&  stval.st_mtime > rman->gstate.thresh.cleanupthreshold) {
               fprintf(stderr, "ERROR: Found recent iteration with possible conflicting operations: \"%s\"\n", request.iteration);
               close(newfd);
               close_dirlist(dirlist);
               return -1;
            }

            // open a file stream for the summary logfile
            FILE* summaryfile = fdopen(sumfd, "r");
            if (summaryfile == NULL) {
               const int err = errno;
               LOG(LOG_ERR, "Failed to open a file stream for summary log for old iteration: \"%s\" (%s)\n",
                    request.iteration, strerror(err));
               close(sumfd);
               close(newfd);
               close_dirlist(dirlist);
               return -1;
            }

            // parse args into a temporary state struct, for comparison
            rmanstate tmpstate;
            memset(&tmpstate, 0, sizeof(tmpstate));
            tmpstate.config = rman->config;
            int parseargres = parse_program_args(&tmpstate, summaryfile);
            fclose(summaryfile);
            if (parseargres > 0) {
               printf("Skipping over previous iteration with incompatible config: \"%s\"\n", request.iteration);
               continue;
            }

            if (parseargres) {
               const int err = errno;
               LOG(LOG_ERR, "Failed to parse arguments from summary log of old iteration: \"%s\" (%s)\n",
                    request.iteration, strerror(err));
               close(newfd);
               close_dirlist(dirlist);
               return -1;
            }

            // check if previous run args are a superset if this run's
            if (tmpstate.gstate.thresh.gcthreshold > rman->gstate.thresh.gcthreshold  ||
                 tmpstate.gstate.thresh.repackthreshold > rman->gstate.thresh.repackthreshold  ||
                 tmpstate.gstate.thresh.rebuildthreshold > rman->gstate.thresh.rebuildthreshold  ||
                 tmpstate.gstate.thresh.cleanupthreshold > rman->gstate.thresh.cleanupthreshold  ||
                 tmpstate.gstate.dryrun != rman->gstate.dryrun) {
               printf("Skipping over previous iteration with incompatible arguments: \"%s\"\n", request.iteration);
               continue;
            }
         }

         dirlist[1] = fdopendir(newfd);
         if (dirlist[1] == NULL) {
            const int err = errno;
            LOG(LOG_ERR, "Failed to open DIR reference for previous iteration log root: \"%s\" (%s)\n",
                 request.iteration, strerror(err));
            close(newfd);
            close_dirlist(dirlist);
            return -1;
         }

         logdepth++;
         LOG(LOG_INFO, "Entered iteration dir: \"%s\"\n", request.iteration);
         logcount = 0;
      }
      else if (logdepth == 2) { // identify the NS subdirs
         errno = 0; /* readdir(3) */

         // scan through the iteration dir, looking for namespaces
         struct dirent* entry;
         while ((entry = readdir(dirlist[1]))) {
            // ignore '.'-prefixed entries
            if (strncmp(entry->d_name, ".", 1) == 0) { continue; }
            // ignore the summary log
            if (strcmp(entry->d_name, SUMMARY_FILENAME) == 0) { continue; }
            // all other entries are assumed to be namespaces
            break;
         }

         if (errno) {
            const int err = errno;
            LOG(LOG_ERR, "Failed to read iteration dir: \"%s\" (%s)\n", request.iteration, strerror(err));
            close_dirlist(dirlist);
            return -1;
         }

         if (entry == NULL) {
            // no Namespaces remain, we are done
            if (logcount == 0  &&  rman->execprevroot == NULL) {
               // if this is a dead iteration, with no logs remaining, just delete it
               if (unlinkat(dirfd(dirlist[1]), SUMMARY_FILENAME, 0)) {
                  fprintf(stderr, "WARNING: Failed to remove summary log of previous iteration: \"%s\"\n", request.iteration);
               }
               else if (unlinkat(dirfd(dirlist[0]), request.iteration, AT_REMOVEDIR)) {
                  fprintf(stderr, "WARNING: Failed to remove previous iteration log root: \"%s\"\n", request.iteration);
               }
            }

            closedir(dirlist[1]);
            dirlist[1] = NULL; // just to be explicit about clearing this value
            request.nsindex = 0;
            logdepth--;
            LOG(LOG_INFO, "Finished processing iteration dir: \"%s\"\n", request.iteration);
            if (logcount) {
               printf("This run will incorporate %zu logfiles from: \"%s/%s\"\n", logcount, scanroot, request.iteration);
            }
            continue;
         }

         // check what index this NS corresponds to
         char* oldnsid = strdup(entry->d_name);
         if (oldnsid == NULL) {
            LOG(LOG_ERR, "Failed to duplicate dirent name: \"%s\"\n", entry->d_name);
            close_dirlist(dirlist);
            return -1;
         }

         char* tmpparse = oldnsid;
         while (*tmpparse != '\0') { if (*tmpparse == '#') { *tmpparse = '/'; } tmpparse++; }
         HASH_NODE* lnode = NULL;
         if (hash_lookup(rman->oldlogs, oldnsid, &lnode)) {
            // if we can't map this to a NS entry, just ignore it
            fprintf(stderr, "WARNING: Encountered resource logs from a previoius iteration (\"%s\")\n"
                             "         associated with an unrecognized namespace (\"%s\").\n"
                             "         These logfiles will be ignored by the current run.\n"
                             "         See \"%s/%s/%s\"\n", request.iteration, entry->d_name, scanroot, request.iteration, entry->d_name);
            free(oldnsid);
         }
         else {
            free(oldnsid);
            // pull the NS index value from the corresponding hash node
            linfo = (loginfo*)(lnode->content);
            request.nsindex = linfo->nsindex;
            // open the corresponding subdir and traverse into it
            int newfd = openat(dirfd(dirlist[1]), entry->d_name, O_DIRECTORY | O_RDONLY, 0);
            if (newfd < 0) {
               const int err = errno;
               LOG(LOG_ERR, "Failed to open log NS subdir: \"%s/%s/%s\" (%s)\n", scanroot, request.iteration, entry->d_name, strerror(err));
               close_dirlist(dirlist);
               return -1;
            }

            dirlist[2] = fdopendir(newfd);
            if (dirlist[2] == NULL) {
               const int err = errno;
               LOG(LOG_ERR, "Failed to open DIR reference for previous iteration log root: \"%s\" (%s)\n",
                    request.iteration, strerror(err));
               close(newfd);
               close_dirlist(dirlist);
               return -1;
            }

            logdepth++;
            continue;
         }
      }
      else { // identify the rank to tgt
         errno = 0; /* readdir(3) */

         // scan through the NS dir, looking for individual logfiles
         struct dirent* entry;
         while ((entry = readdir(dirlist[2]))) {
            // ignore '.'-prefixed entries
            if (strncmp(entry->d_name, ".", 1) == 0) { continue; }
            // ignore error-prefixed entries
            if (strncmp(entry->d_name, ERROR_LOG_PREFIX, strlen(ERROR_LOG_PREFIX)) == 0) { continue; }
            // all other entries are assumed to be logfiles
            break;
         }

         if (errno) {
            const int err = errno;
            LOG(LOG_ERR, "Failed to read NS dir: \"%s/%s/%s\" (%s)\n", scanroot, request.iteration,
                 (rman->nslist[request.nsindex])->idstr, strerror(err));
            close_dirlist(dirlist);
            return -1;
         }

         if (entry == NULL) {
            // no iterations remain, we are done
            closedir(dirlist[2]);
            dirlist[2] = NULL; // just to be explicit about clearing this value
            request.ranknum = 0;
            logdepth--;
            continue;
         }

         // identify the rank number associated with this logfile
         char* fparse = entry->d_name;
         while (*fparse != '\0') {
            if (*fparse == '-') { fparse++; break; }
            fparse++;
         }

         char* endptr = NULL;
         unsigned long long parseval = strtoull(fparse, &endptr, 10);
         if (endptr == NULL  ||  *endptr != '\0'  ||  *fparse == '\0'  ||  parseval >= SIZE_MAX  ||  parseval >= ULLONG_MAX) {
            fprintf(stderr, "WARNING: Failed to identify rank number associated with logfile \"%s/%s/%s/%s\"\n"
                             "         The logfile will be skipped\n",
                             scanroot, request.iteration, (rman->nslist[request.nsindex])->idstr, entry->d_name);
            continue;
         }

         request.ranknum = (size_t)parseval;

         // populate the HASH_TABLE entry with the newly completed request
         if (linfo->logcount % OLDLOG_PREALLOC == 0) {
            // logs are allocated in groups of OLDLOG_PREALLOC, so we can use modulo to check for overflow
            workrequest* newrequests = realloc(linfo->requests, (linfo->logcount + OLDLOG_PREALLOC) * sizeof(*newrequests));
            if (newrequests == NULL) {
               LOG(LOG_ERR, "Failed to allocate a list of old log requests with length %zu\n", linfo->logcount + OLDLOG_PREALLOC);
               close_dirlist(dirlist);
               return -1;
            }

            linfo->requests = newrequests;
         }

         LOG(LOG_INFO, "Detected old logfile for iteration \"%s\", NS \"%s\", Rank %zu\n",
              request.iteration, (rman->nslist[request.nsindex])->idstr, request.ranknum);
         linfo->requests[linfo->logcount] = request; // NOTE -- I believe this assignment is safe,
                                                     //         because .iteration is a static char array
         linfo->logcount++;
         logcount++;
      }
   }

   LOG(LOG_INFO, "Finished scan of old resource logs below \"%s\"\n", scanroot);
   return 0;
}
