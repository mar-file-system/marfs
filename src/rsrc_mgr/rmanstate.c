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

#include <stdio.h>

#include "rsrc_mgr/findoldlogs.h"
#include "rsrc_mgr/loginfo.h"
#include "rsrc_mgr/parse_program_args.h"
#include "rsrc_mgr/rmanstate.h"
#include "rsrc_mgr/summary_log_setup.h"
#include "rsrc_mgr/work.h"

#define DEFAULT_PRODUCER_COUNT 16
#define DEFAULT_CONSUMER_COUNT 32

static int find_namespaces(rmanstate *rman, const char *ns_path, const int recurse) {
   // Identify our target NS
   marfs_position pos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if (config_establishposition(&pos, rman->config)) {
       fprintf(stderr, "ERROR: Failed to establish a MarFS root NS position\n");
       return -1;
   }

   char* nspathdup = strdup(ns_path); // this is neccessary due to how config_traverse works
   if (nspathdup == NULL) {
      fprintf(stderr, "ERROR: Failed to duplicate NS path string: \"%s\"\n",
              ns_path);
      return -1;
   }

   const int travret = config_traverse(rman->config, &pos, &nspathdup, 1);

   free(nspathdup);

   if (travret < 0) {
      if (rman->ranknum == 0)
         fprintf(stderr, "ERROR: Failed to identify NS path target: \"%s\"\n",
                 ns_path);
      return -1;
   }
   if (travret) {
      if (rman->ranknum == 0) {
         fprintf(stderr, "ERROR: Path target is not a NS, but a subpath of depth %d: \"%s\"\n",
                 travret, ns_path);
      }
      return -1;
   }

   // Generate our NS list
   marfs_ns* curns = pos.ns;
   rman->nscount = 1;
   rman->nslist = malloc(sizeof(marfs_ns*));
   *(rman->nslist) = pos.ns;
   while (curns) {
      // we can use hash_iterate, as this is guaranteed to be the only proc using this config struct
      HASH_NODE* subnode = NULL;
      int iterres = 0;
      if (curns->subspaces && recurse) {
         iterres = hash_iterate(curns->subspaces, &subnode);
         if (iterres < 0) {
             fprintf(stderr, "ERROR: Failed to iterate through subspaces of \"%s\"\n", curns->idstr);
             return -1;
         }
         else if (iterres) {
            marfs_ns* subspace = (marfs_ns*)(subnode->content);
            // only process non-ghost subspaces
            if (subspace->ghtarget == NULL) {
               LOG(LOG_INFO, "Adding subspace \"%s\" to our NS list\n", subspace->idstr);
               // note and enter the subspace
               rman->nscount++;
               // yeah, this is very inefficient; but we're only expecting 10s to 1000s of elements
               marfs_ns** newlist = realloc(rman->nslist, sizeof(marfs_ns*) * rman->nscount);
               if (newlist == NULL) {
                   fprintf(stderr, "ERROR: Failed to allocate NS list of length %zu\n", rman->nscount);
                   return -1;
               }
               rman->nslist = newlist;
               rman->nslist[rman->nscount - 1] = subspace;
               curns = subspace;
               continue;
            }
         }
      }
      if (iterres == 0) {
         // check for completion condition
         if (curns == pos.ns) {
            // iteration over the original NS target means we're done
            curns = NULL;
         }
         else {
            curns = curns->pnamespace;
         }
      }
   }

   // abandon our current position
   if (config_abandonposition(&pos)) {
       fprintf(stderr, "WARNING: Failed to abandon MarFS traversal position\n");
       return -1;
   }

   return 0;
}

int read_last_log(rmanstate *rman, const time_t skip) {
   // check for previous run execution
   if (rman->execprevroot) {
      // open the summary log of that run
      size_t alloclen = strlen(rman->execprevroot) + 1 + strlen(rman->iteration) + 1 + strlen(SUMMARY_FILENAME) + 1;
      char* sumlogpath = malloc(sizeof(char) * alloclen);
      snprintf(sumlogpath, alloclen, "%s/%s/%s", rman->execprevroot, rman->iteration, SUMMARY_FILENAME);

      FILE* sumlog = fopen(sumlogpath, "r");
      if (sumlog == NULL) {
         const int err = errno;
         fprintf(stderr, "ERROR: Failed to open previous run's summary log: \"%s\" (%s)\n",
                 sumlogpath, strerror(err));
         return -1;
      }

      const int ppa = parse_program_args(rman, sumlog);
      fclose(sumlog);

      if (ppa != 0) {
         fprintf(stderr, "ERROR: Failed to parse previous run info from summary log: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }
      free(sumlogpath);

      if (rman->quotas) {
         if (rman->ranknum == 0) {
            fprintf(stderr, "WARNING: Ignoring quota processing for execution of previous run\n");
         }
         rman->quotas = 0;
      }

      if (rman->gstate.dryrun == 0) {
         if (rman->ranknum == 0) {
            fprintf(stderr, "ERROR: Cannot pick up execution of a non-'dry-run'\n");
         }
         return -1;
      }

      // incorporate logfiles from the previous run
      if (rman->ranknum == 0 && findoldlogs(rman, rman->execprevroot, 0)) {
         fprintf(stderr, "ERROR: Failed to identify previous run's logfiles: \"%s\"\n",
                 rman->execprevroot);
         return -1;
      }
   }
   else if (rman->gstate.dryrun == 0 && rman->ranknum == 0) {
      // otherwise, scan for and incorporate logs from previous modification runs
      if (findoldlogs(rman, rman->logroot, skip)) {
         fprintf(stderr, "ERROR: Failed to scan for previous iteration logs under \"%s\"\n",
                 rman->logroot);
         return -1;
      }
   }

   return 0;
}

void rmanstate_init(rmanstate *rman, int rank, int rankcount) {
   memset(rman, 0, sizeof(*rman));
   rman->gstate.numprodthreads = DEFAULT_PRODUCER_COUNT;
   rman->gstate.numconsthreads = DEFAULT_CONSUMER_COUNT;
   rman->gstate.rebuildloc.pod = -1;
   rman->gstate.rebuildloc.cap = -1;
   rman->gstate.rebuildloc.scatter = -1;
   rman->ranknum = (size_t)rank;
   rman->totalranks = (size_t)rankcount;
   rman->workingranks = 1;
   if (rankcount > 1) {
       rman->workingranks = rman->totalranks - 1;
   }
   rman->logroot = DEFAULT_LOG_ROOT;
}

int rmanstate_complete(rmanstate *rman, const char *config_path, const char *ns_path,
                       const time_t log_skip, const int recurse, pthread_mutex_t *erasuremutex) {
   if ((rman->config = config_init(config_path, erasuremutex)) == NULL) {
       fprintf(stderr, "ERROR: Failed to initialize MarFS config: \"%s\"\n", config_path);
       return -1;
   }

   if (find_namespaces(rman, ns_path, recurse) != 0) {
       fprintf(stderr, "Error: Failed to find namespace\n");
       return -1;
   }

   rman->distributed = calloc(sizeof(size_t), rman->nscount);
   rman->terminatedworkers = calloc(sizeof(char), rman->totalranks);
   rman->walkreport = calloc(sizeof(*rman->walkreport), rman->nscount);
   rman->logsummary = calloc(sizeof(*rman->logsummary), rman->nscount);

   if (read_last_log(rman, log_skip) != 0) {
       fprintf(stderr, "ERROR: Failed to open previous run's log\n");
       return -1;
   }

   if (summary_log_setup(rman, 1) != 0) {
       fprintf(stderr, "ERROR: Failed to set up summary\n");
       return -1;
   }

   return 0;
}

void rmanstate_fini(rmanstate* rman, char abort) {
    free(rman->preservelogtgt);
    if ((void *) rman->logroot != (void *) DEFAULT_LOG_ROOT) {
        free(rman->logroot);
    }
    free(rman->execprevroot);

    if (rman->summarylog) {
        fclose(rman->summarylog);
    }

    if (rman->tq) {
       if (!abort) {
          LOG(LOG_ERR, "Encountered active TQ with no abort condition specified\n");
          fprintf(stderr, "Encountered active TQ with no abort condition specified\n");
          rman->fatalerror = 1;
       }
       tq_set_flags(rman->tq, TQ_ABORT);
       if (rman->gstate.rinput) {
          resourceinput_purge(&rman->gstate.rinput);
          resourceinput_term(&rman->gstate.rinput);
       }

       // gather all thread status values
       rthread_state* tstate = NULL;
       while (tq_next_thread_status(rman->tq, (void**)&tstate) > 0) {
           // verify thread status
           free(tstate);
       }
       tq_close(rman->tq);
    }

    if (rman->gstate.rpst) {
       repackstreamer_abort(rman->gstate.rpst);
    }

    if (rman->gstate.rlog) {
       resourcelog_abort(&rman->gstate.rlog);
    }

    if (rman->gstate.rinput) {
       resourceinput_destroy(&rman->gstate.rinput);
    }

    if (rman->gstate.pos.ns) {
       config_abandonposition(&rman->gstate.pos);
    }

    free(rman->logsummary);
    free(rman->walkreport);
    free(rman->terminatedworkers);
    free(rman->distributed);
    free(rman->nslist);

    if (rman->oldlogs) {
       HASH_NODE* resnode = NULL;
       size_t ncount = 0;
       if (hash_term(rman->oldlogs, &resnode, &ncount) == 0) {
          // free all subnodes and requests
          for (size_t nindex = 0; nindex < ncount; nindex++) {
             loginfo* linfo = (loginfo*)resnode[nindex].content;
             if (linfo) {
                free(linfo->requests);
                free(linfo);
             }
             free(resnode[nindex].name);
          }

          free(resnode); // these were allocated in one block, and thus require only one free()
       }
    }

    if (rman->config) {
        config_term(rman->config);
    }

    memset(rman, 0, sizeof(*rman));
}
