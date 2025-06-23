#ifndef _RESOURCE_MANAGER_STATE_H
#define _RESOURCE_MANAGER_STATE_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <sys/time.h>

#include "config/config.h"
#include "hash/hash.h"
#include "rsrc_mgr/resourcelog.h"
#include "rsrc_mgr/streamwalker.h"
#include "rsrc_mgr/resourcethreads.h"
#include "thread_queue/thread_queue.h"

typedef struct {
   // Per-Run Rank State
   size_t        ranknum;
   size_t        totalranks;
   size_t        workingranks;

   // Per-Run MarFS State
   marfs_config* config;

   // Old Logfile Progress Tracking
   HASH_TABLE    oldlogs;

   // NS Progress Tracking
   size_t        nscount;
   marfs_ns**    nslist;
   size_t*       distributed;

   // Global Progress Tracking
   char          fatalerror;
   char          nonfatalerror;
   char*         terminatedworkers;
   streamwalker_report* walkreport;
   operation_summary*   logsummary;

   // Thread State
   rthread_global_state gstate;
   TQ_Init_Opts tqopts;          // executable specific function pointers template; other members are ignored
   ThreadQueue tq;

   // Output Logging
   FILE* summarylog;

   // arg reference vals
   char        quotas;
   char        iteration[ITERATION_STRING_LEN];
   char*       execprevroot;
   char*       logroot;
   char*       preservelogtgt;
} rmanstate;

// initialize rmanstate with some default values, but not everything
void rmanstate_init(rmanstate *rman, int rank, int rankcount);

// complete rmanstate initialization
int rmanstate_complete(rmanstate *rman, const char *config_path, const char *ns_path,
                       const time_t log_skip, const int recurse, pthread_mutex_t *erasuremutex);

// destroy the rmanstate
void rmanstate_fini(rmanstate* rman, char abort);

#endif
