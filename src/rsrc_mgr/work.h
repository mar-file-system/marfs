#ifndef _RESOURCE_MANAGER_WORK_H
#define _RESOURCE_MANAGER_WORK_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "rsrc_mgr/common.h"
#include "rsrc_mgr/resourceprocessing.h"
#include "rsrc_mgr/rmanstate.h"

#define MAX_ERROR_BUFFER MAX_STR_BUFFER + 100  // define our error strings as slightly larger than the error message itself

typedef enum {
   RLOG_WORK,      // request to process an existing resource log (either previous dry-run or dead run pickup)
   NS_WORK,        // request to process a portion of a NS
   COMPLETE_WORK,  // request to complete outstanding work (quiesce all threads and close all streams)
   TERMINATE_WORK, // request to terminate the rank
   ABORT_WORK      // request to abort all processing and terminate
} worktype;

typedef struct {
   worktype  type;
   // NS target info
   size_t    nsindex;
   size_t    refdist;
   // Log target info
   char      iteration[ITERATION_STRING_LEN];
   size_t    ranknum;
} workrequest;

typedef struct {
   workrequest request;
   // Work results
   char                 haveinfo;
   streamwalker_report  report;
   operation_summary    summary;
   char                 errorlog;
   char                 fatalerror;
   char                 errorstr[MAX_ERROR_BUFFER];
} workresponse;

int handlerequest(rmanstate* rman, workrequest* request, workresponse* response);
int handleresponse(rmanstate* rman, size_t ranknum, workresponse* response, workrequest* request);

#endif
