#ifndef _RESOURCETHREADS_H
#define _RESOURCETHREADS_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "rsrc_mgr/resourceinput.h"
#include "rsrc_mgr/resourceprocessing.h"
#include "thread_queue/thread_queue.h"

#define MAX_STR_BUFFER 1024

typedef struct {
   // Required MarFS Values
   marfs_position  pos;

   // Operation Values
   char            dryrun;
   thresholds      thresh;
   char            lbrebuild;
   ne_location     rebuildloc;

   // Thread Values
   RESOURCEINPUT   rinput;
   RESOURCELOG     rlog;
   REPACKSTREAMER  rpst;
   unsigned int    numprodthreads;
   unsigned int    numconsthreads;
} rthread_global_state;

typedef struct {
   // universal thread state
   unsigned int              tID;  // thread ID
   char               fatalerror;  // flag indicating some form of fatal thread error
   char errorstr[MAX_STR_BUFFER];  // error string buffer
   rthread_global_state*  gstate;  // global state reference
   // producer thread state
   MDAL_SCANNER  scanner;  // MDAL reference scanner ( if open )
   char*         rdirpath;
   streamwalker  walker;
   opinfo*       gcops;
   opinfo*       repackops;
   opinfo*       rebuildops;
   // producer thread totals
   size_t        streamcount;
   streamwalker_report report;
} rthread_state;

/**
 * Resource thread initialization ( producers and consumers )
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_init( unsigned int tID, void* global_state, void** state );

/**
 * Resource thread consumer behavior
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_all_consumer( void** state, void** work_todo );

/**
 * Resource thread producer behavior
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_all_producer( void** state, void** work_tofill );
int rthread_quota_producer( void** state, void** work_tofill );

/**
 * Resource thread termination ( producers and consumers )
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
void rthread_term( void** state, void** prev_work, TQ_Control_Flags flg );

#endif // _RESOURCETHREADS_H
