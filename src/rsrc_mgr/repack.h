#ifndef _RESOURCE_MANAGER_REPACK_H
#define _RESOURCE_MANAGER_REPACK_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <pthread.h>

#include "datastream/datastream.h"

typedef struct repackstreamer {
   // synchronization and access control
   pthread_mutex_t lock;

   // state info
   size_t streamcount;
   DATASTREAM* streamlist;
   char* streamstatus;
}* REPACKSTREAMER;

/**
 * Initialize a new repackstreamer
 * @return REPACKSTREAMER : New repackstreamer, or NULL on failure
 */
REPACKSTREAMER repackstreamer_init(void);

/**
 * Checkout a repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to checkout from
 * @return DATASTREAM* : Checked out datastream, or NULL on failure
 */
DATASTREAM* repackstreamer_getstream( REPACKSTREAMER repackst );

/**
 * Return a previously checked out repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to return to
 * @param DATASTREAM* stream : Repack datastream to return
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_returnstream( REPACKSTREAMER repackst, DATASTREAM* stream );

/**
 * Terminate the given repackstreamer and close all associated datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to close
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_complete( REPACKSTREAMER repackst );

/**
 * Abort the given repackstreamer, bypassing all locks and releasing all datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to abort
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_abort( REPACKSTREAMER repackst );

#endif
