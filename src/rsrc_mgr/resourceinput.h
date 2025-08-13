#ifndef _RESOURCEINPUT_H
#define _RESOURCEINPUT_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <pthread.h>
#include <stdint.h>

#include "config/config.h"
#include "mdal/mdal.h"
#include "rsrc_mgr/common.h"
#include "rsrc_mgr/resourcelog.h"

typedef struct {
   // synchronization and access control
   pthread_mutex_t  lock;     // no simultaneous access
   pthread_cond_t   complete; // signaled when all work is handed out
   pthread_cond_t   updated;  // signaled when new work is added, or input state is otherwise changed
   char             prepterm; // flag value set when clients should prepare for termination
   size_t           clientcount; // count of clients still running
   // previous log info
   RESOURCELOG      rlog;
   // state info
   marfs_ns*        ns;
   MDAL_CTXT        ctxt;
   // reference info
   ssize_t          refindex;
   ssize_t          refmax;
}*RESOURCEINPUT;

/**
 * Initialize a given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be initialized
 * @param marfs_ns* ns : MarFS NS associated with the new resourceinput
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the previous NS
 *                         NOTE -- caller should never modify this again
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_init( RESOURCEINPUT* resourceinput, marfs_position* pos, size_t clientcount );

/**
 * Update the given RESOURCEINPUT structure to use the given logpath as a new input source
 * @param RESOURCEINPUT* resourceinput : Resourceinput to update
 * @param const char* logpath : Path of the new (RECORD) resourcelog
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_setlogpath( RESOURCEINPUT* resourceinput, const char* logpath );

/**
 * Set the active reference dir range of the given resourceinput
 * NOTE -- this will fail if the range has not been fully traversed
 * @param RESOURCEINPUT* resourceinput : Resourceinput to have its range set
 * @param ssize_t start : Starting index of the reference dir range
 * @param ssize_t end : Ending index of the reference dir range ( non-inclusive )
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_setrange( RESOURCEINPUT* resourceinput, size_t start, size_t end );

/**
 * Get the next ref index to be processed from the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to get the next ref index from
 * @param opinfo** nextop : Reference to be populated with a new op from an input logfile
 * @param MDAL_SCANNER* scanner : Reference to be populated with a new reference scanner
 * @param char** rdirpath : Reference to be populated with the path of a newly opened reference dir
 * @return int : Zero, if no inputs are currently available;
 *               One, if an input was produced;
 *               Ten, if the caller should prepare for termination ( resourceinput is preparing to be closed )
 */
int resourceinput_getnext( RESOURCEINPUT* resourceinput, opinfo** nextop, MDAL_SCANNER* scanner, char** rdirpath );

/**
 * Destroy all available inputs and signal threads to prepare or for imminent termination
 * NOTE -- this is useful for aborting, if a thread has hit a fatal error
 * @param RESOURCEINPUT* resourceinput : Resourceinput to purge
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_purge( RESOURCEINPUT* resourceinput );

/**
 * Wait for the given resourceinput to have available inputs, or for immenent termination
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforupdate( RESOURCEINPUT* resourceinput );

/**
 * Wait for the given resourceinput to be terminated ( synchronizing like this ensures ALL work gets enqueued )
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforterm( RESOURCEINPUT* resourceinput );

/**
 * Wait for all inputs in the given resourceinput to be consumed
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforcomp( RESOURCEINPUT* resourceinput );

/**
 * Terminate the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_term( RESOURCEINPUT* resourceinput );

/**
 * Destroy the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_destroy( RESOURCEINPUT* resourceinput );

#endif
