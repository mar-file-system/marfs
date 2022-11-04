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

#include "marfs_auto_config.h"
#ifdef DEBUG_RM
#define DEBUG DEBUG_RM
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "resourcethreads"
#include <logging.h>

#include "resourcethreads.h"


//   -------------   RESOURCE INPUT FUNCTIONS    -------------

/**
 * Initialize a given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be initialized
 * @param marfs_ns* ns : MarFS NS associated with the new resourceinput
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the previous NS
 *                         NOTE -- caller should never modify this again
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_init( RESOURCEINPUT* resourceinput, marfs_position* pos, size_t clientcount ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput != NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos == NULL  ||  pos->ns == NULL  ||  pos->ctxt == NULL ) {
      LOG( LOG_ERR, "Received NULL position/NS/CTXT ref\n" );
      errno = EINVAL;
      return -1;
   }
   // allocate a new resourceinput
   RESOURCEINPUT rin = malloc( sizeof( struct resourceinput_struct ) );
   if ( rin == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new resourceinput\n" );
      return -1;
   }
   // initialize resourceinput values
   rin->ns = pos->ns;
   rin->ctxt = pos->ctxt;
   rin->rlog = NULL;
   rin->refmax = 0;
   rin->refindex = 0;
   rin->prepterm = 0;
   rin->clientcount = clientcount;
   if ( pthread_cond_init( &(rin->complete), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize 'complete' condition for new resourceinput\n" );
      free( rin );
      return -1;
   }
   if ( pthread_cond_init( &(rin->updated), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize 'complete' condition for new resourceinput\n" );
      free( rin );
      return -1;
   }
   if ( pthread_mutex_init( &(rin->lock), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize lock for new resourceinput\n" );
      pthread_cond_destroy( &(rin->complete) );
      free( rin );
      return -1;
   }
   *resourceinput = rin;
   return 0;
}

/**
 * Update the given RESOURCEINPUT structure to use the given logpath as a new input source
 * @param RESOURCEINPUT* resourceinput : Resourceinput to update
 * @param const char* logpath : Path of the new (RECORD) resourcelog
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_setlogpath( RESOURCEINPUT* resourceinput, const char* logpath ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   // verify that we don't have an active resourcelog
   if ( rin->rlog ) {
      LOG( LOG_ERR, "Already have an active resourcelog value\n" );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // don't allow inputs to be updated if threads are already preping for termination
   if ( rin->prepterm ) {
      LOG( LOG_ERR, "Resourceinput cannot be updated while preparing for termination\n" );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // initialize the new resourcelog
   if ( resourcelog_init( &(rin->rlog), logpath, RESOURCE_READ_LOG, rin->ns ) ) {
      LOG( LOG_ERR, "Failed to initialize resourcelog input: \"%s\"\n", logpath );
      pthread_mutex_unlock( &(rin->lock) );
      return -1;
   }
   pthread_cond_broadcast( &(rin->updated) ); // notify all waiting threads
   pthread_mutex_unlock( &(rin->lock) );
   LOG( LOG_INFO, "Successfully initialized input resourcelog \"%s\"\n", logpath );
   return 0;
}

/**
 * Set the active reference dir range of the given resourceinput
 * NOTE -- this will fail if the range has not been fully traversed
 * @param RESOURCEINPUT* resourceinput : Resourceinput to have its range set
 * @param ssize_t start : Starting index of the reference dir range
 * @param ssize_t end : Ending index of the reference dir range ( non-inclusive )
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_setrange( RESOURCEINPUT* resourceinput, size_t start, size_t end ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   // verify that the ref range has already been traversed
   if ( rin->refindex != rin->refmax ) {
      LOG( LOG_ERR, "Ref range of tracker has not been fully traversed\n" );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // don't allow inputs to be updated if threads are already preping for termination
   if ( rin->prepterm ) {
      LOG( LOG_ERR, "Resourceinput cannot be updated while preparing for termination\n" );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // check that range values are valid
   if ( start >= end  ||  end > rin->ns->prepo->metascheme.refnodecount ) {
      LOG( LOG_ERR, "Invalid reference range values: ( start = %zu / end = %zu / max = %zu )\n", start, end, rin->ns->prepo->metascheme.refnodecount );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // set range values
   rin->refindex = start;
   rin->refmax = end;
   pthread_cond_broadcast( &(rin->updated) ); // notify all waiting threads
   pthread_mutex_unlock( &(rin->lock) );
   return 0;
}

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
int resourceinput_getnext( RESOURCEINPUT* resourceinput, opinfo** nextop, MDAL_SCANNER* scanner, char** rdirpath ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   // check for active resourcelog
   if ( rin->rlog ) {
      if ( resourcelog_readop( &(rin->rlog), nextop ) ) {
         LOG( LOG_ERR, "Failed to read operation from active resourcelog\n" );
         resourcelog_abort( &(rin->rlog) );
         pthread_mutex_unlock( &(rin->lock) );
         return -1;
      }
      // check for completion of log
      if ( *nextop == NULL ) {
         LOG( LOG_INFO, "Statelog has been completely read\n" );
         if ( resourcelog_term( &(rin->rlog), NULL, NULL ) ) {
            // nothing to do but complain
            LOG( LOG_WARNING, "Failed to properly terminate input resourcelog\n" );
         }
         rin->rlog = NULL; // be certain this is NULLed out
      }
      else {
         // provide the read value
         pthread_mutex_unlock( &(rin->lock) );
         LOG( LOG_INFO, "Produced a new operation from logfile input\n" );
         return 1;
      }
   }
   // check if the ref range has already been traversed
   if ( rin->refindex == rin->refmax ) {
      LOG( LOG_INFO, "Resource inputs have been fully traversed\n" );
      int retval = 0;
      if ( rin->prepterm ) {
         LOG( LOG_INFO, "Caller should prepare for termination\n" );
         retval = 10;
      }
      pthread_mutex_unlock( &(rin->lock) );
      return retval;
   }
   // retrieve the next reference index value
   ssize_t res = rin->refindex;
   rin->refindex++;
   LOG( LOG_INFO, "Passing out reference index: %zd\n", res );
   // open the corresponding reference scanner
   MDAL nsmdal = rin->ns->prepo->metascheme.mdal;
   HASH_NODE* node = rin->ns->prepo->metascheme.refnodes + res;
   MDAL_SCANNER scanres = nsmdal->openscanner( rin->ctxt, node->name );
   if ( scanres == NULL ) { // just complain if we failed to open the dir
      LOG( LOG_ERR, "Failed to open scanner for refdir: \"%s\" ( index %zd )\n", node->name, res );
      rin->refindex--;
      pthread_mutex_unlock( &(rin->lock) );
      return -1;
   }
   // check for ref range completion
   if ( rin->refindex == rin->refmax ) {
      LOG( LOG_INFO, "Ref range has been completed\n" );
      pthread_cond_signal( &(rin->complete) );
   }
   pthread_mutex_unlock( &(rin->lock) );
   *scanner = scanres;
   *rdirpath = node->name;
   return 1;
}

/**
 * Destroy all available inputs and signal threads to prepare or for imminent termination
 * NOTE -- this is useful for aborting, if a thread has hit a fatal error
 * @param RESOURCEINPUT* resourceinput : Resourceinput to purge
 * @param size_t removeclients : Count of total clients to remove ( these will not participate in waitforterm() )
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_purge( RESOURCEINPUT* resourceinput, size_t removeclients ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   LOG( LOG_INFO, "Purging resource inputs\n" );
   // potentially terminate our resourcelog
   if ( rin->rlog  &&  resourcelog_abort( &(rin->rlog) ) ) {
      // nothing to do but complain
      LOG( LOG_WARNING, "Failed to abort input resourcelog\n" );
   }
   rin->rlog = NULL; // be certain this is NULLed out
   // set ref range values to indicate completion
   rin->refindex = rin->refmax;
   // set flag to indicate threads should prepare for termination
   if ( rin->prepterm < 1 ) { rin->prepterm = 1; }
   // decrement client counts
   rin->clientcount -= removeclients;
   // make sure all threads wake up
   pthread_cond_broadcast( &(rin->updated) );
   pthread_cond_broadcast( &(rin->complete) );
   pthread_mutex_unlock( &(rin->lock) );
   return 0;
}

/**
 * Wait for the given resourceinput to have available inputs, or for immenent termination
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforupdate( RESOURCEINPUT* resourceinput ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   // wait for the ref range to be traversed
   while ( rin->rlog == NULL  &&  rin->refindex == rin->refmax  &&  rin->prepterm == 0 ) {
      if ( pthread_cond_wait( &(rin->updated), &(rin->lock) ) ) {
         LOG( LOG_ERR, "Failed to wait on 'updated' condition value\n" );
         pthread_mutex_unlock( &(rin->lock) );
         return -1;
      }
   }
   pthread_mutex_unlock( &(rin->lock) );
   LOG( LOG_INFO, "Detected available inputs\n" );
   return 0;
}

/**
 * Wait for the given resourceinput to be terminated ( synchronizing like this ensures ALL work gets enqueued )
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforterm( RESOURCEINPUT* resourceinput ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   rin->clientcount--; // show that we are waiting
   pthread_cond_signal( &(rin->complete) ); // signal the master proc to check status
   // wait for the master proc to signal us
   while ( rin->prepterm < 2 ) {
      if ( pthread_cond_wait( &(rin->updated), &(rin->lock) ) ) {
         LOG( LOG_ERR, "Failed to wait on 'updated' condition value\n" );
         pthread_mutex_unlock( &(rin->lock) );
         return -1;
      }
   }
   rin->clientcount++; // show that we are exiting
   pthread_cond_signal( &(rin->complete) );
   pthread_mutex_unlock( &(rin->lock) );
   LOG( LOG_INFO, "Ready for input termination\n" );
   return 0;
}

/**
 * Wait for all inputs in the given resourceinput to be consumed
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforcomp( RESOURCEINPUT* resourceinput ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   // wait for the ref range to be traversed
   while ( rin->rlog  ||  rin->refindex != rin->refmax ) {
      if ( pthread_cond_wait( &(rin->complete), &(rin->lock) ) ) {
         LOG( LOG_ERR, "Failed to wait on 'complete' condition value\n" );
         pthread_mutex_unlock( &(rin->lock) );
         return -1;
      }
   }
   pthread_mutex_unlock( &(rin->lock) );
   LOG( LOG_INFO, "Detected input completion\n" );
   return 0;
}

/**
 * Terminate the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_term( RESOURCEINPUT* resourceinput ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // acquire the structure lock
   if ( pthread_mutex_lock( &(rin->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourceinput lock\n" );
      return -1;
   }
   // verify that the resourcelog has been completed
   if ( rin->rlog ) {
      LOG( LOG_ERR, "Statelog of inputs has not yet been fully traversed\n" );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // verify that the ref range has been traversed
   if ( rin->refindex != rin->refmax ) {
      LOG( LOG_ERR, "Ref range of inputs has not yet been fully traversed\n" );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   size_t origclientcount = rin->clientcount; // remember the total number of clients
   // signal clients to prepare for termination
   rin->prepterm = 1;
   pthread_cond_broadcast( &(rin->updated) );
   while ( rin->clientcount ) {
      if ( pthread_cond_wait( &(rin->complete), &(rin->lock) ) ) {
         LOG( LOG_ERR, "Failed to wait on 'complete' condition value for clients to synchronize\n" );
         pthread_mutex_unlock( &(rin->lock) );
         return -1;
      }
   }
   // signal clients to exit
   rin->prepterm = 2;
   pthread_cond_broadcast( &(rin->updated) );
   while ( rin->clientcount < origclientcount ) {
      if ( pthread_cond_wait( &(rin->complete), &(rin->lock) ) ) {
         LOG( LOG_ERR, "Failed to wait on 'complete' condition value for clients to exit\n" );
         pthread_mutex_unlock( &(rin->lock) );
         return -1;
      }
   }
   // begin destroying the structure
   *resourceinput = NULL;
   pthread_cond_destroy( &(rin->complete) );
   pthread_mutex_unlock( &(rin->lock) );
   pthread_mutex_destroy( &(rin->lock) );
   // DO NOT free ctxt or NS
   free( rin );
   return 0;
}

/**
 * Terminate the given resourceinput, without checking for completion of inputs
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_abort( RESOURCEINPUT* resourceinput ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput == NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCEINPUT rin = *resourceinput;
   // attempt to acquire the mutex lock, but don't let it stop us from killing the struct
   struct timespec waittime = {
      .tv_sec = 5, // maximum wait of 5sec to acquire the lock
      .tv_nsec = 0
   };
   char havelock = 1;
   if ( pthread_mutex_timedlock( &(rin->lock), &(waittime) ) ) {
      LOG( LOG_WARNING, "Failed to acquire lock, but pressing on regardless\n" );
      havelock = 0;
   }
   // signal clients to prepare for termination, but don't wait on them forever
   size_t origclientcount = rin->clientcount; // remember the total number of clients
   rin->prepterm = 1;
   char stillwaitin = 1;
   waittime.tv_sec = 10; // max wait of 10 seconds for conditions
   pthread_cond_broadcast( &(rin->updated) );
   while ( stillwaitin  &&  havelock  &&  rin->clientcount ) {
      if ( pthread_cond_timedwait( &(rin->complete), &(rin->lock), &(waittime) ) ) {
         LOG( LOG_WARNING, "Pressing on after failed wait for clients to synchronize\n" );
         stillwaitin = 0;
      }
   }
   // signal clients to exit
   rin->prepterm = 2;
   stillwaitin = 1;
   pthread_cond_broadcast( &(rin->updated) );
   while ( stillwaitin  &&  havelock  &&  rin->clientcount < origclientcount ) {
      if ( pthread_cond_timedwait( &(rin->complete), &(rin->lock), &(waittime) ) ) {
         LOG( LOG_WARNING, "Pressing on after failed wait for clients to exit\n" );
         stillwaitin = 0;
      }
   }
   // begin destroying the structure
   *resourceinput = NULL;
   // abort the resourcelog, if present
   if ( rin->rlog  &&  resourcelog_abort( &(rin->rlog) ) ) {
      LOG( LOG_WARNING, "Failed to abort input resourcelog\n" );
   }
   pthread_cond_destroy( &(rin->complete) );
   if ( havelock ) { pthread_mutex_unlock( &(rin->lock) ); }
   pthread_mutex_destroy( &(rin->lock) );
   // DO NOT free ctxt or NS
   free( rin );
   return 0;
}


//   -------------   THREAD BEHAVIOR FUNCTIONS    -------------

/**
 * Resource thread initialization ( producers and consumers )
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_init_func( unsigned int tID, void* global_state, void** state ) {
   // cast values to appropriate types
   rthread_global_state* gstate = (rthread_global_state*)global_state;
   // allocate thread state
   rthread_state* tstate = malloc( sizeof( struct rthread_state_struct ) );
   if ( tstate == NULL ) {
      LOG( LOG_ERR, "Thread %u failed to allocate state structure\n", tID );
      return -1;
   }
   // populate thread state
   bzero( tstate, sizeof( struct rthread_state_struct ) );
   tstate->tID = tID;
   tstate->gstate = gstate;
   *state = tstate;
   LOG( LOG_INFO, "Thread %u has initialized\n", tstate->tID );
   return 0;
}

/**
 * Resource thread consumer behavior
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_consumer_func( void** state, void** work_todo ) {
   // cast values to appropriate types
   rthread_state* tstate = (rthread_state*)(*state);
   opinfo* op = (opinfo*)(*work_todo);
   // execute operation
   if ( op ) {
      if ( tstate->gstate->dryrun ) {
         LOG( LOG_INFO, "Thread %u is discarding ( DRY-RUN ) a %s operation on StreamID \"%s\"\n", tstate->tID,
              (op->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
              (op->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
              (op->type == MARFS_REBUILD_OP)    ? "REBUILD" :
              (op->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN", op->ftag.streamid );
         resourcelog_freeopinfo( op );
      }
      else {
         LOG( LOG_INFO, "Thread %u is executing a %s operation on StreamID \"%s\"\n", tstate->tID,
              (op->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
              (op->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
              (op->type == MARFS_REBUILD_OP)    ? "REBUILD" :
              (op->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN", op->ftag.streamid );
         if ( process_executeoperation( &(tstate->gstate->pos), op, &(tstate->gstate->rlog), tstate->gstate->rpst ) ) {
            LOG( LOG_ERR, "Thread %u has encountered critical error during operation execution\n", tstate->tID );
            *work_todo = NULL;
            tstate->fatalerror = 1;
            // ensure termination of all other threads ( avoids possible deadlock )
            if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
               LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
            }
            return -1;
         }
      }
      *work_todo = NULL;
   }
   return 0;
}

/**
 * Resource thread producer behavior
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_producer_func( void** state, void** work_tofill ) {
   // cast values to appropriate types
   rthread_state* tstate = (rthread_state*)(*state);
   // loop until we have an op to enqueue
   opinfo* newop = NULL;
   while ( newop == NULL ) {
      if ( tstate->repackops ) {
         // enqueue previously produced rebuild op(s)
         newop = tstate->repackops;
         tstate->repackops = NULL; // remove state reference, so we don't repeat
      }
      else if ( tstate->gcops ) {
         // enqueue previously produced GC op(s)
         // first, we must identify where the op chain transitions from obj to ref deletions
         opinfo* refdel = NULL;
         opinfo* gcparse = tstate->gcops;
         while ( gcparse ) {
            if ( gcparse->type == MARFS_DELETE_REF_OP ) { refdel = gcparse; break; }
            gcparse = gcparse->next;
         }
         // NOTE -- object deletions always preceed reference deletions, so it should be safe to just check the first
         if ( tstate->gcops->type == MARFS_DELETE_OBJ_OP ) {
            if ( tstate->gcops->count > 1 ) {
               // temporarily strip our op chain down to a single DEL-OBJ op, followed by all reference deletions
               opinfo* orignext = tstate->gcops->next;
               tstate->gcops->next = refdel;
               // split the object deletion op apart into multiple work packages
               newop = resourcelog_dupopinfo( tstate->gcops );
               // restore original op chain structure
               tstate->gcops->next = orignext;
               // check for duplicated op chain
               if ( newop == NULL ) {
                  LOG( LOG_WARNING, "Failed to duplicate GC op prior to distribution!\n" );
                  // can't split this op apart, so just pass out the whole thing anyway
                  newop = tstate->gcops;
                  tstate->gcops = NULL; // remove state reference, so we don't repeat
               }
               newop->count = 1; // set to a single object deletion
               tstate->gcops->count--; // note one less op to distribute
               delobj_info* delobjinf = (delobj_info*) tstate->gcops->extendedinfo;
               delobjinf->offset++; // note to skip over one additional leading object
            }
            else {
               // check if we have additional ops between the lead op and the first ref del op
               if ( tstate->gcops->next != refdel ) {
                  // duplicate the REF-DEL portion of the op chain
                  opinfo* refdup = resourcelog_dupopinfo( refdel );
                  if ( refdup == NULL ) {
                     LOG( LOG_WARNING, "Failed to duplicate GC REF-DEL op prior to distribution!\n" );
                     // can't split this op apart, so just pass out the whole thing anyway
                     newop = tstate->gcops;
                     tstate->gcops = NULL; // remove state reference, so we don't repeat
                  }
                  // need to strip off our leading op, and attach it to a new chain
                  opinfo* orignext = tstate->gcops->next;
                  tstate->gcops->next = refdup;
                  newop = tstate->gcops;
                  tstate->gcops = orignext;
               }
            }
         }
         if ( newop == NULL ) {
            // just pass our whatever ops remain
            newop = tstate->gcops;
            tstate->gcops = NULL; // remove state reference, so we don't repeat
         }
      }
      else if ( tstate->walker ) {
         // walk our current datastream
         int walkres = process_iteratestreamwalker( tstate->walker, &(tstate->gcops), &(tstate->repackops), &(newop) );
         if ( walkres < 0 ) { // check for failure
            LOG( LOG_ERR, "Thread %u failed to walk a stream beginning in refdir \"%s\" of NS \"%s\"\n",
                 tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr );
            snprintf( tstate->errorstr, MAX_STR_BUFFER,
                      "Thread %u failed to walk a stream beginning in refdir \"%s\" of NS \"%s\"\n",
                      tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr );
            tstate->fatalerror = 1;
            // ensure termination of all other threads ( avoids possible deadlock )
            if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
               LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
            }
            return -1;
         }
         if ( walkres > 0 ) {
            // log every operation prior to distributing them
            if ( newop != NULL ) {
               if ( resourcelog_processop( &(tstate->gstate->rlog), newop, NULL ) ) {
                  LOG( LOG_ERR, "Thread %u failed to log start of a REBUILD operation\n", tstate->tID );
                  snprintf( tstate->errorstr, MAX_STR_BUFFER,
                            "Thread %u failed to log start of a REBUILD operation\n", tstate->tID );
                  resourcelog_freeopinfo( newop );
                  return -1;
               }
            }
            if ( tstate->repackops ) {
               if ( resourcelog_processop( &(tstate->gstate->rlog), tstate->repackops, NULL ) ) {
                  LOG( LOG_ERR, "Thread %u failed to log start of a REPACK operation\n", tstate->tID );
                  snprintf( tstate->errorstr, MAX_STR_BUFFER,
                            "Thread %u failed to log start of a REPACK operation\n", tstate->tID );
                  if ( newop ) { resourcelog_freeopinfo( newop ); }
                  return -1;
               }
            }
            if ( tstate->gcops ) {
               if ( resourcelog_processop( &(tstate->gstate->rlog), tstate->gcops, NULL ) ) {
                  LOG( LOG_ERR, "Thread %u failed to log start of a GC operation\n", tstate->tID );
                  snprintf( tstate->errorstr, MAX_STR_BUFFER,
                            "Thread %u failed to log start of a GC operation\n", tstate->tID );
                  if ( newop ) { resourcelog_freeopinfo( newop ); }
                  return -1;
               }
            }
         }
         if ( walkres == 0 ) { // check for end of stream
            LOG( LOG_INFO, "Thread %u has reached the end of a datastream\n", tstate->tID );
            streamwalker_report tmpreport = {0};
            if ( process_closestreamwalker( tstate->walker, &(tmpreport) ) ) {
               LOG( LOG_ERR, "Thread %u failed to close a streamwalker\n", tstate->tID );
               snprintf( tstate->errorstr, MAX_STR_BUFFER, "Thread %u failed to close a streamwalker\n", tstate->tID );
               tstate->fatalerror = 1;
               tstate->walker = NULL; // don't repeat a close attempt
               // ensure termination of all other threads ( avoids possible deadlock )
               if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
                  LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
               }
               return -1;
            }
            tstate->walker = NULL;
            tstate->report.fileusage   += tmpreport.fileusage;
            tstate->report.byteusage   += tmpreport.byteusage;
            tstate->report.filecount   += tmpreport.filecount;
            tstate->report.objcount    += tmpreport.objcount;
            tstate->report.bytecount   += tmpreport.bytecount;
            tstate->report.streamcount += tmpreport.streamcount;
            tstate->report.delobjs     += tmpreport.delobjs;
            tstate->report.delfiles    += tmpreport.delfiles;
            tstate->report.delstreams  += tmpreport.delstreams;
            tstate->report.volfiles    += tmpreport.volfiles;
            tstate->report.rpckfiles   += tmpreport.rpckfiles;
            tstate->report.rpckbytes   += tmpreport.rpckbytes;
            tstate->report.rbldobjs    += tmpreport.rbldobjs;
            tstate->report.rbldbytes   += tmpreport.rbldbytes;
         }
      }
      else if ( tstate->scanner ) {
         // iterate through the scanner, looking for new operations to dispatch
         char* reftgt = NULL;
         ssize_t tgtval = 0;
         int scanres = process_refdir( tstate->gstate->pos.ns, tstate->scanner, tstate->rdirpath, &(reftgt), &(tgtval) );
         if ( scanres == 0 ) {
            LOG( LOG_INFO, "Thread %u has finished scan of reference dir \"%s\"\n", tstate->tID, tstate->rdirpath );
            // NULL out our dir references, just in case
            tstate->scanner = NULL;
            tstate->rdirpath = NULL;
         }
         else if ( scanres == 1 ) { // start of a new datastream to be walked
            // only copy relevant threshold values for this walk
            thresholds tmpthresh = tstate->gstate->thresh;
            if ( !(tstate->gstate->lbrebuild) ) { tmpthresh.rebuildthreshold = 0; }
            LOG( LOG_INFO, "Thread %u beginning streamwalk from reference file \"%s\"\n", tstate->tID, reftgt );
            tstate->walker = process_openstreamwalker( &(tstate->gstate->pos), reftgt, tmpthresh, &(tstate->gstate->rebuildloc) );
            if ( tstate->walker == NULL ) {
               LOG( LOG_ERR, "Thread %u failed to open streamwalker for \"%s\" of NS \"%s\"\n",
                             tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr );
               snprintf( tstate->errorstr, MAX_STR_BUFFER,
                         "Thread %u failed to open streamwalker for \"%s\" of NS \"%s\"\n",
                         tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr );
               tstate->fatalerror = 1;
               if ( reftgt ) { free( reftgt ); }
               // ensure termination of all other threads ( avoids possible deadlock )
               if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
                  LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
               }
               return -1;
            }
            tstate->streamcount++;
         }
         else if ( scanres == 2 ) { // rebuild marker file
            if ( tstate->gstate->lbrebuild ) { //skip marker files, if we're rebuilding based on object location
               LOG( LOG_INFO, "Skipping rebuild marker file, as we are doing location-based rebuild: \"%s\"\n", reftgt );
            }
            else {
               errno = 0;
               newop = process_rebuildmarker( &(tstate->gstate->pos), reftgt, tstate->gstate->thresh.rebuildthreshold, tgtval );
               if ( newop == NULL  &&  errno != ETIME ) { // only ignore failure due to recently created marker file
                  LOG( LOG_ERR, "Thread %u failed to process rebuild marker \"%s\" of NS \"%s\"\n",
                                tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr );
                  snprintf( tstate->errorstr, MAX_STR_BUFFER,
                            "Thread %u failed to process rebuild marker \"%s\" of NS \"%s\"\n",
                            tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr );
                  tstate->fatalerror = 1;
                  if ( reftgt ) { free( reftgt ); }
                  // ensure termination of all other threads ( avoids possible deadlock )
                  if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
                     LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
                  }
                  return -1;
               }
            }
         }
         else if ( scanres == 3 ) { // repack marker file
            // TODO
         }
         else if ( scanres == 10 ) {
            // ignore unknown entry type
            LOG( LOG_WARNING, "Thread %u ignoring unknown reference entry: \"%s/%s\"\n",
                              tstate->tID, tstate->rdirpath, reftgt );
         }
         else {
            // an error occurred
            LOG( LOG_ERR, "Thread %u failed to process reference dir \"%s\" of NS \"%s\"\n",
                          tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr );
            snprintf( tstate->errorstr, MAX_STR_BUFFER,
                      "Thread %u failed to process reference dir \"%s\" of NS \"%s\"\n",
                      tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr );
            tstate->fatalerror = 1;
            // ensure termination of all other threads ( avoids possible deadlock )
            if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
               LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
            }
            return -1;
         }
         // free temporary reference target string
         if ( reftgt ) { free( reftgt ); }
      }
      else {
         // pull from our resource input reference
         int inputres = 0;
         while ( (inputres = resourceinput_getnext( &(tstate->gstate->rinput), &(newop), &(tstate->scanner), &(tstate->rdirpath) )) == 0 ) {
            // wait until inputs are available
            LOG( LOG_INFO, "Thread %u is waiting for inputs\n", tstate->tID );
            if ( resourceinput_waitforupdate( &(tstate->gstate->rinput) ) ) {
               LOG( LOG_ERR, "Thread %u failed to wait for resourceinput update while scanning NS \"%s\"\n",
                             tstate->tID, tstate->gstate->pos.ns->idstr );
               snprintf( tstate->errorstr, MAX_STR_BUFFER,
                         "Thread %u failed to wait for resourceinput update while scanning NS \"%s\"\n",
                         tstate->tID, tstate->gstate->pos.ns->idstr );
               tstate->fatalerror = 1;
               // ensure termination of all other threads ( avoids possible deadlock )
               if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
                  LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
               }
               return -1;
            }
         }
         // check for termination condition
         if ( inputres == 10 ) {
            LOG( LOG_INFO, "Thread %u is waiting for termination\n", tstate->tID );
            if ( resourceinput_waitforterm( &(tstate->gstate->rinput) ) ) {
               LOG( LOG_ERR, "Thread %u failed to wait for input termination\n", tstate->tID );
               snprintf( tstate->errorstr, MAX_STR_BUFFER,
                         "Thread %u failed to wait for input termination\n", tstate->tID );
               tstate->fatalerror = 1;
               // ensure termination of all other threads ( avoids possible deadlock )
               if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
                  LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
               }
               return -1;
            }
            LOG( LOG_INFO, "Thread %u is signaling FINISHED state\n", tstate->tID );
            return 1;
         }
         // check for failure
         if ( inputres < 0 ) {
            LOG( LOG_INFO, "Thread %u failed to retrieve next input while scanning NS \"%s\"\n",
                           tstate->tID, tstate->gstate->pos.ns->idstr );
            snprintf( tstate->errorstr, MAX_STR_BUFFER,
                      "Thread %u failed to retrieve next input while scanning NS \"%s\"\n",
                      tstate->tID, tstate->gstate->pos.ns->idstr );
            tstate->fatalerror = 1;
            // ensure termination of all other threads ( avoids possible deadlock )
            if ( resourceinput_purge( &(tstate->gstate->rinput), 1 ) ) {
               LOG( LOG_WARNING, "Failed to purge resource input following fatal error\n" );
            }
            return -1;
         }
      }
   }
   LOG( LOG_INFO, "Thread %u dispatching a %s%s operation on StreamID \"%s\"\n", tstate->tID,
        (newop->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
        (newop->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
        (newop->type == MARFS_REBUILD_OP)    ? "REBUILD" :
        (newop->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN",
        (newop->next == NULL) ? "" :
        (newop->next->type == MARFS_DELETE_OBJ_OP) ? " + DEL-OBJ" :
        (newop->next->type == MARFS_DELETE_REF_OP) ? " + DEL-REF" :
        (newop->next->type == MARFS_REBUILD_OP)    ? " + REBUILD" :
        (newop->next->type == MARFS_REPACK_OP)     ? " + REPACK"  : " + UNKNOWN", newop->ftag.streamid );
   // actually populate our work package
   *work_tofill = (void*)newop;
   return 0;
}

/**
 * Resource thread termination ( producers and consumers )
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
void rthread_term_func( void** state, void** prev_work, TQ_Control_Flags flg ) {
   // cast values to appropriate types
   rthread_state* tstate = (rthread_state*)(*state);
   // producers may need to cleanup remaining state
   if ( tstate->repackops ) {
      LOG( LOG_INFO, "Thread %u is destroying non-issued REPACK ops\n" );
      resourcelog_freeopinfo( tstate->repackops );
      tstate->repackops = NULL;
   }
   if ( tstate->gcops ) {
      LOG( LOG_ERR, "Thread %u is destroying remaining GC ops\n" );
      resourcelog_freeopinfo( tstate->gcops );
      tstate->gcops = NULL;
      // this is non-standard, so ensure we note an error
      if ( !(tstate->fatalerror) ) {
         snprintf( tstate->errorstr, MAX_STR_BUFFER,
                   "Thread %u held non-issued GC ops at termination\n", tstate->tID );
         tstate->fatalerror = 1;
      }
   }
   if ( tstate->scanner ) {
      LOG( LOG_ERR, "Thread %u is destroying remaining scanner handle\n" );
      tstate->gstate->pos.ns->prepo->metascheme.mdal->closescanner( tstate->scanner );
      // this is non-standard, so ensure we note an error
      if ( !(tstate->fatalerror) ) {
         snprintf( tstate->errorstr, MAX_STR_BUFFER,
                   "Thread %u held an open MDAL_SCANNER at termination\n", tstate->tID );
         tstate->fatalerror = 1;
      }
   }
   // merely note termination ( state struct itself will be freed by master proc )
   LOG( LOG_INFO, "Thread %u is terminating\n", tstate->tID );
}

