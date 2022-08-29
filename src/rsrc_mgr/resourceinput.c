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

#include "statelog.h"

typedef struct resourceinput_struct {
   // synchronization and access control
   pthread_mutex_t  lock;     // no simultaneous access
   pthread_cond_t   complete; // signaled when all work is handed out
   pthread_cond_t   updated;  // signaled when new work is added
   // previous log info
   STATELOG         statelog;
   // state info
   marfs_ns*        ns;
   MDAL_CTXT        ctxt;
   // reference info
   ssize_t          refindex;
   ssize_t          refmax;
}*RESOURCEINPUT;


//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Initialize a given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be initialized
 * @param marfs_ns* ns : MarFS NS associated with the new resourceinput
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the previous NS
 *                         NOTE -- caller should never modify this again
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_init( RESOURCEINPUT* resourceinput, marfs_ns* ns, MDAL_CTXT ctxt ) {
   // check for valid ref
   if ( resourceinput == NULL  ||  *resourceinput != NULL ) {
      LOG( LOG_ERR, "Received an invalid resourceinput arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( ns == NULL  ||  ctxt == NULL ) {
      LOG( LOG_ERR, "Received NULL NS/CTXT ref\n" );
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
   rin->ns = ns;
   rin->ctxt = ctxt;
   rin->statelog = NULL;
   rin->refmax = 0;
   rin->refindex = 0;
   if ( pthread_cond_init( &(rin->complete), NULL ) ) {
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
   // verify that we don't have an active statelog
   if ( rin->statelog ) {
      LOG( LOG_ERR, "Already have an active statelog value\n" );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // initialize the new statelog
   if ( statelog_init( &(rin->statelog), RESOURCE_READ_LOG, logpath ) ) {
      LOG( LOG_ERR, "Failed to initialize statelog input: \"%s\"\n", logpath );
      pthread_mutex_unlock( &(rin->lock) );
      return -1;
   }
   pthread_cond_signal( &(rin->updated) ); // note that new inputs are available
   pthread_mutex_unlock( &(rin->lock) );
   LOG( LOG_INFO, "Successfully initialized input statelog \"%s\"\n", logpath );
   return 0;
}

/**
 * Set the active reference dir range of the given resourceinput
 * NOTE -- this will fail if the range has not been fully traversed
 * @param RESOURCEINPUT* resourceinput : Resourceinput to have its range set
 * @param ssize_t start : Starting index of the reference dir range
 * @param ssize_t end : Ending index of the reference dir range
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
   // check that range values are valid
   if ( start > end  ||  end >= rin->ns->prepo->metascheme.refnodecount ) {
      LOG( LOG_ERR, "Invalid reference range values: ( start = %zu / end = %zu / max = %zu )\n", start, end, rin->ns->prepo->metascheme.refnodecount );
      pthread_mutex_unlock( &(rin->lock) );
      errno = EINVAL;
      return -1;
   }
   // set range values
   rin->refindex = start;
   rin->refmax = end + 1;
   pthread_cond_signal( &(rin->updated) ); // note that new inputs are available
   pthread_mutex_unlock( &(rin->lock) );
   return 0;
}

/**
 * Get the next ref index to be processed from the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to get the next ref index from
 * @return MDAL_SCANNER : New reference scanner, or NULL if one wasn't opened
 *                        ( on error, or if none remain in the ref range )
 */
int resourceinput_getnext( RESOURCEINPUT* resourceinput, opinfo* nextop, MDAL_SCANNER scanner ) {
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
   // check for active statelog
   if ( rin->statelog ) {
      if ( statelog_readop( &(rin->statelog), &nextop ) ) {
         LOG( LOG_ERR, "Failed to read operation from active statelog\n" );
         statelog_abort( &(rin->statelog) );
         pthread_mutex_unlock( &(rin->lock) );
         return -1;
      }
      // check for completion of log
      if ( nextop == NULL ) {
         LOG( LOG_INFO, "Statelog has been completely read\n" );
         if ( statelog_term( &(rin->statelog), NULL, NULL ) ) {
            // nothing to do but complain
            LOG( LOG_WARNING, "Failed to properly terminate input statelog\n" );
         }
         rin->statelog == NULL; // be certain this is NULLed out
      }
      else {
         // provide the read value
         pthread_mutex_unlock( &(rin->lock) );
         return 1;
      }
   }
   // check if the ref range has already been traversed
   if ( rin->refindex == rin->refmax ) {
      LOG( LOG_INFO, "Resource inputs have been fully traversed\n" );
      pthread_cond_signal( &(rin->complete) );
      pthread_mutex_unlock( &(rin->lock) );
      return 0;
   }
   // retrieve the next reference index value
   ssize_t res = rin->refindex;
   rin->refindex++;
   LOG( LOG_INFO, "Passing out reference index: %zd\n", rin->refindex );
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
   return 1;
}

/**
 * Wait for the given resourceinput to have available inputs
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
   while ( rin->statelog == NULL  &&  rin->refindex == rin->refmax ) {
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
   while ( rin->statelog  ||  rin->refindex != rin->refmax ) {
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
   // verify that the statelog has been completed
   if ( rin->statelog ) {
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
   // begin destroying the structure
   *resourceinput = NULL;
   pthread_cond_destory( &(rin->complete) );
   pthread_mutex_unlock( &(rin->lock) );
   pthread_mutex_destory( &(rin->lock) );
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
   // verify that the statelog has been completed
   if ( rin->statelog  &&  statelog_abort( &(rin->statelog) ) ) {
      LOG( LOG_WARNING, "Failed to abort input statelog\n" );
   }
   // begin destroying the structure
   *resourceinput = NULL;
   pthread_cond_destory( &(rin->complete) );
   pthread_mutex_destory( &(rin->lock) );
   // DO NOT free ctxt or NS
   free( rin );
   return 0;
}

