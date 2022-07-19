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


typedef struct reftracker_struct {
   // synchronization and access control
   pthread_mutex_t  lock;
   pthread_cond_t   complete;
   // state info
   marfs_ns*        ns;
   MDAL_CTXT        ctxt;
   ssize_t          refindex;
   ssize_t          refmax;
}*REFTRACKER;


//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Initialize a given reftracker
 * @param REFTRACKER* reftracker : Reftracker to be initialized
 * @param marfs_ns* ns : MarFS NS associated with the new reftracker
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the previous NS
 *                         NOTE -- caller should never modify this again
 * @return int : Zero on success, or -1 on failure
 */
int reftracker_init( REFTRACKER* reftracker, marfs_ns* ns, MDAL_CTXT ctxt ) {
   // check for valid ref
   if ( reftracker == NULL  ||  *reftracker != NULL ) {
      LOG( LOG_ERR, "Received an invalid reftracker arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( ns == NULL  ||  ctxt == NULL ) {
      LOG( LOG_ERR, "Received NULL NS/CTXT ref\n" );
      errno = EINVAL;
      return -1;
   }
   // allocate a new reftracker
   REFTRACKER reft = malloc( sizeof( struct reftracker_struct ) );
   if ( reft == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new reftracker\n" );
      return -1;
   }
   // initialize reftracker values
   reft->ns = ns;
   reft->ctxt = ctxt;
   reft->refmax = 0;
   reft->refindex = 0;
   if ( pthread_cond_init( &(reft->complete), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize 'complete' condition for new reftracker\n" );
      free( reft );
      return -1;
   }
   if ( pthread_mutex_init( &(reft->lock), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize lock for new reftracker\n" );
      pthread_cond_destroy( &(reft->complete) );
      free( reft );
      return -1;
   }
   *reftracker = reft;
   return 0;
}

/**
 * Set the active reference dir range of the given reftracker
 * NOTE -- this will fail if the range has not been fully traversed
 * @param REFTRACKER* reftracker : Reftracker to have its range set
 * @param ssize_t start : Starting index of the reference dir range
 * @param ssize_t end : Ending index of the reference dir range
 * @return int : Zero on success, or -1 on failure
 */
int reftracker_setrange( REFTRACKER* reftracker, size_t start, size_t end ) {
   // check for valid ref
   if ( reftracker == NULL  ||  *reftracker == NULL ) {
      LOG( LOG_ERR, "Received an invalid reftracker arg\n" );
      errno = EINVAL;
      return -1;
   }
   // acquire the structure lock
   if ( pthread_mutex_lock( &((*reftracker)->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire reftracker lock\n" );
      return -1;
   }
   // verify that the ref range has already been traversed
   if ( (*reftracker)->refindex != (*reftracker)->refmax ) {
      LOG( LOG_ERR, "Ref range of tracker has not been fully traversed\n" );
      pthread_mutex_unlock( &((*reftracker)->lock) );
      errno = EINVAL;
      return -1;
   }
   // check that range values are valid
   if ( start > end  ||  end >= (*reftracker)->ns->prepo->metascheme.refnodecount ) {
      LOG( LOG_ERR, "Invalid reference range values: ( start = %zu / end = %zu / max = %zu )\n", start, end, (*reftracker)->ns->prepo->metascheme.refnodecount );
      pthread_mutex_unlock( &((*reftracker)->lock) );
      errno = EINVAL;
      return -1;
   }
   // set range values
   (*reftracker)->refindex = start;
   (*reftracker)->refmax = end + 1;
   pthread_mutex_unlock( &((*reftracker)->lock) );
   return 0;
}

/**
 * Get the next ref index to be processed from the given reftracker
 * @param REFTRACKER* reftracker : Reftracker to get the next ref index from
 * @param char* eor : 'End-of-Range' flag reference, to be populated with 1 if 
 *                    the end of the current ref range has been reached, or zero 
 *                    otherwise
 * @return MDAL_SCANNER : New reference scanner, or NULL if one wasn't opened
 *                        ( on error, or if none remain in the ref range )
 */
MDAL_SCANNER reftracker_getref( REFTRACKER* reftracker, char* eor ) {
   // check for a valid char ref
   if ( eor == NULL ) {
      LOG( LOG_ERR, "Received a NULL eor arg\n" );
      errno = EINVAL;
      return NULL;
   }
   // check for valid ref
   if ( reftracker == NULL  ||  *reftracker == NULL ) {
      LOG( LOG_ERR, "Received an invalid reftracker arg\n" );
      *eor = 0;
      errno = EINVAL;
      return NULL;
   }
   // acquire the structure lock
   if ( pthread_mutex_lock( &((*reftracker)->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire reftracker lock\n" );
      *eor = 0;
      return NULL;
   }
   // check if the ref range has already been traversed
   if ( (*reftracker)->refindex == (*reftracker)->refmax ) {
      LOG( LOG_INFO, "Ref range of tracker has already been fully traversed\n" );
      pthread_mutex_unlock( &((*reftracker)->lock) );
      *eor = 1;
      return NULL;
   }
   // retrieve the next reference index value
   ssize_t res = (*reftracker)->refindex;
   (*reftracker)->refindex++;
   LOG( LOG_INFO, "Passing out reference index: %zd\n", (*reftracker)->refindex );
   // open the corresponding reference scanner
   MDAL nsmdal = (*reftracker)->ns->prepo->metascheme.mdal;
   HASH_NODE* node = (*reftracker)->ns->prepo->metascheme.refnodes + res;
   MDAL_SCANNER scanner = nsmdal->openscanner( (*reftracker)->ctxt, node->name );
   if ( scanner == NULL ) { // just complain if we failed to open the dir
      LOG( LOG_ERR, "Failed to open scanner for refdir: \"%s\" ( index %zd )\n", node->name, res );
   }
   // check for ref range completion
   if ( (*reftracker)->refindex == (*reftracker)->refmax ) {
      LOG( LOG_INFO, "Ref range has been completed\n" );
      pthread_cond_signal( &((*reftracker)->complete) );
   }
   pthread_mutex_unlock( &((*reftracker)->lock) );
   *eor = 0;
   return scanner;
}

/**
 * Wait for all ref indexes in the given reftracker to be consumed
 * @param REFTRACKER* reftracker : Reftracker to wait on
 * @param int : Zero on success, or -1 on failure
 */
int reftracker_waitforcomp( REFTRACKER* reftracker ) {
   // check for valid ref
   if ( reftracker == NULL  ||  *reftracker == NULL ) {
      LOG( LOG_ERR, "Received an invalid reftracker arg\n" );
      errno = EINVAL;
      return -1;
   }
   // acquire the structure lock
   if ( pthread_mutex_lock( &((*reftracker)->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire reftracker lock\n" );
      return -1;
   }
   // wait for the ref range to be traversed
   while ( (*reftracker)->refindex != (*reftracker)->refmax ) {
      if ( pthread_cond_wait( &((*reftracker)->complete), &((*reftracker)->lock) ) ) {
         LOG( LOG_ERR, "Failed to wait on 'complete' condition value\n" );
         pthread_mutex_unlock( &((*reftracker)->lock) );
         return -1;
      }
   }
   pthread_mutex_unlock( &((*reftracker)->lock) );
   return 0;
}

/**
 * Terminate the given reftracker
 * @param REFTRACKER* reftracker : Reftracker to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int reftracker_term( REFTRACKER* reftracker ) {
   // check for valid ref
   if ( reftracker == NULL  ||  *reftracker == NULL ) {
      LOG( LOG_ERR, "Received an invalid reftracker arg\n" );
      errno = EINVAL;
      return -1;
   }
   // acquire the structure lock
   if ( pthread_mutex_lock( &((*reftracker)->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire reftracker lock\n" );
      return -1;
   }
   // verify that the ref range has been traversed
   if ( (*reftracker)->refindex != (*reftracker)->refmax ) {
      LOG( LOG_ERR, "Ref range of tracker has not yet been fully traversed\n" );
      pthread_mutex_unlock( &((*reftracker)->lock) );
      errno = EINVAL;
      return -1;
   }
   // begin destroying the structure
   REFTRACKER reft = *reftracker;
   *reftracker = NULL;
   pthread_cond_destory( &((*reftracker)->complete) );
   pthread_mutex_unlock( &((*reftracker)->lock) );
   pthread_mutex_destory( &((*reftracker)->lock) );
   MDAL nsmdal = (*reftracker)->ns->prepo->metascheme.mdal;
   nsmdal->destroyctxt( (*reftracker)->ctxt );
   free( reft );
   return 0;
}

