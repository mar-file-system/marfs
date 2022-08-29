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




//   -------------   INTERNAL DEFINITIONS    -------------

typedef struct repackstreamer_struct {
   // synchronization and access control
   pthread_mutex_t lock;
   // state info
   size_t streamcount;
   DATASTREAM* streamlist;
   char* streamstatus;
}* REPACKSTREAMER;


//   -------------   INTERNAL FUNCTIONS    -------------




//   -------------   EXTERNAL FUNCTIONS    -------------

REPACKSTREAMER repackstreamer_init( ) {
   // allocate a new struct
   REPACKSTREAMER repackst = malloc( sizeof( struct repackstreamer_struct ) );
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Failed to allocate new repackstreamer\n" );
      return NULL;
   }
   // populate all struct elements
   if ( pthread_mutex_init( &(repackst->lock), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize lock\n" );
      free( repackst );
      return NULL;
   }
   repackst->streamcount = 10;
   repackst->streamlist = calloc( 10, sizeof( DATASTREAM ) );
   if ( repackst->streamlist == NULL ) {
      LOG( LOG_ERR, "Failed to initialize streamlist\n" );
      pthread_mutex_destroy( &(repackst->lock) );
      free( repackst );
      return NULL;
   }
   repackst->streamstatus = calloc( 10, sizeof(char) );
   if ( repackst->streamstatus == NULL ) {
      LOG( LOG_ERR, "Failed to initialize streamstatus\n" );
      free( repackst->streamlist );
      pthread_mutex_destroy( &(repackst->lock) );
      free( repackst );
      return NULL;
   }
   return repackst;
}

DATASTREAM* repackstreamer_getstream( REPACKSTREAMER repackst ) {
   // check for NULL arg
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return NULL
   }
   // acquire struct lock
   if ( pthread_mutex_lock( &(repackst->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire repackstreamer lock\n" );
      return NULL;
   }
   // check for available datastreams
   size_t index = 0;
   for ( ; index < repackst->streamcount; index++ ) {
      if ( repackst->streamstatus[index] == 0 ) {
         repackst->streamstatus[index] = 1;
         pthread_mutex_unlock( &(repackst->lock) );
         LOG( LOG_INFO, "Handing out available stream at position %zu\n", index );
         return repackst->streamlist + index;
      }
   }
   // no avaialable datastreams, so we must expand our allocation ( double the current count )
   LOG( LOG_INFO, "Expanding allocation to %zu streams\n", repackst->streamcount * 2 );
   DATASTREAM* newstlist = realloc( repackst->streamlist, sizeof( DATASTREAM ) * ( repackst->streamcount * 2 ) );
   if ( newstlist == NULL ) {
      LOG( LOG_ERR, "Failed to reallocate streamlist to a length of %zu entries\n", repackst->streamcount * 2 );
      pthread_mutex_unlock( &(repackst->lock) );
      return NULL;
   }
   repackst->streamlist = newstlist; // might end up leaving this expanded, but that's fine
   char* newststatus = realloc( repackst->streamstatus, sizeof( char ) * ( repackst->streamcount * 2 ) );
   if ( newststatus == NULL ) {
      LOG( LOG_ERR, "Failed to reallocate streamstatus to a length of %zu entries\n", repackst->streamcount * 2 );
      pthread_mutex_unlock( &(repackst->lock) );
      return NULL;
   }
   repackst->streamstatus = newststatus;
   repackst->streamcount *= 2;
   // zero out new allocation
   size_t newpos = index; // cache the lowest, newly-allocated index
   for ( ; index < repackst->streamcount; index++ ) {
      repackst->streamlist[index] = NULL;
      repackst->streamstatus[index] = 0;
   }
   // hand out a newly-allocated stream
   repackst->streamstatus[newpos] = 1;
   pthread_mutex_unlock( &(repackst->lock) );
   LOG( LOG_INFO, "Handing out newly-allocated position %zu\n", newpos );
   return repackst->streamlist + newpos;
}

int repackstreamer_returnstream( REPACKSTREAMER repackst, DATASTREAM* stream ) {
   // check for NULL args
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return -1
   }
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL stream ref\n" );
      errno = EINVAL;
      return -1;
   }
   // acquire struct lock
   if ( pthread_mutex_lock( &(repackst->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire repackstreamer lock\n" );
      return -1;
   }
   // calculate the corresponding index of this stream
   size_t index = (size_t)(stream - repackst->streamlist);
   // sanity check the result
   if ( index >= repackst->streamcount  ||  stream < repackst->streamlist ) {
      LOG( LOG_ERR, "Returned stream is not a member of allocated list\n" );
      pthread_mutex_unlock( &(repackst->lock) );
      return -1;
   }
   // ensure the stream was indeed passed out
   if ( repackst->streamstatus[index] != 1 ) {
      LOG( LOG_ERR, "Returned stream %zu was not currently active\n", index );
      pthread_mutex_unlock( &(repackst->lock) );
      return -1;
   }
   // update status and return
   repackst->streamstatus[index] = 0;
   pthread_mutex_unlock( &(repackst->lock) );
   LOG( LOG_INFO, "Stream %zu has been returned\n", index );
   return 0;
}

int repackstreamer_complete( REPACKSTREAMER repackst ) {
   // check for NULL arg
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return -1
   }
   // acquire struct lock
   if ( pthread_mutex_lock( &(repackst->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire repackstreamer lock\n" );
      return -1;
   }
   // iterate over all streams
   int retval = 0;
   char prevactive = 1;
   size_t index = 0;
   for ( ; index < repackst->streamcount; index++ ) {
      if ( *(repackst->streamlist + index ) != NULL ) {
         // minor sanity check, not even certain that this is a true failure
         if ( prevactive == 0 ) {
            LOG( LOG_WARNING, "Encountered active stream at index %zu with previous stream being NULL\n" );
         }
         // close the active stream
         int closeres = datastream_close( repackst->streamlist + index );
         if ( closeres ) {
            LOG( LOG_ERR, "Failed to close repack stream %zu\n", index );
            if ( retval == 0 ) { retval = closeres; }
         }
      }
      else { prevactive = 0; }
   }
   // free all allocations
   free( repackst->streamstatus );
   free( repackst->streamlist );
   pthread_mutex_unlock( &(repackst->lock) );
   pthread_mutex_destroy( &(repackst->lock) );
   free( repackst );
   return retval;
}

int repackstreamer_abort( REPACKSTREAMER repackst ) {
   // check for NULL arg
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return -1
   }
   // don't bother acquiring the lock
   // iterate over all streams
   int retval = 0;
   char prevactive = 1;
   size_t index = 0;
   for ( ; index < repackst->streamcount; index++ ) {
      if ( *(repackst->streamlist + index ) != NULL ) {
         // minor sanity check, not even certain that this is a true failure
         if ( prevactive == 0 ) {
            LOG( LOG_WARNING, "Encountered active stream at index %zu with previous stream being NULL\n" );
         }
         // release the active stream
         int closeres = datastream_release( repackst->streamlist + index );
         if ( closeres ) {
            LOG( LOG_ERR, "Failed to release repack stream %zu\n", index );
            if ( retval == 0 ) { retval = closeres; }
         }
      }
      else { prevactive = 0; }
   }
   // free all allocations
   free( repackst->streamstatus );
   free( repackst->streamlist );
   pthread_mutex_destroy( &(repackst->lock) );
   free( repackst );
   return retval;
}

