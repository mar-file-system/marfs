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
#define LOG_PREFIX "resourceprocessing"
#include <logging.h>

#include "resourceprocessing.h"

#include "datastream/datastream.h"

#include <dirent.h>
#include <string.h>


//   -------------   INTERNAL DEFINITIONS    -------------

// ENOATTR is not always defined, so define a convenience val
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

typedef struct repackstreamer_struct {
   // synchronization and access control
   pthread_mutex_t lock;
   // state info
   size_t streamcount;
   DATASTREAM* streamlist;
   char* streamstatus;
}* REPACKSTREAMER;

typedef struct streamwalker_struct {
   // initialization info
   marfs_position pos;
   time_t      gcthresh;       // time threshold for GCing a file ( none performed if zero )
   time_t      repackthresh;   // time threshold for repacking a file ( none performed if zero )
   time_t      rebuildthresh;  // time threshold for rebuilding a file ( none performed, if zero )
   ne_location rebuildloc;     // location value of objects to be rebuilt
   // report info
   streamwalker_report report; // running totals for encountered stream elements
   // iteration info
   size_t      fileno;    // current file position in the datastream
   size_t      objno;     // current object position in the datastream
   HASH_TABLE  reftable;  // NS reference position table to be used for stream iteration
   struct stat stval;     // stat value of the most recently encountered file
   FTAG        ftag;      // FTAG value of the most recently checked file ( not necessarily previous )
   GCTAG       gctag;     // GCTAG value of the most recently checked file ( not necessarily previous )
   // cached info
   size_t      headerlen;    // recovery header length for the active datastream
   char*       ftagstr;      // FTAG string buffer
   size_t      ftagstralloc; // allocated length of the FTAG string buffer
   // GC info
   opinfo*     gcops;        // garbage collection operation list
   size_t      activefiles;  // count of active files referencing the current object
   size_t      activeindex;  // index of the most recently encountered active file
   // repack info
   opinfo*     rpckops;      // repack operation list
   size_t      activebytes;  // active bytes in the current object
   // rebuild info
   opinfo*     rbldops;      // rebuild operation list
}* streamwalker;


//   -------------   REPACKSTREAMER FUNCTIONS    -------------

/**
 * Initialize a new repackstreamer
 * @return REPACKSTREAMER : New repackstreamer, or NULL on failure
 */
REPACKSTREAMER repackstreamer_init(void) {
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

/**
 * Checkout a repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to checkout from
 * @return DATASTREAM* : Checked out datastream, or NULL on failure
 */
DATASTREAM* repackstreamer_getstream( REPACKSTREAMER repackst ) {
   // check for NULL arg
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return NULL;
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

/**
 * Return a previously checked out repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to return to
 * @param DATASTREAM* stream : Repack datastream to return
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_returnstream( REPACKSTREAMER repackst, DATASTREAM* stream ) {
   // check for NULL args
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return -1;
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

/**
 * Terminate the given repackstreamer and close all associated datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to close
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_complete( REPACKSTREAMER repackst ) {
   // check for NULL arg
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return -1;
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

/**
 * Abort the given repackstreamer, bypassing all locks and releasing all datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to abort
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_abort( REPACKSTREAMER repackst ) {
   // check for NULL arg
   if ( repackst == NULL ) {
      LOG( LOG_ERR, "Received a NULL repackstreamer ref\n" );
      errno = EINVAL;
      return -1;
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


//   -------------   INTERNAL FUNCTIONS    -------------


void process_deleteobj( marfs_position* pos, opinfo* op ) {
   marfs_ds* ds = &(pos->ns->prepo->datascheme);
   size_t countval = 0;
   // check for extendedinfo
   delobj_info* delobjinf = (delobj_info*)op->extendedinfo;
   if ( delobjinf != NULL ) {
      countval = delobjinf->offset; // skip ahead by some offset, if specified
   }
   while ( countval < op->count + delobjinf->offset ) {
      // identify the object target of the op
      FTAG tmptag = op->ftag;
      tmptag.objno += countval;
      char* objname = NULL;
      ne_erasure erasure;
      ne_location location;
      if ( datastream_objtarget( &(tmptag), ds, &(objname), &(erasure), &(location) ) ) {
         LOG( LOG_ERR, "Failed to identify object target %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         return;
      }
      // delete the object
      LOG( LOG_INFO, "Deleting object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid );
      int olderrno = errno;
      if ( ne_delete( ds->nectxt, objname, location ) ) {
         if ( errno == ENOENT ) {
            LOG( LOG_INFO, "Object %zu of stream \"%s\" was already deleted\n", tmptag.objno, tmptag.streamid );
         }
         else {
            LOG( LOG_ERR, "Failed to delete object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid );
            free( objname );
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            return;
         }
      }
      errno = olderrno;
      free( objname );
      countval++;
   }
   return;
}


void process_deleteref( const marfs_position* pos, opinfo* op ) {
   // verify we have extendedinfo
   delref_info* delrefinf = (delref_info*)op->extendedinfo;
   if ( delrefinf == NULL ) {
      LOG( LOG_ERR, "DEL-REF op is missing extendedinfo\n" );
      op->errval = EINVAL;
      return;
   }
   // convenience refs
   MDAL mdal = pos->ns->prepo->metascheme.mdal;
   // first, need to attach a GCTAG to the previous active file
   GCTAG gctag = {
      .refcnt = op->count,
      .delzero = delrefinf->delzero,
      .eos = delrefinf->eos,
      .inprog = 0
   };
   // note any previously deleted refs between us and prev_active_index
   if ( op->ftag.fileno > delrefinf->prev_active_index + 1 ) {
      gctag.refcnt += (op->ftag.fileno - 1) - delrefinf->prev_active_index;
      LOG( LOG_INFO, "Including previous gap of %zu files ( %zu resultant gap )\n",
                     (op->ftag.fileno - 1) - delrefinf->prev_active_index, gctag.refcnt );
   }
   if ( op->count ) { gctag.inprog = 1; } // set inprog, if we're actually doing any reference deletions
   size_t gctaglen = gctag_tostr( &(gctag), NULL, 0 );
   if ( gctaglen < 1 ) {
      LOG( LOG_ERR, "Failed to identify length of GCTAG for stream \"%s\"\n", op->ftag.streamid );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      return;
   }
   char* gctagstr = malloc( sizeof(char) * (gctaglen + 1) );
   if ( gctagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a GCTAG string for stream \"%s\"\n", op->ftag.streamid );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      return;
   }
   if ( gctag_tostr( &(gctag), gctagstr, gctaglen+1 ) != gctaglen ) {
      LOG( LOG_ERR, "GCTAG has an inconsistent length for stream \"%s\"\n", op->ftag.streamid );
      free( gctagstr );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      return;
   }
   // identify the appropriate reference table to be used for path determination
   HASH_TABLE reftable = NULL;
   if ( op->ftag.refbreadth == pos->ns->prepo->metascheme.refbreadth  &&
        op->ftag.refdepth == pos->ns->prepo->metascheme.refdepth  &&
        op->ftag.refdigits == pos->ns->prepo->metascheme.refdigits ) {
      // we can safely use the default reference table
      reftable = pos->ns->prepo->metascheme.reftable;
   }
   else {
      // we must generate a fresh ref table, based on FTAG values
      reftable = config_genreftable( NULL, NULL, op->ftag.refbreadth, op->ftag.refdepth, op->ftag.refdigits );
      if ( reftable == NULL ) {
         LOG( LOG_ERR, "Failed to generate reference table with values ( breadth=%d, depth=%d, digits=%d )\n",
              op->ftag.refbreadth, op->ftag.refdepth, op->ftag.refdigits );
         free( gctagstr );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         return;
      }
   }
   // identify the reference path of our initial target
   FTAG tmptag = op->ftag;
   tmptag.fileno = delrefinf->prev_active_index;
   char* reftgt = datastream_genrpath( &(tmptag), reftable );
   if ( reftgt == NULL ) {
      LOG( LOG_ERR, "Failed to identify reference path of active fileno %zu of stream \"%s\"\n", tmptag.fileno, op->ftag.streamid );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      free( gctagstr );
      return;
   }
   MDAL_FHANDLE activefile = mdal->openref( pos->ctxt, reftgt, O_RDWR, 0 );
   if ( activefile == NULL ) {
      LOG( LOG_ERR, "Failed to open handle for active fileno %zu of stream \"%s\"\n", tmptag.fileno, op->ftag.streamid );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      free( reftgt );
      free( gctagstr );
      return;
   }
   LOG( LOG_INFO, "Attaching GCTAG \"%s\" to reference file \"%s\"\n", gctagstr, reftgt );
   if ( mdal->fsetxattr( activefile, 1, GCTAG_NAME, gctagstr, gctaglen, 0 ) ) {
      LOG( LOG_ERR, "Failed to attach GCTAG \"%s\" to reference file \"%s\"\n", gctagstr, reftgt );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      free( reftgt );
      free( gctagstr );
      return;
   }
   free( gctagstr );
   // for a zero-count refdel op, we're done here
   if ( op->count == 0 ) {
      if ( mdal->close( activefile ) ) {
         LOG( LOG_WARNING, "Failed to close handle for active file \"%s\"\n", reftgt );
      }
      free( reftgt );
      return;
   }
   // iterate over reference targets
   size_t countval = 0;
   while ( countval < op->count ) {
      // identify the reference path
      tmptag = op->ftag;
      tmptag.fileno += countval;
      char* rpath = datastream_genrpath( &(tmptag), reftable );
      if ( rpath == NULL ) {
         LOG( LOG_ERR, "Failed to identify reference path of fileno %zu of stream \"%s\"\n", tmptag.fileno, op->ftag.streamid );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         mdal->close( activefile );
         free( reftgt );
         return;
      }
      // perform the deletion
      int olderrno = errno;
      errno = 0;
      if ( mdal->unlinkref( pos->ctxt, rpath )  &&  errno != ENOENT ) {
         LOG( LOG_ERR, "Failed to unlink reference path \"%s\"\n", rpath );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         free( rpath );
         mdal->close( activefile );
         free( reftgt );
         return;
      }
      errno = olderrno;
      LOG( LOG_INFO, "Deleted reference path \"%s\"\n", rpath );
      free( rpath );
      countval++;
   }
   // update GCTAG to reflect completion
   gctag.inprog = 0;
   gctaglen = gctag_tostr( &(gctag), NULL, 0 );
   if ( gctaglen < 1 ) {
      LOG( LOG_ERR, "Failed to identify length of GCTAG for stream \"%s\"\n", op->ftag.streamid );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      mdal->close( activefile );
      free( reftgt );
      return;
   }
   gctagstr = malloc( sizeof(char) * (gctaglen + 1) );
   if ( gctagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a GCTAG string for stream \"%s\"\n", op->ftag.streamid );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      mdal->close( activefile );
      free( reftgt );
      return;
   }
   if ( gctag_tostr( &(gctag), gctagstr, gctaglen+1 ) != gctaglen ) {
      LOG( LOG_ERR, "GCTAG has an inconsistent length for stream \"%s\"\n", op->ftag.streamid );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      free( gctagstr );
      mdal->close( activefile );
      free( reftgt );
      return;
   }
   LOG( LOG_INFO, "Updating GCTAG to \"%s\" for reference file \"%s\"\n", gctagstr, reftgt );
   if ( mdal->fsetxattr( activefile, 1, GCTAG_NAME, gctagstr, gctaglen, 0 ) ) {
      LOG( LOG_ERR, "Failed to update GCTAG to \"%s\" for reference file \"%s\"\n", gctagstr, reftgt );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      free( gctagstr );
      mdal->close( activefile );
      free( reftgt );
      return;
   }
   free( gctagstr );
   // close our active file, and terminate
   if ( mdal->close( activefile ) ) {
      LOG( LOG_ERR, "Failed to close handle for active file \"%s\"\n", reftgt );
      op->errval = (errno) ? errno : ENOTRECOVERABLE;
      free( reftgt );
      return;
   }
   free( reftgt );
   return;
}


void process_rebuild( const marfs_position* pos, opinfo* op ) {
   // quick refs
   rebuild_info* rebinf = (rebuild_info*)op->extendedinfo;
   marfs_ds* ds = &(pos->ns->prepo->datascheme);
   marfs_ms* ms = &(pos->ns->prepo->metascheme);
   size_t countval = 0;
   while ( countval < op->count ) {
      // identify the object target of the op
      FTAG tmptag = op->ftag;
      tmptag.objno += countval;
      char* objname = NULL;
      ne_erasure erasure;
      ne_location location;
      if ( datastream_objtarget( &(tmptag), ds, &(objname), &(erasure), &(location) ) ) {
         LOG( LOG_ERR, "Failed to identify object target %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         return;
      }
      // open an object handle
      ne_handle obj = ne_open( ds->nectxt, objname, location, erasure, NE_REBUILD );
      if ( obj == NULL ) {
         LOG( LOG_ERR, "Failed to open rebuild handle for object \"%s\"\n", objname );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         free( objname );
         return;
      }
      free( objname );
      // if we have an rtag value, seed it in prior to rebuilding
      if ( rebinf  &&  rebinf->rtag.meta_status  &&  rebinf->rtag.data_status ) {
         if ( ne_seed_status( obj, &(rebinf->rtag) ) ) {
            LOG( LOG_WARNING, "Failed to seed rtag status into handle for object %zu of stream \"%s\"\n",
                              tmptag.objno, tmptag.streamid );
         }
      }
      // rebuild the object, performing up to 2 attempts
      char iteration = 0;
      while ( iteration < 2 ) {
         LOG( LOG_INFO, "Rebuilding object %zu of stream \"%s\" (attempt %d)\n",
                        tmptag.objno, tmptag.streamid, (int)iteration + 1 );
         int rebuildres = ne_rebuild( obj, NULL, NULL );
         if ( rebuildres < 0 ) {
            LOG( LOG_ERR, "Failed to rebuild object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid );
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            if ( ne_abort( obj ) ) {
               LOG( LOG_ERR, "Failed to properly abort rebuild handle for object %zu of stream \"%s\"\n",
                             tmptag.objno, tmptag.streamid );
            }
            return;
         }
         else { break; }
         iteration++;
      }
      // check for excessive rebuild reattempts
      if ( iteration >= 2 ) {
         LOG( LOG_ERR, "Excessive reattempts for rebuild of object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         if ( ne_abort( obj ) ) {
            LOG( LOG_ERR, "Failed to properly abort rebuild handle for object %zu of stream \"%s\"\n",
                          tmptag.objno, tmptag.streamid );
         }
         return;
      }
      // close the object reference
      if ( ne_close( obj, NULL, NULL ) ) {
         LOG( LOG_ERR, "Failed to finalize rebuild of object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         return;
      }
      countval++;
   }
   // potentially cleanup the rtag
   if ( rebinf  &&  rebinf->rtag.meta_status  &&  rebinf->rtag.data_status ) {
      // generate the RTAG name
      char* rtagstr = rtag_getname( op->ftag.objno );
      if ( rtagstr == NULL ) {
         LOG( LOG_ERR, "Failed to identify the name of object %zu RTAG in stream \"%s\"\n", op->ftag.objno, op->ftag.streamid );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         return;
      }
      // open a file handle for the marker
      if ( rebinf->markerpath == NULL ) {
         LOG( LOG_ERR, "No marker path available by which to remove RTAG of object %zu in stream \"%s\"\n",
                       op->ftag.objno, op->ftag.streamid );
         op->errval = EINVAL;
         free( rtagstr );
         return;
      }
      MDAL_FHANDLE mhandle = ms->mdal->openref( pos->ctxt, rebinf->markerpath, O_RDONLY, 0 );
      if ( mhandle == NULL ) {
         LOG( LOG_ERR, "Failed to open handle for marker path \"%s\"\n", rebinf->markerpath );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         free( rtagstr );
         return;
      }
      // remove the RTAG
      if ( ms->mdal->fremovexattr( mhandle, 1, rtagstr ) ) {
         LOG( LOG_ERR, "Failed to remove \"%s\" xattr from marker file \"%s\"\n", rtagstr, rebinf->markerpath );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         ms->mdal->close( mhandle );
         free( rtagstr );
         return;
      }
      free( rtagstr );
      // close our handle
      if ( ms->mdal->close( mhandle ) ) {
         LOG( LOG_ERR, "Failed to close handle for marker file \"%s\"\n", rebinf->markerpath );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         return;
      }
   }
   // potentially cleanup the rebuild marker
   if ( rebinf  &&  rebinf->markerpath ) {
      // unlink the rebuild marker
      if ( ms->mdal->unlinkref( pos->ctxt, rebinf->markerpath ) ) {
         LOG( LOG_ERR, "Failed to unlink marker file \"%s\"\n", rebinf->markerpath );
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         return;
      }
   }
   // rebuild complete
   return;
}

// TODO
void process_repack( const marfs_position* pos, opinfo* op, REPACKSTREAMER rpckstr ) {
}


void destroystreamwalker( streamwalker walker ) {
   if ( walker ) {
      marfs_ms* ms = &(walker->pos.ns->prepo->metascheme);
      if ( walker->reftable  &&  walker->reftable != ms->reftable ) {
         // destroy the custom hash table
         HASH_NODE* nodelist = NULL;
         size_t count = 0;
         if ( hash_term( walker->reftable, &(nodelist), &(count) ) ) {
            LOG( LOG_WARNING, "Failed to delete non-NS reference table\n" );
         }
         else {
            while ( count ) {
               count--;
               free( nodelist[count].name );
            }
            free( nodelist );
         }
      }
      if ( walker->ftagstr ) { free( walker->ftagstr ); }
      if ( walker->gcops ) { resourcelog_freeopinfo( walker->gcops ); }
      if ( walker->rpckops ) { resourcelog_freeopinfo( walker->rpckops ); }
      if ( walker->rbldops ) { resourcelog_freeopinfo( walker->rbldops ); }
      if ( walker->ftag.ctag ) { free( walker->ftag.ctag ); }
      if ( walker->ftag.streamid ) { free( walker->ftag.streamid ); }
      free( walker );
   }
}

int process_getfileinfo( const char* reftgt, char getxattrs, streamwalker walker, char* filestate ) {
   MDAL mdal = walker->pos.ns->prepo->metascheme.mdal;
   if ( getxattrs ) {
      // open the target file
      int olderrno = errno;
      errno = 0;
      MDAL_FHANDLE handle = mdal->openref( walker->pos.ctxt, reftgt, O_RDONLY, 0 );
      if ( handle == NULL ) {
         if ( errno == ENOENT ) {
            LOG( LOG_INFO, "Reference file does not exist: \"%s\"\n", reftgt );
            *filestate = 0;
            return 0;
         }
         LOG( LOG_ERR, "Failed to open current reference file target: \"%s\"\n", reftgt );
         return -1;
      }
      // attempt to retrieve the GC tag
      // NOTE -- it is ESSENTIAL to do this prior to the FTAG, so that ftagstr always contains the actual FTAG string
      errno = 0;
      ssize_t getres = mdal->fgetxattr( handle, 1, GCTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1 );
      // check for overflow
      if ( getres > 0  &&  getres >= walker->ftagstralloc ) {
         // increase our allocated string length
         char* newstr = malloc( sizeof(char) * (getres + 1) );
         if ( newstr == NULL ) {
            LOG( LOG_ERR, "Failed to increase gctag string allocation to length of %zu\n", getres + 1 );
            mdal->close( handle );
            return -1;
         }
         // swap the new reference in
         free( walker->ftagstr );
         walker->ftagstr = newstr;
         walker->ftagstralloc = getres + 1;
         // pull the xattr again
         if ( mdal->fgetxattr( handle, 1, GCTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1 ) != getres ) {
            LOG( LOG_ERR, "Inconsistent length for gctag of reference file target: \"%s\"\n", reftgt );
            mdal->close( handle );
            return -1;
         }
      }
      // check for error ( missing xattr is acceptable here though )
      if ( getres <= 0  &&  errno != ENODATA ) {
         LOG( LOG_ERR, "Failed to retrieve gctag of reference file target: \"%s\"\n", reftgt );
         mdal->close( handle );
         return -1;
      }
      else if ( getres > 0 ) {
         // we must parse the GC tag value
         *(walker->ftagstr + getres) = '\0'; // ensure our string is NULL terminated
         if ( gctag_initstr( &(walker->gctag), walker->ftagstr ) ) {
            LOG( LOG_ERR, "Failed to parse GCTAG for reference file target: \"%s\"\n", reftgt );
            mdal->close( handle );
            return -1;
         }
      }
      else {
         // no GCTAG, so zero out values
         walker->gctag.refcnt = 0;
         walker->gctag.eos = 0;
         walker->gctag.inprog = 0;
         walker->gctag.delzero = 0;
      }
      // retrieve FTAG of the current file
      getres = mdal->fgetxattr( handle, 1, FTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1 );
      // check for overflow
      if ( getres >= walker->ftagstralloc ) {
         // double our allocated string length
         char* newstr = malloc( sizeof(char) * (getres + 1) );
         if ( newstr == NULL ) {
            LOG( LOG_ERR, "Failed to increase ftag string allocation to length of %zu\n", getres + 1 );
            mdal->close( handle );
            return -1;
         }
         // swap the new reference in
         free( walker->ftagstr );
         walker->ftagstr = newstr;
         walker->ftagstralloc = getres + 1;
         // pull the xattr again
         if ( mdal->fgetxattr( handle, 1, FTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1 ) != getres ) {
            LOG( LOG_ERR, "Inconsistent length for ftag of reference file target: \"%s\"\n", reftgt );
            mdal->close( handle );
            return -1;
         }
      }
      // check for error
      if ( getres <= 0 ) {
         LOG( LOG_ERR, "Failed to retrieve ftag of reference file target: \"%s\"\n", reftgt );
         mdal->close( handle );
         return -1;
      }
      *(walker->ftagstr + getres) = '\0'; // ensure our string is NULL terminated
      // potentially clear old ftag values
      if ( walker->ftag.ctag ) { free( walker->ftag.ctag ); }
      if ( walker->ftag.streamid ) { free( walker->ftag.streamid ); }
      // parse the ftag
      if ( ftag_initstr( &(walker->ftag), walker->ftagstr ) ) {
         LOG( LOG_ERR, "Failed to parse ftag value of reference file target: \"%s\"\n", reftgt );
         mdal->close( handle );
         return -1;
      }
      // stat the file
      if ( mdal->fstat( handle, &(walker->stval) ) ) {
         LOG( LOG_ERR, "Failed to stat reference file target via handle: \"%s\"\n", reftgt );
         mdal->close( handle );
         return -1;
      }
      // finally, close the file
      if ( mdal->close( handle ) ) {
         // just complain
         LOG( LOG_WARNING, "Failed to close handle for reference target: \"%s\"\n", reftgt );
      }
      // restore old values
      errno = olderrno;
      // NOTE -- The resource manager will skip pulling RTAG xattrs, in this specific case.
      //         The 'location-based' rebuild, performed by this code, is intended for worst-case data damage situations.
      //         It is intended to rebuild all objects tied to a specific location, without the need for a client to read 
      //         and tag those objects in advance.  As such, the expectation is that no RTAG values will exist.  
      //         If they do, they will be rebuilt seperately, via their rebuild marker file.
      // populate state value based on link count
      *filestate = ( walker->stval.st_nlink > 1 ) ? 2 : 1;
      return 0;
   }
   int olderrno = errno;
   errno = 0;
   // stat the file by path
   if ( mdal->statref( walker->pos.ctxt, reftgt, &(walker->stval) ) ) {
      if ( errno == ENOENT ) {
         LOG( LOG_INFO, "Reference file does not exist: \"%s\"\n", reftgt );
         *filestate = 0;
         return 0;
      }
      LOG( LOG_ERR, "Failed to stat reference file target via handle: \"%s\"\n", reftgt );
      return -1;
   }
   // restore old values
   errno = olderrno;
   // zero out some xattr values, so we don't get confused
   walker->gctag.refcnt = 0;
   walker->gctag.eos = 0;
   walker->gctag.delzero = 0;
   walker->gctag.inprog = 0;
   // populate state value based on link count
   *filestate = ( walker->stval.st_nlink > 1 ) ? 2 : 1;
   return 0;
}

int process_identifyoperation( opinfo** opchain, operation_type type, FTAG* ftag, opinfo** optgt ) {
   opinfo* prevop = NULL;
   if ( opchain  &&  *opchain ) {
      // check for any existing ops of this type in the chain
      opinfo* parseop = *opchain;
      while ( parseop ) {
         // for most ops, matching on type is sufficent to reuse the same op tgt
         // For object deletions, repacks, and rebuilds, we need to check that the new tgt is in the same 'chain'
         if ( parseop->type == type  &&
              ( type != MARFS_DELETE_OBJ_OP  ||  ftag->objno == (parseop->ftag.objno + parseop->count) )  &&
              ( type != MARFS_REPACK_OP  ||  ftag->fileno == (parseop->ftag.fileno + parseop->count) )  &&
              ( type != MARFS_REBUILD_OP  ||  ftag->objno == (parseop->ftag.objno + parseop->count) )
            ) {
            *optgt = parseop;
            return 0;
         }
         prevop = parseop;
         parseop = parseop->next;
      }
   }
   // allocate a new operation struct
   opinfo* newop = malloc( sizeof( struct opinfo_struct ) );
   if ( newop == NULL ) {
      LOG( LOG_ERR,"Failed to allocate new opinfo structure\n" );
      return -1;
   } 
   newop->type = type;
   newop->extendedinfo = NULL;
   newop->start = 1;
   newop->count = 0;
   newop->errval = 0;
   newop->ftag = *ftag;
//      if ( prevop  &&  strcmp( prevop->ftag.ctag, ftag->ctag ) == 0 ) {
//         newop->ftag.ctag = prevop->ftag.ctag;
//      }
   // create new strings, so we don't have a potential double-free in the future
   newop->ftag.ctag = strdup( ftag->ctag );
   if ( newop->ftag.ctag == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate FTAG ctag string: \"%s\"\n", ftag->ctag );
      free( newop );
      return -1;
   }
   newop->ftag.streamid = strdup( ftag->streamid );
   if ( newop->ftag.streamid == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate FTAG streamid string: \"%s\"\n", ftag->streamid );
      free( newop->ftag.ctag );
      free( newop );
      return -1;
   }
   // allocate extended info
   switch( type ) {
      case MARFS_DELETE_OBJ_OP:
         newop->extendedinfo = calloc( 1, sizeof( struct delobj_info_struct ) );
         break;
      case MARFS_DELETE_REF_OP:
         newop->extendedinfo = calloc( 1, sizeof( struct delref_info_struct ) );
         break;
      case MARFS_REBUILD_OP:
         newop->extendedinfo = calloc( 1, sizeof( struct rebuild_info_struct ) );
         break;
      case MARFS_REPACK_OP:
         newop->extendedinfo = calloc( 1, sizeof( struct repack_info_struct ) );
         break;
   }
   // check for allocation failure
   if ( newop->extendedinfo == NULL ) {
      LOG( LOG_ERR, "Failed to allocate operation extended info\n" );
      free( newop->ftag.streamid );
      free( newop->ftag.ctag );
      free( newop );
      return -1;
   }
   newop->next = NULL;
   // insert the new op into the chain and return its reference
   if ( opchain ) {
      if ( type == MARFS_DELETE_REF_OP  &&  prevop ) {
         // special case, reference deletion should always be inserted at the tail
         prevop->next = newop;
      }
      else {
         // default to inserting at the head
         opinfo* prevhead = *opchain;
         *opchain = newop;
         newop->next = prevhead;
      }
   }
   *optgt = newop;
   return 0;
} 


//   -------------   RESOURCE PROCESSING FUNCTIONS    -------------

/**
 * Process the next entry from the given refdir scanner
 * @param marfs_ns* ns : Reference to the current NS
 * @param MDAL_SCANNER refdir : Scanner reference to iterate through
 * @param char** reftgt : Reference to be populated with the next reference path tgt
 *                        Left NULL if the ref dir has been completely traversed
 * @param ssize_t* tgtval : Reference to be populated with the tgt's file/objno value
 *                          ( see ftag_metainfo() return value )
 * @return int : Value of zero -> the reference dir has been completed and closed,
 *               Value of one -> entry is populated with the start of a datastream,
 *               Value of two -> entry is populated with a rebuild marker file,
 *               Value of three -> entry is populated with a repack marker file,
 *               Value of ten -> entry is of an unknown type
 *               Value of negative one -> an error occurred
 */
int process_refdir( marfs_ns* ns, MDAL_SCANNER refdir, const char* refdirpath, char** reftgt, ssize_t* tgtval ) {
	// validate args
	if ( ns == NULL ) {
      LOG( LOG_ERR, "Received a NULL NS ref\n" );
      errno = EINVAL;
      return -1;
   }
   if ( refdir == NULL ) {
      LOG( LOG_ERR, "Received a NULL scanner\n" );
      errno = EINVAL;
      return -1;
   }
   if ( refdirpath == NULL ) {
      LOG( LOG_ERR, "Received a NULL reference dir path\n" );
      errno = EINVAL;
      return -1;
   }
   if ( reftgt == NULL ) {
      LOG( LOG_ERR, "Received a NULL reftgt value\n" );
      errno = EINVAL;
      return -1;
   }
   if ( tgtval == NULL ) {
      LOG( LOG_ERR, "Received a NULL tgtval value\n" );
      errno = EINVAL;
      return -1;
   }
   // scan through the dir until we find something of interest
   MDAL mdal = ns->prepo->metascheme.mdal;
   struct dirent* dent;
   int olderrno = errno;
   errno = 0;
   char type = 9;
   while ( (dent = mdal->scan( refdir )) != NULL ) {
      // skip any 'hidden' files or default ( '.'/'..' ) entries
      if ( *(dent->d_name) == '.' ) {
         continue;
      }
      // identify the entry type
      ssize_t parseval = ftag_metainfo( dent->d_name, &type );
      if ( parseval < 0 ) {
         LOG( LOG_WARNING, "Failed to identify entry type: \"%s\"\n", dent->d_name );
         // unknown type ( set to 9, since we'll return type + 1 )
         type = 9;
         // rezero errno, so it doesn't mess up our scan() error check
         errno = 0;
      }
      else if ( type == 0  &&  parseval != 0 ) {
         // skip any non-zero reference targets
         continue;
      }
      *tgtval = parseval;
      break; // exit the loop by default
   }
   // check for scan failure
   if ( errno ) {
      LOG( LOG_ERR, "Detected failure of scan() for refdir \"%s\"\n", refdirpath );
      return -1;
   }
   else if ( dent == NULL ) { // check for EOF
      if ( mdal->closescanner( refdir ) ) {
         // just complain
         LOG( LOG_WARNING, "Failed to close scanner for ref dir \"%s\"\n", refdirpath );
      }
      return 0;
   }
   // populate the reftgt string
   int rpathlen = snprintf( NULL, 0, "%s/%s", refdirpath, dent->d_name );
   if ( rpathlen < 1 ) {
      LOG( LOG_ERR, "Failed to identify length of ref path for \"%s\"\n", dent->d_name );
      return -1;
   }
   *reftgt = malloc( sizeof(char) * (rpathlen + 1) );
   if ( reftgt == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for ref path of \"%s\"\n", dent->d_name );
      return -1;
   }
   if ( snprintf( *reftgt, rpathlen + 1, "%s/%s", refdirpath, dent->d_name ) != rpathlen ) {
      LOG( LOG_ERR, "Inconsistent length for ref path of \"%s\"\n", dent->d_name );
      free( *reftgt );
      *reftgt = NULL;
      errno = EDOM;
      return -1;
   }
   errno = olderrno; // restore old errno
   return ( (int)type + 1 );
}

/**
 * Generate a rebuild opinfo element corresponding to the given marker and object
 * @param char* markerpath : Reference path of the rebuild marker ( will be bundled into the opinfo ref )
 * @param time_t rebuildthresh : Rebuild threshold value ( files more recent than this will be ignored )
 * @param size_t objno : Index of the object corresponding to the marker
 * @return opinfo* : Reference to the newly generated op, or NULL on failure
 *                   NOTE -- errno will be set to ETIME, specifically in the case of the marker being too recent
 *                           ( ctime >= rebuildthresh )
 */
opinfo* process_rebuildmarker( marfs_position* pos, char* markerpath, time_t rebuildthresh, size_t objno ) {
   // check args
   if ( pos == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_position reference\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( markerpath == NULL ) {
      LOG( LOG_ERR, "Received a NULL markerpath reference\n" );
      errno = EINVAL;
      return NULL;
   }
   // convenience refs
   marfs_ms* ms = &(pos->ns->prepo->metascheme);
   // open a file handle for the marker
   MDAL_FHANDLE mhandle = ms->mdal->openref( pos->ctxt, markerpath, O_RDONLY, 0 );
   if ( mhandle == NULL ) {
      LOG( LOG_ERR, "Failed to open handle for marker path \"%s\"\n", markerpath );
      return NULL;
   }
   // stat the marker
   struct stat stval;
   if ( ms->mdal->fstat( mhandle, &(stval) ) ) {
      LOG( LOG_ERR, "Failed to stat via handle for marker path \"%s\"\n", markerpath );
      return NULL;
   }
   if ( stval.st_ctime >= rebuildthresh ) {
      LOG( LOG_INFO, "Marker path \"%s\" ctime is too recent to rebuild\n", markerpath );
      ms->mdal->close( mhandle );
      errno = ETIME;
      return NULL;
   }
   // retrieve the FTAG value
   ssize_t ftagstrlen = ms->mdal->fgetxattr( mhandle, 1, FTAG_NAME, NULL, 0 );
   if ( ftagstrlen < 2 ) {
      LOG( LOG_ERR, "Failed to retrieve FTAG from marker file \"%s\"\n", markerpath );
      ms->mdal->close( mhandle );
      return NULL;
   }
   char* ftagstr = malloc( sizeof(char) * (ftagstrlen + 1) );
   if ( ftagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate FTAG string of length %zu\n", ftagstrlen + 1 );
      ms->mdal->close( mhandle );
      return NULL;
   }
   if ( ms->mdal->fgetxattr( mhandle, 1, FTAG_NAME, ftagstr, ftagstrlen ) != ftagstrlen ) {
      LOG( LOG_ERR, "FTAG of marker file \"%s\" has an inconsistent length\n", markerpath );
      free( ftagstr );
      ms->mdal->close( mhandle );
      return NULL;
   }
   *(ftagstr + ftagstrlen) = '\0'; // ensure a NULL-terminated string
   // allocate a new operation struct
   opinfo* op = calloc( 1, sizeof( struct opinfo_struct ) );
   if ( op == NULL ) {
      LOG( LOG_ERR, "Failed to allocate opinfo struct\n" );
      free( ftagstr );
      ms->mdal->close( mhandle );
      return NULL;
   }
   rebuild_info* rinfo = calloc( 1, sizeof( struct rebuild_info_struct ) );
   if ( rinfo == NULL ) {
      LOG( LOG_ERR, "Failed to allocate rebuild extended info struct\n" );
      free( op );
      free( ftagstr );
      ms->mdal->close( mhandle );
      return NULL;
   }
   op->type = MARFS_REBUILD_OP;
   op->extendedinfo = (void*)rinfo;
   op->start = 1;
   op->count = 1;
   // parse in the FTAG
   if ( ftag_initstr( &(op->ftag), ftagstr ) ) {
      LOG( LOG_ERR, "Failed to parse FTAG value from marker file \"%s\"\n", markerpath );
      free( rinfo );
      free( op );
      free( ftagstr );
      ms->mdal->close( mhandle );
      return NULL;
   }
   free( ftagstr );
   // allocate health arrays for the rebuild info RTAG
   rinfo->rtag.meta_status = calloc( sizeof(char), op->ftag.protection.N + op->ftag.protection.E );
   if ( rinfo->rtag.meta_status == NULL ) {
      LOG( LOG_ERR, "Failed to allocate rebuild info meta_status array\n" );
      free( rinfo );
      free( op );
      ms->mdal->close( mhandle );
      return NULL;
   }
   rinfo->rtag.data_status = calloc( sizeof(char), op->ftag.protection.N + op->ftag.protection.E );
   if ( rinfo->rtag.data_status == NULL ) {
      LOG( LOG_ERR, "Failed to allocate rebuild info data_status array\n" );
      free( rinfo->rtag.meta_status );
      free( rinfo );
      free( op );
      ms->mdal->close( mhandle );
      return NULL;
   }
   // generate the RTAG name
   char* rtagname = rtag_getname( objno );
   if ( rtagname == NULL ) {
      LOG( LOG_ERR, "Failed to identify the name of object %zu RTAG name\n", objno );
      free( rinfo );
      free( op );
      ms->mdal->close( mhandle );
      return NULL;
   }
   // retrieve the RTAG from the marker file
   // NOTE -- RTAG is just a rebuild speedup.  A missing value doesn't prevent rebuild completion.
   //         Therefore, ENOATTR is acceptable.  Anything else, however, is unusual enough to abort for.
   ssize_t rtaglen = ms->mdal->fgetxattr( mhandle, 1, rtagname, NULL, 0 );
   if ( rtaglen < 1  &&  errno != ENOATTR ) {
      LOG( LOG_ERR, "Failed to retrieve \"%s\" value from marker file \"%s\"\n", rtagname, markerpath );
      free( rtagname );
      free( rinfo );
      free( op );
      ms->mdal->close( mhandle );
      return NULL;
   }
   if ( rtaglen > 0 ) {
      // allocate RTAG string
      char* rtagstr = malloc( sizeof(char) * (1 + rtaglen) );
      if ( rtagstr == NULL ) {
         LOG( LOG_ERR, "Failed to allocate an RTAG string\n" );
         free( rtagname );
         free( rinfo );
         free( op );
         ms->mdal->close( mhandle );
         return NULL;
      }
      // retrieve the RTAG value, for real
      if ( ms->mdal->fgetxattr( mhandle, 1, rtagname, rtagstr, rtaglen + 1 ) != rtaglen ) {
         LOG( LOG_ERR, "\"%s\" value of \"%s\" marker file has an inconsistent length\n", rtagname, markerpath );
         free( rtagstr );
         free( rtagname );
         free( rinfo );
         free( op );
         ms->mdal->close( mhandle );
         return NULL;
      }
      *(rtagstr + rtaglen) = '\0'; // ensure a NULL-terminated value
      // populate RTAG entry
      if ( rtag_initstr( &(rinfo->rtag), op->ftag.protection.N + op->ftag.protection.E, rtagstr ) ) {
         LOG( LOG_ERR, "Failed to parse \"%s\" value of marker file \"%s\"\n", rtagname, markerpath );
         free( rtagstr );
         free( rtagname );
         free( rinfo );
         free( op );
         ms->mdal->close( mhandle );
         return NULL;
      }
      free( rtagstr );
   }
   free( rtagname );
   // duplicate marker path
   rinfo->markerpath = strdup( markerpath );
   if ( rinfo->markerpath == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate rebuild marker path \"%s\"\n", markerpath );
      free( rinfo );
      free( op );
      ms->mdal->close( mhandle );
      return NULL;
   }
   // close our handle
   if ( ms->mdal->close( mhandle ) ) {
      LOG( LOG_ERR, "Failed to close handle for marker file \"%s\"\n", markerpath );
      free( rinfo );
      free( op );
      return NULL;
   }
   return op;
}

/**
 * Open a streamwalker based on the given fileno zero reference target
 * @param marfs_position* pos : MarFS position to be used by this walker
 * @param const char* reftgt : Reference path of the first ( fileno zero ) file of the datastream
 * @param thresholds thresh : Threshold values to be used for determining operation targets
 * @param ne_location* rebuildloc : Location-based rebuild target
 * @return streamwalker : Newly generated streamwalker, or NULL on failure
 */
streamwalker process_openstreamwalker( marfs_position* pos, const char* reftgt, thresholds thresh, ne_location* rebuildloc ) {
	// validate args
	if ( pos == NULL ) {
      LOG( LOG_ERR, "Received a NULL position ref\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( pos->ns == NULL ) {
      LOG( LOG_ERR, "Received a position ref with no defined NS\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( reftgt == NULL ) {
      LOG( LOG_ERR, "Received a NULL reference tgt path\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( thresh.rebuildthreshold  &&  rebuildloc == NULL ) {
      LOG( LOG_ERR, "Rebuild threshold is set, but no rebuild location was specified\n" );
      errno = EINVAL;
      return NULL;
   }
   // allocate a new streamwalker struct
   streamwalker walker = malloc( sizeof( struct streamwalker_struct ) );
   if ( walker == NULL ) {
      LOG( LOG_ERR, "Failed to allocate streamwalker\n" );
      return NULL;
   }
   // establish position
   walker->pos = *pos;
   if ( walker->pos.ctxt == NULL ) {
      LOG( LOG_ERR, "Received a marfs position for NS \"%s\" with no associated CTXT\n", pos->ns->idstr );
      free( walker );
      errno = EINVAL;
      return NULL;
   }
   // populate initialization elements
   walker->gcthresh = thresh.gcthreshold;
   walker->repackthresh = thresh.repackthreshold;
   walker->rebuildthresh = thresh.rebuildthreshold;
   if ( rebuildloc ) {
      walker->rebuildloc = *rebuildloc;
   }
   else {
      bzero( &(walker->rebuildloc), sizeof( ne_location ) );
   }
   // zero out report info
   bzero( &(walker->report), sizeof( streamwalker_report ) );
   // initialize iteration info
   walker->fileno = 0;
   walker->objno = 0;
   // populate a bunch of placeholder info for the remainder
   walker->reftable = NULL;
   bzero( &(walker->stval), sizeof( struct stat ) );
   bzero( &(walker->ftag), sizeof( FTAG ) );
   bzero( &(walker->gctag), sizeof( GCTAG ) );
   walker->headerlen = 0;
   walker->ftagstr = malloc( sizeof(char) * 1024 );
   if ( walker->ftagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate initial ftag string buffer\n" );
      free( walker );
      return NULL;
   }
   walker->ftagstralloc = 1024;
   walker->gcops = NULL;
   walker->activefiles = 0;
   walker->activeindex = 0;
   walker->rpckops = NULL;
   walker->activebytes = 0;
   walker->rbldops = NULL;
   // retrieve xattrs from the inital stream file
   char filestate = 0;
   if ( process_getfileinfo( reftgt, 1, walker, &(filestate) )  ||  !(filestate) ) {
      LOG( LOG_ERR, "Failed to get info from initial reference target: \"%s\"\n", reftgt );
      free( walker->ftagstr );
      free( walker );
      return NULL;
   }
   // calculate our header length
   RECOVERY_HEADER header = {
      .majorversion = walker->ftag.majorversion,
      .minorversion = walker->ftag.minorversion,
      .ctag = walker->ftag.ctag,
      .streamid = walker->ftag.streamid
   };
   walker->headerlen = recovery_headertostr(&(header), NULL, 0);
   // calculate the ending position of this file
   size_t dataperobj = ( walker->ftag.objsize ) ?
                         walker->ftag.objsize - (walker->headerlen + walker->ftag.recoverybytes) : 0;
   size_t endobj = walker->ftag.objno; // update ending object index
   if ( dataperobj ) {
      // calculate the final object referenced by this file
      size_t finobjbounds = (walker->ftag.bytes + walker->ftag.offset - walker->headerlen) / dataperobj;
      if ( (walker->ftag.state & FTAG_DATASTATE) >= FTAG_FIN  &&
            finobjbounds  &&
            (walker->ftag.bytes + walker->ftag.offset - walker->headerlen) % dataperobj == 0 ) {
         finobjbounds--;
      }
      endobj += finobjbounds;
   }
   // TODO sanity checks
   // perform state checking for this first file
   char assumeactive = 0;
   char eos = walker->ftag.endofstream;
   if ( (walker->ftag.state & FTAG_DATASTATE) < FTAG_FIN ) { eos = 1; }
   if ( walker->gctag.eos ) { eos = 1; }
   if ( filestate == 1 ) {
      // file is inactive
      if ( walker->gcthresh  &&  walker->stval.st_ctime < walker->gcthresh ) {
         // this file is elligible for GC
         if ( eos ) {
            // only GC this initial ref if it is the last one remaining
            opinfo* optgt = NULL;
            if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_REF_OP, &(walker->ftag), &(optgt) ) ) {
               LOG( LOG_ERR, "Failed to identify operation target for deletion of file %zu\n", walker->ftag.fileno );
               destroystreamwalker( walker );
               return NULL;
            }
            // impossible to have an existing op; populate new op
            optgt->count = 1;
            delref_info* delrefinf = optgt->extendedinfo;
            delrefinf->prev_active_index = walker->activeindex;
            delrefinf->eos = 1;
            walker->report.delfiles++;
            walker->report.delstreams++;
         }
         // potentially generate object GC ops, only if we haven't already
         if ( (endobj != walker->objno  ||  eos)  &&  !(walker->gctag.delzero) ) {
            // potentially generate GC ops for objects spanned by this file
            FTAG tmptag = walker->ftag;
            opinfo* optgt;
            size_t finobj = endobj;
            if ( eos ) { finobj++; } // include the final obj referenced by this file, specifically if no files follow
            while ( tmptag.objno < finobj ) {
               // generate ops for all but the last referenced object
               optgt = NULL;
               if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(tmptag), &(optgt) ) ) {
                  LOG( LOG_ERR, "Failed to identify operation target for deletion of spanned obj %zu\n", tmptag.objno );
                  destroystreamwalker( walker );
                  return NULL;
               }
               // sanity check
               if ( optgt->count + optgt->ftag.objno != tmptag.objno ) {
                  LOG( LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n",
                                optgt->count + optgt->ftag.objno, tmptag.objno );
                  destroystreamwalker( walker );
                  return NULL;
               }
               // update operation
               optgt->count++;
               // update our record
               walker->report.delobjs++;
               // iterate to the next obj
               tmptag.objno++;
            }
            // need to generate a 'dummy' refdel op, specifically to drop a GCTAG on file zero
            optgt = NULL;
            if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_REF_OP, &(walker->ftag), &(optgt) ) ) {
               LOG( LOG_ERR, "Failed to identify operation target for attachment of delzero tag\n" );
               destroystreamwalker( walker );
               return NULL;
            }
            delref_info* delrefinf = (delref_info*)optgt->extendedinfo;
            delrefinf->delzero = 1;
            delrefinf->eos = eos;
         }
      }
      else if ( walker->stval.st_ctime >= walker->gcthresh ) {
         // this file was too recently deactivated to gc
         walker->report.volfiles++;
         assumeactive = 1;
      }
   }
   walker->report.filecount++;
   walker->report.bytecount += walker->ftag.bytes;
   walker->report.streamcount++;
   // NOTE -- technically, objcount will run one object 'ahead' until iteration completion ( we don't count final obj )
   if ( !(walker->gctag.delzero) ) { walker->report.objcount += endobj + 1; } // note first obj set, if not already deleted
   else if ( !(eos) ) { walker->report.objcount += 1; } // only count ahead if this is the sole file remaining
   LOG( LOG_INFO, "Noting %zu active objects ( one ahead ) from file zero\n", walker->report.objcount );
   if ( filestate > 1 ) {
      // file is active
      walker->report.fileusage++;
      walker->report.byteusage += walker->ftag.bytes;
      // TODO potentially generate repack op
      // TODO potentially generate rebuild op
   }
   if ( filestate > 1  ||  assumeactive ) {
      // update state to reflect active initial file
      walker->activefiles++;
      walker->activebytes += walker->ftag.bytes;
   }
   // update walker state to reflect new target
   walker->objno = endobj;
   // identify the appropriate reference table for stream iteration
   if ( walker->ftag.refbreadth == walker->pos.ns->prepo->metascheme.refbreadth  &&
        walker->ftag.refdepth == walker->pos.ns->prepo->metascheme.refdepth  &&
        walker->ftag.refdigits == walker->pos.ns->prepo->metascheme.refdigits ) {
      // we can safely use the default reference table
      walker->reftable = walker->pos.ns->prepo->metascheme.reftable;
   }
   else {
      // we must generate a fresh ref table, based on FTAG values
      walker->reftable = config_genreftable( NULL, NULL, walker->ftag.refbreadth, walker->ftag.refdepth, walker->ftag.refdigits );
      if ( walker->reftable == NULL ) {
         LOG( LOG_ERR, "Failed to generate reference table with values ( breadth=%d, depth=%d, digits=%d )\n",
              walker->ftag.refbreadth, walker->ftag.refdepth, walker->ftag.refdigits );
         destroystreamwalker( walker );
         return NULL;
      }
   }
   // return the initialized walker
   return walker;
}

/**
 * Iterate over a datastream, accumulating quota values and identifying operation targets
 * NOTE -- This func will return all possible operations, given walker settings.  It is up to the caller whether those ops 
 *         will actually be executed via process_operation().
 * @param streamwalker walker : Streamwalker to be iterated
 * @param opinfo** gcops : Reference to be populated with generated GC operations
 * @param opinfo** repackops : Reference to be populated with generated repack operations
 * @param opinfo** rebuildops : Reference to be populated with generated rebuild operations
 * @return int : 0, if the end of the datastream was reached and no new operations were generated;
 *               1, if new operations were generated by this iteration;
 *               -1, if a failure occurred
 */
int process_iteratestreamwalker( streamwalker walker, opinfo** gcops, opinfo** repackops, opinfo** rebuildops ) {
   // validate args
   if ( walker == NULL ) {
      LOG( LOG_ERR, "Received NULL streamwalker\n" );
      errno = EINVAL;
      return -1;
   }
   if ( walker->gcthresh != 0  &&  gcops == NULL ) {
      LOG( LOG_ERR, "Received NULL gcops reference when the walker is set to produce those operations\n" );
      errno = EINVAL;
      return -1;
   }
   if ( walker->repackthresh != 0  &&  repackops == NULL ) {
      LOG( LOG_ERR, "Received NULL repackops reference when the walker is set to produce those operations\n" );
      errno = EINVAL;
      return -1;
   }
   // set up some initial values
   marfs_ds* ds = &(walker->pos.ns->prepo->datascheme);
   size_t objsize = walker->pos.ns->prepo->datascheme.objsize;   // current repo-defined chunking limit
   size_t repackbytethresh = (objsize - walker->headerlen) / 2;
   char pullxattrs = ( walker->gcthresh == 0  &&  walker->repackthresh == 0 ) ? 0 : 1;
   char dispatchedops = 0;
   // iterate over all reference targets
   while ( walker->ftag.endofstream == 0  &&  (walker->ftag.state & FTAG_DATASTATE) >= FTAG_FIN ) {
      // calculate offset of next file tgt
      size_t tgtoffset = 1;
      if ( walker->gctag.refcnt ) {
         // handle any existing gctag value
         tgtoffset += walker->gctag.refcnt; // skip over count indicated by the tag
         if ( walker->gctag.inprog  &&  walker->gcthresh ) {
            LOG( LOG_INFO, "Cleaning up in-progress deletion of %zu reference files from previous instance\n", walker->gctag.refcnt );
            opinfo* optgt = NULL;
            walker->ftag.fileno++; // temporarily increase fileno, to match that of the subsequent ref deletion tgt
            if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_REF_OP, &(walker->ftag), &(optgt) ) ) {
               LOG( LOG_ERR, "Failed to identify operation target for GCTAG recovery\n" );
               walker->ftag.fileno--;
               return -1;
            }
            walker->ftag.fileno--;
            if ( optgt->count ) {
               // sanity check
               if ( optgt->ftag.fileno + optgt->count - 1 != walker->fileno ) {
                  LOG( LOG_ERR, "Failed sanity check: Active ref del op (%zu) does not reach walker fileno (%zu)\n",
                                optgt->ftag.fileno + optgt->count - 1, walker->fileno );
                  return -1;
               }
               // add the reference deletions to our existing operation
               optgt->count += walker->gctag.refcnt;
               delref_info* extinf = optgt->extendedinfo;
               if ( walker->gctag.eos ) { extinf->eos = 1; }
            }
            else {
               // populate newly created op
               optgt->count = walker->gctag.refcnt;
               optgt->ftag.fileno = walker->ftag.fileno; // be SURE that this is not a dummy op, targeting fileno zero
               delref_info* extinf = optgt->extendedinfo;
               extinf->prev_active_index = walker->activeindex;
               extinf->eos = walker->gctag.eos;
            }
            walker->report.delfiles += walker->gctag.refcnt;
            // clear the 'inprogress' state, just to make sure we never repeat this process
            walker->gctag.inprog = 0;
         }
         // this could potentially indicate a premature EOS
         if ( walker->gctag.eos ) {
            LOG( LOG_INFO, "GC tag indicates EOS at fileno %zu\n", walker->fileno );
            break;
         }
      }
      // generate next target info
      FTAG tmptag = walker->ftag;
      tmptag.fileno = walker->fileno;
      tmptag.fileno += tgtoffset;
      char* reftgt = datastream_genrpath( &(tmptag), walker->reftable );
      if ( reftgt == NULL ) {
         LOG( LOG_ERR, "Failed to generate reference path for corrected tgt ( %zu )\n", walker->fileno );
         return -1;
      }
      // pull info for the next reference target
      char filestate = -1;
      char prevdelzero = walker->gctag.delzero;
      char haveftag = pullxattrs;
      if ( process_getfileinfo( reftgt, pullxattrs, walker, &(filestate) ) ) {
         LOG( LOG_ERR, "Failed to get info for reference target: \"%s\"\n", reftgt );
         return -1;
      }
      pullxattrs = ( walker->gcthresh == 0  &&  walker->repackthresh == 0 ) ? 0 : 1; // return to default behavior
      if ( filestate == 0 ) {
         // file doesn't exist ( likely that we skipped a GCTAG on the previous file )
         // decrement to the previous index and make sure to check for xattrs
         if ( walker->fileno == 0 ) {
            // can't decrement beyond the beginning of the datastream
            LOG( LOG_ERR, "Initial reference target does not exist: \"%s\"\n", reftgt );
            return -1;
         }
         if ( walker->fileno == walker->ftag.fileno ) {
            // looks like we already pulled xattrs from the previous file, and must not have found a GCTAG
            LOG( LOG_ERR, "Datastream break detected at file number %zu: \"%s\"\n", walker->fileno, reftgt );
            return -1;
         }
         // generate the rpath of the previous file
         free( reftgt );
         tmptag.fileno = walker->fileno;
         reftgt = datastream_genrpath( &(tmptag), walker->reftable );
         if ( reftgt == NULL ) {
            LOG( LOG_ERR, "Failed to generate reference path for previous file ( %zu )\n", walker->fileno );
            return -1;
         }
         LOG( LOG_INFO, "Pulling xattrs from previous fileno ( %zu ) due to missing fileno %zu\n",
              walker->fileno, walker->fileno + 1 );
         // failure or missing file is unacceptable here
         if ( process_getfileinfo( reftgt, 1, walker, &(filestate) )  ||  filestate == 0 ) {
            LOG( LOG_ERR, "Failed to get info for previous ref tgt: \"%s\"\n", reftgt );
            free( reftgt );
            return -1;
         }
         free( reftgt );
         continue; // restart this iteration, now with all info available
      }
      free( reftgt );
      // many checks are only appropriate if we're pulling xattrs
      size_t endobj = walker->objno;
      char eos = 0;
      if ( haveftag ) {
         // check for innapropriate FTAG value
         if ( walker->ftag.fileno != walker->fileno + tgtoffset ) {
            LOG( LOG_ERR, "Invalid FTAG filenumber (%zu) on file %zu\n", walker->ftag.fileno, walker->fileno + tgtoffset );
            return -1;
         }
         size_t dataperobj = ( walker->ftag.objsize ) ?
           walker->ftag.objsize - (walker->headerlen + walker->ftag.recoverybytes) : 0; // data chunk size for this stream
         endobj = walker->ftag.objno; // update ending object index
         eos = walker->ftag.endofstream;
         if ( (walker->ftag.state & FTAG_DATASTATE) < FTAG_FIN ) { eos = 1; }
         if ( walker->gctag.refcnt  &&  walker->gctag.eos ) { eos = 1; }
         // calculate final object referenced by this file
         if ( dataperobj ) {
            // calculate the final object referenced by this file
            size_t finobjbounds = (walker->ftag.bytes + walker->ftag.offset - walker->headerlen) / dataperobj;
            if ( (walker->ftag.state & FTAG_DATASTATE) >= FTAG_FIN  &&
                  finobjbounds  &&
                  (walker->ftag.bytes + walker->ftag.offset - walker->headerlen) % dataperobj == 0 ) {
               finobjbounds--;
            }
            endobj += finobjbounds;
         }
         // check for object transition
         if ( walker->ftag.objno != walker->objno ) {
            // note the previous obj in counts
            walker->report.objcount++;
            // we may need to delete the previous object IF we are GCing AND no active refs existed for that obj
            //    AND it is not an already a deleted object0
            if ( walker->gcthresh  &&  walker->activefiles == 0  &&
                 ( walker->objno != 0  ||  prevdelzero == 0 ) ) {
               // need to prepend an object deletion operation for the previous objno
               LOG( LOG_INFO, "Adding deletion op for object %zu\n", walker->objno );
               opinfo* optgt = NULL;
               FTAG tmptag = walker->ftag;
               tmptag.objno = walker->objno;
               if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(tmptag), &(optgt) ) ) {
                  LOG( LOG_ERR, "Failed to identify operation target for deletion of object %zu\n", walker->objno );
                  return -1;
               }
               // sanity check
               if ( optgt->count + optgt->ftag.objno != walker->objno ) {
                  LOG( LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n",
                                optgt->count + optgt->ftag.objno, walker->objno );
                  return -1;
               }
               // update operation
               optgt->count++;
               // update our record
               walker->report.delobjs++;
               if ( walker->objno == 0 ) {
                  LOG( LOG_INFO, "Updating DEL-REF op to note deletion of object zero\n" );
                  // need to ensure we specifically note deletion of initial object
                  optgt = NULL;
                  if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_REF_OP, &(walker->ftag), &(optgt) ) ) {
                     LOG( LOG_ERR, "Failed to identify operation target for noting deletion of obj zero\n" );
                     return -1;
                  }
                  // NOTE -- don't increment count ( as we normally would ), as we aren't actually deleting another ref
                  delref_info* delrefinf = (delref_info*)optgt->extendedinfo;
                  delrefinf->delzero = 1;
               }
            }
            // need to handle repack ops
            if ( walker->rpckops ) {
               if ( walker->activebytes >= repackbytethresh ) {
                  // discard all ops
                  LOG( LOG_INFO, "Discarding repack ops due to excessive active bytes: %zu\n", walker->activebytes );
                  resourcelog_freeopinfo( walker->rpckops );
               }
               else {
                  // dispatch all ops
                  walker->report.rpckfiles += walker->rpckops->count;
                  walker->report.rpckbytes += ( (struct repack_info_struct*) walker->rpckops->extendedinfo )->totalbytes;
                  *repackops = walker->rpckops;
                  dispatchedops = 1; // note to exit after this file
               }
               walker->rpckops = NULL;
            }
            // possibly update rebuild ops
            if ( walker->rebuildthresh  &&  walker->activefiles  &&  walker->stval.st_ctime < walker->rebuildthresh ) {
               // check if object targets our rebuild location
               char* objname = NULL;
               ne_erasure erasure;
               ne_location location;
               if ( datastream_objtarget( &(walker->ftag), ds, &objname, &erasure, &location) ) {
                  LOG( LOG_ERR, "Failed to populate object target info for object %zu of stream \"%s\"\n", walker->ftag.objno, walker->ftag.streamid );
                  return -1;
               }
               // check for location match
               if ( (walker->rebuildloc.pod < 0  ||  walker->rebuildloc.pod == location.pod )  &&
                    (walker->rebuildloc.cap < 0  ||  walker->rebuildloc.cap == location.cap )  &&
                    (walker->rebuildloc.scatter < 0  ||  walker->rebuildloc.scatter == location.scatter ) ) {
                  // TODO generate a rebuild op for this object
               }
               free( objname );
            }
            // update state
            walker->activefiles = 0; // update active file count for new obj
            walker->activebytes = 0; // update active byte count for new obj
            walker->objno = walker->ftag.objno; // progress to the new obj
         }
      }
      char assumeactive = 0;
      if ( filestate == 1 ) {
         // file is inactive
         if ( walker->gcthresh  &&  walker->stval.st_ctime < walker->gcthresh  &&  haveftag ) {
            // this file is elligible for GC; create/update a reference deletion op
            opinfo* optgt = NULL;
            if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_REF_OP, &(walker->ftag), &(optgt) ) ) {
               LOG( LOG_ERR, "Failed to identify operation target for deletion of file %zu\n", walker->ftag.fileno );
               return -1;
            }
            delref_info* delrefinf = NULL;
            if ( optgt->count ) {
               // update existing op
               optgt->count++;
               optgt->count += walker->gctag.refcnt;
               delrefinf = optgt->extendedinfo;
               // sanity check
               if ( delrefinf->prev_active_index != walker->activeindex ) {
                  LOG( LOG_ERR, "Active delref op active index (%zu) does not match current val (%zu)\n",
                                delrefinf->prev_active_index, walker->activeindex );
                  return -1;
               }
               if ( delrefinf->eos == 0 ) { delrefinf->eos = eos; }
            }
            else {
               // populate new op
               optgt->ftag.fileno = walker->ftag.fileno; // be SURE that this is not a dummy op, targeting fileno zero
               optgt->count = 1;
               optgt->count += walker->gctag.refcnt;
               delrefinf = optgt->extendedinfo;
               delrefinf->prev_active_index = walker->activeindex;
               delrefinf->eos = eos;
            }
            walker->report.delfiles++;
            if ( endobj != walker->objno ) {
               // potentially generate a GC op for the first obj referenced by this file
               FTAG tmptag = walker->ftag;
               if ( walker->activefiles == 0  &&  (walker->ftag.objno != 0  ||  walker->gctag.delzero == 0) ) {
                  LOG( LOG_INFO, "Adding deletion op for inactive file initial object %zu\n", walker->ftag.objno );
                  optgt = NULL;
                  if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(tmptag), &(optgt) ) ) {
                     LOG( LOG_ERR, "Failed to identify operation target for deletion of initial spanned obj %zu\n", tmptag.objno );
                     return -1;
                  }
                  // sanity check
                  if ( optgt->count + optgt->ftag.objno != tmptag.objno ) {
                     LOG( LOG_ERR, "Existing obj deletion count (%zu = Count%zu+Tag%zu) does not match current obj (%zu)\n",
                                   optgt->count + optgt->ftag.objno, optgt->count, optgt->ftag.objno, tmptag.objno );
                     return -1;
                  }
                  // update operation
                  optgt->count++;
                  // potentially note deletion of object zero
                  if ( tmptag.objno == 0 ) { delrefinf->delzero = 1; }
                  // update our record
                  walker->report.delobjs++;
               }
               // potentially generate GC ops for objects spanned by this file
               tmptag.objno++;
               while ( tmptag.objno < endobj ) {
                  // generate ops for all but the last referenced object
                  LOG( LOG_INFO, "Adding deletion op for inactive spanned object %zu\n", tmptag.objno );
                  optgt = NULL;
                  if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(tmptag), &(optgt) ) ) {
                     LOG( LOG_ERR, "Failed to identify operation target for deletion of spanned obj %zu\n", tmptag.objno );
                     return -1;
                  }
                  // sanity check
                  if ( optgt->count + optgt->ftag.objno != tmptag.objno ) {
                     LOG( LOG_ERR, "Existing obj deletion count (%zu = Count%zu+Tag%zu) does not match current obj (%zu)\n",
                                   optgt->count + optgt->ftag.objno, optgt->count, optgt->ftag.objno, tmptag.objno );
                     return -1;
                  }
                  // update operation
                  optgt->count++;
                  // update our record
                  walker->report.delobjs++;
                  // iterate to the next obj
                  tmptag.objno++;
               }
            }
            // potentially generate a GC op for the final object of the stream
            if ( eos  &&  (endobj != walker->objno  ||  walker->activefiles == 0) ) {
               FTAG tmptag = walker->ftag;
               tmptag.objno = endobj;
               optgt = NULL;
               if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(tmptag), &(optgt) ) ) {
                  LOG( LOG_ERR, "Failed to identify operation target for deletion of final stream obj %zu\n", tmptag.objno );
                  return -1;
               }
               // sanity check
               if ( optgt->count + optgt->ftag.objno != tmptag.objno ) {
                  LOG( LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n",
                                optgt->count + optgt->ftag.objno, walker->objno );
                  return -1;
               }
               // update operation
               optgt->count++;
               // update our record
               walker->report.delobjs++;
            }
         }
         else if ( walker->stval.st_ctime >= walker->gcthresh ) {
            // this file was too recently deactivated to gc
            walker->report.volfiles++;
            assumeactive = 1;
         }
      }
      // note newly encountered file
      walker->report.filecount++;
      if ( haveftag ) { walker->report.bytecount += walker->ftag.bytes; }
      else { walker->report.bytecount += walker->stval.st_size; }
      if ( filestate > 1 ) {
         // file is active
         walker->report.fileusage++;
         if ( haveftag ) { walker->report.byteusage += walker->ftag.bytes; }
         else { walker->report.byteusage += walker->stval.st_size; }
         // TODO manage repack ops
         // TODO potentially generate rebuild ops
      }
      // potentially update values based on spanned objects
      if ( walker->objno != endobj ) {
         walker->activefiles = 0; // update active file count for new obj
         walker->activebytes = 0; // update active byte count for new obj
         walker->report.objcount += endobj - walker->objno;
      }
      if ( filestate > 1  ||  assumeactive ) {
         // handle GC state
         walker->activeindex = walker->fileno + tgtoffset;
         walker->activefiles++;
         // handle repack state
         if ( haveftag ) { walker->activebytes += walker->ftag.bytes; }
         else { walker->activebytes += walker->stval.st_size; }
         // dispatch any GC ops
         if ( walker->gcops ) {
            *gcops = walker->gcops;
            dispatchedops = 1; // note to exit after this file
            walker->gcops = NULL;
         }
      }
      // update walker state to reflect new target
      walker->fileno += tgtoffset;
      walker->objno = endobj;
      // check for termination due to operation dispatch
      if ( dispatchedops ) {
         LOG( LOG_INFO, "Exiting due to operation dispatch\n" );
         return 1;
      }
   }
   // dispatch any remianing ops
   if ( walker->gcops ) {
      *gcops = walker->gcops;
      dispatchedops = 1;
      walker->gcops = NULL;
   }
   if ( walker->rpckops ) {
      if ( walker->activebytes >= repackbytethresh ) {
         // discard all ops
         LOG( LOG_INFO, "Discarding repack ops due to excessive active bytes: %zu\n", walker->activebytes );
         resourcelog_freeopinfo( walker->rpckops );
      }
      else {
         // record repack counts
         opinfo* repackparse = walker->rpckops;
         while ( repackparse ) {
            walker->report.rpckfiles += repackparse->count;
            walker->report.rpckbytes += ( (struct repack_info_struct*) repackparse->extendedinfo )->totalbytes;
            repackparse = repackparse->next;
         }
         // dispatch all ops
         *repackops = walker->rpckops;
         dispatchedops = 1;
      }
      walker->rpckops = NULL;
   }
   if ( walker->rbldops ) {
      *rebuildops = walker->rbldops;
      dispatchedops = 1;
      walker->rbldops = NULL;
   }
   if ( dispatchedops ) {
      LOG( LOG_INFO, "Returning note of operation dispatch\n" );
      return 1;
   }
   LOG( LOG_INFO, "Stream traversal complete: \"%s\"\n", walker->ftag.streamid );
   return 0;
}

/**
 * Close the given streamwalker
 * @param streamwalker walker : Streamwalker to be closed
 * @param streamwalker_report* report : Reference to a report to be populated with final counts
 * @return int : Zero on success, 1 if the walker was closed prior to iteration completion, or -1 on failure
 */
int process_closestreamwalker( streamwalker walker, streamwalker_report* report ) {
   // check args
   if ( walker == NULL ) {
      LOG( LOG_ERR, "Received a NULL streamwalker reference\n" );
      errno = EINVAL;
      return -1;
   }
   // populate final report
   if ( report )  {
      *report = walker->report;
   }
   // check for incomplete walker
   int retval = 0;
   if ( walker->gcops  ||  walker->rpckops  ||  walker->rbldops  ||
        (
            walker->ftag.endofstream == 0  &&
            (walker->gctag.refcnt == 0  ||  walker->gctag.eos == 0)  &&
            (walker->ftag.state & FTAG_DATASTATE) >= FTAG_FIN
        ) ) {
      LOG( LOG_WARNING, "Streamwalker closed prior to iteration completion\n" );
      retval = 1;
   }
   destroystreamwalker( walker );
   return retval;
}

/**
 * Perform the given operation
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the current NS
 * @param opinfo* op : Reference to the operation to be performed
 *                     NOTE -- this will be updated to reflect operation completion / error
 * @param RESOURCELOG* log : Resource log to be updated with op completion / error
 * @param REPACKSTREAMER rpckstr : Repack streamer to be used for repack operations
 * @return int : Zero on success, or -1 on failure
 *               NOTE -- This func will not return 'failure' unless a critical internal error occurs.
 *                       'Standard' operation errors will simply be reflected in the op struct itself.
 */
int process_executeoperation( marfs_position* pos, opinfo* op, RESOURCELOG* rlog, REPACKSTREAMER rpkstr ) {
   // check arguments
   if ( op == NULL ) {
      LOG( LOG_ERR, "Received a NULL operation value\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_position value\n" );
      resourcelog_freeopinfo( op );
      errno = EINVAL;
      return -1;
   }
   if ( pos->ctxt == NULL ) {
      LOG( LOG_ERR, "Received a marfs_position value with no MDAL_CTXT\n" );
      resourcelog_freeopinfo( op );
      errno = EINVAL;
      return -1;
   }
   if ( !(op->start) ) {
      LOG( LOG_ERR, "Received a non-start operation value\n" );
      resourcelog_freeopinfo( op );
      errno = EINVAL;
      return -1;
   }
   char abortops = 0;
   while ( op ) {
      // break off the first op of the chain
      opinfo* nextop = op->next;
      op->next = NULL;
      if ( abortops ) {
         LOG( LOG_ERR, "Skipping %s operation due to previous op errors\n",
                       (op->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
                       (op->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
                       (op->type == MARFS_REBUILD_OP)    ? "REBUILD" :
                       (op->type == MARFS_REPACK_OP)     ? "REPACK"  :
                       "UNKNOWN" );
         op->errval = EAGAIN;
      }
      else {
         // execute the operation
         switch ( op->type ) {
            case MARFS_DELETE_OBJ_OP:
               LOG( LOG_INFO, "Performing object deletion op on stream \"%s\"\n", op->ftag.streamid );
               process_deleteobj( pos, op );
               break;
            case MARFS_DELETE_REF_OP:
               LOG( LOG_INFO, "Performing reference deletion op on stream \"%s\"\n", op->ftag.streamid );
               process_deleteref( pos, op );
               break;
            case MARFS_REBUILD_OP:
               LOG( LOG_INFO, "Performing rebuild op on stream \"%s\"\n", op->ftag.streamid );
               process_rebuild( pos, op );
               break;
            case MARFS_REPACK_OP:
               LOG( LOG_INFO, "Performing repack op on stream \"%s\"\n", op->ftag.streamid );
               process_repack( pos, op, rpkstr );
               break;
            default:
               LOG( LOG_ERR, "Unrecognized operation type value\n" );
               resourcelog_freeopinfo( op );
               if ( nextop ) { resourcelog_freeopinfo( nextop ); }
               return -1;
         }
      }
      // log operation end, and check if we can progress
      char progress = 0;
      op->start = 0;
      LOG( LOG_INFO, "Logging end of operation stream \"%s\"\n", op->ftag.streamid );
      if ( resourcelog_processop( rlog, op, &(progress) ) ) {
         LOG( LOG_ERR, "Failed to log end of operation on stream \"%s\"\n", op->ftag.streamid );
         resourcelog_freeopinfo( op );
         if ( nextop ) { resourcelog_freeopinfo( nextop ); }
         return -1;
      }
      resourcelog_freeopinfo( op );
      if ( progress == 0 ) {
         LOG( LOG_INFO, "Terminating execution, as portions of this op have not yet been completed\n" );
         if ( nextop ) { resourcelog_freeopinfo( nextop ); }
         return 0;
      }
      if ( progress < 0 ) {
         LOG( LOG_ERR, "Previous operation failures will prevent execution of remaining op chain\n" );
         abortops = 1;
      }
      op = nextop;
   }
   LOG( LOG_INFO, "Terminating execution, as all operations were completed\n" );
   return 0;
}


