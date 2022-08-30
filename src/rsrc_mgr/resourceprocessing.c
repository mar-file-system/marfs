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


typedef struct threshold_struct {
   time_t gcthreshold;      // files newer than this will not be GCd
                            //    Recommendation -- this should be fairly old ( # of days ago )
   time_t rrthreshold;      // files newer than this will not be repacked or rebuilt
                            //    Recommendation -- this should be quite recent ( # of minutes ago )
   time_t cleanupthreshold; // files newer than this will not be cleaned up ( i.e. repack marker files )
                            //    Recommendation -- this should be semi-recent ( # of hours ago )
   // NOTE -- setting any of these values too close to the current time risks undefined resource 
   //         processing behavior ( i.e. trying to cleanup ongoing repacks )
} thresholds;


typedef struct gctag_struct {
   size_t skipcnt;
   char eos;
   char inprog;
} GCTAG;

#define GCTAG_NAME "GC-MODIFIED"

typedef struct streamwalker_struct {
   // initialization info
   marfs_ns*  ns;
   MDAL_CTXT  ctxt;
   time_t     gcthresh;      // time threshold for GCing a file ( none performed, if zero )
   time_t     repackthresh;  // time threshold for repacking a file ( none performed if zero )
   // quota info
   size_t     inodecnt;
   size_t     bytecnt;
   // iteration info
   size_t     fileindex;
   size_t     objindex;
   size_t     objinitial;
   char*      reftgt;
   FTAG       ftag;
   char*      ftagstr;
   size_t     ftagstralloc;
   // location estimation info
   char       verifyall;
   size_t     headerlen;
   size_t     minoffset;
   size_t     maxoffset;
   // GC info
   opinfo*    gcops;
   opinfo*    gcopstail;
   size_t     activefiles;
   size_t     activeindex;
   // repack info
   opinfo*    rpckops;
   opinfo*    rpckopstail;
   size_t     activebytes;
}* streamwalker;


//   -------------   INTERNAL DEFINITIONS    -------------

#define ASSUMED_RECOV_PATHLEN 4096

//   -------------   INTERNAL FUNCTIONS    -------------

int process_getfileinfo( streamwalker walker, char getxattrs, char* filestate, struct stat* stval, GCTAG* gctag ) {
   MDAL mdal = walker->ns->prepo->metascheme.mdal;
   // potentially populate the reference path
   if ( walker->reftgt == NULL ) {
      FTAG tmptag = walker->ftag;
      tmptag.fileno = walker->fileindex;
      walker->reftgt = datastream_genrpath( &(tmptag), &(walker->ns->prepo->metascheme) );
      if ( walker->reftgt == NULL ) {
         LOG( LOG_ERR, "Failed to generate reference path of fileno %zu\n", walker->fileindex );
         return -1;
      }
   }
   if ( getxattrs ) {
      // open the target file
      int olderrno = errno;
      errno = 0;
      MDAL_FHANDLE handle = mdal->openref( ctxt, walker->reftgt, O_RDONLY, 0 );
      if ( handle == NULL ) {
         if ( errno = ENOENT ) {
            LOG( LOG_INFO, "Reference file does not exist: \"%s\"\n", walker->reftgt );
            *filestate = 0;
            return 0;
         }
         LOG( LOG_ERR, "Failed to open current reference file target: \"%s\"\n", walker->reftgt );
         return -1;
      }
      // retrieve FTAG of the current file
      ssize_t getres = mdal->fgetxattr( handle, 1, FTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1 );
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
            LOG( LOG_ERR, "Inconsistent length for ftag of reference file target: \"%s\"\n", walker->reftgt );
            mdal->close( handle );
            return -1;
         }
      }
      // check for error
      if ( getres <= 0 ) {
         LOG( LOG_ERR, "Failed to retrieve ftag of reference file target: \"%s\"\n", walker->reftgt );
         mdal->close( handle );
         return -1;
      }
      *(ftagstr + getres) = '\0'; // ensure our string is NULL terminated
      // parse the ftag
      FTAG ftag;
      if ( ftag_initstr( &(walker->ftag), walker->ftagstr ) ) {
         LOG( LOG_ERR, "Failed to parse ftag value of reference file target: \"%s\"\n", walker->reftgt );
         mdal->close( handle );
         return -1;
      }
      // attempt to retrieve the GC tag
      errno = 0;
      getres = mdal->fgetxattr( handle, 1, GCTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1 );
      // check for overflow
      if ( getres >= walker->ftagstralloc ) {
         // double our allocated string length
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
            LOG( LOG_ERR, "Inconsistent length for gctag of reference file target: \"%s\"\n", walker->reftgt );
            mdal->close( handle );
            return -1;
         }
      }
      // check for error
      if ( getres <= 0  &&  errno != ENODATA ) {
         LOG( LOG_ERR, "Failed to retrieve gctag of reference file target: \"%s\"\n", walker->reftgt );
         mdal->close( handle );
         return -1;
      }
      else if ( getres > 0 ) {
         // we must parse the GC tag value
         *(ftagstr + getres) = '\0'; // ensure our string is NULL terminated
         char* parse = NULL;
         unsigned long long parseval = strtoull( walker->ftagstr, &(parse), 10 );
         if ( parse == NULL  ||  *parse != ' '  ||  parseval == ULONG_MAX ) {
            LOG( LOG_ERR, "Failed to parse skip value of GCTAG for reference file target: \"%s\"\n", walker->reftgt );
            mdal->close( handle );
            return -1;
         }
         gctag->skipcnt = (size_t) parseval;
         parse++;
         if ( *parse == 'E' ) {
            gctag->eos = 1;
         }
         else if ( *parse == 'C' ) {
            gctag->eos = 0;
         }
         else {
            LOG( LOG_ERR, "GCTAG with innappriate EOS value for reference file target: \"%s\"\n", walker->reftgt );
            mdal->close( handle );
            return -1;
         }
         parse++;
         if ( *parse != '\0' ) {
            if ( *parse != ' ' ) {
               LOG( LOG_ERR, "Failed to parse 'in prog' in GCTAG of reference file target: \"%s\"\n", walker->reftgt );
               mdal->close( handle );
               return -1;
            }
            parse++;
            if ( *parse != 'P' ) {
               LOG( LOG_ERR, "Failed to parse 'in prog' in GCTAG of reference file target: \"%s\"\n", walker->reftgt );
               mdal->close( handle );
               return -1;
            }
            gctag->inprog = 1;
         }
         else {
            gctag->inprog = 0;
         }
      }
      // stat the file
      if ( mdal->fstat( handle, stval ) ) {
         LOG( LOG_ERR, "Failed to stat reference file target via handle: \"%s\"\n", walker->reftgt );
         mdal->close( handle );
         return -1;
      }
      // finally, close the file
      if ( mdal->close( handle ) ) {
         // just complain
         LOG( LOG_WARNING, "Failed to close handle for reference target: \"%s\"\n", walker->reftgt );
      }
      // restore old values
      errno = olderrno;
      // populate state value based on link count
      *filestate = ( stval->st_nlink > 1 ) ? 2 : 1;
      return 0;
   }
   int olderrno = errno;
   errno = 0;
   // stat the file by path
   if ( mdal->statref( walker->ctxt, walker->reftgt, stval ) ) {
      if ( errno = ENOENT ) {
         LOG( LOG_INFO, "Reference file does not exist: \"%s\"\n", walker->reftgt );
         *filestate = 0;
         return 0;
      }
      LOG( LOG_ERR, "Failed to stat reference file target via handle: \"%s\"\n", walker->reftgt );
      return -1;
   }
   // restore old values
   errno = olderrno;
   // populate state value based on link count
   *filestate = ( stval->st_nlink > 1 ) ? 2 : 1;
   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Process the next entry from the given refdir scanner
 * @param marfs_ns* ns : Reference to the current NS
 * @param MDAL_SCANNER refdir : Scanner reference to iterate through
 * @param char** reftgt : Reference to be populated with the next reference path tgt
 *                        Left NULL if the ref dir has been completely traversed
 * @return int : Value of zero -> the reference dir has been completed and closed,
 *               Value of one -> entry is populated with the start of a datastream,
 *               Value of two -> entry is populated with a rebuild marker file,
 *               Value of three -> entry is populated with a repack marker file,
 *               Value of ten -> entry is of an unknown type
 *               Value of negative one -> an error occurred
 */
int process_refdir( marfs_ns* ns, MDAL_SCANNER refdir, const char* refdirpath, char** reftgt ) {
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
   // scan through the dir until we find something of interest
   MDAL mdal = ns->prepo->metascheme.mdal;
   struct dirent* dent;
   int olderrno = errno;
   errno = 0;
   while ( (dent = mdal->scan( refdir )) != NULL ) {
      // skip any 'hidden' files or default ( '.'/'..' ) entries
      if ( *(dent->d_name) == '.' ) {
         continue;
      }
      // identify the entry type
      char type;
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
   int strlen = snprintf( NULL, 0, "%s/%s", refdirpath, dent->d_name );
   if ( strlen < 1 ) {
      LOG( LOG_ERR, "Failed to identify length of ref path for \"%s\"\n", dent->d_name );
      return -1;
   }
   *reftgt = malloc( sizeof(char) * (strlen + 1) );
   if ( reftgt == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for ref path of \"%s\"\n", dent->d_name );
      return -1;
   }
   if ( snprintf( *reftgt, strlen + 1, "%s/%s", refdirpath, dent->d_name ) != strlen ) {
      LOG( LOG_ERR, "Inconsistent length for ref path of \"%s\"\n", dent->d_name );
      free( *reftgt );
      *reftgt = NULL;
      errno = EDOM;
      return -1;
   }
   errno = olderrno; // restore old errno
   return ( (int)type + 1 );
}

streamwalker process_openstreamwalker( marfs_ns* ns, MDAL_CTXT ctxt, const char* reftgt, time_t gcthresh, time_t rpckthresh ) {
	// validate args
	if ( ns == NULL ) {
      LOG( LOG_ERR, "Received a NULL NS ref\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL ctxt\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( reftgt == NULL ) {
      LOG( LOG_ERR, "Received a NULL reference tgt path\n" );
      errno = EINVAL;
      return NULL;
   }

}

/**
 * Iterate over a datastream, from the given reference position, accumulating quota values and identifying 
 *    operation targets
 * NOTE -- This func will return ALL possible operations.  It is up to the caller whether those ops 
 *         will actually be executed via process_operation().
 * @param marfs_ns* ns : Reference to the current NS
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the current NS
 * @param char** reftgt : Reference to a string containing the target reference path
 *                        NOTE -- Upon successful return of this func, this reference will be updated to 
 *                                contain the subsequent target ref path of the stream.  Thus, repeated 
 *                                calls with the same &(char*) will progress through the stream.
 *                                When the reference is populated with NULL, stream processing has concluded.
 * @param thresholds thresh : Threshold time values ( see struct def, above )
 * @param size_t* inodecnt : Reference to the inode count value, to be incremented as active files are found
 * @param size_t* bytescnt : Reference to the byte count value, to be incremented as active files are found
 * @return opinfo* : Reference to a new operation set to be performed, or NULL if a new operation was not found
 *                   NOTE -- A NULL return combined with a non-NULL reftgt indicates an error
 */
int process_iteratestreamwalker( streamwalker walker, opinfo* gcops, opinfo* repackops ) {
   // validate args
   if ( walker == NULL ) {
      LOG( LOG_ERR, "Received NULL streamwalker\n" );
      errno = EINVAL;
      return -1;
   }
   // set up some initial values
   marfs_ms* ms = &(walker->ns->prepo->metascheme);
   MDAL mdal = ms->mdal;
   size_t objfiles = walker->ns->prepo->datascheme.objfiles;
   size_t objsize = walker->ns->prepo->datascheme.objsize;
   size_t dataperobj = walker->ftag.objsize - walker->recovheaderlen;
   size_t lastverified = walker->ftag.fileno;
   char pullxattrs = 0;
   if ( walker->verifyall  ||  walker->ftag.streamid == NULL ) { pullxattrs = 1; }
   // iterate over all reference targets
   while ( walker->reftgt != NULL ) {
      // pull xattr values if we're close to the end of the current object
      if ( walker->maxoffset + walker->finfoestimate >= dataperobj ) { pullxattrs = 1; }
      // pull info for the next reference target
      char haveftag = pullxattrs;
      char filestate = -1;
      struct stat stval;
      GCTAG gctag;
      if ( process_getfileinfo( walker, pullxattrs, &(filestate), &(stval), &(gctag) ) ) {
         LOG( LOG_ERR, "Failed to get info for reference target: \"%s\"\n", walker->reftgt );
         return -1;
      }
      pullxattrs = walker->verifyall; // assume we won't be pulling xattrs again, unless we're verifying everything
      if ( filestate == 0 ) {
         // file doesn't exist ( likely that we skipped a GCTAG on the previous file )
         // decrement to the previous index and make sure to check for xattrs
         if ( walker->fileno == 0 ) {
            // can't decrement beyond the beginning of the datastream
            LOG( LOG_ERR, "Initial reference target does not exist: \"%s\"\n", walker->reftgt );
            return -1;
         }
         if ( (walker->fileno - 1) == walker->finfo.fileno ) {
            // looks like we already pulled xattrs from the previous file, and must not have found a GCTAG
            LOG( LOG_ERR, "Datastream break detected at file number %zu: \"%s\"\n", walker->fileno, walker->reftgt );
            return -1;
         }
         // replace the active rpath with that of the previous file
         char* origtgt = walker->reftgt;
         FTAG tmptag = walker->ftag;
         tmptag.fileno = walker->fileno - 1;
         walker->reftgt = datastream_genrpath( &(tmptag), ms );
         if ( walker->reftgt == NULL ) {
            LOG( LOG_ERR, "Failed to generate reference path for previous file ( %zu )\n", walker->fileno - 1 );
            return -1;
         }
         free( origtgt );
         LOG( LOG_INFO, "Pulling xattrs from previous fileno ( %zu ) due to missing fileno %zu\n",
              walker->fileno - 1, walker->fileno );
         walker->fileno--;
         pullxattrs = 1;
         continue;
      }
      // check for possible object transition
      if ( !(haveftag)  &&  (stval.st_size + walker->maxoffset + walker->finfoestimate >= dataperobj) ) {
         // object transition is too likely, we need to pull the FTAG of this file
         LOG( LOG_INFO, "Pulling xattrs from fileno %zu due to likely object transition\n", walker->fileno );
         pullxattrs = 1;
         continue;
      }
      // check for verified object transition
      opinfo* dispatchops = NULL;
      if ( haveftag  &&  walker->ftag.objno != walker->objno ) {
         // this is only acceptible if we have either verified the previous file OR
         //    BOTH the previous file referenced the previous object AND this file 
         //    is at the head of the next object
         if ( walker->fileno != lastverified  &&
              ( walker->objno + 1 != walker->ftag.objno  ||  walker->ftag.offset != walker->recovheaderlen ) ) {
            // this datastream has an unusual structure
            // safest to return to the most recently verified file and to verify every subsequent file
            walker->verifyall = 1;
            if ( walker->activeindex > ( walker->fileno - walker->objinitial ) ) {
               walker->activeindex -= ( walker->fileno - walker->objinitial ); // decrement offset to match new position
            }
            else if ( walker->gcops ) {
               walker->activeindex = 
            }
            walker->fileno = walker->objinitial;  // update position
            walker->inodecnt -= walker->activefiles;
            walker->bytecnt -= walker->activebytes;
            walker->activefiles = 0;
            walker->activebytes = 0;
            walker->minoffset = 0;
            walker->maxoffset = 0;
            // clear all unissued operations
            statelog_freeopinfo( walker->gcops );
            statelog_freeopinfo( walker->rpckops );
            walker->gcops = NULL;
            walker->gcopstail = NULL;
            walker->rpckops = NULL;
            walker->rpckopstail = NULL;
            // generate the new reference target path
            char* origtgt = walker->reftgt;
            FTAG tmptag = walker.ftag;
            tmptag.fileno = walker->fileno;
            walker->reftgt = datastream_genrpath( &(tmptag), ms );
            if ( walker->reftgt == NULL ) {
               LOG( LOG_ERR, "Failed to generate ref path tgt for fileno %zu for emergency rescan\n", walker->fileno );
               return -1;
            }
            free( origtgt );
            continue;
         }
         // we need to dispatch any previous operations
         if ( walker->gcops ) {
            dispatchops = walker->gcops;
            if ( walker->activefiles == 0 ) {
               // need to append an object deletion operation
               walker->gcopstail->next = malloc( sizeof( struct opinfo_struct ) );
               if ( walker->gcopstail->next == NULL ) {
                  LOG( LOG_ERR, "Failed to allocate object deletion op for object %zu\n" );
                  return -1;
               }
               opinfo* objdelop = walker->gcopstail->next;
               objdelop->type = MARFS_DELETE_OBJ_OP;
               objdelop->extendedinfo = NULL;
               objdelop->start = 1;
               objdelop->count = 0;
               objdelop->errval = 0;
               objdelop->ftag = walker->gcops->ftag;
               objdelop->ftag.objno = walker->objno;
               objdelop->next = NULL;
            }
            walker->gcops = NULL; // remove the gc ops from the walker handle
         }
      }
      else if ( filestate == 1 ) {
         // file is inactive
      }
      else {
         // file is active
      }
      // generate some traversal values
   }
}

/**
 * Perform the given operation
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the current NS
 * @param opinfo* op : Reference to the operation to be performed
 *                     NOTE -- this will be updated to reflect operation completion / error
 * @return int : Zero on success, or -1 on failure
 *               NOTE -- This func will not return 'failure' unless a critical internal error occurs.
 *                       'Standard' operation errors will simply be reflected in the op struct itself.
 */
int process_operation( MDAL_CTXT ctxt, opinfo* op ) {
}


