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
   time_t repackthreshold;  // files newer than this will not be repacked
                            //    Recommendation -- this should be quite recent ( # of minutes ago )
   time_t rebuildthreshold; // files newer than this will not be rebuilt ( have data errors repaired )
                            //    Recommendation -- this should be quite recent ( # of minutes ago )
   time_t cleanupthreshold; // files newer than this will not be cleaned up ( i.e. repack marker files )
                            //    Recommendation -- this should be semi-recent ( # of hours ago )
   // NOTE -- setting any of these values too close to the current time risks undefined resource 
   //         processing behavior ( i.e. trying to cleanup ongoing repacks )
   // NOTE -- setting any of these values to zero will cause the corresponding operations to be skipped
} thresholds;

typedef struct gctag_struct {
   size_t refcnt;
   char eos;
   char inprog;
   char delzero;
} GCTAG;

#define GCTAG_NAME "GC-MODIFIED"

typedef struct streamwalker_report_struct {
   // quota info
   size_t fileusage;  // count of active files
   size_t byteusage;  // count of active bytes
   // stream info
   size_t filecount;  // count of files in the datastream
   size_t objcount;   // count of objects in the datastream
   // GC info
   size_t delobjs;    // count of deleted objects
   size_t delfiles;   // count of deleted files
   size_t volfiles;   // count of 'volatile' files ( those deleted to recently for gc )
   // repack info
   size_t rpkfiles;   // count of files repacked
   size_t rpkbytes;   // count of bytes repacked
   size_t freedobjs;  // count of objects now elligible for deletion
   // rebuild info
   size_t rbldobjs;
   size_t rbldbytes;
} streamwalker_report;

typedef struct streamwalker_struct {
   // initialization info
   marfs_ns*   ns;
   MDAL_CTXT   ctxt;
   time_t      gcthresh;      // time threshold for GCing a file ( none performed if zero )
   time_t      repackthresh;  // time threshold for repacking a file ( none performed if zero )
   time_t      rebuildthresh; // time threshold for rebuilding a file ( none performed, if zero )
   ne_location rebuildloc;
   // report info
   streamwalker_report report;
   // iteration info
   size_t      fileindex;
   size_t      objindex;
   struct stat stval;
   FTAG        ftag;
   GCTAG       gctag;
   // cached info
   size_t      headerlen;
   char*       ftagstr;
   size_t      ftagstralloc;
   // GC info
   opinfo*     gcops;
   size_t      activefiles;
   size_t      activeindex;
   // repack info
   opinfo*     rpckops;
   size_t      activebytes;
   // rebuild info
   opinfo*     rbldops;
}* streamwalker;


//   -------------   INTERNAL DEFINITIONS    -------------


//   -------------   INTERNAL FUNCTIONS    -------------

int process_getfileinfo( const char* reftgt, char getxattrs, streamwalker walker, char* filestate ) {
   MDAL mdal = walker->ns->prepo->metascheme.mdal;
   if ( getxattrs ) {
      // open the target file
      int olderrno = errno;
      errno = 0;
      MDAL_FHANDLE handle = mdal->openref( ctxt, reftgt, O_RDONLY, 0 );
      if ( handle == NULL ) {
         if ( errno = ENOENT ) {
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
      getres = mdal->fgetxattr( handle, 1, GCTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1 );
      // check for overflow
      if ( getres >= walker->ftagstralloc ) {
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
         *(ftagstr + getres) = '\0'; // ensure our string is NULL terminated
         char* parse = NULL;
         unsigned long long parseval = strtoull( walker->ftagstr, &(parse), 10 );
         if ( parse == NULL  ||  *parse != ' '  ||  parseval == ULONG_MAX ) {
            LOG( LOG_ERR, "Failed to parse skip value of GCTAG for reference file target: \"%s\"\n", reftgt );
            mdal->close( handle );
            return -1;
         }
         walker->gctag->refcnt = (size_t) parseval;
         parse++;
         // get EOS flag
         if ( *parse == 'E' ) {
            walker->gctag->eos = 1;
         }
         else if ( *parse == 'C' ) {
            walker->gctag->eos = 0;
         }
         else {
            LOG( LOG_ERR, "GCTAG with inappriate EOS value for reference file target: \"%s\"\n", reftgt );
            mdal->close( handle );
            return -1;
         }
         parse++;
         // check for inprog
         if ( *parse != '\0' ) {
            if ( *parse != ' ' ) {
               LOG( LOG_ERR, "Failed to parse 'in prog' in GCTAG of reference file target: \"%s\"\n", reftgt );
               mdal->close( handle );
               return -1;
            }
            parse++;
            if ( *parse != 'P'  &&  *parse != 'D' ) {
               LOG( LOG_ERR, "Failed to parse 'in prog' in GCTAG of reference file target: \"%s\"\n", reftgt );
               mdal->close( handle );
               return -1;
            }
            if ( *parse == 'P' ) 
               walker->gctag->inprog = 1;
            else
               walker->gctag->inprog = 0;
            parse++;
            // check for delzero
            if ( *parse != '\0' ) {
               if ( *parse != ' ' ) {
                  LOG( LOG_ERR, "Failed to parse 'del zero' in GCTAG of reference file target: \"%s\"\n", reftgt );
                  mdal->close( handle );
                  return -1;
               }
               parse++;
               if ( *parse != 'D' ) {
                  LOG( LOG_ERR, "Failed to parse 'del zero' in GCTAG of reference file target: \"%s\"\n", reftgt );
                  mdal->close( handle );
                  return -1;
               }
               walker->gctag->delzero = 1;
            }
            else {
               walker->gctag->delzero = 0;
            }
         }
         else {
            walker->gctag->inprog = 0;
            walker->gctag->delzero = 0;
         }
      }
      else {
         // no GCTAG, so zero out values
         walker->gctag.refcnt = 0;
         walker->gctag.eos = 0;
         walker->gctag.inprogress = 0;
         walker->gctag.delzero = 0;
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
      *(ftagstr + getres) = '\0'; // ensure our string is NULL terminated
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
      if ( mdal->fstat( handle, walker->stval ) ) {
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
      // populate state value based on link count
      *filestate = ( walker->stval->st_nlink > 1 ) ? 2 : 1;
      return 0;
   }
   int olderrno = errno;
   errno = 0;
   // stat the file by path
   if ( mdal->statref( walker->ctxt, reftgt, walker->stval ) ) {
      if ( errno = ENOENT ) {
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
   *filestate = ( walker->stval->st_nlink > 1 ) ? 2 : 1;
   return 0;
}

int process_identifyoperation( opinfo** opchain, operation_type type, FTAG* ftag, opinfo** optgt ) {
   opinfo* prevop = NULL;
   if ( opchain  &&  *opchain ) {
      // check for any existing ops of this type in the chain
      opinfo* parseop = *opchain;
      while ( parseop ) {
         // for most ops, matching on type is sufficent to reuse the same op tgt
         // For repacks & rebuilds, we need to check that the new tgt is in the same 'chain' as the existing one
         if ( parseop->type == type  &&
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
   if ( prevop  &&  strcmp( prevop->ftag.ctag, ftag->ctag ) == 0 ) {
      newop->ftag.ctag = prevop->ftag.ctag;
   }
   else {
      newop->ftag.ctag = strdup( ftag->ctag );
      if ( newop->ftag.ctag == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate FTAG ctag string: \"%s\"\n", ftag->ctag );
         free( newop );
         return -1;
      }
   }
   if ( prevop  &&  strcmp( prevop->ftag.streamid, ftag->streamid ) == 0 ) {
      newop->ftag.streamid = prevop->ftag.streamid;
   }
   else {
      newop->ftag.streamid = strdup( ftag->streamid );
      if ( newop->ftag.streamid == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate FTAG streamid string: \"%s\"\n", ftag->streamid );
         free( newop->ftag.ctag );
         free( newop );
         return -1;
      }
   }
   size_t ftagstrlen = ftag_tostr( &(newop->ftag), NULL, 0 );
   if ( ftagstrlen < 1 ) {
      LOG( LOG_ERR, "Failed to identify length of FTAG string for operation info\n" );
      free( newop->ftag.streamid );
      free( newop->ftag.ctag );
      free( newop );
      return -1;
   }
   newop->ftagstr = malloc( sizeof(char) * (ftagstrlen + 1) );
   if ( newop->ftagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate FTAG string for operation info\n" );
      free( newop->ftag.streamid );
      free( newop->ftag.ctag );
      free( newop );
      return -1;
   }
   if ( ftag_tostr( &(newop->ftag), newop->ftagstr, ftagstrlen + 1 ) != ftagstrlen ) {
      LOG( LOG_ERR, "Inconsistent length of FTAG string for operation info\n" );
      free( newop->ftagstr );
      free( newop->ftag.streamid );
      free( newop->ftag.ctag );
      free( newop );
      return -1;
   }
   // potentially allocate extended info
   char needext = 0;
   switch( type ) {
      case MARFS_DELETE_OBJ_OP:
         break; // nothing to be done
      case MARFS_DELETE_REF_OP:
         newop->extendedinfo = malloc( sizeof( struct delref_info_struct ) );
         needext = 1;
         break;
      case MARFS_REBUILD_OP:
         newop->extendedinfo = malloc( sizeof( struct rebuild_info_struct ) );
         needext = 1;
         break;
      case MARFS_REPACK_OP:
         newop->extendedinfo = malloc( sizeof( struct repack_info_struct ) );
         needext = 1;
         break;
   }
   // check for allocation failure
   if ( needext &&  newop->extendedinfo == NULL ) {
      LOG( LOG_ERR, "Failed to allocate operation extended info\n" );
      free( newop->ftagstr );
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

streamwalker process_openstreamwalker( marfs_ns* ns, MDAL_CTXT ctxt, const char* reftgt, thresholds thresh, ne_location* rebuildloc ) {
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
   if ( thresh.rebuildthresh  &&  rebuildloc == NULL ) {
      LOG( LOG_ERR, "Rebuild threshold is set, but no rebuild location was specified\n" );
      errno = EINVAL;
      return NULL;
   }

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
   marfs_ms* ms = &(walker->ns->prepo->metascheme);
   marfs_ds* ds = &(walker->ns->prepo->datascheme);
   MDAL mdal = ms->mdal;
   size_t objfiles = walker->ns->prepo->datascheme.objfiles; // current repo-defined packing limit
   size_t objsize = walker->ns->prepo->datascheme.objsize;   // current repo-defined chunking limit
   size_t dataperobj = ( walker->ftag.objsize ) ? walker->ftag.objsize - walker->recovheaderlen : 0; // data chunk size for this stream
   size_t repackbytethresh = (objsize - walker->recovheaderlen) / 2;
   char pullxattrs = ( walker->gcthresh == 0  &&  walker->repackthresh == 0 ) ? 0 : 1;
   // iterate over all reference targets
   while ( ftag.endofstream == 0  &&  (ftag.state & FTAG_DATASTATE) >= FTAG_FIN ) {
      // calculate offset of next file tgt
      size_t tgtoffset = 1;
      if ( walker->gctag.refcnt ) {
         // handle any existing gctag value
         tgtoffset += walker->gctag.refcnt; // skip over count indicated by the tag
         if ( walker->gctag.inprogress  &&  walker->gcthresh ) {
            LOG( LOG_INFO, "Cleaning up in-progress deletion of %zu reference files from previous instance\n", gctag.refcnt );
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
               tgtop->count += gctag.refcnt;
               delref_info* extinf = &(tgtop->extendedinfo);
               if ( gctag.eos ) { extinf->eos = 1; }
            }
            else {
               // populate newly created op
               tgtop->count = gctag.refcnt;
               delref_info* extinf = &(tgtop->extendedinfo);
               extinf->prev_active_index = walker->activeindex;
               extinf->eos = gctag.eos;
            }
            // clear the 'inprogress' state, just to make sure we never repeat this process
            gctag.inprogress = 0;
         }
         // this could potentially indicate a premature EOS
         if ( gctag.eos ) {
            LOG( LOG_INFO, "GC tag indicates EOS at fileno %zu\n", walker->fileno );
            break;
         }
      }
      // generate next target info
      FTAG tmptag = walker->ftag;
      tmptag.fileno += tgtoffset;
      char* reftgt = datastream_genrpath( &(tmptag), ms );
      if ( reftgt == NULL ) {
         LOG( LOG_ERR, "Failed to generate reference path for corrected tgt ( %zu )\n", walker->fileno );
         return -1;
      }
      // pull info for the next reference target
      char filestate = -1;
      char haveftag = pullxattrs;
      if ( process_getfileinfo( reftgt, pullxattrs, walker, &(filestate) ) ) {
         LOG( LOG_ERR, "Failed to get info for reference target: \"%s\"\n", walker->reftgt );
         return -1;
      }
      pullxattrs = ( walker->gcthresh == 0  &&  walker->repackthresh == 0 ) ? 0 : 1; // return to default behavior
      char dispatchedops = 0;
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
         reftgt = datastream_genrpath( &(walker->ftag), ms );
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
         continue; // restart this iteration, now with all info available
      }
      free( reftgt );
      // many checks are only appropriate if we're pulling xattrs
      size_t endobj = walker->objno;
      char eos = 0;
      if ( haveftag ) {
         // check for innapropriate FTAG value
         if ( walker->ftag.fileno != walker->fileno + tgtoffest ) {
            LOG( LOG_ERR, "Invalid FTAG filenumber (%zu) on file %zu\n", walker->ftag.fileno, walker->fileno + tgtoffset );
            return -1;
         }
         endobj = walker->ftag.objno; // update ending object index
         eos = walker->ftag.endofstream;
         if ( (walker->ftag.state & FTAG_DATASTATE) < FTAG_FIN ) { eos = 1; }
         // calculate final object referenced by this file
         if ( dataperobj ) {
            // calculate the final object referenced by this file
            size_t finobjbounds = (walker->ftag->bytes + walker->ftag->offset - walker->recovheaderlen) / dataperobj;
            if ( (walker->ftag->state & FTAG_DATASTATE) >= FTAG_FIN  &&
                  finobjbounds  &&
                  (walker->ftag->bytes + walker->ftag->offset - headerlen) % dataperobj == 0 ) {
               finobjbounds--;
            }
            endobj += finobjbounds;
         }
         // check for object transition
         if ( walker->ftag.objno != walker->objno ) {
            // we may need to delete the previous object IF we are GCing AND no active refs existed for that obj
            //    AND it is not an already a deleted object0
            if ( walker->gcthresh  &&  walker->activefiles == 0  &&
                 ( walker->objno != 0  ||  walker->gctag->delzero == 0 ) ) {
               // need to prepend an object deletion operation
               opinfo* optgt = NULL;
               if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(walker->ftag), &(optgt) ) ) {
                  LOG( LOG_ERR, "Failed to identify operation target for deletion of object %zu\n", walker->objno );
                  return -1;
               }
               // sanity check
               if ( optgt->count + optgt->ftag.objno != walker->objno ) {
                  LOG( LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n", walker->objno );
                  return -1;
               }
               // update operation
               optgt->count++;
               // update our record
               walker->record.delobjs++;
            }
            // need to handle repack ops
            if ( walker->rpckops ) {
               if ( walker->activebytes >= repackbytethresh ) {
                  // discard all ops
                  LOG( LOG_INFO, "Discarding repack ops due to excessive active bytes: %zu\n", walker->activebytes );
                  statelog_freeopinfo( walker->rpckops );
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
            if ( walker->rebuildthresh  &&  walker->stval.st_ctime < walker->rbldthresh ) {
               // check if object targets our rebuild location
               char* objname = NULL;
               ne_erasure erasure;
               ne_location location;
               if ( datastream_objtarget( &(walker->ftag), ds, &objname, &erasure, &location) ) {
                  LOG( LOG_ERR, "Failed to populate object target info for object %zu of stream \"%s\"\n", walker->ftag.objno, walker->ftag.streamid );
                  return -1;
               }
               // check for location match
               if ( (walker->rbldloc.pod < 0  ||  walker->rbldloc.pod == location.pod )  &&
                    (walker->rbldloc.cap < 0  ||  walker->rbldloc.cap == location.cap )  &&
                    (walker->rbldloc.scatter < 0  ||  walker->rbldloc.scatter == location.scatter ) ) {
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
            if ( optgt->count ) {
               // update existing op
               optgt->count++;
               struct delref_info_struct* delrefinf = &(optgt->extendedinfo);
               // sanity check
               if ( delrefinf->prev_active_index != walker->prevactive ) {
                  LOG( LOG_ERR, "Active delref op active index (%zu) does not match current val (%zu)\n",
                                delrefinf->prev_active_index, walker->prevactive );
                  return -1;
               }
               if ( delrefinf->eos == 0 ) { delrefinf->eos = eos; }
            }
            else {
               // populate new op
               struct delref_info_struct* delrefinf = &(optgt->extendedinfo);
               delrefinf->prev_active_index = walker->prevactive;
               delrefinf->eos = eos;
            }
            if ( endobj != walker->objno ) {
               // potentially generate a GC op for the first obj referenced by this file
               FTAG tmptag = walker->ftag;
               tmptag.objno = walker->objno;
               if ( walker->activefiles == 0 ) {
                  optgt = NULL;
                  if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(walker->ftag), &(optgt) ) ) {
                     LOG( LOG_ERR, "Failed to identify operation target for deletion of initial spanned obj %zu\n", curobj );
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
                  walker->record.delobjs++;
               }
               // potentially generate GC ops for objects spanned by this file
               tmptag.objno++;
               while ( tmptag.objno < endobj ) {
                  // generate ops for all but the last referenced object
                  optgt = NULL;
                  if ( process_identifyoperation( &(walker->gcops), MARFS_DELETE_OBJ_OP, &(tmptag), &(optgt) ) ) {
                     LOG( LOG_ERR, "Failed to identify operation target for deletion of spanned obj %zu\n", tmptag.objno );
                     return -1;
                  }
                  // sanity check
                  if ( optgt->count + optgt->ftag.objno != curobj ) {
                     LOG( LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n",
                                   optgt->count + optgt->ftag.objno, tmptag.objno );
                     return -1;
                  }
                  // update operation
                  optgt->count++;
                  // update our record
                  walker->record.delobjs++;
                  // iterate to the next obj
                  tmptag.objno++;
               }
            }
         }
         else if ( walker->stval.st_ctime >= walker->gcthresh ) {
            // this file was too recently deactivated to gc
            walker->report.volfiles++;
            assumeactive = 1;
         }
      }
      if ( filestate > 1 ) {
         // file is active
         walker->report.fileusage++;
         if ( haveftag ) { walker->report.byteusage += walker->ftag.bytes; }
         else { walker->report.byteusage += walker->stval.st_size; }
         // TODO manage repack ops
         // TODO potentially generate rebuild ops
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
      walker->fileno += tgtoffest;
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
         statelog_freeopinfo( walker->rpckops );
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
 * Perform the given operation
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the current NS
 * @param opinfo* op : Reference to the operation to be performed
 *                     NOTE -- this will be updated to reflect operation completion / error
 *                     NOTE -- this function WILL NOT execute the entire op chain, only the head op
 * @return int : Zero on success, or -1 on failure
 *               NOTE -- This func will not return 'failure' unless a critical internal error occurs.
 *                       'Standard' operation errors will simply be reflected in the op struct itself.
 */
int process_executeoperation( MDAL_CTXT ctxt, opinfo* op ) {
}


