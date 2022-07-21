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


typedef enum
{
   RESOURCE_RECORD_LOG,
   RESOURCE_MODIFY_LOG
} rsrclog_type;

typedef enum
{
   MARFS_DELETE_OBJ_OP,
   MARFS_DELETE_REF_OP,
   MARFS_REBUILD_OP,
   MARFS_REPACK_OP
} operation_type;

typedef struct opinfo_struct {
   operation_type type;  // which class of operation
   char start;           // flag indicating the start of an op ( if zero, this entry indicates completion )
   size_t count;         // how many subsequent targets are there
   int errval;           // errno value of the attempted op ( always zero for operation start )
   FTAG ftag;            // which FTAG value is the target
   struct opinfo_struct* next; // subsequent ops in this chain
} opinfo;

typedef struct operation_summary_struct {
   size_t deletion_object_count;
   size_t deletion_object_failures;
   size_t deletion_reference_count;
   size_t deletion_reference_failures;
   size_t rebuild_count;
   size_t rebuild_failures;
   size_t repack_count;
   size_t repack_failures;
} operation_summary;


//   -------------   INTERNAL DEFINITIONS    -------------

#define MAX_BUFFER 8192 // maximum character buffer to be used for parsing/printing log lines
                        //    program will abort if limit is exceeded when reading or writing
#define RECORD_LOG_PREFIX "RESOURCE-RECORD-LOGFILE\n" // prefix for a 'record'-log
                                                      //    - only op starts, no completions
#define MODIFY_LOG_PREFIX "RESOURCE-MODIFY-LOGFILE\n" // prefix for a 'modify'-log
                                                      //    - mix of op starts and completions

typedef struct statelog_struct {
   // synchronization and access control
   pthread_mutex_t   lock;
   pthread_cond_t    nooutstanding;  // left NULL for a 'record' log
   ssize_t           outstandingcnt; // count of scanners still running ( threads that could potentially submit more ops )
                                     //  always zero for a 'record' log
   // state info
   operation_summary summary;
   HASH_TABLE        inprogress;  // left NULL for a 'record' log
   int               logfile;
   char*             logfilepath;
}*STATELOG;

//   -------------   INTERNAL FUNCTIONS    -------------

/**
 * Free the given opinfo struct
 * @param opinfo* op : Reference to the opinfo struct to be freed
 */
void freeopinfo( opinfo* op ) {
   if ( op ) {
      if ( op->ftag.ctag ) { free( op->ftag.ctag ); }
      if ( op->ftag.streamid ) { free( op->ftag.streamid ); }
      free( op );
   }
}

/**
 * Clean up the provided statelog
 * @param STATELOG stlog : Reference to the statelog to be cleaned
 * @param char destroy : If non-zero, the entire statelog structure will be freed
 *                       If zero, all state will be purged, but the struct can be reinitialized
 */
void cleanuplog( STATELOG stlog, char destroy ) {
   HASH_NODE* nodelist = NULL;
   size_t index = 0;
   if ( stlog->inprogress ) {
      hash_term( stlog->inprogress, &nodelist, &index );
      while ( index ) {
         opinfo* opindex = (nodelist + index - 1)->content;
         while ( opindex != NULL ) {
            opinfo* freeop = opindex;
            opindex = opindex->next;
            freeopinfo( freeop );
         }
         free( (nodelist + index - 1)->name );
         index--;
      }
      free( nodelist );
      stlog->inprogress = NULL;
   }
   if ( stlog->logfilepath ) { free( stlog->logfilepath ); }
   if ( stlog->logfile > 0 ) { close( stlog->logfile ); }
   if ( destroy ) {
      if ( stlog->nooutstanding ) { pthread_cond_destroy( &(stlog->nooutstanding) ); }
      if ( stlog->lock ) { pthread_mutex_destroy( &(stlog->lock) ); }
      free( stlog );
   }
   return;
}

/**
 * Parse a new operation ( or sequence of them ) from the given logfile
 * @param int logfile : Reference to the logfile to parse a line from
 * @param char* eof : Reference to a character to be populated with an exit flag value
 *                    1 if we hit EOF on the file on a line division
 *                    -1 if we hit EOF in the middle of a line
 *                    zero otherwise
 * @return opinfo* : Reference to a new set of operation info structs ( caller must free )
 * NOTE -- Under most failure conditions, the logfile offset will be returned to its original value.
 *         This is not the case if parsing reaches EOF, in which case, offset will be left there.
 */
opinfo* parselogline( int logfile, char* eof ) {
   char buffer[MAX_BUFFER] = {0};
   char* tgtchar = buffer;
   off_t origoff = lseek( logfile, 0, SEEK_CUR );
   if ( origoff < 0 ) {
      LOG( LOG_ERR, "Failed to identify current logfile offset\n" );
      return NULL;
   }
   // read in an entire line
   // NOTE -- Reading one char at a time isn't very efficient, but we don't expect parsing of 
   //         logfiles to be a significant performance factor.  This approach greatly simplifies 
   //         char buffer mgmt.
   ssize_t readbytes;
   while ( (readbytes = read( logfile, tgtchar, 1 )) == 1 ) {
      // check for end of line
      if ( *tgtchar == '\n' ) { break; }
      // check for excessive string length
      if ( tgtchar - buffer >= MAX_BUFFER - 1 ) {
         LOG( LOG_ERR, "Parsed line exceeds memory limits\n" );
         lseek( logfile, origoff, SEEK_SET );
         *eof = 0;
         return NULL;
      }
      tgtchar++;
   }
   if ( *tgtchar != '\n' ) {
      if ( readbytes == 0 ) {
         if ( tgtchar == buffer ) {
            LOG( LOG_INFO, "Hit EOF on logfile\n" );
            *eof = 1;
            return NULL;
         }
         LOG( LOG_ERR, "Hit mid-line EOF on logfile\n" );
         *eof = -1;
         return NULL;
      }
      LOG( LOG_ERR, "Encountered error while reading from logfile\n" );
      lseek( logfile, origoff, SEEK_SET );
      *eof = 0;
      return NULL;
   }
   *eof = 0; // preemptively populate with zero
   // allocate our operation node
   opinfo* op = malloc( sizeof( struct opinfo_struct ) );
   if ( op == NULL ) {
      LOG( LOG_ERR, "Failed to allocate opinfo struct for logfile line\n" );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   op->start = 0;
   op->count = 0;
   op->errval = 0;
   op->next = NULL;
   // parse the op type
   char* parseloc = buffer;
   if ( strncmp( buffer, "DEL-OBJ ", 8 ) == 0 ) {
      op->type = MARFS_DELETE_OBJ_OP;
      parseloc += 8;
   }
   else if ( strncmp( buffer, "DEL-REF ", 8 ) == 0 ) {
      op->type = MARFS_DELETE_REF_OP;
      parseloc += 8;
   }
   else if ( strncmp( buffer, "REBUILD ", 8 ) == 0 ) {
      op->type = MARFS_REBUILD_OP;
      parseloc += 8;
   }
   else if ( strncmp( buffer, "REPACK ", 7 ) == 0 ) {
      op->type = MARFS_REPACK_OP;
      parseloc += 7;
   }
   else {
      LOG( LOG_ERR, "Unrecognized operation type value: \"%s\"\n", buffer );
      free( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   if ( *parseloc == 'S' ) {
      op->start == 1;
   }
   else if ( *parseloc != 'E' ) {
      LOG( LOG_ERR, "Unexpected START string value: '\%c'\n", *parseloc );
      free( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   if ( *(parseloc + 1) != ' ' ) {
      LOG( LOG_ERR, "Unexpected trailing character after START value: '%c'\n", *(parseloc + 1) );
      free( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   parseloc += 2;
   // parse the count value
   char* endptr = NULL;
   unsigned long long parseval = strtoull( parseloc, &endptr, 10 );
   if ( endptr == NULL  ||  *endptr != ' ' ) {
      LOG( LOG_ERR, "Failed to parse COUNT value with unexpected char: '%c'\n", *endptr );
      free( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   op->count = (size_t)parseval;
   parseloc = endptr + 1;
   // parse the errno value
   long sparseval = strtol( parseloc, &endptr, 10 );
   if ( endptr == NULL  ||  *endptr != ' ' ) {
      LOG( LOG_ERR, "Failed to parse ERRNO value with unexpected char: '%c'\n", *endptr );
      free( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   op->errval = (int)sparseval;
   parseloc = endptr + 1;
   // parse the NEXT value
   char nextval = 0;
   if ( *(tgtchar - 1) == '-' ) {
      if ( *(tgtchar - 2) != ' ' ) {
         LOG( LOG_ERR, "Unexpected char preceeds NEXT flag: '%c'\n", *(tgtchar - 2) );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      nextval = 1; // note that we need to append another op
      tgtchar -= 2; // pull this back, so we'll trim off the NEXT value
   }
   // parse the FTAG value
   *tgtchar = '\0'; // trim the string, to make FTAG parsing easy
   if ( ftag_initstr( &(op->ftag), parseloc ) ) {
      LOG( LOG_ERR, "Failed to parse FTAG value of log line\n" );
      free( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   // finally, parse in any subsequent linked ops
   if ( nextval ) {
      // NOTE -- Recursive parsing isn't the most efficient approach.
      //         Simple though, and, once again, we don't expect logfile parsing to be a 
      //         significant performance consideration.
      op->next = parselogline( logfile, eof );
      if ( op->next == NULL ) {
         LOG( LOG_ERR, "Failed to parse linked operation\n" );
         free( op );
         if ( *eof == 0 ) { lseek( logfile, origoff, SEEK_SET ); }
         return NULL;
      }
   }
   return op;
}

/**
 * Print the specified operation info ( or chain of them ) to the specified logfile
 * @param int logfile : File descriptor for the target logfile
 * @param opinfo* op : Reference to the operation to be printed
 * @return int : Zero on success, or -1 on failure
 */
int printlogline( int logfile, opinfo* op ) {
   char buffer[MAX_BUFFER];
   ssize_t usedbuff;
   off_t origoff = lseek( logfile, 0, SEEK_CUR );
   if ( origoff < 0 ) {
      LOG( LOG_ERR, "Failed to identify current logfile offset\n" );
      return -1;
   }
   // populate the type string of the operation
   switch ( op->type ) {
      case MARFS_DELETE_OBJ_OP:
         if ( snprintf( buffer, MAX_BUFFER, "%s ", "DEL-OBJ" ) != 8 ) {
            LOG( LOG_ERR, "Failed to populate 'DEL-OBJ' type string\n" );
            return -1;
         }
         usedbuff += 8;
         break;
      case MARFS_DELETE_REF_OP:
         if ( snprintf( buffer, MAX_BUFFER, "%s ", "DEL-REF" ) != 8 ) {
            LOG( LOG_ERR, "Failed to populate 'DEL-REF' type string\n" );
            return -1;
         }
         usedbuff += 8;
         break;
      case MARFS_REBUILD_OP:
         if ( snprintf( buffer, MAX_BUFFER, "%s ", "REBUILD" ) != 8 ) {
            LOG( LOG_ERR, "Failed to populate 'REBUILD' type string\n" );
            return -1;
         }
         usedbuff += 8;
         break;
      case MARFS_REPACK_OP:
         if ( snprintf( buffer, MAX_BUFFER, "%s ", "REPACK" ) != 7 ) {
            LOG( LOG_ERR, "Failed to populate 'REPACK' type string\n" );
            return -1;
         }
         usedbuff += 7;
         break;
      default:
         LOG( LOG_ERR, "Unrecognized TYPE value of operation\n" );
         return -1;
   }
   if ( usedbuff >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Operation string exceeds memory allocation limits\n" );
      return -1;
   }
   // populate start flag
   if ( op->start  &&  snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "%c ", 'S' ) != 2 ) {
      LOG( LOG_ERR, "Failed to populate 'S' start flag string\n" );
      return -1;
   }
   else if ( snprintf( buffer + usedbuff, "%c ", 'E' ) != 2 ) {
      LOG( LOG_ERR, "Failed to populate 'E' start flag string\n" );
      return -1;
   }
   usedbuff += 2;
   if ( usedbuff >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Operation string exceeds memory allocation limits\n" );
      return -1;
   }
   // populate the count string
   ssize_t printres;
   if ( (printres = snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "%zu ", op->count )) < 2 ) {
      LOG( LOG_ERR, "Failed to populate \"%zu\" count string\n", op->count );
      return -1;
   }
   usedbuff += printres;
   if ( usedbuff >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Operation string exceeds memory allocation limits\n" );
      return -1;
   }
   // populate the errval string
   if ( (printres = snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "%d ", op->errval )) < 2 ) {
      LOG( LOG_ERR, "Failed to populate \"%d\" errval string\n", op->errval );
      return -1;
   }
   usedbuff += printres;
   if ( usedbuff >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Operation string exceeds memory allocation limits\n" );
      return -1;
   }
   // populate the FTAG string
   if ( (printres = ftag_tostr( &(op->ftag), buffer + usedbuff, MAX_BUFFER - usedbuff )) < 1 ) {
      LOG( LOG_ERR, "Failed to populate FTAG string\n" );
      return -1;
   }
   usedbuff += printres;
   if ( usedbuff >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Operation string exceeds memory allocation limits\n" );
      return -1;
   }
   // populate the NEXT flag
   if ( op->next ) {
      if ( snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, " -" ) != 2 ) {
         LOG( LOG_ERR, "Failed to populate NEXT flag string\n" );
         return -1;
      }
      usedbuff += 2;
      if ( usedbuff >= MAX_BUFFER ) {
         LOG( LOG_ERR, "Operation string exceeds memory allocation limits\n" );
         return -1;
      }
   }
   // populate EOL
   *(buffer + usedbuff) = '\n';
   usedbuff++;
   if ( usedbuff >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Operation string exceeds memory allocation limits\n" );
      return -1;
   }
   *(buffer + usedbuff) = '\0'; // NULL-terminate, just in case
   // finally, output the full op line
   if ( write( logfile, buffer, usedbuff ) != usedbuff ) {
      LOG( LOG_ERR, "Failed to write operation string of length %zd to logfile\n", usedbuff );
      return -1;
   }
   // potentially output trailing ops recursively
   if ( op->next ) {
      return printlogline( logfile, op->next );
   }
   return 0;
}

/**
 * Incorporate the given opinfo string into the given statelog
 * @param STATELOG* stlog : statelog to be updated
 * @param opinfo* newop : Operation(s) to be included
 *                        NOTE -- This func will never free opinfo structs
 * @return int : Zero on success, or -1 on failure
 */
int processopinfo( STATELOG* stlog, opinfo* newop ) {
   // identify the trailing op of the given chain
   opinfo* finop = newop;
   while ( finop->next ) {
      finop = finop->next;
      if ( finop->start != newop->start ) {
         LOG( LOG_ERR, "Operation chain has inconsistent START value\n" );
         return -1;
      }
   }
   // map this operation into our inprogress hash table
   HASH_NODE* node = NULL;
   if ( hash_lookup( stlog->inprogress, newop->tgt, &node ) < 0 ) {
      LOG( LOG_ERR, "Failed to map operation on \"%s\" into inprogress HASH_TABLE\n", newop->tgt );
      return -1;
   }
   // traverse the attached operations, looking for a match
   opinfo* opindex = node->content;
   opinfo* prevop = NULL;
   while ( opindex != NULL ) {
      if ( (ftag_cmp( &opindex->ftag, &newop->ftag ) == 0)  &&
           (opindex->type == newop->type) ) {
         break;
      }
      prevop = opindex;
      opindex = opindex->next;
   }
   if ( opindex != NULL ) {
      // repeat of operation start can be ignored
      if ( newop->start == 0 ) {
         // otherwise, for op completion, we'll need to process each operation in the chain...
         opinfo* parseop = newop; // new op being added to totals
         opinfo* parseindex = opindex; // old op being compared against
         opinfo* previndex = opindex;  // end of the chain of old ops
         while ( parseop ) {
            // ...note each in our totals...
            switch( parseop->type ) {
               case MARFS_DELETE_OBJ_OP:
                  stlog->summary.delete_object_count++;
                  if ( parseop->errval ) { stlog->summary.delete_object_failures++; }
                  break;
               case MARFS_DELETE_REF_OP:
                  stlog->summary.delete_reference_count++;
                  if ( parseop->errval ) { stlog->summary.delete_reference_failures++; }
                  break;
               case MARFS_REBUILD_OP:
                  stlog->summary.rebuild_count++;
                  if ( parseop->errval ) { stlog->summary.rebuild_failures++; }
                  break;
               case MARFS_REPACK_OP:
                  stlog->summary.repack_count++;
                  if ( parseop->errval ) { stlog->summary.repack_failures++; }
                  break;
               default:
                  LOG( LOG_ERR, "Unrecognized operation type value\n" );
                  return -1;
            }
            // progress to the next op in the chain, validating that it matches the subsequent in the opindex chain
            parseop = parseop->next;
            previndex = parseindex; // keep track of where the index op chain terminates
            parseindex = parseindex->next;
            if ( parseop ) {
               // at this point, any variation between operation chains is a fatal error
               if ( parseindex == NULL  ||  ftag_cmp( &parseop->ftag, &parseindex->ftag )  ||  parseop->type != parseindex->type ) {
                  LOG( LOG_ERR, "Operation completion chain does not match outstainding operation chain\n" );
                  return -1;
               }
            }
         }
         // ...and remove the matching op(s) from inprogress
         if ( prevop ) {
            // pull the matching ops out of the list
            prevop->next = previndex->next;
         }
         else {
            // no previous op means the matching op is the first one; just remove it
            node->content = previndex->next;
         }
      }
      // a matching op means the parsed operation can be discarded
   }
   else {
      // the parsed line should indicate the start of a new operation
      if ( newop->start == 0 ) {
         LOG( LOG_ERR, "Parsed completion of op from logfile \"%s\" with no parsed start of op\n", stlog->logfilepath );
         return -1;
      }
      // stitch the parsed op onto the front of our inprogress list
      finop->next = node->content;
      node->content = newop;
   }
   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Generates the pathnames of logfiles and parent dirs
 * @param char create : Create flag
 *                      If non-zero, this func will attempt to create all intermediate directory paths ( not the final tgt )
 * @param const char* logroot : Root of the logfile tree
 * @param const char* iteration : ID string for this program iteration ( can be left NULL to gen parent path )
 * @param marfs_ns* ns : MarFS NS to process ( can be left NULL to gen parent path, ignored if prev is NULL )
 * @param ssize_t ranknum : Processing rank ( can be < 0 to gen parent path, ignored if prev is NULL )
 * @return char* : Path of the corresponding log location, or NULL if an error occurred
 *                 NOTE -- It is the caller's responsibility to free this string
 */
char* statelog_genlogpath( char create, const char* logroot, const char* iteration, marfs_ns* ns, ssize_t ranknum ) {
   // if we have a NS, identify an appropriate FS path
   char* nspath = NULL;
   if ( ns ) {
      nspath = strdup( ns->idstr );
      char* tmpparse = nspath;
      while ( *tmpparse != '\0' ) {
         if ( *tmpparse == '/' ) { *tmpparse = '#'; }
      }
   }
   // identify length of the constructed path
   ssize_t pathlen = 0;
   if ( iteration  &&  nspath  &&  ranknum >= 0 ) {
      pathlen = snprintf( NULL, 0, "%s/%s/%s/statelog-%zu", logroot, iteration, nspath, ranknum );
   }
   else if ( iteration  &&  nspath ) {
      pathlen = snprintf( NULL, 0, "%s/%s/%s", logroot, iteration, nspath );
   }
   else if ( iteration ) {
      pathlen = snprintf( NULL, 0, "%s/%s", logroot, iteration );
   }
   else {
      pathlen = snprintf( NULL, 0, "%s", logroot );
   }
   if ( pathlen < 1 ) {
      LOG( LOG_ERR, "Failed to identify strlen of logfile path\n" );
      if ( nspath ) { free( nspath ); }
      return NULL;
   }
   // allocate the path
   char* path = malloc( sizeof(char) * (pathlen + 1) );
   if ( path == NULL ) {
      LOG( LOG_ERR, "Failed to allocate %zu bytes for logfile path\n", pathlen + 1 );
      if ( nspath ) { free( nspath ); }
      return NULL;
   }
   // populate the path root
   ssize_t lrootlen = snprintf( path, pathlen + 1, "%s", logroot );
   if ( lrootlen < 1  ||  lrootlen >= pathlen ) {
      LOG( LOG_ERR, "Failed to populate logfile root path\n" );
      if ( nspath ) { free( nspath ); }
      free( path );
      return NULL;
   }
   // potentially exit here
   if ( iteration == NULL ) {
      LOG( LOG_INFO, "Generated root path: \"%s\"\n", path );
      if ( nspath ) { free( nspath ); }
      return path;
   }
   // create, if necessary
   if ( create  &&  mkdir( path, 0700 )  &&  errno != EEXIST ) {
      LOG( LOG_ERR, "Failed to create log root dir: \"%s\"\n", path );
      if ( nspath ) { free( nspath ); }
      free( path );
      return NULL;
   }
   // populate the path iteration
   ssize_t iterlen = snprintf( path + lrootlen, (pathlen - lrootlen) + 1, "/%s", iteration );
   if ( iterlen < 1  ||  iterlen >= (pathlen - lrootlen) ) {
      LOG( LOG_ERR, "Failed to populate logfile iteration path: \"%s\"\n", iteration );
      if ( nspath ) { free( nspath ); }
      free( path );
      return NULL;
   }
   // potentially exit here
   if ( nspath == NULL ) {
      LOG( LOG_INFO, "Generated iteration path: \"%s\"\n", path );
      return path;
   }
   // create, if necessary
   if ( create  &&  mkdir( path, 0700 )  &&  errno != EEXIST ) {
      LOG( LOG_ERR, "Failed to create logfile iteration dir: \"%s\"\n", path );
      free( nspath );
      free( path );
      return NULL;
   }
   // populate the path ns
   ssize_t nslen = snprintf( path + lrootlen + iterlen, (pathlen - (lrootlen + iterlen)) + 1, "/%s", nspath );
   if ( nslen < 1  ||  nslen >= ((pathlen - lrootlen) - iterlen) ) {
      LOG( LOG_ERR, "Failed to populate NS path value: \"%s\"\n", nspath );
      free( nspath );
      free( path );
      return NULL;
   }
   // create NS parent paths, if necessary
   if ( create ) {
      // have to iterate over and create all intermediate dirs
      char* parse = path + lrootlen + iterlen + 1;
      while ( *parse != '\0' ) {
         if ( *parse == '/' ) {
            *parse = '\0';
            if ( mkdir( path, 0700 )  &&  errno != EEXIST ) {
               LOG( LOG_ERR, "Failed to create log NS subdir: \"%s\"\n", path );
               free( nspath );
               free( path );
               return NULL;
            }
            *parse = '/';
         }
         parse++;
      }
   }
   free( nspath ); // done with this value
   // potentially exit here
   if ( ranknum < 0 ) {
      LOG( LOG_INFO, "Generated NS log path: \"%s\"\n", path );
      return path;
   }
   // create, if necessary
   if ( create  &&  mkdir( path, 0700 )  &&  errno != EEXIST ) {
      LOG( LOG_ERR, "Failed to create logfile NS dir: \"%s\"\n", path );
      free( path );
      return NULL;
   }
   // populate the final logfile path
   // NOTE -- we never create this file in this func
   if ( snprintf( path + lrootlen + iterlen + nslen, (pathlen - (lrootlen + iterlen + nslen)) + 1, "/statelog-%zu", ranknum ) !=
         (pathlen - (lrootlen + iterlen + nsidlen)) ) {
      LOG( LOG_ERR, "Logfile path has inconsistent length\n" );
      free( path );
      return NULL;
   }
   return path;
}

/**
 * Initialize a statelog, associated with the given logging root, namespace, and rank
 * @param STATELOG* statelog : Statelog to be initialized
 * @param const char* logroot : Root of the FS tree for statelog storage
 * @param marfs_ns* ns : Reference to the Namespace associated with this statelog
 * @param size_t ranknum : Processing rank number for this statelog
 * @param char cleanup : If zero, leave any existing statelog files intact;
 *                       If non-zero, cleanup and incorporate any existing statelog files 
 *                       associated with the same Namespace ID string
 * @return int : Zero on success, or -1 on failure
 */
int statelog_init( STATELOG* statelog, const char* logroot, marfs_ns* ns, size_t ranknum, char cleanup ) {
   // check for invalid args
   if ( statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( logroot == NULL ) {
      LOG( LOG_ERR, "Received a NULL logroot value\n" );
      errno = EINVAL;
      return -1;
   }
   if ( ns == NULL ) {
      LOG( LOG_ERR, "Received a NULL namespace value\n" );
      errno = EINVAL;
      return -1;
   }
   // identify our actual statelog
   char newstlog = 0;
   STATELOG stlog = *statelog;
   if ( stlog == NULL ) {
      // allocate a new statelog
      stlog = malloc( sizeof( struct statelog_struct ) );
      if ( stlog == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for a new statelog\n" );
         return -1;
      }
      if ( pthread_mutex_init( &(stlog->lock), NULL ) ) {
         LOG( LOG_ERR, "Failed to initialize lock on new statelog struct\n" );
         free( stlog );
         return -1;
      }
      if ( pthread_cond_init( &(stlog->nooutstanding), NULL ) ) {
         LOG( LOG_ERR, "Failed to initialize condition on new statelog struct\n" );
         pthread_mutex_destroy( &(stlog->lock) );
         free( stlog );
         return -1;
      }         hash_term( stlog->inprogress, NULL, NULL );
         while ( index ) {
            free( (nodelist + index - 1)->name );
            index--;
         }
         free( nodelist );
         free( stlog->logfilepath );
         if ( newstlog ) {
            pthread_cond_destroy( &(stlog->nooutstanding) );
            pthread_mutex_destroy( &(stlog->lock) );
            free( stlog );
         }
      stlog->outstandingcnt = 0;
      bzero( &(stlog->summary), sizeof( struct operation_summary_struct ) );
      stlog->inprogress = NULL;
      stlog->logfile = -1;
      stlog->logfilepath = NULL;
      newstlog = 1;
   }
   else {
      // attempt to reuse the existing statelog
      if ( stlog->logfilepath ) {
         LOG( LOG_ERR, "The passed statelog reference has not been finalized\n" );
         errno = EINVAL;
         return -1;
      }
   }
   // initialize our logging path
   stlog->logfilepath = genlogfilepath( logroot, ns, ranknum );
   if ( stlog->logfilepath == NULL ) {
      LOG( LOG_ERR, "Failed to identify logfile path\n" );
      if ( newstlog ) {
         pthread_cond_destroy( &(stlog->nooutstanding) );
         pthread_mutex_destroy( &(stlog->lock) );
         free( stlog );
      }
      return -1;
   }
   // initialize our HASH_TABLE
   HASH_NODE* nodelist = malloc( sizeof(HASH_NODE) * ns->prepo->metascheme.refnodecount );
   if ( nodelist == NULL ) {
      LOG( LOG_ERR, "Failed to allocate nodelist for in progress hash table\n" );
      free( stlog->logfilepath );
      if ( newstlog ) {
         pthread_cond_destroy( &(stlog->nooutstanding) );
         pthread_mutex_destroy( &(stlog->lock) );
         free( stlog );
      }
      return -1;
   }
   size_t index = 0;
   for ( ; index < ns->prepo->metascheme.refnodecount; index++ ) {
      // initialize node list to mirror the reference nodes
      (nodelist + index)->content = NULL;
      (nodelist + index)->weight = (ns->prepo->metascheme.refnodelist + index)->weight;
      (nodelist + index)->name = strdup( (ns->prepo->metascheme.refnodelist + index)->name );
      if ( (nodelist + index)->name == NULL ) {
         LOG( LOG_ERR, "Failed to allocate name of node %zu\n", index );
         while ( index ) {
            free( (nodelist + index - 1)->name );
            index--;
         }
         free( nodelist );
         free( stlog->logfilepath );
         if ( newstlog ) {
            pthread_cond_destroy( &(stlog->nooutstanding) );
            pthread_mutex_destroy( &(stlog->lock) );
            free( stlog );
         }
         return -1;
      }
   }
   stlog->inprogress = hash_init( nodelist, ns->prepo->metascheme.refnodecount, 0 );
   if ( stlog->inprogress == NULL ) {
      LOG( LOG_ERR, "Failed to initialize inprogress hash table\n" );
      while ( index ) {
         free( (nodelist + index - 1)->name );
         index--;
      }
      free( nodelist );
      free( stlog->logfilepath );
      if ( newstlog ) {
         pthread_cond_destroy( &(stlog->nooutstanding) );
         pthread_mutex_destroy( &(stlog->lock) );
         free( stlog );
      }
      return -1;
   }
   // open our logfile
   int openmode = O_CREAT | O_RDWR;
   if ( cleanup == 0 ) { openmode |= O_EXCL; }
   stlog->logfile = open( stlog->logfilepath, openmode, 0700 );
   if ( stlog->logfile < 0 ) {
      LOG( LOG_ERR, "Failed to open statelog: \"%s\"\n", stlog->logfilepath );
      cleanuplog( stlog, newstlog );
      return -1;
   }
   // parse over logfile entries
   char eof = 0;
   opinfo* parsedop = NULL;
   while ( (parsedop = parselogline( stlog->logfile, &eof )) != NULL ) {
      // map this operation into our inprogress hash table
      if ( processopinfo( stlog, parsedop ) < 0 ) {
         LOG( LOG_ERR, "Failed to process lines from logfile: \"%s\"\n", stlog->logfilepath );
         cleanuplog( stlog, newstlog );
         return -1;
      }
   }
   if ( eof == 0 ) {
      LOG( LOG_ERR, "Failed to parse existing logfile: \"%s\"\n", stlog->logfilepath );
      cleanuplog( stlog, newstlog );
      return -1;
   }
   // potentially integrate info from any pre-existing logfiles
   if ( cleanup ) {
      // identify and open our parent dir
      char* parentparse = stlog->logfilepath;
      char* prevent = NULL;
      for ( ; *parentparse != '\0'; parentparse++ ) {
         if ( *parentparse == '/' ) { prevent = parentparse; } // update previous dir sep
      }
      if ( prevent == NULL ) {
         LOG( LOG_ERR, "Failed to identify parent dir of logfile path: \"%s\"\n", stlog->logfilepath );
         cleanuplog( stlog, newstlog );
         return -1;
      }
      *prevent = '\0'; // insert a NULL, to allow us to open the parent dir
      DIR* parentdir = opendir( stlog->logfilepath );
      *prevent = '/';
      if ( parentdir == NULL ) {
         LOG( LOG_ERR, "Failed to open parent dir of logfile: \"%s\"\n", stlog->logfilepath );
         cleanuplog( stlog, newstlog );
         return -1;
      }
      // readdir to identify all lingering logfiles
      int olderr = errno;
      errno = 0;
      struct dirent* entry = NULL;
      while ( (entry = readdir( parentdir )) != NULL ) {
         if ( strcmp( prevent + 1, entry->name ) ) {
            // open and parse all old logfiles under the same dir
            LOG( LOG_INFO, "Attempting cleanup of existing logfile: \"%s\"\n", entry->name );
            int oldlog = openat( dirfd(parentdir), entry->name, O_RDONLY );
            if ( oldlog < 0 ) {
               LOG( LOG_ERR, "Failed to open existing logfile for cleanup: \"%s\"\n", entry->name );
               closedir( parentdir );
               cleanuplog( stlog, newstlog );
               return -1;
            }
            while ( (parsedop = parselogline( oldlog, &eof )) != NULL ) {
               // duplicate this op into our initial logfile
               if ( printlogline( stlog->logfile, parsedop ) ) {
                  LOG( LOG_ERR, "Failed to duplicate op from old logfile \"%s\" into active log: \"%s\"\n",
                       entry->name, stlog->logfilename );
                  close( oldlog );
                  closedir( parentdir );
                  cleanuplog( stlog, newstlog );
                  return -1;
               }
               // map any ops to our hash table
               if ( processopinfo( stlog, parsedop ) < 0 ) {
                  LOG( LOG_ERR, "Failed to process lines from old logfile: \"%s\"\n", entry->name );
                  close( oldlog );
                  closedir( parentdir );
                  cleanuplog( stlog, newstlog );
                  return -1;
               }
            }
            if ( eof == 0 ) {
               LOG( LOG_ERR, "Failed to parse old logfile: \"%s\"\n", entry->name );
               close( oldlog );
               closedir( parentdir );
               cleanuplog( stlog, newstlog );
               return -1;
            }
            close( oldlog );
            // delete the old logfile
            if ( unlinkat( dirfd(parentdir), entry->name ) ) {
               LOG( LOG_ERR, "Failed to unlink old logfile: \"%s\"\n", entry->name );
               closedir( parentdir );
               cleanuplog( stlog, newstlog );
               return -1;
            }
         }
      }
      closedir( parentdir );
   }
   // finally done
   *statelog = stlog;
   return 0; 
}

/**
 * Record that a certain number of threads are currently processing
 * @param STATELOG* statelog : Statelog to be updated
 * @param size_t numops : Number of additional processors ( can be negative to reduce cnt )
 * @return int : Zero on success, or -1 on failure
 */
int statelog_update_inflight( STATELOG* statelog, ssize_t numops ) {
   // check for invalid args
   if ( statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   STATELOG stlog = *statelog;
   if ( stlog == NULL  ||  stlog->logfile < 1 ) {
      LOG( LOG_ERR, "Received an uninitialized statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   // acquire statelog lock
   if ( pthread_mutex_lock( &(stlog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire statelog lock\n" );
      return -1;
   }
   // check for excessive reduction
   if ( statelog->outstandingcnt < -(numops) ) {
      LOG( LOG_WARNING, "Value of %zd would result in negative thread count\n", numops );
      numops = -(statelog->outstandingcnt);
   }
   // modify count
   statelog->outstandingcnt += numops;
   // check for quiesced state
   if ( stlog->outstandingcnt == 0  &&  pthread_cond_signal( &(stlog->nooutstandingops) ) ) {
      LOG( LOG_ERR, "Failed to signal 'no outstanding ops' condition\n" );
      pthread_mutex_unlock( &(stlog->lock) );
      return -1;
   }
   if ( pthread_mutex_unlock( &(stlog->lock) ) ) {
      LOG( LOG_ERR, "Failed to release statelog lock\n" );
      return -1;
   }
   return 0;
}

/**
 * Record the start of an operation
 * @param STATELOG* statelog : Statelog to be updated
 * @param const char* target : Target of the operation
 * @param operation_type type : Operation type
 * @return int : Zero on success, or -1 on failure
 */
int statelog_op_start( STATELOG* statelog, const char* target, operation_type type ) {
   // check for invalid args
   if ( statelog == NULL  ||  *statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   STATELOG stlog = *statelog;
   // acquire the statelog lock
   if ( pthread_mutex_lock( &(stlog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire statelog lock\n" );
      return -1;
   }
   // allocate our operation node and tgt string
   opinfo* op = malloc( sizeof( struct opinfo_struct ) );
   if ( op == NULL ) {
      LOG( LOG_ERR, "Failed to allocate opinfo struct for operation start\n" );
      pthread_mutex_unlock( &(stlog->lock) );
      return -1;
   }
   op->type = type;
   op->start = 1;
   op->errval = 0;
   op->next = NULL;
   op->tgt  = strdup( target );
   if ( op->tgt == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate tgt string for op start: \"%s\"\n", target );
      pthread_mutex_unlock( &(stlog->lock) );
      free( op );
      return -1;
   }
   // process the new opinfo
   if ( processopinfo( statelog, op ) ) {
      LOG( LOG_ERR, "Failed to process operation on \"%s\" tgt\n", stlog->tgt );
      pthread_mutex_unlock( &(stlog->lock) );
      return -1;
   }
   pthread_mutex_unlock( &(stlog->lock) );
   return 0;
}

/**
 * Record the completion of an operation
 * @param STATELOG* statelog : Statelog to be updated
 * @param const char* target : Target of the operation
 * @param operation_type type : Operation type
 * @param int errval : 
 * @return int : Zero on success, or -1 on failure
 */
int statelog_op_end( STATELOG* statelog, const char* target, operation_type type, int errval ) {
   // check for invalid args
   if ( statelog == NULL  ||  *statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   STATELOG stlog = *statelog;
   // acquire the statelog lock
   if ( pthread_mutex_lock( &(stlog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire statelog lock\n" );
      return -1;
   }
   // allocate our operation node and tgt string
   opinfo* op = malloc( sizeof( struct opinfo_struct ) );
   if ( op == NULL ) {
      LOG( LOG_ERR, "Failed to allocate opinfo struct for operation start\n" );
      pthread_mutex_unlock( &(stlog->lock) );
      return -1;
   }
   op->type = type;
   op->start = 0;
   op->errval = errval;
   op->next = NULL;
   op->tgt  = strdup( target );
   if ( op->tgt == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate tgt string for op start: \"%s\"\n", target );
      pthread_mutex_unlock( &(stlog->lock) );
      free( op );
      return -1;
   }
   // process the new opinfo
   if ( processopinfo( statelog, op ) ) {
      LOG( LOG_ERR, "Failed to process operation on \"%s\" tgt\n", stlog->tgt );
      pthread_mutex_unlock( &(stlog->lock) );
      return -1;
   }
   else {
      stlog->outstandingcnt--;
   }
   if ( stlog->outstandingcnt == 0  &&  pthread_cond_signal( &(stlog->nooutstandingops) ) ) {
      LOG( LOG_ERR, "Failed to signal 'no outstanding ops' condition\n" );
      pthread_mutex_unlock( &(stlog->lock) );
      return -1;
   }
   pthread_mutex_unlock( &(stlog->lock) );
   return 0;
}

/**
 * Finalize a given statelog, but leave allocated ( saves time on future initializations )
 * NOTE -- this will wait until there are currently no ops in flight
 * @param STATELOG* statelog : Statelog to be finalized
 * @param operation_summary* summary : Reference to be populated with summary values ( ignored if NULL )
 * @param const char* log_preservation_tgt : FS location where the state logfile should be relocated to
 *                                           If NULL, the file is deleted
 * @return int : Zero on success, or -1 on failure
 */
int statelog_fin( STATELOG* statelog, operation_summary* summary, const char* log_preservation_tgt ) {
   // check for invalid args
   if ( statelog == NULL  ||  *statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   STATELOG stlog = *statelog;
   // acquire the statelog lock
   if ( pthread_mutex_lock( &(stlog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire statelog lock\n" );
      return -1;
   }
   // wait for all outstanding ops to complete
   while ( stlog->outstandingcnt ) {
      if ( pthread_cond_wait( &stlog->nooutstandingops, &stlog->lock ) ) {
         LOG( LOG_ERR, "Failed to wait for 'no outstanding ops' condition\n" );
         pthread_mutex_unlock( &(stlog->lock) );
         return -1;
      }
   }
   // potentially record summary info
   if ( summary ) { *summary = stlog->summary; }
   cleanuplog( stlog, 0 );
   return 0;
}

/**
 * Deallocate and finalize a given statelog
 * NOTE -- this will wait until there are currently no ops in flight
 * @param STATELOG* statelog : Statelog to be terminated
 * @param operation_summary* summary : Reference to be populated with summary values ( ignored if NULL )
 * @param const char* log_preservation_tgt : FS location where the state logfile should be relocated to
 *                                           If NULL, the file is deleted
 * @return int : Zero on success, or -1 on failure
 */
int statelog_term( STATELOG* statelog, operation_summary* summary, const char* log_preservation_tgt ) {
   // check for invalid args
   if ( statelog == NULL  ||  *statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   STATELOG stlog = *statelog;
   // acquire the statelog lock
   if ( pthread_mutex_lock( &(stlog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire statelog lock\n" );
      return -1;
   }
   // wait for all outstanding ops to complete
   while ( stlog->outstandingcnt ) {
      if ( pthread_cond_wait( &stlog->nooutstandingops, &stlog->lock ) ) {
         LOG( LOG_ERR, "Failed to wait for 'no outstanding ops' condition\n" );
         pthread_mutex_unlock( &(stlog->lock) );
         return -1;
      }
   }
   // potentially record summary info
   if ( summary ) { *summary = stlog->summary; }
   cleanuplog( stlog, 1 );
   *statelog = NULL;
   return 0;
}


