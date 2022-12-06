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
#define LOG_PREFIX "resourcelog"
#include <logging.h>

#include "resourcelog.h"

#include <pthread.h>

//   -------------   INTERNAL DEFINITIONS    -------------

#define MAX_BUFFER 8192 // maximum character buffer to be used for parsing/printing log lines
                        //    program will abort if limit is exceeded when reading or writing
#define RECORD_LOG_PREFIX "RESOURCE-RECORD-LOGFILE\n" // prefix for a 'record'-log
                                                      //    - only op starts, no completions
#define MODIFY_LOG_PREFIX "RESOURCE-MODIFY-LOGFILE\n" // prefix for a 'modify'-log
                                                      //    - mix of op starts and completions

typedef struct resourcelog_struct {
   // synchronization and access control
   pthread_mutex_t   lock;
   pthread_cond_t    nooutstanding;  // left NULL for a 'record' log
   ssize_t           outstandingcnt; // count of scanners still running ( threads that could potentially submit more ops )
                                     //  always zero for a 'record' log
   // state info
   resourcelog_type     type;
   operation_summary summary;
   HASH_TABLE        inprogress;  // left NULL for a 'record' log
   int               logfile;
   char*             logfilepath;
}*RESOURCELOG;

//   -------------   INTERNAL FUNCTIONS    -------------

/**
 * Clean up the provided resourcelog ( lock must be held )
 * @param RESOURCELOG rsrclog : Reference to the resourcelog to be cleaned
 * @param char destroy : If non-zero, the entire resourcelog structure will be freed
 *                       If zero, all state will be purged, but the struct can be reinitialized
 */
void cleanuplog( RESOURCELOG rsrclog, char destroy ) {
   HASH_NODE* nodelist = NULL;
   size_t index = 0;
   if ( rsrclog->inprogress ) {
      hash_term( rsrclog->inprogress, &nodelist, &index );
      while ( index ) {
         opinfo* opindex = (nodelist + index - 1)->content;
         resourcelog_freeopinfo( opindex );
         free( (nodelist + index - 1)->name );
         index--;
      }
      free( nodelist );
      rsrclog->inprogress = NULL;
   }
   if ( rsrclog->logfilepath ) { free( rsrclog->logfilepath ); }
   if ( rsrclog->logfile > 0 ) { close( rsrclog->logfile ); }
   if ( destroy ) {
      pthread_cond_destroy( &(rsrclog->nooutstanding) );
      pthread_mutex_unlock( &(rsrclog->lock) );
      pthread_mutex_destroy( &(rsrclog->lock) );
      free( rsrclog );
   }
   else {
      pthread_mutex_unlock( &(rsrclog->lock) );
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
         }
         else {
            LOG( LOG_ERR, "Hit mid-line EOF on logfile\n" );
            *eof = -1;
         }
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
   op->extendedinfo = NULL;
   op->start = 0;
   op->count = 0;
   op->errval = 0;
   op->next = NULL;
   op->ftag.ctag = NULL;
   op->ftag.streamid = NULL;
   // parse the op type and extended info
   char* parseloc = buffer;
   if ( strncmp( buffer, "DEL-OBJ ", 8 ) == 0 ) {
      LOG( LOG_INFO, "Parsing a MARFS_DELETE_OBJ operation\n" );
      op->type = MARFS_DELETE_OBJ_OP;
      parseloc += 8;
      // allocate delobj_info
      delobj_info* extinfo = malloc( sizeof( struct delobj_info_struct ) );
      if ( extinfo == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for DEL-OBJ extended info\n" );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      // parse in delobj_info
      if ( strncmp( parseloc, "{ ", 2 ) ) {
         LOG( LOG_ERR, "Missing '{ ' header for DEL-OBJ extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 2;
      char* endptr = NULL;
      unsigned long long parseval = strtoull( parseloc, &endptr, 10 );
      if ( endptr == NULL  ||  *endptr != ' ' ) {
         LOG( LOG_ERR, "DEL-OBJ extended info has unexpected char in prev_active_index string: '%c'\n", *endptr );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      extinfo->offset = (size_t)parseval;
      parseloc = endptr + 1;
      if ( strncmp( parseloc, "} ", 2 ) ) {
         LOG( LOG_ERR, "Missing '} ' tail for DEL-OBJ extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 2;
      // attach delobj_info
      op->extendedinfo = extinfo;
   }
   else if ( strncmp( buffer, "DEL-REF ", 8 ) == 0 ) {
      LOG( LOG_INFO, "Parsing a MARFS_DELETE_REF operation\n" );
      op->type = MARFS_DELETE_REF_OP;
      parseloc += 8;
      // allocate delref_info
      delref_info* extinfo = malloc( sizeof( struct delref_info_struct ) );
      if ( extinfo == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for DEL-REF extended info\n" );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      // parse in delref_info
      if ( strncmp( parseloc, "{ ", 2 ) ) {
         LOG( LOG_ERR, "Missing '{ ' header for DEL-REF extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 2;
      char* endptr = NULL;
      unsigned long long parseval = strtoull( parseloc, &endptr, 10 );
      if ( endptr == NULL  ||  *endptr != ' ' ) {
         LOG( LOG_ERR, "DEL-REF extended info has unexpected char in prev_active_index string: '%c'\n", *endptr );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      extinfo->prev_active_index = (size_t)parseval;
      parseloc = endptr + 1;
      if ( strncmp( parseloc, "DZ ", 3 ) == 0 ) {
         extinfo->delzero = 1;
      }
      else if ( strncmp( parseloc, "-- ", 3 ) == 0 ) {
         extinfo->delzero = 0;
      }
      else {
         LOG( LOG_ERR, "Encountered unrecognized DEL-ZERO value in DEL-REF extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 3;
      if ( strncmp( parseloc, "EOS", 3 ) == 0 ) {
         extinfo->eos = 1;
      }
      else if ( strncmp( parseloc, "CNT", 3 ) == 0 ) {
         extinfo->eos = 0;
      }
      else {
         LOG( LOG_ERR, "Encountered unrecognized EOS value in DEL-REF extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 3;
      if ( strncmp( parseloc, " } ", 3 ) ) {
         LOG( LOG_ERR, "Missing ' } ' tail for DEL-REF extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 3;
      // attach delref_info
      op->extendedinfo = extinfo;
   }
   else if ( strncmp( buffer, "REBUILD ", 8 ) == 0 ) {
      LOG( LOG_INFO, "Parsing a MARFS_REBUILD operation\n" );
      op->type = MARFS_REBUILD_OP;
      parseloc += 8;
      // parse in rebuild_info
      rebuild_info* extinfo = NULL;
      if ( strncmp( parseloc, "{ ", 2 ) == 0 ) {
         // allocate rebuild_info
         extinfo = malloc( sizeof( struct rebuild_info_struct ) );
         if ( extinfo == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for REBUILD extended info\n" );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         parseloc += 2;
         char* endptr = parseloc;
         while ( *endptr != ' '  &&  *endptr != '\n' ) { endptr++; }
         if ( *endptr != ' ' ) {
            LOG( LOG_ERR, "Failed to parse markerpath from REBUILD extended info\n" );
            free( extinfo );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         *endptr = '\0'; // temporarily truncate string
         extinfo->markerpath = strdup( parseloc );
         if ( extinfo->markerpath == NULL ) {
            LOG( LOG_ERR, "Failed to duplicate markerpath from REBUILD extended info\n" );
            free( extinfo );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         *endptr = ' ';
         parseloc = endptr + 1;
         endptr = NULL;
         unsigned long long parseval = strtoull( parseloc, &endptr, 10 );
         if ( endptr == NULL  ||  *endptr != ' ' ) {
            LOG( LOG_ERR, "REBUILD extended info has unexpected char in stripewidth string: '%c'\n", *endptr );
            free( extinfo );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         endptr++;
         parseloc = endptr;
         while ( *endptr != '\0'  &&  *endptr != '\n'  &&  *endptr != ' ' ) { endptr++; }
         if ( *endptr != ' ' ) {
            LOG( LOG_ERR, "Failed to identify end of rtag marker in REBUILD extended info string\n" );
            free( extinfo );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         extinfo->rtag.meta_status = calloc( (size_t)parseval, sizeof(char) );
         extinfo->rtag.data_status = calloc( (size_t)parseval, sizeof(char) );
         extinfo->rtag.csum = NULL;
         if ( extinfo->rtag.meta_status == NULL  ||  extinfo->rtag.data_status == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for REBUILD extended info meta/data_status arrays\n" );
            if ( extinfo->rtag.meta_status ) { free( extinfo->rtag.meta_status ); }
            if ( extinfo->rtag.data_status ) { free( extinfo->rtag.data_status ); }
            free( extinfo );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         *endptr = '\0'; // truncate string to make rtag parsing easier
         if ( rtag_initstr( &(extinfo->rtag), (size_t)parseval, parseloc ) ) {
            LOG( LOG_ERR, "Failed to parse rtag value of REBUILD extended info: \"%s\"\n", parseloc );
            free( extinfo->rtag.meta_status );
            free( extinfo->rtag.data_status );
            free( extinfo );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         *endptr = ' ';
         parseloc = endptr;
         if ( strncmp( parseloc, " } ", 3 ) ) {
            LOG( LOG_ERR, "Missing ' } ' tail for REBUILD extended info\n" );
            free( extinfo->rtag.meta_status );
            free( extinfo->rtag.data_status );
            free( extinfo );
            free( op );
            lseek( logfile, origoff, SEEK_SET );
            return NULL;
         }
         parseloc += 3;
      }
      else {
         LOG( LOG_INFO, "Rebuild op is lacking extended info\n" );
      }
      // attach rebuild_info
      op->extendedinfo = extinfo;
   }
   else if ( strncmp( buffer, "REPACK ", 7 ) == 0 ) {
      LOG( LOG_INFO, "Parsing a MARFS_REPACK operation\n" );
      op->type = MARFS_REPACK_OP;
      parseloc += 7;
      // allocate repack_info
      repack_info* extinfo = malloc( sizeof( struct repack_info_struct ) );
      if ( extinfo == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for REPACK extended info\n" );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      // parse in repack_info
      if ( strncmp( parseloc, "{ ", 2 ) ) {
         LOG( LOG_ERR, "Missing '{ ' header for REPACK extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 2;
      char* endptr = NULL;
      unsigned long long parseval = strtoull( parseloc, &endptr, 10 );
      if ( endptr == NULL  ||  *endptr != ' ' ) {
         LOG( LOG_ERR, "REPACK extended info has unexpected char in totalbytes string: '%c'\n", *endptr );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      extinfo->totalbytes = (size_t)parseval;
      parseloc = endptr;
      if ( strncmp( parseloc, " } ", 3 ) ) {
         LOG( LOG_ERR, "Missing ' } ' tail for REPACK extended info\n" );
         free( extinfo );
         free( op );
         lseek( logfile, origoff, SEEK_SET );
         return NULL;
      }
      parseloc += 3;
      // attach repack_info
      op->extendedinfo = extinfo;
   }
   else {
      LOG( LOG_ERR, "Unrecognized operation type value: \"%s\"\n", buffer );
      free( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   // parse the start value
   if ( *parseloc == 'S' ) {
      op->start = 1;
   }
   else if ( *parseloc != 'E' ) {
      LOG( LOG_ERR, "Unexpected START string value: '\%c'\n", *parseloc );
      resourcelog_freeopinfo( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   if ( *(parseloc + 1) != ' ' ) {
      LOG( LOG_ERR, "Unexpected trailing character after START value: '%c'\n", *(parseloc + 1) );
      resourcelog_freeopinfo( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   parseloc += 2;
   // parse the count value
   char* endptr = NULL;
   unsigned long long parseval = strtoull( parseloc, &endptr, 10 );
   if ( endptr == NULL  ||  *endptr != ' ' ) {
      LOG( LOG_ERR, "Failed to parse COUNT value with unexpected char: '%c'\n", *endptr );
      resourcelog_freeopinfo( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   op->count = (size_t)parseval;
   parseloc = endptr + 1;
   // parse the errno value
   long sparseval = strtol( parseloc, &endptr, 10 );
   if ( endptr == NULL  ||  *endptr != ' ' ) {
      LOG( LOG_ERR, "Failed to parse ERRNO value with unexpected char: '%c'\n", *endptr );
      resourcelog_freeopinfo( op );
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
         resourcelog_freeopinfo( op );
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
      resourcelog_freeopinfo( op );
      lseek( logfile, origoff, SEEK_SET );
      return NULL;
   }
   // finally, parse in any subsequent linked ops
   if ( nextval ) {
      // NOTE -- Recursive parsing isn't the most efficient approach.
      //         Simple though, and, once again, we don't expect logfile parsing to be a 
      //         significant performance consideration.
      LOG( LOG_INFO, "Recursively parsing subsequent operation\n" );
      op->next = parselogline( logfile, eof );
      if ( op->next == NULL ) {
         LOG( LOG_ERR, "Failed to parse linked operation\n" );
         resourcelog_freeopinfo( op );
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
   ssize_t usedbuff = 0;
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
         if ( op->extendedinfo ) {
            delobj_info* delobj = (delobj_info*)op->extendedinfo;
            ssize_t extinfoprint = snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "{ %zu } ",
                                             delobj->offset );
            if ( extinfoprint < 6 ) {
               LOG( LOG_ERR, "Failed to populate DEL-OBJ extended info string\n" );
               return -1;
            }
            usedbuff += extinfoprint;
         }
         break;
      case MARFS_DELETE_REF_OP:
         if ( snprintf( buffer, MAX_BUFFER, "%s ", "DEL-REF" ) != 8 ) {
            LOG( LOG_ERR, "Failed to populate 'DEL-REF' type string\n" );
            return -1;
         }
         usedbuff += 8;
         if ( op->extendedinfo ) {
            delref_info* delref = (delref_info*)op->extendedinfo;
            ssize_t extinfoprint = snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "{ %zu %s %s } ",
                                             delref->prev_active_index,
                                             (delref->delzero == 0 ) ? "--" : "DZ",
                                             (delref->eos == 0) ? "CNT" : "EOS" );
            if ( extinfoprint < 10 ) {
               LOG( LOG_ERR, "Failed to populate DEL-REF extended info string\n" );
               return -1;
            }
            usedbuff += extinfoprint;
         }
         break;
      case MARFS_REBUILD_OP:
         if ( snprintf( buffer, MAX_BUFFER, "%s ", "REBUILD" ) != 8 ) {
            LOG( LOG_ERR, "Failed to populate 'REBUILD' type string\n" );
            return -1;
         }
         usedbuff += 8;
         if ( op->extendedinfo ) {
            rebuild_info* rebuild = (rebuild_info*)op->extendedinfo;
            ssize_t extinfoprint = snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "{ %s %u ",
                                             rebuild->markerpath,
                                             op->ftag.protection.N + op->ftag.protection.E );
            if ( extinfoprint < 6 ) {
               LOG( LOG_ERR, "Failed to populate first portion of REBUILD extended info string\n" );
               return -1;
            }
            usedbuff += extinfoprint;
            if ( usedbuff >= MAX_BUFFER ) {
               LOG( LOG_ERR, "REBUILD Operation string exceeds memory allocation limits\n" );
               return -1;
            }
            size_t rtagprint = rtag_tostr( &(rebuild->rtag), op->ftag.protection.N + op->ftag.protection.E,
                                           buffer + usedbuff, MAX_BUFFER - usedbuff );
            if ( rtagprint < 1 ) {
               LOG( LOG_ERR, "Failed to populate REBUILD extended info rtag string\n" );
               return -1;
            }
            usedbuff += rtagprint;
            if ( snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, " } " ) != 3 ) {
               LOG( LOG_ERR, "Failed to print tail string of REBUILD extended info\n" );
               return -1;
            }
            usedbuff += 3;
         }
         break;
      case MARFS_REPACK_OP:
         if ( snprintf( buffer, MAX_BUFFER, "%s ", "REPACK" ) != 7 ) {
            LOG( LOG_ERR, "Failed to populate 'REPACK' type string\n" );
            return -1;
         }
         usedbuff += 7;
         if ( op->extendedinfo ) {
            repack_info* repack = (repack_info*)op->extendedinfo;
            ssize_t extinfoprint = snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "{ %zu } ",
                                             repack->totalbytes );
            if ( extinfoprint < 6 ) {
               LOG( LOG_ERR, "Failed to populate REPACK extended info string\n" );
               return -1;
            }
            usedbuff += extinfoprint;
         }
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
   if ( op->start ) {
      if ( snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "%c ", 'S' ) != 2 ) {
         LOG( LOG_ERR, "Failed to populate 'S' start flag string\n" );
         return -1;
      }
   }
   else if ( snprintf( buffer + usedbuff, MAX_BUFFER - usedbuff, "%c ", 'E' ) != 2 ) {
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
 * Incorporate the given opinfo string into the given resourcelog
 * @param RESOURCELOG rsrclog : resourcelog to be updated
 * @param opinfo* newop : Operation(s) to be included
 *                        NOTE -- This func will never free opinfo structs
 * @param char* progressop : Flag to be populated with the completion status of an operation set
 *                           If set to zero, the updated operation is still ongoing
 *                           If set to one, the updated operation has sucessfully completed
 *                           If set to negative one, the updated operation completed with errors
 * @return int : Zero on success, or -1 on failure
 */
int processopinfo( RESOURCELOG rsrclog, opinfo* newop, char* progressop, char* dofree ) {
   // identify the trailing op of the given chain
   opinfo* finop = newop;
   size_t oplength = 1;
   while ( finop->next ) {
      oplength++;
      finop = finop->next;
      if ( finop->start != newop->start ) {
         LOG( LOG_ERR, "Operation chain has inconsistent START value\n" );
         return -1;
      }
   }
   // map this operation into our inprogress hash table
   HASH_NODE* node = NULL;
   if ( hash_lookup( rsrclog->inprogress, newop->ftag.streamid, &node ) < 0 ) {
      LOG( LOG_ERR, "Failed to map operation on \"%s\" into inprogress HASH_TABLE\n", newop->ftag.streamid );
      return -1;
   }
   // traverse the attached operations, looking for a match
   char activealike = 0; // track if we have active ops of the same type in the same 'segment'
   opinfo* opindex = node->content;
   opinfo* prevop = NULL;
   while ( opindex != NULL ) {
      if ( strcmp( opindex->ftag.streamid, newop->ftag.streamid ) == 0 ) {
         // check for exact match ( type, fileno, objno )
         if ( (opindex->ftag.objno == newop->ftag.objno)  &&
              (opindex->ftag.fileno == newop->ftag.fileno)  &&
              (opindex->type == newop->type) ) {
            break;
         }
         else if ( opindex->type == newop->type ) {
            activealike = 1; // found an active op of the same type
         }
         else {
            activealike = 0; // op 'segement' is broken, reset
         }
      }
      else {
         activealike = 0; // op 'segement' is broken, reset
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
            // check for excessive count
            if ( parseop->count > parseindex->count ) {
               LOG( LOG_ERR, "Processed op count ( %zu ) exceeds expected count ( %zu )\n", parseop->count, parseindex->count );
               return -1;
            }
            // ...note each in our totals...
            switch( parseop->type ) {
               case MARFS_DELETE_OBJ_OP:
                  LOG( LOG_INFO, "Noted completion of a MARFS_DELETE_OBJ operation\n" );
                  rsrclog->summary.deletion_object_count += parseop->count;
                  if ( parseop->errval ) { rsrclog->summary.deletion_object_failures += parseop->count; }
                  break;
               case MARFS_DELETE_REF_OP:
                  LOG( LOG_INFO, "Noted completion of a MARFS_DELETE_REF operation\n" );
                  rsrclog->summary.deletion_reference_count += parseop->count;
                  if ( parseop->errval ) { rsrclog->summary.deletion_reference_failures += parseop->count; }
                  break;
               case MARFS_REBUILD_OP:
                  LOG( LOG_INFO, "Noted completion of a MARFS_REBUILD operation\n" );
                  rsrclog->summary.rebuild_count += parseop->count;
                  if ( parseop->errval ) { rsrclog->summary.rebuild_failures += parseop->count; }
                  break;
               case MARFS_REPACK_OP:
                  LOG( LOG_INFO, "Noted completion of a MARFS_REPACK operation\n" );
                  rsrclog->summary.repack_count += parseop->count;
                  if ( parseop->errval ) { rsrclog->summary.repack_failures += parseop->count; }
                  break;
               default:
                  LOG( LOG_ERR, "Unrecognized operation type value\n" );
                  return -1;
            }
            if ( parseop->errval ) { parseindex->errval = parseop->errval; } // potentially note an error
            // check for completion of the specified op
            if ( parseindex->count != parseop->count ) {
               // first operation is not complete, so cannot progress op chain
               parseindex->count -= parseop->count; // decrement count
               if ( progressop ) { *progressop = 0; }
               *dofree = 1; // not incorporating these ops, so the caller should free
               return 0;
            }
            // potentially check for subsequent active op in the same segment
            if ( parseop->next == NULL  &&  parseindex->next  &&
                 strcmp( parseop->ftag.streamid, parseindex->next->ftag.streamid ) == 0  &&
                 parseop->type == parseindex->next->type ) { activealike = 1; }
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
            else if ( progressop ) {
               if ( activealike ) { *progressop = 0; } // do not progress if alike ops remain in a single 'segment'
               else if ( previndex->errval == 0 ) { *progressop = 1; } // corresponding ops were completed without error
               else { *progressop = -1; } // note that the corresponding ops were completed with errors
            }
            // decrement in-progress cnt
            rsrclog->outstandingcnt--;
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
         // free the removed operation chain
         previndex->next = NULL; // so we don't end up freeing any predecessors
         resourcelog_freeopinfo( opindex );
      }
      // a matching op means the parsed operation can be discarded
      // tell the caller to free their own op chain
      *dofree = 1;
   }
   else {
      // should indicate the start of a new operation
      if ( newop->start == 0 ) {
         LOG( LOG_ERR, "Parsed completion of op from logfile \"%s\" with no parsed start of op\n", rsrclog->logfilepath );
         return -1;
      }
      if ( rsrclog->type == RESOURCE_MODIFY_LOG ) {
         // stitch the new op onto the front of our inprogress list
         finop->next = node->content;
         node->content = newop;
         // note that we have another op in flight
         rsrclog->outstandingcnt += oplength;
         // tell the caller NOT to free this chain
         *dofree = 0;
      }
      else { // for RECORD logs, note all ops in our totals
         opinfo* parseop = newop; // new op being added to totals
         while ( parseop ) {
            // ...note each in our totals...
            switch( parseop->type ) {
               case MARFS_DELETE_OBJ_OP:
                  rsrclog->summary.deletion_object_count += parseop->count;
                  LOG( LOG_INFO, "Recorded MARFS_DELETE_OBJ operation\n" );
                  break;
               case MARFS_DELETE_REF_OP:
                  rsrclog->summary.deletion_reference_count += parseop->count;
                  LOG( LOG_INFO, "Recorded MARFS_DELETE_REF operation\n" );
                  break;
               case MARFS_REBUILD_OP:
                  rsrclog->summary.rebuild_count += parseop->count;
                  LOG( LOG_INFO, "Recorded MARFS_REBUILD operation\n" );
                  break;
               case MARFS_REPACK_OP:
                  rsrclog->summary.repack_count += parseop->count;
                  LOG( LOG_INFO, "Recorded MARFS_REPACK operation\n" );
                  break;
               default:
                  LOG( LOG_ERR, "Unrecognized operation type value\n" );
                  return -1;
            }
            parseop = parseop->next;
         }
         // tell the caller to free their own op chain
         *dofree = 1;
      }
   }
   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Free the given opinfo struct chain
 * @param opinfo* op : Reference to the opinfo chain to be freed
 */
void resourcelog_freeopinfo( opinfo* op ) {
   char* prevctag = NULL;
   char* prevstreamid = NULL;
   while ( op ) {
      if ( op->extendedinfo ) {
         switch ( op->type ) { // only REBUILD and DEL-REF ops should have extended info at all
            case MARFS_REBUILD_OP:
               {
               rebuild_info* extinfo = (rebuild_info*)op->extendedinfo;
               if ( extinfo->markerpath ) { free( extinfo->markerpath ); }
               if ( extinfo->rtag.meta_status ) { free( extinfo->rtag.meta_status ); }
               if ( extinfo->rtag.data_status ) { free( extinfo->rtag.data_status ); }
               break;
               }
            default: // nothing extra to be done for other ops
               break;
         }
         free( op->extendedinfo );
      }
      // free ctag+streamid only if they are not simply duplicated references throughout the op chain
      // NOTE -- This won't catch if non-neighbor ops have the same string values, but I am considering that a degenerate case.
      //         Behavior would be a double free by this func.
      if ( op->ftag.ctag  &&  op->ftag.ctag != prevctag ) { prevctag = op->ftag.ctag; free( op->ftag.ctag ); }
      if ( op->ftag.streamid  &&  op->ftag.streamid != prevstreamid ) { prevstreamid = op->ftag.streamid; free( op->ftag.streamid ); }
      opinfo* nextop = op->next;
      free( op );
      op = nextop;
   }
}

/**
 * Duplicate the given opinfo struct chain
 * @param opinfo* op : Reference to the opinfo chain to be duplicated
 * @return opinfo* : Reference to the newly created duplicate chain
 */
opinfo* resourcelog_dupopinfo( opinfo* op ) {
   // validate arg
   if ( op == NULL ) {
      LOG( LOG_ERR, "Received a NULL opinfo reference\n" );
      return NULL;
   }
   // parse over the op chain
   opinfo* genchain = NULL;
   opinfo** newop = &(genchain);
   while ( op ) {
      // allocate a new op struct
      *newop = malloc( sizeof( struct opinfo_struct ) );
      if ( *newop == NULL ) {
         LOG( LOG_ERR, "Failed to allocate new operation info struct\n" );
         resourcelog_freeopinfo( genchain );
         return NULL;
      }
      **newop = *op; // just duplicate op values, to start
      (*newop)->ftag.ctag = NULL;
      (*newop)->ftag.streamid = NULL;
      (*newop)->extendedinfo = NULL; // always NULL these out, just in case
      // allocate new ctag/streamid
      (*newop)->ftag.ctag = strdup( op->ftag.ctag );
      if ( (*newop)->ftag.ctag == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate CTAG string\n" );
         resourcelog_freeopinfo( genchain );
         return NULL;
      }
      (*newop)->ftag.streamid = strdup( op->ftag.streamid );
      if ( (*newop)->ftag.streamid == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate StreamID string\n" );
         resourcelog_freeopinfo( genchain );
         return NULL;
      }
      // potentially populate extended info
      if ( op->extendedinfo ) {
         switch ( op->type ) { // only REBUILD and DEL-REF ops should have extended info at all
            case MARFS_DELETE_OBJ_OP:
               {
                  delobj_info* extinfo = (delobj_info*)op->extendedinfo;
                  delobj_info* newinfo = malloc( sizeof( struct delobj_info_struct ) );
                  if ( newinfo == NULL ) {
                     LOG( LOG_ERR, "Failed to allocate delobj extended info\n" );
                     resourcelog_freeopinfo( genchain );
                     return NULL;
                  }
                  *newinfo = *extinfo;
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            case MARFS_DELETE_REF_OP:
               {
                  delref_info* extinfo = (delref_info*)op->extendedinfo;
                  delref_info* newinfo = malloc( sizeof( struct delref_info_struct ) );
                  if ( newinfo == NULL ) {
                     LOG( LOG_ERR, "Failed to allocate delref extended info\n" );
                     resourcelog_freeopinfo( genchain );
                     return NULL;
                  }
                  *newinfo = *extinfo;
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            case MARFS_REBUILD_OP:
               {
                  rebuild_info* extinfo = (rebuild_info*)op->extendedinfo;
                  rebuild_info* newinfo = NULL;
                  // must allow for rebuild ops without extended info ( rebuild by location case )
                  if ( extinfo ) {
                     // duplicate extended info
                     newinfo = malloc( sizeof( struct rebuild_info_struct ) );
                     if ( newinfo == NULL ) {
                        LOG( LOG_ERR, "Failed to allocate rebuild extended info\n" );
                        resourcelog_freeopinfo( genchain );
                        return NULL;
                     }
                     newinfo->markerpath = strdup( extinfo->markerpath );
                     if ( newinfo->markerpath == NULL ) {
                        LOG( LOG_ERR, "Failed to duplicate rebuild marker path\n" );
                        free( newinfo );
                        resourcelog_freeopinfo( genchain );
                        return NULL;
                     }
                     newinfo->rtag = extinfo->rtag;
                     newinfo->rtag.meta_status = NULL;
                     newinfo->rtag.data_status = NULL;
                     newinfo->rtag.csum = NULL;
                     size_t stripewidth = op->ftag.protection.N + op->ftag.protection.E;
                     if ( extinfo->rtag.meta_status ) {
                        newinfo->rtag.meta_status = calloc( stripewidth, sizeof(char) );
                        if ( newinfo->rtag.meta_status == NULL ) {
                           LOG( LOG_ERR, "Failed to allocate meta_status array of length %zu\n", stripewidth );
                           free( newinfo->markerpath );
                           free( newinfo );
                           resourcelog_freeopinfo( genchain );
                           return NULL;
                        }
                        memcpy( newinfo->rtag.meta_status, extinfo->rtag.meta_status, sizeof(char) * stripewidth );
                     }
                     if ( extinfo->rtag.data_status ) {
                        newinfo->rtag.data_status = calloc( stripewidth, sizeof(char) );
                        if ( newinfo->rtag.data_status == NULL ) {
                           LOG( LOG_ERR, "Failed to allocate data_status array of length %zu\n", stripewidth );
                           if ( newinfo->rtag.meta_status ) { free( newinfo->rtag.meta_status ); }
                           free( newinfo->markerpath );
                           free( newinfo );
                           resourcelog_freeopinfo( genchain );
                           return NULL;
                        }
                        memcpy( newinfo->rtag.data_status, extinfo->rtag.data_status, sizeof(char) * stripewidth );
                     }
                  }
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            case MARFS_REPACK_OP:
               {
                  repack_info* extinfo = (repack_info*)op->extendedinfo;
                  repack_info* newinfo = malloc( sizeof( struct repack_info_struct ) );
                  if ( newinfo == NULL ) {
                     LOG( LOG_ERR, "Failed to allocate repack extended info\n" );
                     resourcelog_freeopinfo( genchain );
                     return NULL;
                  }
                  *newinfo = *extinfo;
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            default: // nothing extra to be done for other ops
               LOG( LOG_ERR, "Unrecognized operation type with extended info value\n" );
               resourcelog_freeopinfo( genchain );
               return NULL;
         }
      }
      LOG( LOG_INFO, "Successfully duplicated operation\n" );
      op = op->next;
      newop = &( (*newop)->next );
   }
   return genchain;
}

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
char* resourcelog_genlogpath( char create, const char* logroot, const char* iteration, marfs_ns* ns, ssize_t ranknum ) {
   // check for invalid args
   if ( logroot == NULL ) {
      LOG( LOG_ERR, "Received a NULL logroot value\n" );
      errno = EINVAL;
      return NULL;
   }
   // if we have a NS, identify an appropriate FS path
   char* nspath = NULL;
   if ( ns ) {
      nspath = strdup( ns->idstr );
      char* tmpparse = nspath;
      while ( *tmpparse != '\0' ) {
         if ( *tmpparse == '/' ) { *tmpparse = '#'; }
         tmpparse++;
      }
   }
   // identify length of the constructed path
   ssize_t pathlen = 0;
   if ( iteration  &&  nspath  &&  ranknum >= 0 ) {
      pathlen = snprintf( NULL, 0, "%s/%s/%s/resourcelog-%zu", logroot, iteration, nspath, ranknum );
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
   if ( lrootlen < 1  ||  lrootlen > pathlen ) {
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
   if ( iterlen < 1  ||  iterlen > (pathlen - lrootlen) ) {
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
   if ( nslen < 1  ||  nslen > ((pathlen - lrootlen) - iterlen) ) {
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
   if ( snprintf( path + lrootlen + iterlen + nslen, (pathlen - (lrootlen + iterlen + nslen)) + 1, "/resourcelog-%zu", ranknum ) !=  (pathlen - (lrootlen + iterlen + nslen)) ) {
      LOG( LOG_ERR, "Logfile path has inconsistent length\n" );
      free( path );
      return NULL;
   }
   return path;
}

/**
 * Initialize a resourcelog, associated with the given logging root, namespace, and rank
 * @param RESOURCELOG* resourcelog : Statelog to be initialized
 *                             NOTE -- This can either be a NULL value, or a resourcelog which was 
 *                                     previously terminated / finalized
 * @param const char* logpath : Location of the resourcelog file
 * @param resourcelog_type type : Type of resourcelog to open
 * @param marfs_ns* ns : MarFS NS being operated upon
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_init( RESOURCELOG* resourcelog, const char* logpath, resourcelog_type type, marfs_ns* ns ) {
   // check for invalid args
   if ( resourcelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( type != RESOURCE_RECORD_LOG  &&  type != RESOURCE_MODIFY_LOG  &&  type != RESOURCE_READ_LOG ) {
      LOG( LOG_ERR, "Unknown resourcelog type value\n" );
      errno = EINVAL;
      return -1;
   }
   if ( type != RESOURCE_READ_LOG  &&  ns == NULL ) {
      LOG( LOG_ERR, "Recieved a NULL 'ns' arg for a non-read log\n" );
      errno = EINVAL;
      return -1;
   }
   if ( logpath == NULL ) {
      LOG( LOG_ERR, "Received a NULL logpath value\n" );
      errno = EINVAL;
      return -1;
   }
   // identify our actual resourcelog
   RESOURCELOG rsrclog = *resourcelog;
   // allocate a new resourcelog
   rsrclog = malloc( sizeof( struct resourcelog_struct ) );
   if ( rsrclog == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new resourcelog\n" );
      return -1;
   }
   if ( pthread_mutex_init( &(rsrclog->lock), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize lock on new resourcelog struct\n" );
      free( rsrclog );
      return -1;
   }
   if ( pthread_cond_init( &(rsrclog->nooutstanding), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize condition on new resourcelog struct\n" );
      pthread_mutex_destroy( &(rsrclog->lock) );
      free( rsrclog );
      return -1;
   }
   if ( pthread_mutex_lock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire new resourcelog lock\n" );
      pthread_cond_destroy( &(rsrclog->nooutstanding) );
      pthread_mutex_destroy( &(rsrclog->lock) );
      free( rsrclog );
      return -1;
   }
   rsrclog->outstandingcnt = 0;
   rsrclog->type = type; // may be updated later
   bzero( &(rsrclog->summary), sizeof( struct operation_summary_struct ) );
   rsrclog->inprogress = NULL;
   rsrclog->logfile = -1;
   rsrclog->logfilepath = NULL;
   // initialize our logging path
   rsrclog->logfilepath = strdup( logpath );
   if ( rsrclog->logfilepath == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate logfile path: \"%s\"\n", logpath );
      pthread_mutex_unlock( &(rsrclog->lock) );
      pthread_cond_destroy( &(rsrclog->nooutstanding) );
      pthread_mutex_destroy( &(rsrclog->lock) );
      free( rsrclog );
      return -1;
   }
   // open our logfile
   int openmode = O_CREAT | O_EXCL | O_WRONLY;
   if ( type == RESOURCE_READ_LOG ) { openmode = O_RDONLY; }
   rsrclog->logfile = open( rsrclog->logfilepath, openmode, 0600 );
   if ( rsrclog->logfile < 0 ) {
      LOG( LOG_ERR, "Failed to open resourcelog: \"%s\"\n", rsrclog->logfilepath );
      cleanuplog( rsrclog, 1 );
      return -1;
   }
   // when reading an existing logfile, behavior is significantly different
   if ( type == RESOURCE_READ_LOG ) {
      // read in the header value of an existing log file
      char buffer[128] = {0};
      char recordshortest;
      ssize_t shortestprefx;
      ssize_t extraread;
      if ( strlen( RECORD_LOG_PREFIX ) < strlen( MODIFY_LOG_PREFIX ) ) {
         recordshortest = 1;
         shortestprefx = strlen( RECORD_LOG_PREFIX );
         extraread = strlen( MODIFY_LOG_PREFIX ) - shortestprefx;
      }
      else {
         recordshortest = 0;
         shortestprefx = strlen( MODIFY_LOG_PREFIX );
         extraread = strlen( RECORD_LOG_PREFIX ) - shortestprefx;
      }
      if ( shortestprefx + extraread >= 128 ) {
         LOG( LOG_ERR, "Logfile header strings exceed memory allocation!\n" );
         cleanuplog( rsrclog, 1 );
         return -1;
      }
      if ( read( rsrclog->logfile, buffer, shortestprefx ) != shortestprefx ) {
         LOG( LOG_ERR, "Failed to read prefix string of length %zd from logfile: \"%s\"\n",
                       shortestprefx, rsrclog->logfilepath );
         cleanuplog( rsrclog, 1 );
         return -1;
      }
      // string comparison, accounting for possible variety of header length, is a bit complex
      if ( recordshortest ) {
         // check if this is a RECORD log first
         if ( strncmp( buffer, RECORD_LOG_PREFIX, shortestprefx ) ) {
            // not a RECORD log, so read in extra MODIFY prefix chars, if necessary
            if ( extraread > 0  &&  read( rsrclog->logfile, buffer + shortestprefx, extraread ) != extraread ) {
               LOG( LOG_ERR, "Failed to read in trailing chars of MODIFY prefix\n" );
               cleanuplog( rsrclog, 1 );
               return -1;
            }
            // check for MODIFY prefix
            if ( strncmp( buffer, MODIFY_LOG_PREFIX, shortestprefx + extraread ) ) {
               LOG( LOG_ERR, "Failed to identify header prefix of logfile: \"%s\"\n", rsrclog->logfilepath );
               cleanuplog( rsrclog, 1 );
               return -1;
            }
            // we are reading a MODIFY log
            LOG( LOG_INFO, "Identified as a MODIFY log source: \"%s\"\n", rsrclog->logfilepath );
            rsrclog->type = RESOURCE_MODIFY_LOG | RESOURCE_READ_LOG;
         }
         else {
            // we are reading a RECORD log
            LOG( LOG_INFO, "Identified as a RECORD log source: \"%s\"\n", rsrclog->logfilepath );
            rsrclog->type = RESOURCE_RECORD_LOG | RESOURCE_READ_LOG;
         }
      }
      else {
         // check if this is a MODIFY log first
         if ( strncmp( buffer, MODIFY_LOG_PREFIX, shortestprefx ) ) {
            // not a MODIFY log, so read in extra RECORD prefix chars, if necessary
            if ( extraread > 0  &&  read( rsrclog->logfile, buffer + shortestprefx, extraread ) != extraread ) {
               LOG( LOG_ERR, "Failed to read in trailing chars of RECORD prefix\n" );
               cleanuplog( rsrclog, 1 );
               return -1;
            }
            // check for RECORD prefix
            if ( strncmp( buffer, RECORD_LOG_PREFIX, shortestprefx + extraread ) ) {
               LOG( LOG_ERR, "Failed to identify header prefix of logfile: \"%s\"\n", rsrclog->logfilepath );
               cleanuplog( rsrclog, 1 );
               return -1;
            }
            // we are reading a RECORD log
            LOG( LOG_INFO, "Identified as a RECORD log source: \"%s\"\n", rsrclog->logfilepath );
            rsrclog->type = RESOURCE_RECORD_LOG | RESOURCE_READ_LOG;
         }
         else {
            // we are reading a MODIFY log
            LOG( LOG_INFO, "Identified as a MODIFY log source: \"%s\"\n", rsrclog->logfilepath );
            rsrclog->type = RESOURCE_MODIFY_LOG | RESOURCE_READ_LOG;
         }
      }
      // when reading a log, we can exit early
      if ( pthread_mutex_unlock( &(rsrclog->lock) ) ) {
         LOG( LOG_ERR, "Failed to relinquish resourcelog lock\n" );
         cleanuplog( rsrclog, 1 );
         return -1;
      }
      *resourcelog = rsrclog;
      return 0;
   }
   // write out our log prefix
   if ( rsrclog->type == RESOURCE_MODIFY_LOG ) {
      if ( write( rsrclog->logfile, MODIFY_LOG_PREFIX, strlen( MODIFY_LOG_PREFIX ) ) !=
            strlen( MODIFY_LOG_PREFIX ) ) {
         LOG( LOG_ERR, "Failed to write out MODIFY log header to new logfile\n" );
         cleanuplog( rsrclog, 1 );
         return -1;
      }
   }
   else {
      if ( write( rsrclog->logfile, RECORD_LOG_PREFIX, strlen( RECORD_LOG_PREFIX ) ) !=
            strlen( RECORD_LOG_PREFIX ) ) {
         LOG( LOG_ERR, "Failed to write out RECORD log header to new logfile\n" );
         cleanuplog( rsrclog, 1 );
         return -1;
      }
   }
   // initialize our HASH_TABLE
   HASH_NODE* nodelist = malloc( sizeof(HASH_NODE) * ns->prepo->metascheme.refnodecount );
   if ( nodelist == NULL ) {
      LOG( LOG_ERR, "Failed to allocate nodelist for in progress hash table\n" );
      cleanuplog( rsrclog, 1 );
      return -1;
   }
   size_t index = 0;
   for ( ; index < ns->prepo->metascheme.refnodecount; index++ ) {
      // initialize node list to mirror the reference nodes
      (nodelist + index)->content = NULL;
      (nodelist + index)->weight = (ns->prepo->metascheme.refnodes + index)->weight;
      (nodelist + index)->name = strdup( (ns->prepo->metascheme.refnodes + index)->name );
      if ( (nodelist + index)->name == NULL ) {
         LOG( LOG_ERR, "Failed to allocate name of node %zu\n", index );
         while ( index ) {
            free( (nodelist + index - 1)->name );
            index--;
         }
         free( nodelist );
         cleanuplog( rsrclog, 1 );
         return -1;
      }
   }
   rsrclog->inprogress = hash_init( nodelist, ns->prepo->metascheme.refnodecount, 0 );
   if ( rsrclog->inprogress == NULL ) {
      LOG( LOG_ERR, "Failed to initialize inprogress hash table\n" );
      while ( index ) {
         free( (nodelist + index - 1)->name );
         index--;
      }
      free( nodelist );
      cleanuplog( rsrclog, 1 );
      return -1;
   }

//
//   // TODO : This code no longer belongs here, but might be useful elsewhere
//   // parse over logfile entries
//   char eof = 0;
//   opinfo* parsedop = NULL;
//   while ( (parsedop = parselogline( rsrclog->logfile, &eof )) != NULL ) {
//      // map this operation into our inprogress hash table
//      if ( processopinfo( rsrclog, parsedop ) < 0 ) {
//         LOG( LOG_ERR, "Failed to process lines from logfile: \"%s\"\n", rsrclog->logfilepath );
//         cleanuplog( rsrclog, 1 );
//         return -1;
//      }
//   }
//   if ( eof == 0 ) {
//      LOG( LOG_ERR, "Failed to parse existing logfile: \"%s\"\n", rsrclog->logfilepath );
//      cleanuplog( rsrclog, 1 );
//      return -1;
//   }
//   // potentially integrate info from any pre-existing logfiles
//   if ( cleanup ) {
//      // identify and open our parent dir
//      char* parentparse = rsrclog->logfilepath;
//      char* prevent = NULL;
//      for ( ; *parentparse != '\0'; parentparse++ ) {
//         if ( *parentparse == '/' ) { prevent = parentparse; } // update previous dir sep
//      }
//      if ( prevent == NULL ) {
//         LOG( LOG_ERR, "Failed to identify parent dir of logfile path: \"%s\"\n", rsrclog->logfilepath );
//         cleanuplog( rsrclog, 1 );
//         return -1;
//      }
//      *prevent = '\0'; // insert a NULL, to allow us to open the parent dir
//      DIR* parentdir = opendir( rsrclog->logfilepath );
//      *prevent = '/';
//      if ( parentdir == NULL ) {
//         LOG( LOG_ERR, "Failed to open parent dir of logfile: \"%s\"\n", rsrclog->logfilepath );
//         cleanuplog( rsrclog, 1 );
//         return -1;
//      }
//      // readdir to identify all lingering logfiles
//      int olderr = errno;
//      errno = 0;
//      struct dirent* entry = NULL;
//      while ( (entry = readdir( parentdir )) != NULL ) {
//         if ( strcmp( prevent + 1, entry->name ) ) {
//            // open and parse all old logfiles under the same dir
//            LOG( LOG_INFO, "Attempting cleanup of existing logfile: \"%s\"\n", entry->name );
//            int oldlog = openat( dirfd(parentdir), entry->name, O_RDONLY );
//            if ( oldlog < 0 ) {
//               LOG( LOG_ERR, "Failed to open existing logfile for cleanup: \"%s\"\n", entry->name );
//               closedir( parentdir );
//               cleanuplog( rsrclog, 1 );
//               return -1;
//            }
//            while ( (parsedop = parselogline( oldlog, &eof )) != NULL ) {
//               // duplicate this op into our initial logfile
//               if ( printlogline( rsrclog->logfile, parsedop ) ) {
//                  LOG( LOG_ERR, "Failed to duplicate op from old logfile \"%s\" into active log: \"%s\"\n",
//                       entry->name, rsrclog->logfilepath );
//                  close( oldlog );
//                  closedir( parentdir );
//                  cleanuplog( rsrclog, 1 );
//                  return -1;
//               }
//               // map any ops to our hash table
//               if ( processopinfo( rsrclog, parsedop ) < 0 ) {
//                  LOG( LOG_ERR, "Failed to process lines from old logfile: \"%s\"\n", entry->name );
//                  close( oldlog );
//                  closedir( parentdir );
//                  cleanuplog( rsrclog, 1 );
//                  return -1;
//               }
//            }
//            if ( eof == 0 ) {
//               LOG( LOG_ERR, "Failed to parse old logfile: \"%s\"\n", entry->name );
//               close( oldlog );
//               closedir( parentdir );
//               cleanuplog( rsrclog, 1 );
//               return -1;
//            }
//            close( oldlog );
//            // delete the old logfile
//            if ( unlinkat( dirfd(parentdir), entry->name ) ) {
//               LOG( LOG_ERR, "Failed to unlink old logfile: \"%s\"\n", entry->name );
//               closedir( parentdir );
//               cleanuplog( rsrclog, 1 );
//               return -1;
//            }
//         }
//      }
//      closedir( parentdir );
//   }




   // finally done
   if ( pthread_mutex_unlock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to relinquish resourcelog lock\n" );
      cleanuplog( rsrclog, 1 );
      return -1;
   }
   *resourcelog = rsrclog;
   return 0; 
}

/**
 * Replay all operations from a given inputlog ( reading from a MODIFY log ) into a given 
 *  outputlog ( writing to a MODIFY log ), then delete and terminate the inputlog
 * NOTE -- This function is intended for picking up state from a previously aborted run.
 * @param RESOURCELOG* inputlog : Source inputlog to be read from
 * @param RESOURCELOG* outputlog : Destination outputlog to be written to
 * @param int (*filter)( const opinfo* op ) : Function pointer defining an operation filter ( ignored if NULL )
 *                                            *param const opinfo* : Reference to the op to potentially include
 *                                            *return int : Zero if the op should be included, non-zero if not
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_replay( RESOURCELOG* inputlog, RESOURCELOG* outputlog, int (*filter)( const opinfo* op ) ) {
   // check for invalid args
   if ( inputlog == NULL  ||  outputlog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( *inputlog == NULL  ||  *outputlog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog\n" );
      errno = EINVAL;
      return -1;
   }
   if ( !((*inputlog)->type & RESOURCE_READ_LOG) ) {
      LOG( LOG_ERR, "Invalid input resourcelog type values\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCELOG inrsrclog = *inputlog;
   RESOURCELOG outrsrclog = *outputlog;
   // acquire both resourcelog locks
   if ( pthread_mutex_lock( &(inrsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire input resourcelog lock\n" );
      return -1;
   }
   if ( pthread_mutex_lock( &(outrsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire output resourcelog lock\n" );
      pthread_mutex_unlock( &(inrsrclog->lock) );
      return -1;
   }
   // process all entries from the current resourcelog
   size_t opcnt = 0;
   opinfo* parsedop = NULL;
   char eof = 0;
   while ( (parsedop = parselogline( inrsrclog->logfile, &eof )) != NULL ) {
      // duplicate the parsed op ( for printing )
      opinfo* dupop = resourcelog_dupopinfo( parsedop );
      if ( dupop == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate parsed operation chain\n" );
         pthread_mutex_unlock( &(inrsrclog->lock) );
         pthread_mutex_unlock( &(outrsrclog->lock) );
         resourcelog_freeopinfo( parsedop );
         return -1;
      }
      char dofree = 1;
      // check our filter
      if ( filter == NULL  ||  filter( dupop ) == 0 ) {
         // incorporate the op into our current state
         if ( (outrsrclog->type & ~(RESOURCE_READ_LOG)) == RESOURCE_MODIFY_LOG ) {
            dofree = 0;
            if ( processopinfo( outrsrclog, parsedop, NULL, &(dofree) ) < 0 ) {
               LOG( LOG_ERR, "Failed to process lines from old logfile: \"%s\"\n", inrsrclog->logfilepath );
               pthread_mutex_unlock( &(inrsrclog->lock) );
               pthread_mutex_unlock( &(outrsrclog->lock) );
               resourcelog_freeopinfo( dupop );
               resourcelog_freeopinfo( parsedop );
               return -1;
            }
         }
         // duplicate this op into our output logfile ( must use duplicate, as parsedop->next may be modified )
         if ( printlogline( outrsrclog->logfile, dupop ) ) {
            LOG( LOG_ERR, "Failed to duplicate op from input logfile \"%s\" into active log: \"%s\"\n",
                 inrsrclog->logfilepath, outrsrclog->logfilepath );
            pthread_mutex_unlock( &(inrsrclog->lock) );
            pthread_mutex_unlock( &(outrsrclog->lock) );
            resourcelog_freeopinfo( dupop );
            if ( dofree )
               resourcelog_freeopinfo( parsedop );
            return -1;
         }
      }
      resourcelog_freeopinfo( dupop );
      if ( dofree )
         resourcelog_freeopinfo( parsedop );
      opcnt++;
   }
   if ( eof != 1 ) {
      LOG( LOG_ERR, "Failed to parse input logfile: \"%s\"\n", inrsrclog->logfilepath );
      pthread_mutex_unlock( &(inrsrclog->lock) );
      pthread_mutex_unlock( &(outrsrclog->lock) );
      return -1;
   }
   LOG( LOG_INFO, "Replayed %zu ops from input log ( \"%s\" ) into output log ( \"%s\" )\n",
                  opcnt, inrsrclog->logfilepath, outrsrclog->logfilepath );
   // cleanup the inputlog
   *inputlog = NULL;
   pthread_mutex_unlock( &(inrsrclog->lock) );
   if ( resourcelog_term( &(inrsrclog), NULL, 1 ) ) {
      LOG( LOG_ERR, "Failed to cleanup input logfile\n" );
      pthread_mutex_unlock( &(outrsrclog->lock) );
      return -1;
   }
   if ( outrsrclog->outstandingcnt == 0 ) {
      // potentially signal that the output log has quiesced
      pthread_cond_signal( &(outrsrclog->nooutstanding) );
   }
   pthread_mutex_unlock( &(outrsrclog->lock) );
   return 0;
}

/**
 * Record that a certain number of threads are currently processing
 * @param RESOURCELOG* resourcelog : Statelog to be updated
 * @param size_t numops : Number of additional processors ( can be negative to reduce cnt )
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_update_inflight( RESOURCELOG* resourcelog, ssize_t numops ) {
   // check for invalid args
   if ( resourcelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCELOG rsrclog = *resourcelog;
   if ( rsrclog == NULL  ||  rsrclog->logfile < 1 ) {
      LOG( LOG_ERR, "Received an uninitialized resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   // acquire resourcelog lock
   if ( pthread_mutex_lock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourcelog lock\n" );
      return -1;
   }
   // check for excessive reduction
   if ( rsrclog->outstandingcnt < -(numops) ) {
      LOG( LOG_WARNING, "Value of %zd would result in negative thread count\n", numops );
      numops = -(rsrclog->outstandingcnt);
   }
   // modify count
   rsrclog->outstandingcnt += numops;
   // check for quiesced state
   if ( rsrclog->outstandingcnt == 0  &&  pthread_cond_signal( &(rsrclog->nooutstanding) ) ) {
      LOG( LOG_ERR, "Failed to signal 'no outstanding ops' condition\n" );
      pthread_mutex_unlock( &(rsrclog->lock) );
      return -1;
   }
   if ( pthread_mutex_unlock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to release resourcelog lock\n" );
      return -1;
   }
   return 0;
}

/**
 * Process the given operation
 * @param RESOURCELOG* resourcelog : Statelog to update ( must be writing to this resourcelog )
 * @param opinfo* op : Operation ( or op sequence ) to process
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_processop( RESOURCELOG* resourcelog, opinfo* op, char* progress ) {
   // check for invalid args
   if ( resourcelog == NULL  ||  *resourcelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( (*resourcelog)->type & RESOURCE_READ_LOG ) {
      LOG( LOG_ERR, "Cannot update a reading resourcelog\n" );
      errno = EINVAL;
      return -1;
   }
   if ( op == NULL ) {
      LOG( LOG_ERR, "Received a NULL op reference\n" );
      errno = EINVAL;
      return -1;
   }
   // produce a duplicate of the given op chain
   opinfo* dupop = resourcelog_dupopinfo( op );
   if ( dupop == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate operation chain\n" );
      return -1;
   }
   RESOURCELOG rsrclog = *resourcelog;
   // acquire resourcelog lock
   if ( pthread_mutex_lock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourcelog lock\n" );
      resourcelog_freeopinfo( dupop );
      return -1;
   }
   // special check for RECORD log
//   if ( rsrclog->type == RESOURCE_RECORD_LOG ) {
//      // traverse the op chain to ensure we don't have any completions slipping into this RECORD log
//      opinfo* parseop = dupop;
//      while ( parseop ) {
//         if ( parseop->start == 0 ) {
//            LOG( LOG_ERR, "Detected op completion struct in chain for RECORD log\n" );
//            errno = EINVAL;
//            pthread_mutex_unlock( &(rsrclog->lock) );
//            resourcelog_freeopinfo( dupop );
//            return -1;
//         }
//         parseop = parseop->next;
//      }
//   }
   // incorporate operation info
   char dofree = 0;
   if ( processopinfo( rsrclog, dupop, progress, &(dofree) ) ) {
      LOG( LOG_ERR, "Failed to incorportate op info into MODIFY log\n" );
      pthread_mutex_unlock( &(rsrclog->lock) );
      resourcelog_freeopinfo( dupop );
      return -1;
   }
   // output the operation to the actual log file ( must use the initial, unmodified op )
   if ( printlogline( rsrclog->logfile, op ) ) {
      LOG( LOG_ERR, "Failed to output operation info to logfile: \"%s\"\n", rsrclog->logfilepath );
      pthread_mutex_unlock( &(rsrclog->lock) );
      if ( dofree )
         resourcelog_freeopinfo( dupop );
      return -1;
   }
   // check for quiesced state
   if ( rsrclog->type == RESOURCE_MODIFY_LOG  &&
        rsrclog->outstandingcnt == 0  &&  pthread_cond_signal( &(rsrclog->nooutstanding) ) ) {
      LOG( LOG_ERR, "Failed to signal 'no outstanding ops' condition\n" );
      pthread_mutex_unlock( &(rsrclog->lock) );
      if ( dofree )
         resourcelog_freeopinfo( dupop );
      return -1;
   }
   if ( pthread_mutex_unlock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to release resourcelog lock\n" );
      if ( dofree )
         resourcelog_freeopinfo( dupop );
      return -1;
   }
   if ( dofree )
      resourcelog_freeopinfo( dupop );
   return 0;
}

/**
 * Parse the next operation info sequence from the given RECORD resourcelog
 * @param RESOURCELOG* resourcelog : Statelog to read
 * @param opinfo** op : Reference to be populated with the parsed operation info sequence
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_readop( RESOURCELOG* resourcelog, opinfo** op ) {
   // check for invalid args
   if ( resourcelog == NULL  ||  *resourcelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( (*resourcelog)->type != (RESOURCE_RECORD_LOG | RESOURCE_READ_LOG) ) {
      LOG( LOG_ERR, "Statelog is not a RECORD log, open for read\n" );
      errno = EINVAL;
      return -1;
   }
   if ( op == NULL ) {
      LOG( LOG_ERR, "Receieved a NULL value instead of an opinfo* reference\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCELOG rsrclog = *resourcelog;
   // acquire resourcelog lock
   if ( pthread_mutex_lock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourcelog lock\n" );
      return -1;
   }
   // parse a new op sequence from the logfile
   char eof = 0;
   opinfo* parsedop = parselogline( rsrclog->logfile, &eof );
   if ( parsedop == NULL ) {
      if ( eof < 0 ) {
         LOG( LOG_ERR, "Hit unexpected EOF on logfile: \"%s\"\n", rsrclog->logfilepath );
         pthread_mutex_unlock( &(rsrclog->lock) );
         return -1;
      }
      if ( eof == 0 ) {
         LOG( LOG_ERR, "Failed to parse operation info from logfile: \"%s\"\n", rsrclog->logfilepath );
         pthread_mutex_unlock( &(rsrclog->lock) );
         return -1;
      }
      LOG( LOG_INFO, "Hit EOF on logfile: \"%s\"\n", rsrclog->logfilepath );
   }
   // release the log lock
   if ( pthread_mutex_unlock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to release resourcelog lock\n" );
      return -1;
   }
   *op = parsedop;
   return 0;
}

/**
 * Deallocate and finalize a given resourcelog
 * NOTE -- this will fail if there are currently any ops in flight
 * @param RESOURCELOG* resourcelog : Statelog to be terminated
 * @param operation_summary* summary : Reference to be populated with summary values ( ignored if NULL )
 * @param char delete : Flag indicating whether the logfile should be deleted on termination
 *                      If non-zero, the file is deleted
 * @return int : Zero on success, 1 if the log was preserved due to errors, or -1 on failure
 */
int resourcelog_term( RESOURCELOG* resourcelog, operation_summary* summary, char delete ) {
   // check for invalid args
   if ( resourcelog == NULL  ||  *resourcelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCELOG rsrclog = *resourcelog;
   // acquire the resourcelog lock
   if ( pthread_mutex_lock( &(rsrclog->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire resourcelog lock\n" );
      return -1;
   }
   // abort if any outstanding ops remain
   if ( rsrclog->outstandingcnt != 0 ) {
      LOG( LOG_ERR, "Resourcelog still has outstanding operations\n" );
      pthread_mutex_unlock( &(rsrclog->lock) );
      errno = EAGAIN;
      return -1;
   }
   // check for any recorded errors
   char errpresent = 0;
   if ( rsrclog->summary.deletion_object_failures  ||  rsrclog->summary.deletion_reference_failures  ||
        rsrclog->summary.rebuild_failures  ||  rsrclog->summary.repack_failures ) { errpresent = 1; }
   // potentially record summary info
   if ( summary ) { *summary = rsrclog->summary; }
   if ( !(errpresent)  &&  delete ) {
      // if there were no errors recorded, just delete the logfile
      if ( unlink( rsrclog->logfilepath ) ) {
         LOG( LOG_ERR, "Failed to unlink log file: \"%s\"\n", rsrclog->logfilepath );
         pthread_mutex_unlock( &(rsrclog->lock) );
         return -1;
      }
      // also, attempt to remove the next two parent dirs ( NS and iteration ) of this logfile, skipping on error
      char pdel = 0;
      while ( pdel < 2 ) {
         // trim logpath at the last '/' char, to get the parent dir path
         char* prevsep = NULL;
         char* pparse = rsrclog->logfilepath;
         while ( *pparse != '\0' ) {
            if ( *pparse == '/' ) {
               prevsep = pparse;
               while ( *pparse == '/' ) { pparse++; } // ignore repeated '/' chars
            }
            else { pparse++; }
         }
         if ( prevsep == NULL ) { LOG( LOG_WARNING, "Skipping deletion of parent path %d, as we failed to find a separator\n", (int)pdel ); break; }
         *prevsep = '\0';
         pdel++;
         if ( rmdir( rsrclog->logfilepath ) ) {
            LOG( LOG_WARNING, "Failed to rmdir parent path %d: \"%s\" (%s)\n", (int)pdel, rsrclog->logfilepath, strerror(errno) );
            break;
         }
      }
   }
   cleanuplog( rsrclog, 1 ); // this will release the lock
   *resourcelog = NULL;
   return (errpresent) ? 1 : 0;
}

/**
 * Deallocate and finalize a given resourcelog without waiting for completion
 * @param RESOURCELOG* resourcelog : Statelog to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_abort( RESOURCELOG* resourcelog ) {
   // check for invalid args
   if ( resourcelog == NULL  ||  *resourcelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL resourcelog reference\n" );
      errno = EINVAL;
      return -1;
   }
   RESOURCELOG rsrclog = *resourcelog;
   cleanuplog( rsrclog, 1 ); // this will release the lock
   *resourcelog = NULL;
   return 0;
}

