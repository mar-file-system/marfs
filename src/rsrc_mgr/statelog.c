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
   MARFS_DELETE_OBJECT_OP,
   MARFS_DELETE_REFERENCE_OP,
   MARFS_REBUILD_OP,
   MARFS_REPACK_OP
} operation_type;

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


typedef struct statelog_struct {
   // synchronization and access control
   pthread_mutex_t   lock;
   pthread_cond_t    nooutstanding;
   ssize_t           outstanding_ops;
   // state info
   operation_summary summary;
   HASH_TABLE        inprogress;
   int               logfile;
   char*             logfilepath;
}*STATELOG;

typedef struct opinfo_struct {
   char* tgt;
   operation_type type;
   char start;
   int errval;
   struct opinfo_struct* next;
} opinfo;


//   -------------   INTERNAL DEFINITIONS    -------------

#define MAX_BUFFER 4096 // maximum character buffer to be used for parsing/printing log lines ( limits length of target string, especially )


//   -------------   INTERNAL FUNCTIONS    -------------

char* genlogfilepath( const char* logroot, marfs_ns* ns, size_t ranknum ) {
   ssize_t pathlen = snprintf( NULL, 0, "%s/%s/statelog-%zu", logroot, ns->idstr, ranknum );
   if ( pathlen < 1 ) {
      LOG( LOG_ERR, "Failed to identify strlen of logfile path\n" );
      return NULL;
   }
   char* path = malloc( sizeof(char) * (pathlen + 1) );
   if ( path == NULL ) {
      LOG( LOG_ERR, "Failed to allocate %zu bytes for logfile path\n", pathlen + 1 );
      return NULL;
   }
   ssize_t lrootlen = snprintf( path, pathlen, "%s/", logroot );
   if ( lrootlen < 1  ||  lrootlen >= pathlen ) {
      LOG( LOG_ERR, "Failed to populate logfile root path\n" );
      free( path );
      return NULL;
   }
   ssize_t nsidlen = 0;
   char* parse = ns->idstr;
   for ( ; *parse != '\0'; parse++ ) {
      if ( *parse == '|'  ||  *parse == '/' ) { *(path + lrootlen + nsidlen) = '#'; }
      else { *(path + lrootlen + nsidlen) = *parse; }
      nsidlen++;
   }
   *(path + lrootlen + nsidlen) = '\0';
   // ensure the parent dir is created
   if ( mkdir( path, 0700 )  &&  errno != EEXIST ) {
      LOG( LOG_ERR, "Failed to create parent dir of logfile: \"%s\"\n", path );
      free( path );
      return NULL;
   }
   if ( snprintf( path + lrootlen + nsidlen, (pathlen - (lrootlen + nsidlen)) + 1, "/statelog-%zu", ranknum ) !=
         (pathlen - (lrootlen + nsidlen)) ) {
      LOG( LOG_ERR, "Logfile path has inconsistent length\n" );
      free( path );
      return NULL;
   }
   return path;
}

void cleanuplog( STATELOG* stlog, char destroy ) {
   HASH_NODE* nodelist = NULL;
   size_t index = 0;
   if ( stlog->inprogress ) {
      hash_term( stlog->inprogress, &nodelist, &index );
      while ( index ) {
         opinfo* opindex = (nodelist + index - 1)->content;
         while ( opindex != NULL ) {
            free( opindex->tgt );
            opinfo* freeop = opindex;
            opindex = opindex->next;
            free( opindex );
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
      pthread_cond_destroy( &(stlog->nooutstanding) );
      pthread_mutex_destroy( &(stlog->lock) );
      free( stlog );
   }
   return;
}

opinfo* parselogline( int logfile, char* eof ) {
   char buffer[MAX_BUFFER] = {0};
   char* tgtchar = buffer;
   off_t reversebytes = 0;
   // parse in our target string
   ssize_t readbytes;
   while ( (readbytes = read( logfile, tgtchar, 1 )) == 1 ) {
      reversebytes++;
      // check for terminating chars
      if ( *tgtchar == ' '  ||  *tgtchar == '\n'  ||  *tgtchar == '\0' ) {
         break;
      }
      tgtchar++;
      // check for excessive string length
      if ( tgtchar - buffer >= MAX_BUFFER - 1 ) {
         LOG( LOG_ERR, "Parsed TGT String exceeds memory limits\n" );
         lseek( logfile, -(reversebytes), SEEK_CUR );
         return NULL;
      }
   }
   // check exit condition
   if ( readbytes == 0 ) {
      // hit EOF
      *eof = 1;
      return NULL;
   }
   if ( *tgtchar != ' ' ) {
      LOG( LOG_ERR, "Unexpected termination of TGT string: '%c'\n", *tgtchar );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return NULL;
   }
   *tgtchar = '\0'; // cleanup our separating ' '
   // allocate our operation node and tgt string
   opinfo* op = malloc( sizeof( struct opinfo_struct ) );
   if ( op == NULL ) {
      LOG( LOG_ERR, "Failed to allocate opinfo struct for logfile line\n" );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return NULL;
   }
   op->start = 0;
   op->errval = 0;
   op->next = NULL;
   op->tgt  = strdup( buffer );
   if ( op->tgt == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate tgt string from logfile: \"%s\"\n", buffer );
      free( op );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return NULL;
   }
   // parse the operation type
   if ( (readbytes = read( logfile, buffer, 2 )) != 2 ) {
      LOG( LOG_ERR, "Failed to read in operation type\n" );
      free( op->tgt );
      free( op );
      if ( readbytes > 0 ) {
         // reached EOF
         *eof = 1;
      }
      else {
         lseek( logfile, -(reversebytes), SEEK_CUR );
      }
      return NULL;
   }
   reversebytes+=2;
   switch( buffer[0] ) {
      case 'O':
         op->type = MARFS_DELETE_OBJECT_OP;
         break;
      case 'R':
         op->type = MARFS_DELETE_REFERENCE_OP;
         break;
      case 'B':
         op->type = MARFS_REBUILD_OP;
         break;
      case 'P':
         op->type = MARFS_REPACK_OP;
         break;
      default:
         LOG( LOG_ERR, "Unrecognized operation type value: '%c'\n", buffer[0] );
         free( op->tgt );
         free( op );
         lseek( logfile, -(reversebytes), SEEK_CUR );
         return NULL;
   }
   if ( buffer[1] != ' ' ) {
      LOG( LOG_ERR, "Unexpected trailing char on type value: '%c'\n", buffer[1] );
      free( op->tgt );
      free( op );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return NULL;
   }
   // parse the operation phase
   if ( (readbytes = read( logfile, buffer, 2 )) != 2 ) {
      LOG( LOG_ERR, "Failed to read in operation phase\n" );
      free( op->tgt );
      free( op );
      if ( readbytes > 0 ) {
         // reached EOF
         *eof = 1;
      }
      else {
         lseek( logfile, -(reversebytes), SEEK_CUR );
      }
      return NULL;
   }
   reversebytes+=2;
   switch ( buffer[0] ) {
      case 'S':
         op->start = 1;
      case 'E':
         break;
      default:
         LOG( LOG_ERR, "Unexpected operation phase value: '%c'\n", buffer[0] );
         free( op->tgt );
         free( op );
         lseek( logfile, -(reversebytes), SEEK_CUR );
         return NULL;
   }
   if ( op->start ) {
      // 'start' of operation lines should end here
      if ( buffer[1] != '\n' ) {
         LOG( LOG_ERR, "Unexpected trailing char after start of operation: '%c'\n", buffer[1] );
         free( op->tgt );
         free( op );
         lseek( logfile, -(reversebytes), SEEK_CUR );
         return NULL;
      }
      return op;
   }
   if ( buffer[1] != ' ' ) {
      LOG( LOG_ERR, "Unexpected trailing char after phase of operation: '%c'\n", buffer[1] );
      free( op->tgt );
      free( op );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return NULL;
   }
   // parse the error value
   tgtchar = buffer;
   while ( (readbytes = read( logfile, tgtchar, 1 )) == 1 ) {
      reversebytes++;
      // check for terminating chars
      if ( *tgtchar == ' '  ||  *tgtchar == '\n'  ||  *tgtchar == '\0' ) {
         break;
      }
      tgtchar++;
      // check for excessive string length
      if ( tgtchar - buffer >= MAX_BUFFER - 1 ) {
         LOG( LOG_ERR, "Parsed error value exceeds memory limits\n" );
         free( op->tgt );
         free( op );
         lseek( logfile, -(reversebytes), SEEK_CUR );
         return NULL;
      }
   }
   // check exit condition
   if ( *tgtchar != '\n' ) {
      LOG( LOG_ERR, "Unexpected termination of TGT string: '%c'\n", *tgtchar );
      free( op->tgt );
      free( op );
      if ( readbytes == 0 ) {
         // reached EOF
         *eof = 1;
      }
      else {
         lseek( logfile, -(reversebytes), SEEK_CUR );
      }
      return NULL;
   }
   char* endptr = NULL;
   long long parsevalue = strtoll( buffer, &endptr, 10 );
   if ( endptr == NULL  ||  *endptr != '\n' ) {
      LOG( LOG_ERR, "Unexpected termination of error value string: '%c'\n", *endptr );
      free( op->tgt );
      free( op );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return NULL;
   }
   op->errval = (int)parsevalue;
   return op;
}

int printlogline( int logfile, opinfo* op ) {
   // print out the tgt string
   char buffer[MAX_BUFFER];
   ssize_t usedbuff;
   off_t reversebytes = 0;
   if ( (usedbuff = snprintf( buffer, MAX_BUFFER, "%s ", op->tgt )) >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Failed to fit op target into print buffer: \"%s\"\n", op->tgt );
      return -1;
   }
   ssize_t writeres = write( logfile, buffer, usedbuff );
   if ( writeres > 0 ) { reversebytes = writeres; }
   if ( writeres != usedbuff ) {
      LOG( LOG_ERR, "Failed to write out target info of length %zd to logfile\n", usedbuff );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return -1;
   }
   // print out the operation type and phase
   switch( op->type ) {
      case MARFS_DELETE_OBJECT_OP:
         buffer[0] = 'O';
         break;
      case MARFS_DELETE_REFERENCE_OP:
         buffer[0] = 'R';
         break;
      case MARFS_REBUILD_OP:
         buffer[0] = 'B';
         break;
      case MARFS_REPACK_OP:
         buffer[0] = 'P';
         break;
      default:
         LOG( LOG_ERR, "Unrecognized operation type value\n" );
         lseek( logfile, -(reversebytes), SEEK_CUR );
         return -1;
   }
   buffer[1] = ' ';
   if ( op->start ) {
      buffer[2] = 'S';
      buffer[3] = '\n';
   }
   else {
      buffer[2] = 'E';
      buffer[3] = ' ';
   }
   writeres = write( logfile, buffer, 4 );
   if ( writeres > 0 ) { reversebytes += writeres; }
   if ( writeres != 4 ) {
      LOG( LOG_ERR, "Failed to ouput phase info of operation\n" );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return -1;
   }
   // start of operation messages are now complete
   if ( op->start ) { return 0; }
   // otherwise, move on to error info
   if ( (usedbuff = snprintf( buffer, MAX_BUFFER, "%d\n", op->errval )) >= MAX_BUFFER ) {
      LOG( LOG_ERR, "Failed to fit op errval into print buffer: %d\n", op->errval );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return -1;
   }
   writeres = write( logfile, buffer, usedbuff );
   if ( writeres > 0 ) { reversebytes += writeres; }
   if ( writeres != usedbuff ) {
      LOG( LOG_ERR, "Failed to output errval info of operation\n" );
      lseek( logfile, -(reversebytes), SEEK_CUR );
      return -1;
   }
   return 0;
}

int processopinfo( STATELOG* stlog, opinfo* newop ) {
   // map this operation into our inprogress hash table
   HASH_NODE* node = NULL;
   if ( hash_lookup( stlog->inprogress, newop->tgt, &node ) < 0 ) {
      LOG( LOG_ERR, "Failed to map operation on \"%s\" into inprogress HASH_TABLE\n", newop->tgt );
      free( newop->tgt );
      free( newop );
      return -1;
   }
   // traverse the attached operations, looking for a match
   opinfo* opindex = node->content;
   opinfo* prevop = NULL;
   while ( opindex != NULL ) {
      if ( (strcmp( opindex->tgt, newop->tgt ) == 0)  &&
           (opindex->type == newop->type) ) {
         break;
      }
      prevop = opindex;
      opindex = opindex->next;
   }
   if ( opindex != NULL ) {
      // repeat of operation start can be ignored
      if ( newop->start == 0 ) {
         // otherwise, for op completion, we'll need to note it in our totals...
         switch( newop->type ) {
            case MARFS_DELETE_OBJECT_OP:
               stlog->summary.delete_object_count++;
               if ( newop->errval ) { stlog->summary.delete_object_failures++; }
               break;
            case MARFS_DELETE_REFERENCE_OP:
               stlog->summary.delete_reference_count++;
               if ( newop->errval ) { stlog->summary.delete_reference_failures++; }
               break;
            case MARFS_REBUILD_OP:
               stlog->summary.rebuild_count++;
               if ( newop->errval ) { stlog->summary.rebuild_failures++; }
               break;
            case MARFS_REPACK_OP:
               stlog->summary.repack_count++;
               if ( newop->errval ) { stlog->summary.repack_failures++; }
               break;
            default:
               LOG( LOG_ERR, "Unrecognized operation type value\n" );
               free( newop->tgt );
               free( newop );
               return -1;
         }
         // ...and remove the matching op from inprogress
         if ( prevop ) {
            // pull the matching op out of the list
            prevop->next = opindex->next;
            free( opindex->tgt );
            free( opindex );
         }
         else {
            // no previous op means the matching op is the only one; just remove it
            node->content = NULL;
         }
      }
      // a matching op means the parsed operation can be discarded
      free( newop->tgt );
      free( newop );
   }
   else {
      // the parsed line should indicate the start of a new operation
      if ( newop->start == 0 ) {
         LOG( LOG_ERR, "Parsed completion of op on \"%s\" target from logfile \"%s\" with no parsed start of op\n",
              newop->tgt, stlog->logfilepath );
         free( newop->tgt );
         free( newop );
         return -1;
      }
      // stitch the parsed op onto the front of our inprogress list
      newop->next = node->content;
      node->content = newop;
   }
   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------

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
      stlog->outstanding_ops = 0;
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
            LOG( LOG_INFO, "Attempting cleanup of existing logfile: \"%s\"\n", entry->name );
            int oldlog = openat( dirfd(parentdir), entry->name, O_RDONLY );
         }
      }
   }
}

/**
 * Record that a certain number of operations are in flight
 * @param STATELOG* statelog : Statelog to be updated
 * @param size_t numops : Number of operations now in flight
 * @return int : Zero on success, or -1 on failure
 */
int statelog_update_inflight( STATELOG* statelog, size_t numops ) {
   // check for invalid args
   if ( statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
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
   if ( statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
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
   if ( statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
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
   if ( statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
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
   if ( statelog == NULL ) {
      LOG( LOG_ERR, "Received a NULL statelog reference\n" );
      errno = EINVAL;
      return -1;
   }
}


