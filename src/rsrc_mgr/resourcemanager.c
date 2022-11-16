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

#define GC_THRESH 518400  // Age of deleted files before they are Garbage Collected
                          // Default to 6 days ago
#define RB_L_THRESH 1200  // Age of files before they are rebuilt ( based on location )
                          // Default to 20 minutes ago
#define RB_M_THRESH  600  // Age of files before they are rebuilt ( based on marker )
                          // Default to 10 minutes ago
#define RP_THRESH 259200  // Age of files before they are repacked
                          // Default to 3 days ago
#define CL_THRESH  86400  // Age of intermediate state files before they are cleaned up ( failed repacks, old logs, etc. )
                          // Default to 1 day ago

#define MODIFY_ITERATION_PARENT "RMAN-MODIFY-RUNS"
#define RECORD_ITERATION_PARENT "RMAN-RECORD-RUNS"
#define ERROR_LOG_PARENT "RMAN-ERROR-LOGS"
#define ITERATION_ARGS_FILE "PROGRAM-ARGUMENTS"
#define ITERATION_STRING_LEN 128
#define OLDLOG_PREALLOC 16  // pre-allocate space for 16 logfiles in the oldlogs hash table ( double from there, as needed )

typedef struct rmanstate_struct {
   // Per-Run Rank State
   size_t        ranknum;
   size_t        totalranks;

   // Per-Run MarFS State
   marfs_config* config;

   // Old Logfile Progress Tracking
   HASH_TABLE    oldlogs;

   // NS Progress Tracking
   size_t        nscount;
   marfs_ns**    nslist;
   char*         inprogress;
   size_t*       distributed;

   // Global Progress Tracking
   size_t*       activeworkers;
   streamwalker_report* walkreport;
   operation_summary*   logsummary;

   // Thread State
   rthread_gstate gstate;
   ThreadQueue TQ;

   // arg reference vals
   char        execprev;
   char        iteration[ITERATION_STRING_LEN];
   char*       logroot;
   char*       errorlogtgt;
   char*       preservelogtgt;
} rmanstate;

typedef enum {
   RLOG_WORK,      // request to process an existing resource log ( either previous dry-run or dead run pickup )
   NS_WORK,        // request to process a portion of a NS
   COMPLETE_WORK,  // request to complete outstanding work ( quiesce all threads and close all streams )
   TERMINATE_WORK, // request to terminate the rank
   ABORT_WORK      // request to abort all processing and terminate
} worktype;

typedef struct workrequest_struct {
   worktype  type;
   // NS target info
   size_t    nsindex;
   size_t    refdist;
   // Log target info
   char      iteration[ITERATION_STRING_LEN];
   size_t    ranknum;
} workrequest;

typedef struct workresponse_struct {
   workrequest request;
   // Work results
   char                 haveinfo;
   streamwalker_report  report;
   operation_summary    summary;
   char                 errorlog;
   char                 fatalerror;
   char                 errorstr[MAX_STR_BUFFER];
} workresponse;

typedef struct loginfo_struct {
   size_t nsindex;
   size_t logcount;
   workrequest* requests;
} loginfo;


//   -------------   INTERNAL FUNCTIONS    -------------

// TODO
int setranktgt( rmanstate* rman, marfs_ns* ns ) {
   char* outlogpath = resourcelog_genlogpath( 1, rman->logroot, rman->iteration, ns, rman->ranknum );
   if ( outlogpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify output logfile path of rank %zu for NS \"%s\"\n",
                    rman->ranknum, rman->nslist[request->nsindex]->idstr );
      snprintf( response->errorstr, MAX_STR_BUFFER,
                "Failed to identify output logfile path of rank %zu for NS \"%s\"\n",
                rman->ranknum, rman->nslist[request->nsindex]->idstr );
      resourcelog_abort( &(newlog) );
      free( rlogpath );
      return -1;
   }
}

// TODO
int getNSrange( marfs_ns* ns, size_t totalranks, size_t refdist ) {
}

// TODO
int findoldlogs( rmanstate* rman, const char* scanroot ) {
   // construct a HASH_TABLE to hold old logfile references
   HASH_NODE* lognodelist = calloc( rman->nscount, sizeof( struct hash_node_struct ) );
   if ( lognodelist == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a log node list of length %zu\n", rman->nscount );
      return -1;
   }
   HASH_NODE* nodeparse = lognodelist;
   size_t pindex = 0;
   while ( pindex < rman->nscount ) {
      (lognodelist + pindex)->name = strdup( (rman->nslist + pindex)->idstr );
      if ( (lognodelist + pindex)->name == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate NS ID String: \"%s\"\n", (rman->nslist + pindex)->idstr );
         while ( pindex > 0 ) { pindex--; free( (lognodelist + pindex)->name ); free( (lognodelist + pindex)->content ); }
         free( lognodelist );
         return -1;
      }
      // node weight should be pre-zeroed by calloc()
      (lognodelist + pindex)->content = calloc( 1, sizeof( struct loginfo_struct ) );
      if ( (lognodelist + pindex)->content == NULL ) {
         LOG( LOG_ERR, "Failed to allocate a loginfo struct (%s)\n", strerror(errno) );
         free( (lognodelist + pindex)->name );
         while ( pindex > 0 ) { pindex--; free( (lognodelist + pindex)->name ); free( (lognodelist + pindex)->content ); }
         free( lognodelist );
         return -1;
      }
      loginfo* newinfo = (loginfo*)( (lognodelist + pindex)->content );
      newinfo->nsindex = pindex;
      // logcount and requests are pre-zeroed by calloc()
      pindex++;
   }
   if ( (rman->oldlogs = hash_init( lognodelist, rman->nscount, 1 )) == NULL ) {
      LOG( LOG_ERR, "Failed to initialize old logfile hash table\n" );
      while ( pindex > 0 ) { pindex--; free( (lognodelist + pindex)->name ); free( (lognodelist + pindex)->content ); }
      free( lognodelist );
      return -1;
   }
   // NOTE -- Once the table is initialized, this func won't bother to free it on error.  That is now the caller's responsibility.
   DIR**  dirlist[3] = {0};
   workrequest request = {
      .type = RLOG_WORK,
      .nsindex = 0,
      .refdist = 0,
      .iteration = {0},
      .ranknum = 0
   };
   // open the 'scanroot'
   dirlist[0] = opendir( scanroot );
   if ( dirlist[0] == NULL ) {
      LOG( LOG_ERR, "Failed to open loging root \"%s\" for scanning\n", rman->scanroot );
      return -1;
   }
   size_t logdepth = 1;
   loginfo* linfo = NULL; // for caching a target location for new log request info
   while ( logdepth ) {
      if ( logdepth == 1 ) { // identify the iteration to tgt
         if ( rman->execprev ) {
            // check for potential exit
            if ( dirlist[0] == NULL ) {
               LOG( LOG_INFO, "Preparing to exit, after completing scan of prevexec iteration: \"%s/%s\"\n", scanroot, rman->iteration );
               logdepth--;
               continue;
            }
            // NULL out our scanroot dir, so we never repeat this
            closedir( dirlist[0] );
            dirlist[0] = NULL;
            // only targeting a matching iteration
            if ( snprintf( request.iteration, ITERATION_STRING_LEN, rman->iteration ) < 1 ) {
               LOG( LOG_ERR, "Failed to populate request with running iteration \"%s\"\n", rman->iteration );
               return -1;
            }
         }
         else {
            // scan through the root dir, looking for other iterations
            int olderr = errno;
            errno = 0;
            struct dirent* entry;
            while ( (entry = readdir( dirlist[0] )) ) {
               // ignore '.'-prefixed entries
               if ( strncmp( entry->d_name, ".", 1 ) == 0 ) { continue; }
               // ignore the running iteration name, as that will cause all kinds of problems
               if ( strncmp( entry->d_name, rman->iteration, ITERATION_STRING_LEN ) == 0 ) {
                  LOG( LOG_INFO, "Skipping active iteration path: \"%s\"\n", rman->iteration );
                  continue;
               }
               // all other entries are assumed to be valid iteration targets
               if ( snprintf( request.iteration, ITERATION_STRING_LEN, entry->d_name ) >= ITERATION_STRING_LEN ) {
                  LOG( LOG_ERR, "Failed to populate request string for old iteration: \"%s\"\n", entry->d_name );
                  return -1;
               }
               break;
            }
            if ( errno ) {
               LOG( LOG_ERR, "Failed to read log root dir: \"%s\" (%s)\n", rman->scanroot );
               return -1;
            }
            errno = olderr;
            if ( entry == NULL ) {
               // no iterations remain, we are done
               closedir( dirlist[0] );
               dirlist[0] = NULL; // just to be explicit about clearing this value
               logdepth--;
               LOG( LOG_INFO, "Preparing to exit, after completing scan of previous iterations under \"%s\"\n", scanroot );
               continue;
            }
         }
         // open the dir of the matching iteration
         int newfd = openat( dirfd( dirlist[0] ), request.iteration, O_DIRECTORY | O_RDWR, 0 );
         if ( newfd < 0 ) {
            LOG( LOG_ERR, "Failed to open log root for iteration: \"%s\" (%s)\n", request.iteration, strerror(errno) );
            return -1;
         }
         dirlist[1] = fdopendir( iterstr );
         if ( dirlist[1] == NULL ) {
            LOG( LOG_ERR, "Failed to open DIR reference for previous iteration log root: \"%s\" (%s)\n",
                 request.iteration, strerror(errno) );
            close( newfd );
            return -1;
         }
         logdepth++;
      }
      else if ( logdepth == 2 ) { // identify the NS subdirs
         // scan through the iteration dir, looking for namespaces
         int olderr = errno;
         errno = 0;
         struct dirent* entry;
         while ( (entry = readdir( dirlist[1] )) ) {
            // ignore '.'-prefixed entries
            if ( strncmp( entry->d_name, ".", 1 ) == 0 ) { continue; }
            // all other entries are assumed to be namespaces
            break;
         }
         if ( errno ) {
            LOG( LOG_ERR, "Failed to read iteration dir: \"%s\" (%s)\n", request.iteration, strerror(errno) );
            return -1;
         }
         errno = olderr;
         if ( entry == NULL ) {
            // no iterations remain, we are done
            closedir( dirlist[1] );
            dirlist[1] = NULL; // just to be explicit about clearing this value
            logdepth--;
            continue;
         }
         // check what index this NS corresponds to
         HASH_NODE* lnode = NULL;
         if ( hash_lookup( rman->oldlogs, entry->d_name, &(lnode) ) ) {
            // if we can't map this to a NS entry, just ignore it
            fprintf( stderr, "WARNING: Encountered resource logs from a previoius iteration ( \"%s\" )\n"
                             "         associated with an unrecognized namespace ( \"%s\" ).\n"
                             "         These logfiles will be ignored by the current run.\n"
                             "         See \"%s/%s/%s\"\n", request.iteration, entry->d_name, scanroot, request.iteration, entry->d_name );
         }
         else {
            // pull the NS index value from the corresponding hash node
            linfo = (loginfo*)( lnode->content );
            request.nsindex = linfo->nsindex;
            // open the corresponding subdir and traverse into it
            int newfd = openat( dirfd( dirlist[1] ), entry->d_name, O_DIRECTORY | O_RDWR, 0 );
            if ( newfd < 0 ) {
               LOG( LOG_ERR, "Failed to open log NS subdir: \"%s/%s/%s\" (%s)\n", scanroot, request.iteration, entry->d_name, strerror(errno) );
               return -1;
            }
            dirlist[2] = fdopendir( iterstr );
            if ( dirlist[2] == NULL ) {
               LOG( LOG_ERR, "Failed to open DIR reference for previous iteration log root: \"%s\" (%s)\n",
                    request.iteration, strerror(errno) );
               close( newfd );
               return -1;
            }
            logdepth++;
            continue;
         }
      }
      else { // identify the rank to tgt
         // scan through the NS dir, looking for individual logfiles
         int olderr = errno;
         errno = 0;
         struct dirent* entry;
         while ( (entry = readdir( dirlist[2] )) ) {
            // ignore '.'-prefixed entries
            if ( strncmp( entry->d_name, ".", 1 ) == 0 ) { continue; }
            // all other entries are assumed to be logfiles
            break;
         }
         if ( errno ) {
            LOG( LOG_ERR, "Failed to read NS dir: \"%s/%s/%s\" (%s)\n", scanroot, request.iteration,
                 (rman->nslist + request.nsindex)->idstr, strerror(errno) );
            return -1;
         }
         errno = olderr;
         if ( entry == NULL ) {
            // no iterations remain, we are done
            closedir( dirlist[2] );
            dirlist[2] = NULL; // just to be explicit about clearing this value
            logdepth--;
            continue;
         }
         // identify the rank number associated with this logfile
         char* fparse = entry->d_name;
         while ( *fparse != '\0' ) {
            if ( *fparse == '-' ) { fparse++; break; }
            fparse++;
         }
         char* endptr = NULL;
         unsigned long long parseval = strtoull( fparse, &(endptr), 10 );
         if ( endptr == NULL  ||  *endptr != '\0'  ||  *fparse == '\0'  ||  parseval >= SIZE_MAX  ||  parseval >= ULLONG_MAX ) {
            fprintf( stderr, "WARNING: Failed to identify rank number associated with logfile \"%s/%s/%s/%s\"\n"
                             "         The logfile will be skipped\n",
                             scanroot, request.iteration, (rman->nslist + request.nsindex)->idstr, entry->d_name );
            continue;
         }
         request.ranknum = (size_t)parseval;
         // populate the HASH_TABLE entry with the newly completed request
         if ( linfo->logcount % OLDLOG_PREALLOC == 0 ) {
            // logs are allocated in groups of OLDLOG_PREALLOC, so we can use modulo to check for overflow
            workrequest* newrequests = realloc( linfo->requests, (linfo->logcount + OLDLOG_PREALLOC) * sizeof( struct workrequest_struct ) );
            if ( newrequests == NULL ) {
               LOG( LOG_ERR, "Failed to allocate a list of old log requests with length %zu\n", linfo->logcount + OLDLOG_PREALLOC );
               return -1;
            }
            linfo->requests = newrequests;
         }
         *(linfo->requests + linfo->logcount) = request; // NOTE -- I believe this assignment is safe, because .iteration is a static char array
         linfo->logcount++;
      }
   }
   LOG( LOG_INFO, "Finished scan of old resource logs below \"%s\"\n", scanroot );
   return 0;
}

/**
 * Process the given request
 * @param rmanstate* rman : Current resource manager rank state
 * @param workrequest* request : Request to process
 * @param workresponse* response : Response to populate
 * @return int : Zero if the rank should terminate without sending a response
 *               One if the rank should send the populated response
 *               -1 if the rank should send the populated response, then abort
 */
int handlerequest( rmanstate* rman, workrequest* request, workresponse* response ) {
   // pre-populate response with a 'fatal error' condition, just in case
   response->request = *(request);
   response->haveinfo = 0;
   bzero( &(response->report), sizeof( struct streamwalker_report_struct ) );
   bzero( &(response->summary), sizeof( struct operation_summary_struct ) );
   response->errorlog = 0;
   response->fatalerror = 1;
   snprintf( response->errorstr, MAX_STR_BUFFER, "UNKNOWN-ERROR!\n" );
   // identify and process the request
   if ( request->type == RLOG_WORK ) {
      // potentially update our state to target the NS
      if ( rman->gstate.rlog == NULL ) {
         if ( setranktgt( rman, rman->nslist[request->nsindex] ) ) {
            LOG( LOG_ERR, "Failed to update target of rank %zu to NS \"%s\"\n",
                          rman->ranknum, rman->nslist[request->nsindex]->idstr );
            // leave the errorstr alone, as the helper func will have left a more descriptive message
            return -1;
         }
      }
      // identify the path of the rlog
      char* rlogpath = resourcelog_genlogpath( 0, rman->logroot, request->iteration,
                                                  rman->nslist[request->nsindex], request->ranknum );
      if ( rlogpath == NULL ) {
         LOG( LOG_ERR, "Failed to generate logpath for iteration \"%s\" ranknum \"%s\"\n",
                       rman->nslist[request->nsindex], request->ranknum );
         snprintf( response->errorstr, MAX_STR_BUFFER, "Failed to generate logpath for iteration \"%s\" ranknum \"%s\"\n",
                   rman->nslist[request->nsindex], request->ranknum );
         return -1;
      }
      // determine what we're actually doing with this logfile
      if ( rman->execprev ) {
         // we are using the rlog as an input
         if ( resourceinput_setlog( &(rman->gstate.rinput), rlogpath ) ) {
            LOG( LOG_ERR, "Failed to update rank %zu input to logfile \"%s\"\n", rman->ranknum, rlogpath );
            snprintf( response->errorstr, MAX_STR_BUFFER, "Failed to update rank %zu input to logfile \"%s\"\n",
                      rman->ranknum, rlogpath );
            return -1;
         }
      }
      else {
         // we are just cleaning up this old logfile by replaying it into our current one
         // open the specified rlog for read
         RESOURCELOG newlog = NULL;
         if ( resourcelog_init( &(newlog), rlogpath, RESOURCE_READ_LOG, rman->nslist[request->nsindex] ) ) {
            LOG( LOG_ERR, "Failed to open logfile for read: \"%s\" (%s)\n", rlogpath, strerror(errno) );
            snprintf( response->errorstr, MAX_STR_BUFFER, "Failed to open logfile for read: \"%s\" (%s)\n",
                      rlogpath, strerror(errno) );
            free( rlogpath );
            return -1;
         }
         if ( resourcelog_replay( newlog, rman->gstate.rlog ) ) {
            LOG( LOG_ERR, "Failed to replay logfile \"%s\" into active state\n", rlogpath );
            snprintf( response->errorstr, MAX_STR_BUFFER, "Failed to replay logfile \"%s\"\n", rlogpath );
            resourcelog_abort( &(newlog) );
            free( rlogpath );
            return -1;
         }
      }
      free( rlogpath );
   }
   else if ( request->type == NS_WORK ) {
      // potentially update our state to target the NS
      if ( rman->gstate.rlog == NULL ) {
         if ( setranktgt( rman, rman->nslist[request->nsindex] ) ) {
            LOG( LOG_ERR, "Failed to update target of rank %zu to NS \"%s\"\n",
                          rman->ranknum, rman->nslist[request->nsindex]->idstr );
            // leave the errorstr alone, as the helper func will have left a more descriptive message
            return -1;
         }
      }
      // calculate the start and end reference indices for this request
      int refmin = -1;
      int refmax = -1;
      if ( getNSrange( rman->nslist[request->nsindex], rman->totalranks, request->refdist ) ) {
         LOG( LOG_ERR, "Failed to identify NS reference range values for distribution %zu\n", request->refdist );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Failed to identify NS reference range values for distribution %zu\n", request->refdist );
         return -1;
      }
      // update our input to reference the new target range
      if ( resourceinput_setrange( &(rman->gstate.rinput), refmin, refmax ) ) {
         LOG( LOG_ERR, "Failed to set NS \"%s\" reference range values for distribution %zu\n",
              rman->nslist[request->nsindex]->idstr, request->refdist );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Failed to set NS \"%s\" reference range values for distribution %zu\n",
                   rman->nslist[request->nsindex]->idstr, request->refdist );
         return -1;
      }
      // wait for our input to be exhausted ( don't respond until we are ready for more work )
      if ( resourceinput_waitforcomp( &(rman->gstate.rinput) ) ) {
         LOG( LOG_ERR, "Failed to wait for completion of NS \"%s\" reference distribution %zu\n",
              rman->nslist[request->nsindex]->idstr, request->refdist );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Failed to wait for completion of NS \"%s\" reference distribution %zu\n",
                   rman->nslist[request->nsindex]->idstr, request->refdist );
         return -1;
      }
   }
   else if ( request->type == COMPLETE_WORK ) {
      // complete any outstanding work within the current NS
      if ( rman->gstate.rlog == NULL ) {
         LOG( LOG_ERR, "Rank %zu was asked to complete work, but has none\n", rman->ranknum );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Rank %zu was asked to complete work, but has none\n", rman->ranknum );
         return -1;
      }
      // terminate the resource input
      if ( resourceinput_term( &(rman->gstate.rinput) ) ) {
         LOG( LOG_ERR, "Failed to terminate resourceinput\n" );
         snprintf( response->errorstr, MAX_STR_BUFFER, "Failed to terminate resourceinput\n" );
         return -1;
      }
      // wait for the queue to be marked as FINISHED
      TQ_Control_Flags setflags = 0;
      if ( tq_wait_for_flags( rman->tq, 0, &(setflags) ) ) {
         LOG( LOG_ERR, "Failed to wait on ThreadQueue state flags\n" );
         snprintf( response->errorstr, MAX_STR_BUFFER, "Failed to wait on ThreadQueue state flags\n" );
         return -1;
      }
      char threaderror = 0;
      if ( setflags != TQ_FINISHED ) {
         LOG( LOG_WARNING, "Unexpected ( NON-FINISHED ) ThreadQueue state flags: %u\n", setflags );
         threaderror = 1;
      }
      else {
         // wait for TQ completion
         if ( tq_wait_for_completion( rman->tq ) ) {
            LOG( LOG_ERR, "Failed to wait for ThreadQueue completion\n" );
            snprintf( response->errorstr, MAX_STR_BUFFER, "Failed to wait for ThreadQueue completion\n" );
            return -1;
         }
      }
      // gather all thread status values
      rthread_state* tstate = NULL;
      int retval;
      while ( (retval = tq_next_thread_status( rman->tq, (void**)&(tstate) )) > 0 ) {
         // verify thread status
         if ( tstate == NULL ) {
            LOG( LOG_ERR, "Rank %zu encountered NULL thread state when completing NS \"%s\"\n",
                 rman->ranknum, rman->gstate.pos.ns->idstr );
            snprintf( response->errorstr, MAX_STR_BUFFER,
                      "Rank %zu encountered NULL thread state when completing NS \"%s\"\n",
                      rman->ranknum, rman->gstate.pos.ns->idstr );
            return -1;
         }
         if ( tstate->fatalerror ) {
            LOG( LOG_ERR, "Fatal Error in NS \"%s\": \"%s\"\n",
                 rman->gstate.pos.ns->idstr, tstate->errorstr );
            snprintf( response->errorstr, MAX_STR_BUFFER, "Fatal Error in NS \"%s\": \"%s\"\n",
                      rman->gstate.pos.ns->idstr, tstate->errorstr );
            return -1;
         }
         response->report.fileusage   += tstate->report.fileusage;
         response->report.byteusage   += tstate->report.byteusage;
         response->report.filecount   += tstate->report.filecount;
         response->report.objcount    += tstate->report.objcount;
         response->report.bytecount   += tstate->report.bytecount;
         response->report.streamcount += tstate->report.streamcount;
         response->report.delobjs     += tstate->report.delobjs;
         response->report.delfiles    += tstate->report.delfiles;
         response->report.delstreams  += tstate->report.delstreams;
         response->report.volfiles    += tstate->report.volfiles;
         response->report.rpckfiles   += tstate->report.rpckfiles;
         response->report.rpckbytes   += tstate->report.rpckbytes;
         response->report.rbldobjs    += tstate->report.rbldobjs;
         response->report.rbldbytes   += tstate->report.rbldbytes;
      }
      if ( retval ) {
         LOG( LOG_ERR, "Failed to collect thread states during completion of work on NS \"%s\"\n",
              rman->gstate.pos.ns->idstr );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Failed to collect thread status during completion of work on NS \"%s\"\n",
                   rman->gstate.pos.ns->idstr );
         return -1;
      }
      // close our the thread queue, repack streamer, and logs
      if ( tq_close( rman->tq ) ) {
         LOG( LOG_ERR, "Failed to close ThreadQueue after completion of work on NS \"%s\"\n",
              rman->gstate.pos.ns->idstr );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Failed to close ThreadQueue after completion of work on NS \"%s\"\n",
                   rman->gstate.pos.ns->idstr );
         return -1;
      }
      int rlogret;
      if ( (rlogret = resourcelog_term( &(rman->gstate.rlog), &(response->summary), rman->preservelogtgt )) < 0 ) {
         LOG( LOG_ERR, "Failed to terminate resource log following completion of work on NS \"%s\"\n",
              rman->gstate.pos.ns->idstr );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Failed to close ThreadQueue after completion of work on NS \"%s\"\n",
                   rman->gstate.pos.ns->idstr );
         return -1;
      }
      if (rlogret) { response->errorlog = 1; } // note if our log was preserved due to errors being present
      if ( repackstreamer_complete( rman->gstate.rpst ) ) {
         LOG( LOG_ERR, "Failed to complete repack streamer during completion of NS \"%s\"\n", rman->gstate.pos.ns->idstr );
         snprintf( response->errorstr, MAX_STR_BUFFER,
                   "Failed to complete repack streamer during completion of NS \"%s\"\n", rman->gstate.pos.ns->idstr );
         return -1;
      }
      rman->gstate.rpst = NULL;
   }
   else if ( request->type == TERMINATE_WORK ) {
      // simply note and terminate without generating a response
      return 0;
   }
   else { // assume ABORT_WORK
      // clear our fatal error state, but indicate to abort after sending the response
      response->fatalerror = 0;
      return -1;
   }
   // clear our fatal error state
   response->fatalerror = 0;
   // indicate that we have a response to send
   return 1;
}

/**
 * TODO
 * @param rmanstate* rman : 
 * @param workresponse* response : 
 * @return int : On success, positive count of exited workers ( may be zero );
 *               -1 if a fatal error has occurred
 */
int handleresponse( rmanstate* rman, workresponse* response ) {
}


//   -------------   EXTERNAL FUNCTIONS    -------------


int main(int argc, const char** argv) {
   errno = 0; // init to zero (apparently not guaranteed)
   char* config_path = getenv( "MARFS_CONFIG_PATH" ); // check for config env var
   char* ns_path = ".";
   char recurse = 0;
   rmanstate state = {0};

   // get the initialization time of the program, to identify thresholds
   struct timeval currenttime;
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk\n" );
      return -1;
   }
   time_t gcthresh = currenttime.tv_nsec - GC_THRESH;
   time_t rblthresh = currenttime.tv_nsec - RB_L_THRESH;
   time_t rbmthresh = currenttime.tv_nsec - RB_M_THRESH;
   time_t rpthresh = currenttime.tv_nsec - RP_THRESH;

   // parse all position-independent arguments
   char pr_usage = 0;
   int c;
   while ((c = getopt(argc, (char* const*)argv, "c:n:rilpdQGRPT:L:h")) != -1) {
      switch (c) {
      case 'c':
         config_path = optarg;
         break;
      case 'n':
         ns_path = optarg;
         break;
      case 'r':
         recurse = 1;
         break;
      case 'i':
         state.iteration = optarg;
         break;
      case 'l':
         state.logroot = optarg;
         break;
      case 'p':
         state.preservelogtgt = optarg;
         break;
      case 'd':
         state.dryrun = 1;
         break;
      case 'Q':
         state.quotas = 1;
         break;
      case 'G':
         state.thresh.gcthreshold = 1;
         break;
      case 'R':
         state.thresh.rebuildthreshold = 1;
         break;
      case 'P':
         state.thresh.repackthreshold = 1;
         break;
      case '?':
         printf( OUTPREFX "ERROR: Unrecognized cmdline argument: \'%c\'\n", optopt );
      case 'h':
         pr_usage = 1;
         break;
      default:
         printf("ERROR: Failed to parse command line options\n");
         return -1;
      }
   }

}


