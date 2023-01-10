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
#define LOG_PREFIX "resourcemanager"
#include <logging.h>

#include "resourcethreads.h"
#include <config/config.h>

#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <dirent.h>
#include <mpi.h>

//   -------------   INTERNAL DEFINITIONS    -------------

#define GC_THRESH 604800  // Age of deleted files before they are Garbage Collected
                          // Default to 7 days ago
#define RB_L_THRESH  600  // Age of files before they are rebuilt ( based on location )
                          // Default to 10 minutes ago
#define RB_M_THRESH  120  // Age of files before they are rebuilt ( based on marker )
                          // Default to 2 minutes ago
#define RP_THRESH 259200  // Age of files before they are repacked
                          // Default to 3 days ago
#define CL_THRESH  86400  // Age of intermediate state files before they are cleaned up ( failed repacks, old logs, etc. )
                          // Default to 1 day ago

#define DEFAULT_PRODUCER_COUNT 16
#define DEFAULT_CONSUMER_COUNT 32
#define DEFAULT_LOG_ROOT "/var/log/marfs-rman"
#define MODIFY_ITERATION_PARENT "RMAN-MODIFY-RUNS"
#define RECORD_ITERATION_PARENT "RMAN-RECORD-RUNS"
#define SUMMARY_FILENAME "summary.log"
#define ITERATION_ARGS_FILE "PROGRAM-ARGUMENTS"
#define ITERATION_STRING_LEN 128
#define OLDLOG_PREALLOC 16  // pre-allocate space for 16 logfiles in the oldlogs hash table ( double from there, as needed )
#define MAX_ERROR_BUFFER MAX_STR_BUFFER + 100  // define our error strings as slightly larger than the error message itself

typedef struct rmanstate_struct {
   // Per-Run Rank State
   size_t        ranknum;
   size_t        totalranks;
   size_t        workingranks;

   // Per-Run MarFS State
   marfs_config* config;

   // Old Logfile Progress Tracking
   HASH_TABLE    oldlogs;

   // NS Progress Tracking
   size_t        nscount;
   marfs_ns**    nslist;
   size_t*       distributed;

   // Global Progress Tracking
   char          fatalerror;
   char*         terminatedworkers;
   streamwalker_report* walkreport;
   operation_summary*   logsummary;

   // Thread State
   rthread_global_state gstate;
   ThreadQueue tq;

   // Output Logging
   FILE* summarylog;

   // arg reference vals
   char        quotas;
   char        iteration[ITERATION_STRING_LEN];
   char*       execprevroot;
   char*       logroot;
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
   char                 errorstr[MAX_ERROR_BUFFER];
} workresponse;

typedef struct loginfo_struct {
   size_t nsindex;
   size_t logcount;
   workrequest* requests;
} loginfo;


//   -------------   HELPER FUNCTIONS    -------------

void print_usage_info() {
   printf( "\n"
           "marfs-rman [-c MarFS-Config-File] [-n MarFS-NS-Target] [-r] [-i Iteraion-Name] [-l Log-Root]\n"
           "           [-p Log-Pres-Root] [-d] [-X Execution-Target] [-Q] [-G] [-R] [-P] [-C]\n"
           "           [-T Threshold-Values] [-L Rebuild-Location] [-h]\n"
           "\n"
           " Arguments --\n"
           "  -c MarFS-Config-File : Specifies the path of the MarFS config file to use\n"
           "                         (uses the MARFS_CONFIG_PATH env val, if unspecified)\n"
           "  -n MarFS-NS-Target   : Path of the MarFS Namespace to target for processing\n"
           "                         (defaults to the root NS, if unspecified)\n"
           "  -r                   : Specifies to operate recursively on MarFS Subspaces\n"
           "  -i Iteration-Name    : Specifies the name of this marfs-rman instance\n"
           "                         (defaults to a date string, if unspecified)\n"
           "  -l Log-Root          : Specifies the dir to be used for resource log storage\n"
           "                         (defaults to \"%s\", if unspecified)\n"
           "  -p Log-Pres-Root     : Specifies a location to store resource logs to, post-run\n"
           "                         (logfiles will be deleted, if unspecified)\n"
           "  -d                   : Specifies a 'dry-run', logging but skipping execution of all ops\n"
           "  -X Execution-Target  : Specifies the loging path of a previous 'dry-run' iteration to\n"
           "                         be processed by this run.  The program will NOT scan reference\n"
           "                         paths to identify operations.  Instead, it will exclusively\n"
           "                         perform the operations logged by the targetted iteration.\n"
           "                         This argument is incompatible with any of the below args.\n"
           "  -Q                   : The resource manager will set NS usage values ( files / bytes )\n"
           "  -G                   : The resource manager will perform garbage collection\n"
           "  -R                   : The resource manager will perform rebuilds\n"
           "  -P                   : The resource manager will perform repacks\n"
           "                         (!!!CURRENTLY UNIMPLEMENTED!!!)\n"
           "  -C                   : The resource manager will perform cleanup of failed operations\n"
           "  -T Threshold-Values  : Specifies time threshold values for resource manager ops.\n"
           "                         Value Format = <OpFlag><TimeThresh>[<Unit>]\n"
           "                                           [-<OpFlag><TimeThresh>[<Unit>]]*\n"
           "                         Where, <OpFlag>     = 'G', 'R', 'P', or 'C' ( see prev args )\n"
           "                                <TimeThresh> = A numeric time value\n"
           "                                               ( only files older than this threshold\n"
           "                                                 will be targeted for the specified op )\n"
           "                                <Unit>       = 's' ( seconds ), 'm' ( minutes ),\n"
           "                                               'h' ( hours ), 'd' ( days )\n"
           "                                               ( assumed to be 's', if omitted )\n"
           "  -L Rebuild-Location  : Specifies NE object location to target for rebuilds\n"
           "                         (!!!CURRENTLY UNIMPLEMENTED!!!)\n"
           "  -h                   : Prints this usage info\n"
           "\n",
           DEFAULT_LOG_ROOT );
}

void cleanupstate( rmanstate* rman, char abort ) {
   if ( rman ) {
      if ( rman->preservelogtgt ) { free( rman->preservelogtgt ); }
      if ( rman->logroot )        { free( rman->logroot ); }
      if ( rman->execprevroot )   { free( rman->execprevroot ); }
      if ( rman->summarylog )     { fclose( rman->summarylog ); }
      if ( rman->tq ) {
         if ( !(abort) ) {
            LOG( LOG_ERR, "Encountered active TQ with no abort condition specified\n" );
            fprintf( stderr, "Encountered active TQ with no abort condition specified\n" );
            rman->fatalerror = 1;
         }
         tq_set_flags( rman->tq, TQ_ABORT );
         if ( rman->gstate.rinput ) {
            resourceinput_purge( &(rman->gstate.rinput), rman->gstate.numprodthreads );
            resourceinput_term( &(rman->gstate.rinput) );
         }
         // gather all thread status values
         rthread_state* tstate = NULL;
         while ( tq_next_thread_status( rman->tq, (void**)&(tstate) ) > 0 ) {
            // verify thread status
            if ( tstate ) { free( tstate ); }
         }
         tq_close( rman->tq );
      }
      if ( rman->gstate.rpst ) { repackstreamer_abort( rman->gstate.rpst ); }
      if ( rman->gstate.rlog ) { resourcelog_abort( &(rman->gstate.rlog) ); }
      if ( rman->gstate.rinput ) { resourceinput_abort( &(rman->gstate.rinput) ); }
      if ( rman->gstate.pos.ns ) { config_abandonposition( &(rman->gstate.pos) ); }
      if ( rman->logsummary ) { free( rman->logsummary ); }
      if ( rman->walkreport ) { free( rman->walkreport ); }
      if ( rman->terminatedworkers ) { free( rman->terminatedworkers ); }
      if ( rman->distributed ) { free( rman->distributed ); }
      if ( rman->nslist ) { free( rman->nslist ); }
      if ( rman->oldlogs ) {
         HASH_NODE* resnode = NULL;
         size_t ncount = 0;
         if ( hash_term( rman->oldlogs, &(resnode), &(ncount) ) == 0 ) {
            // free all subnodes and requests
            size_t nindex = 0;
            for ( ; nindex < ncount; nindex++ ) {
               loginfo* linfo = (loginfo*)( (resnode+nindex)->content );
               if ( linfo->requests ) { free( linfo->requests ); }
            }
            free( resnode ); // these were allocated in one block, and thus require only one free()
         }
      }
      if ( rman->config ) { config_term( rman->config ); }
   }
   MPI_Finalize();
}

int error_only_filter( const opinfo* op ) {
   if ( op->errval ) { return 0; }
   return 1;
}

void outputinfo( FILE* output, marfs_ns* ns, streamwalker_report* report , operation_summary* summary ) {
   char userout = 0;
   if ( output == stdout ) { userout = 1; } // limit info output directly to the user
   fprintf( output, "Namespace \"%s\"%s --\n", ns->idstr, (userout) ? " Totals" : " Incremental Values" );
   fprintf( output, "   Walk Report --\n" );
   if ( !(userout) || report->fileusage )   { fprintf( output, "      File Usage = %zu\n", report->fileusage ); }
   size_t bytetrans = report->byteusage;
   char remainder[6] = {0};
   char* unit = "B";
   while ( bytetrans > 1024 ) {
      if ( *unit == 'B' ) { unit = "KiB"; }
      else if ( *unit == 'K' ) { unit = "MiB"; }
      else if ( *unit == 'M' ) { unit = "GiB"; }
      else if ( *unit == 'G' ) { unit = "TiB"; }
      else if ( *unit == 'T' ) { unit = "PiB"; }
      else { break; }
      if ( bytetrans % 1024 ) {
         snprintf( remainder, 6, ".%.3zu", (((bytetrans % 1024) * 1000) + 512) / 1024 );
      } else { *remainder = '\0'; }
      bytetrans /= 1024;
   }
   if ( !(userout) )   { fprintf( output, "      Byte Usage = %zu\n", report->byteusage ); }
   else if ( report->byteusage ) {
      fprintf( output, "      Byte Usage = %zu%s%s\n", bytetrans, remainder, unit );
   }
   if ( !(userout) || report->filecount )   { fprintf( output, "      File Count = %zu\n", report->filecount ); }
   if ( !(userout) || report->objcount )    { fprintf( output, "      Object Count = %zu\n", report->objcount ); }
   bytetrans = report->bytecount;
   *remainder = '\0';
   unit = "B";
   while ( bytetrans > 1024 ) {
      if ( *unit == 'B' ) { unit = "KiB"; }
      else if ( *unit == 'K' ) { unit = "MiB"; }
      else if ( *unit == 'M' ) { unit = "GiB"; }
      else if ( *unit == 'G' ) { unit = "TiB"; }
      else if ( *unit == 'T' ) { unit = "PiB"; }
      else { break; }
      if ( bytetrans % 1024 ) {
         snprintf( remainder, 6, ".%.3zu", (((bytetrans % 1024) * 1000) + 512) / 1024 );
      } else { *remainder = '\0'; }
      bytetrans /= 1024;
   }
   if ( !(userout) )   { fprintf( output, "      Byte Count = %zu\n", report->bytecount ); }
   else if ( report->bytecount ) {
      fprintf( output, "      Byte Count = %zu%s%s\n", bytetrans, remainder, unit );
   }
   if ( !(userout) || report->streamcount ) { fprintf( output, "      Stream Count = %zu\n", report->streamcount ); }
   if ( !(userout) || report->delobjs )     { fprintf( output, "      Object Deletion Candidates = %zu\n", report->delobjs ); }
   if ( !(userout) || report->delfiles )    { fprintf( output, "      File Deletion Candidates = %zu\n", report->delfiles ); }
   if ( !(userout) || report->delstreams )  { fprintf( output, "      Stream Deletion Candidates = %zu\n", report->delstreams ); }
   if ( !(userout) || report->volfiles )    { fprintf( output, "      Volatile File Count = %zu\n", report->volfiles ); }
   if ( !(userout) || report->rpckfiles )   { fprintf( output, "      File Repack Candidates = %zu\n", report->rpckfiles ); }
   if ( !(userout) || report->rpckbytes )   { fprintf( output, "      Repack Candidate Bytes = %zu\n", report->rpckbytes ); }
   if ( !(userout) || report->rbldobjs )    { fprintf( output, "      Object Rebuild Candidates = %zu\n", report->rbldobjs ); }
   if ( !(userout) || report->rbldbytes )   { fprintf( output, "      Rebuild Candidate Bytes = %zu\n", report->rbldbytes ); }
   fprintf( output, "   Operation Summary --\n" );
   if ( !(userout) || summary->deletion_object_count ) {
      fprintf( output, "      Object Deletion Count = %zu ( %zu Failures )\n",
               summary->deletion_object_count, summary->deletion_object_failures );
   }
   if ( !(userout) || summary->deletion_reference_count ) {
      fprintf( output, "      Reference Deletion Count = %zu ( %zu Failures )\n",
               summary->deletion_reference_count, summary->deletion_reference_failures );
   }
   if ( !(userout) || summary->rebuild_count ) {
      fprintf( output, "      Object Rebuild Count = %zu ( %zu Failures )\n",
               summary->rebuild_count, summary->rebuild_failures );
   }
   if ( !(userout) || summary->repack_count ) {
      fprintf( output, "      File Repack Count = %zu ( %zu Failures )\n",
               summary->repack_count, summary->repack_failures );
   }
   fprintf( output, "\n" );
   fflush( output );
   return;
}

int output_program_args( rmanstate* rman ) {
   // start with marfs config version ( config changes could seriously break an attempt to re-execute this later )
   if ( fprintf( rman->summarylog, "%s\n", rman->config->version ) < 1 ) {
      fprintf( stderr, "ERROR: Failed to output config version to summary log\n" );
      return -1;
   }
   // output operation types and threshold values
   if ( rman->gstate.dryrun  &&  fprintf( rman->summarylog, "DRY-RUN\n" ) < 1 ) {
      fprintf( stderr, "ERROR: Failed to output dry-run flag to summary log\n" );
      return -1;
   }
   if ( rman->quotas  &&  fprintf( rman->summarylog, "QUOTAS\n" ) < 1 ) {
      fprintf( stderr, "ERROR: Failed to output quota flag to summary log\n" );
      return -1;
   }
   if ( rman->gstate.thresh.gcthreshold  &&
        fprintf( rman->summarylog, "GC=%llu\n", (unsigned long long) rman->gstate.thresh.gcthreshold ) < 1 ) {
      fprintf( stderr, "ERROR: Failed to output GC threshold to summary log\n" );
      return -1;
   }
   if ( rman->gstate.thresh.repackthreshold  &&
        fprintf( rman->summarylog, "REPACK=%llu\n", (unsigned long long) rman->gstate.thresh.repackthreshold ) < 1 ) {
      fprintf( stderr, "ERROR: Failed to output REPACK threshold to summary log\n" );
      return -1;
   }
   if ( rman->gstate.thresh.rebuildthreshold  &&
        fprintf( rman->summarylog, "REBUILD=%llu\n", (unsigned long long) rman->gstate.thresh.rebuildthreshold ) < 1 ) {
      fprintf( stderr, "ERROR: Failed to output REBUILD threshold to summary log\n" );
      return -1;
   }
   if ( rman->gstate.lbrebuild ) {
      // TODO
   }
   if ( fprintf( rman->summarylog, "\n" ) < 1 ) {
      fprintf( stderr, "ERROR: Failed to output header separator summary log\n" );
      return -1;
   }
   return 0;
}

int parse_program_args( rmanstate* rman, FILE* inputsummary ) {
   // parse in a verify the config version
   char* readline = NULL;
   size_t linealloc = 0;
   ssize_t linelen = getline( &(readline), &(linealloc), inputsummary );
   if ( linelen < 2 ) {
      fprintf( stderr, "WARNING: Failed to read MarFS config version string from previous run's summary log\n" );
      return 1;
   }
   linelen--; // decrement length, to exclude newline
   if ( strncmp( readline, rman->config->version, linelen ) ) {
      fprintf( stderr, "WARNING: Previous run is associated with a different MarFS config version: \"%s\"\n", readline );
      free( readline );
      return 1;
   }
   // parse in operation threshold values and set our state to match
   while ( (linelen = getline( &(readline), &(linealloc), inputsummary )) > 1 ) {
      linelen--; // decrement length, to exclude newline
      if ( strncmp( readline, "QUOTAS", linelen ) == 0 ) {
         rman->quotas = 1;
      }
      else if ( strncmp( readline, "DRY-RUN", linelen ) == 0 ) {
         rman->gstate.dryrun = 1;
      }
      else {
         // look for an '=' char
         char* parse = readline;
         while ( *parse != '=' ) {
            if ( *parse == '\0' ) {
               fprintf( stderr, "ERROR: Failed to parse previous run's operation info ( \"%s\" )\n", readline );
               free( readline );
               return -1;
            }
            parse++;
         }
         *parse = '\0';
         parse++;
         // parse in the numeric value
         char* endptr = NULL;
         unsigned long long parseval = strtoull( parse, &(endptr), 10 );
         if ( endptr == NULL  ||  *endptr != '\n'  ||  parseval == ULLONG_MAX ) {
            fprintf( stderr, "ERROR: Failed to parse previous run's \"%s\" operation threshold: \"%s\"\n", readline, parse );
            free( readline );
            return -1;
         }
         // populate the appropriate value, based on string header
         if ( strcmp( readline, "GC" ) == 0 ) {
            rman->gstate.thresh.gcthreshold = (time_t)parseval;
         }
         else if ( strcmp( readline, "REPACK" ) == 0 ) {
            rman->gstate.thresh.repackthreshold = (time_t)parseval;
         }
         else if ( strcmp( readline, "REBUILD" ) == 0 ) {
            rman->gstate.thresh.rebuildthreshold = (time_t)parseval;
         }
         else {
            fprintf( stderr, "ERROR: Encountered unrecognized operation type in log of previous run: \"%s\"\n", readline );
            free( readline );
            return -1;
         }
      }
   }
   if ( linelen < 1 ) {
      fprintf( stderr, "ERROR: Failed to read operation info from previous run's logfile\n" );
      return -1;
   }
   if ( readline ) { free( readline ); } // done with this string allocation
   return 0;
}

/**
 * Configure the current rank to target the given NS ( configures state and launches worker threads )
 * @param rmanstate* rman : State struct for the current rank
 * @param marfs_ns* ns : New NS to target
 * @param workresponse* response : Response to populate with error description, on failure
 * @return int : Zero on success, -1 on failure
 */
int setranktgt( rmanstate* rman, marfs_ns* ns, workresponse* response ) {
   // update position value
   if ( config_establishposition( &(rman->gstate.pos), rman->config ) ) {
      LOG( LOG_ERR, "Failed to establish a root NS position\n" );
      snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to establish a root NS position\n" );
      return -1;
   }
   char* tmpnspath = NULL;
   if ( config_nsinfo( ns->idstr, NULL, &(tmpnspath) ) ) {
      LOG( LOG_ERR, "Failed to identify NS path of NS \"%s\"\n", ns->idstr );
      snprintf( response->errorstr, MAX_ERROR_BUFFER,
                "Failed to identify NS path of NS \"%s\"\n", ns->idstr );
      return -1;
   }
   char* nspath = strdup( tmpnspath+1 ); // strip off leading '/', to get a relative NS path
   free( tmpnspath );
   if ( config_traverse( rman->config, &(rman->gstate.pos), &(nspath), 0 ) ) {
      LOG( LOG_ERR, "Failed to traverse config to new NS path: \"%s\"\n", nspath );
      snprintf( response->errorstr, MAX_ERROR_BUFFER,
                "Failed to traverse config to new NS path: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   free( nspath );
   if ( rman->gstate.pos.ctxt == NULL  &&  config_fortifyposition( &(rman->gstate.pos) ) ) {
      LOG( LOG_ERR, "Failed to fortify position for new NS: \"%s\"\n", ns->idstr );
      snprintf( response->errorstr, MAX_ERROR_BUFFER,
                "Failed to fortify position for new NS: \"%s\"\n", ns->idstr );
      config_abandonposition( &(rman->gstate.pos) );
      return -1;
   }
   // update our resource input struct
   if ( resourceinput_init( &(rman->gstate.rinput), &(rman->gstate.pos), rman->gstate.numprodthreads ) ) {
      LOG( LOG_ERR, "Failed to initialize resourceinput for new NS target: \"%s\"\n", ns->idstr );
      snprintf( response->errorstr, MAX_ERROR_BUFFER,
                "Failed to initialize resourceinput for new NS target: \"%s\"\n", ns->idstr );
      config_abandonposition( &(rman->gstate.pos) );
      return -1;
   }
   // update our output resource log
   char* outlogpath = resourcelog_genlogpath( 1, rman->logroot, rman->iteration, ns, rman->ranknum );
   if ( outlogpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify output logfile path of rank %zu for NS \"%s\"\n",
                    rman->ranknum, ns->idstr );
      snprintf( response->errorstr, MAX_ERROR_BUFFER,
                "Failed to identify output logfile path of rank %zu for NS \"%s\"\n",
                rman->ranknum, ns->idstr );
      resourceinput_purge( &(rman->gstate.rinput), rman->gstate.numprodthreads );
      resourceinput_term( &(rman->gstate.rinput) );
      config_abandonposition( &(rman->gstate.pos) );
      return -1;
   }
   if ( resourcelog_init( &(rman->gstate.rlog), outlogpath,
                          (rman->gstate.dryrun) ? RESOURCE_RECORD_LOG : RESOURCE_MODIFY_LOG, ns ) ) {
      LOG( LOG_ERR, "Failed to initialize output logfile: \"%s\"\n", outlogpath );
      snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to initialize output logfile: \"%s\"\n", outlogpath );
      free( outlogpath );
      resourceinput_purge( &(rman->gstate.rinput), rman->gstate.numprodthreads );
      resourceinput_term( &(rman->gstate.rinput) );
      config_abandonposition( &(rman->gstate.pos) );
      return -1;
   }
   free( outlogpath );
   // update our repack streamer
   if ( (rman->gstate.rpst = repackstreamer_init()) == NULL ) {
      LOG( LOG_ERR, "Failed to initialize repack streamer\n" );
      snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to initialize repack streamer\n" );
      resourcelog_term( &(rman->gstate.rlog), NULL, 1 );
      resourceinput_purge( &(rman->gstate.rinput), rman->gstate.numprodthreads );
      resourceinput_term( &(rman->gstate.rinput) );
      config_abandonposition( &(rman->gstate.pos) );
      return -1;
   }
   // kick off our worker threads
   TQ_Init_Opts tqopts = {
      .log_prefix = "RManWorker",
      .init_flags = 0,
      .max_qdepth = rman->gstate.numprodthreads + rman->gstate.numconsthreads,
      .global_state = &(rman->gstate),
      .num_threads = rman->gstate.numprodthreads + rman->gstate.numconsthreads,
      .num_prod_threads = rman->gstate.numprodthreads,
      .thread_init_func = rthread_init_func,
      .thread_consumer_func = rthread_consumer_func,
      .thread_producer_func = rthread_producer_func,
      .thread_pause_func = NULL,
      .thread_resume_func = NULL,
      .thread_term_func = rthread_term_func
   };
   if ( (rman->tq = tq_init( &(tqopts) )) == NULL ) {
      LOG( LOG_ERR, "Failed to start ThreadQueue for NS \"%s\"\n", ns->idstr );
      snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to start ThreadQueue for NS \"%s\"\n", ns->idstr );
      repackstreamer_abort( rman->gstate.rpst );
      rman->gstate.rpst = NULL;
      resourcelog_term( &(rman->gstate.rlog), NULL, 1 );
      resourceinput_purge( &(rman->gstate.rinput), rman->gstate.numprodthreads );
      resourceinput_term( &(rman->gstate.rinput) );
      config_abandonposition( &(rman->gstate.pos) );
      return -1;
   }
   // ensure all threads have initialized
   if ( tq_check_init( rman->tq ) ) {
      LOG( LOG_ERR, "Some threads failed to initialize for NS \"%s\"\n", ns->idstr );
      snprintf( response->errorstr, MAX_ERROR_BUFFER, "Some threads failed to initialize for NS \"%s\"\n", ns->idstr );
      tq_set_flags( rman->tq, TQ_ABORT );
      repackstreamer_abort( rman->gstate.rpst );
      rman->gstate.rpst = NULL;
      resourcelog_term( &(rman->gstate.rlog), NULL, 1 );
      resourceinput_purge( &(rman->gstate.rinput), rman->gstate.numprodthreads );
      resourceinput_term( &(rman->gstate.rinput) );
      config_abandonposition( &(rman->gstate.pos) );
      return -1;
   }
   LOG( LOG_INFO, "Rank %zu is now targetting NS \"%s\"\n", rman->ranknum, ns->idstr );
   return 0;
}

/**
 * Calculate the min and max values for the given ref distribution of the NS
 * @param marfs_ns* ns : Namespace to split ref ranges across
 * @param size_t workingranks : Total number of operating ranks
 * @param size_t refdist : Reference distribution index
 * @param size_t* refmin : Reference to be populated with the minimum range value
 * @param size_t* refmin : Reference to be populated with the maximum range value
 */
void getNSrange( marfs_ns* ns, size_t workingranks, size_t refdist, size_t* refmin, size_t* refmax ) {
   size_t refperrank = ns->prepo->metascheme.refnodecount / workingranks;
   size_t remainder = ns->prepo->metascheme.refnodecount % workingranks;
   *refmin = (refdist * refperrank);
   *refmax = (*refmin + refperrank) - ( (refdist >= remainder) ? 1 : 0 );
   LOG( LOG_INFO, "Using Min=%zu / Max=%zu for ref distribution %zu on NS \"%s\"\n", *refmin, *refmax, refdist, ns->idstr );
   return;
}

/**
 * Populate the current rank's oldlogs HASH_TABLE
 * @param rmanstate* rman : State of this rank
 * @param const char* scanroot : Logroot, below which to scan
 * @return int : Zero on success, or -1 on failure
 */
int findoldlogs( rmanstate* rman, const char* scanroot ) {
   // construct a HASH_TABLE to hold old logfile references
   HASH_NODE* lognodelist = calloc( rman->nscount, sizeof( struct hash_node_struct ) );
   if ( lognodelist == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a log node list of length %zu\n", rman->nscount );
      return -1;
   }
   size_t pindex = 0;
   while ( pindex < rman->nscount ) {
      (lognodelist + pindex)->name = strdup( (rman->nslist[pindex])->idstr );
      if ( (lognodelist + pindex)->name == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate NS ID String: \"%s\"\n", (rman->nslist[pindex])->idstr );
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
   // NOTE -- Once the table is initialized, this func won't bother to free it on error.
   //         That is now the caller's responsibility.
   DIR*  dirlist[3] = {0};
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
      LOG( LOG_ERR, "Failed to open loging root \"%s\" for scanning\n", scanroot );
      return -1;
   }
   size_t logcount = 0; // per iteration log count
   size_t logdepth = 1;
   char itercheck = 0;
   loginfo* linfo = NULL; // for caching a target location for new log request info
   while ( logdepth ) {
      if ( logdepth == 1 ) { // identify the iteration to tgt
         if ( rman->execprevroot ) {
            // check for potential exit
            if ( itercheck ) {
               LOG( LOG_INFO, "Preparing to exit, after completing scan of prevexec iteration: \"%s/%s\"\n", scanroot, rman->iteration );
               request.iteration[0] = '\0';
               logdepth--;
               continue;
            }
            // note a pass over this section, so we never repeat
            itercheck = 1;
            // only targeting a matching iteration
            if ( snprintf( request.iteration, ITERATION_STRING_LEN, "%s", rman->iteration ) < 1 ) {
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
               if ( snprintf( request.iteration, ITERATION_STRING_LEN, "%s", entry->d_name ) >= ITERATION_STRING_LEN ) {
                  LOG( LOG_ERR, "Failed to populate request string for old iteration: \"%s\"\n", entry->d_name );
                  return -1;
               }
               break;
            }
            if ( errno ) {
               LOG( LOG_ERR, "Failed to read log root dir: \"%s\" (%s)\n", scanroot );
               return -1;
            }
            errno = olderr;
            if ( entry == NULL ) {
               // no iterations remain, we are done
               closedir( dirlist[0] );
               dirlist[0] = NULL; // just to be explicit about clearing this value
               request.iteration[0] = '\0';
               logdepth--;
               LOG( LOG_INFO, "Preparing to exit, after completing scan of previous iterations under \"%s\"\n", scanroot );
               continue;
            }
         }
         // open the dir of the matching iteration
         int newfd = openat( dirfd( dirlist[0] ), request.iteration, O_DIRECTORY | O_RDONLY, 0 );
         if ( newfd < 0 ) {
            LOG( LOG_ERR, "Failed to open log root for iteration: \"%s\" (%s)\n", request.iteration, strerror(errno) );
            return -1;
         }
         // check if program args are compatible with this run
         if ( !(rman->execprevroot) ) {
            int sumfd = openat( newfd, "summary.log", O_RDONLY, 0 );
            if ( sumfd < 0 ) {
               LOG( LOG_ERR, "Failed to open summary log for old iteration: \"%s\" (%s)\n",
                    request.iteration, strerror(errno) );
               close( newfd );
               return -1;
            }
            FILE* summaryfile = fdopen( sumfd, "r" );
            if ( summaryfile == NULL ) {
               LOG( LOG_ERR, "Failed to open a file stream for summary log for old iteration: \"%s\" (%s)\n",
                    request.iteration, strerror(errno) );
               close( sumfd );
               close( newfd );
               return -1;
            }
            // parse args into a temporary state struct, for comparison
            rmanstate tmpstate;
            bzero( &(tmpstate), sizeof( struct rmanstate_struct ) );
            tmpstate.config = rman->config;
            int parseargres = parse_program_args( &(tmpstate), summaryfile );
            fclose( summaryfile );
            if ( parseargres > 0 ) {
               printf( "Skipping over previous iteration with incompatible config: \"%s\"\n", request.iteration );
               continue;
            }
            if ( parseargres ) {
               LOG( LOG_ERR, "Failed to parse arguments from summary log of old iteration: \"%s\" (%s)\n",
                    request.iteration, strerror(errno) );
               close( newfd );
               return -1;
            }
            // check if previous run args are a superset if this run's
            if ( tmpstate.gstate.thresh.gcthreshold > rman->gstate.thresh.gcthreshold  ||
                 tmpstate.gstate.thresh.repackthreshold > rman->gstate.thresh.repackthreshold  ||
                 tmpstate.gstate.thresh.rebuildthreshold > rman->gstate.thresh.rebuildthreshold  ||
                 tmpstate.gstate.thresh.cleanupthreshold > rman->gstate.thresh.cleanupthreshold  ||
                 tmpstate.gstate.dryrun != rman->gstate.dryrun ) {
               printf( "Skipping over previous iteration with incompatible arguments: \"%s\"\n", request.iteration );
               continue;
            }
         }
         dirlist[1] = fdopendir( newfd );
         if ( dirlist[1] == NULL ) {
            LOG( LOG_ERR, "Failed to open DIR reference for previous iteration log root: \"%s\" (%s)\n",
                 request.iteration, strerror(errno) );
            close( newfd );
            return -1;
         }
         logdepth++;
         LOG( LOG_INFO, "Entered iteration dir: \"%s\"\n", request.iteration );
         logcount = 0;
      }
      else if ( logdepth == 2 ) { // identify the NS subdirs
         // scan through the iteration dir, looking for namespaces
         int olderr = errno;
         errno = 0;
         struct dirent* entry;
         while ( (entry = readdir( dirlist[1] )) ) {
            // ignore '.'-prefixed entries
            if ( strncmp( entry->d_name, ".", 1 ) == 0 ) { continue; }
            // ignore the summary log
            if ( strcmp( entry->d_name, "summary.log" ) == 0 ) { continue; }
            // all other entries are assumed to be namespaces
            break;
         }
         if ( errno ) {
            LOG( LOG_ERR, "Failed to read iteration dir: \"%s\" (%s)\n", request.iteration, strerror(errno) );
            return -1;
         }
         errno = olderr;
         if ( entry == NULL ) {
            // no Namespaces remain, we are done
            if ( logcount == 0  &&  rman->execprevroot == NULL ) {
               // if this is a dead iteration, with no logs remaining, just delete it
               if ( unlinkat( dirfd( dirlist[1] ), "summary.log", 0 ) ) {
                  fprintf( stderr, "WARNING: Failed to remove summary log of previous iteration: \"%s\"\n", request.iteration );
               }
               else if ( unlinkat( dirfd( dirlist[0] ), request.iteration, AT_REMOVEDIR ) ) {
                  fprintf( stderr, "WARNING: Failed to remove previous iteration log root: \"%s\"\n", request.iteration );
               }
            }
            closedir( dirlist[1] );
            dirlist[1] = NULL; // just to be explicit about clearing this value
            request.nsindex = 0;
            logdepth--;
            LOG( LOG_INFO, "Finished processing iteration dir: \"%s\"\n", request.iteration );
            if ( logcount ) {
               printf( "This run will incorporate %zu logfiles from: \"%s/%s\"\n", logcount, scanroot, request.iteration );
            }
            continue;
         }
         // check what index this NS corresponds to
         char* oldnsid = strdup( entry->d_name );
         if ( oldnsid == NULL ) {
            LOG( LOG_ERR, "Failed to duplicate dirent name: \"%s\"\n", entry->d_name );
            return -1;
         }
         char* tmpparse = oldnsid;
         while ( *tmpparse != '\0' ) { if ( *tmpparse == '#' ) { *tmpparse = '/'; } tmpparse++; }
         HASH_NODE* lnode = NULL;
         if ( hash_lookup( rman->oldlogs, oldnsid, &(lnode) ) ) {
            // if we can't map this to a NS entry, just ignore it
            fprintf( stderr, "WARNING: Encountered resource logs from a previoius iteration ( \"%s\" )\n"
                             "         associated with an unrecognized namespace ( \"%s\" ).\n"
                             "         These logfiles will be ignored by the current run.\n"
                             "         See \"%s/%s/%s\"\n", request.iteration, entry->d_name, scanroot, request.iteration, entry->d_name );
            free( oldnsid );
         }
         else {
            free( oldnsid );
            // pull the NS index value from the corresponding hash node
            linfo = (loginfo*)( lnode->content );
            request.nsindex = linfo->nsindex;
            // open the corresponding subdir and traverse into it
            int newfd = openat( dirfd( dirlist[1] ), entry->d_name, O_DIRECTORY | O_RDONLY, 0 );
            if ( newfd < 0 ) {
               LOG( LOG_ERR, "Failed to open log NS subdir: \"%s/%s/%s\" (%s)\n", scanroot, request.iteration, entry->d_name, strerror(errno) );
               return -1;
            }
            dirlist[2] = fdopendir( newfd );
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
                 (rman->nslist[request.nsindex])->idstr, strerror(errno) );
            return -1;
         }
         errno = olderr;
         if ( entry == NULL ) {
            // no iterations remain, we are done
            closedir( dirlist[2] );
            dirlist[2] = NULL; // just to be explicit about clearing this value
            request.ranknum = 0;
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
                             scanroot, request.iteration, (rman->nslist[request.nsindex])->idstr, entry->d_name );
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
         LOG( LOG_INFO, "Detected old logfile for iteration \"%s\", NS \"%s\", Rank %zu\n",
              request.iteration, (rman->nslist[request.nsindex])->idstr, request.ranknum );
         *(linfo->requests + linfo->logcount) = request; // NOTE -- I believe this assignment is safe,
                                                         //         because .iteration is a static char array
         linfo->logcount++;
         logcount++;
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
 * @return int : Zero if the rank should send the populated response and terminate
 *               One if the rank should send the populated response and continue processing
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
   snprintf( response->errorstr, MAX_ERROR_BUFFER, "UNKNOWN-ERROR!\n" );
   // identify and process the request
   if ( request->type == RLOG_WORK ) {
      // potentially update our state to target the NS
      if ( rman->gstate.rlog == NULL ) {
         if ( setranktgt( rman, rman->nslist[request->nsindex], response ) ) {
            LOG( LOG_ERR, "Failed to update target of rank %zu to NS \"%s\"\n",
                          rman->ranknum, rman->nslist[request->nsindex]->idstr );
            // leave the errorstr alone, as the helper func will have left a more descriptive message
            return -1;
         }
      }
      // identify the path of the rlog
      const char* tmplogroot = rman->logroot;
      if ( rman->execprevroot ) { tmplogroot = rman->execprevroot; }
      char* rlogpath = resourcelog_genlogpath( 0, tmplogroot, request->iteration,
                                                  rman->nslist[request->nsindex], request->ranknum );
      if ( rlogpath == NULL ) {
         LOG( LOG_ERR, "Failed to generate logpath for NS \"%s\" ranknum \"%s\"\n",
                       rman->nslist[request->nsindex]->idstr, request->ranknum );
         snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to generate logpath for NS \"%s\" ranknum \"%zu\"\n",
                   rman->nslist[request->nsindex]->idstr, request->ranknum );
         return -1;
      }
      // determine what we're actually doing with this logfile
      if ( rman->execprevroot ) {
         // we are using the rlog as an input
         if ( resourceinput_setlogpath( &(rman->gstate.rinput), rlogpath ) ) {
            LOG( LOG_ERR, "Failed to update rank %zu input to logfile \"%s\"\n", rman->ranknum, rlogpath );
            snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to update rank %zu input to logfile \"%s\"\n",
                      rman->ranknum, rlogpath );
            return -1;
         }
         // wait for our input to be exhausted ( don't respond until we are ready for more work )
         if ( resourceinput_waitforcomp( &(rman->gstate.rinput) ) ) {
            LOG( LOG_ERR, "Failed to wait for completion of logfile \"%s\"\n", request->refdist );
            snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to wait for completion of logfile \"%s\"\n", rlogpath );
            return -1;
         }
      }
      else {
         // we are just cleaning up this old logfile by replaying it into our current one
         // open the specified rlog for read
         RESOURCELOG newlog = NULL;
         if ( resourcelog_init( &(newlog), rlogpath, RESOURCE_READ_LOG, rman->nslist[request->nsindex] ) ) {
            LOG( LOG_ERR, "Failed to open logfile for read: \"%s\" (%s)\n", rlogpath, strerror(errno) );
            snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to open logfile for read: \"%s\" (%s)\n",
                      rlogpath, strerror(errno) );
            free( rlogpath );
            return -1;
         }
         if ( resourcelog_replay( &(newlog), &(rman->gstate.rlog), NULL ) ) {
            LOG( LOG_ERR, "Failed to replay logfile \"%s\" into active state\n", rlogpath );
            snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to replay logfile \"%s\"\n", rlogpath );
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
         if ( setranktgt( rman, rman->nslist[request->nsindex], response ) ) {
            LOG( LOG_ERR, "Failed to update target of rank %zu to NS \"%s\"\n",
                          rman->ranknum, rman->nslist[request->nsindex]->idstr );
            // leave the errorstr alone, as the helper func will have left a more descriptive message
            return -1;
         }
      }
      // calculate the start and end reference indices for this request
      size_t refmin = -1;
      size_t refmax = -1;
      getNSrange( rman->nslist[request->nsindex], rman->workingranks, request->refdist, &(refmin), &(refmax) );
      // update our input to reference the new target range
      if ( resourceinput_setrange( &(rman->gstate.rinput), refmin, refmax ) ) {
         LOG( LOG_ERR, "Failed to set NS \"%s\" reference range values for distribution %zu\n",
              rman->nslist[request->nsindex]->idstr, request->refdist );
         snprintf( response->errorstr, MAX_ERROR_BUFFER,
                   "Failed to set NS \"%s\" reference range values for distribution %zu\n",
                   rman->nslist[request->nsindex]->idstr, request->refdist );
         return -1;
      }
      // wait for our input to be exhausted ( don't respond until we are ready for more work )
      if ( resourceinput_waitforcomp( &(rman->gstate.rinput) ) ) {
         LOG( LOG_ERR, "Failed to wait for completion of NS \"%s\" reference distribution %zu\n",
              rman->nslist[request->nsindex]->idstr, request->refdist );
         snprintf( response->errorstr, MAX_ERROR_BUFFER,
                   "Failed to wait for completion of NS \"%s\" reference distribution %zu\n",
                   rman->nslist[request->nsindex]->idstr, request->refdist );
         return -1;
      }
   }
   else if ( request->type == COMPLETE_WORK ) {
      // complete any outstanding work within the current NS
      if ( rman->gstate.rlog == NULL ) {
         LOG( LOG_ERR, "Rank %zu was asked to complete work, but has none\n", rman->ranknum );
         snprintf( response->errorstr, MAX_ERROR_BUFFER,
                   "Rank %zu was asked to complete work, but has none\n", rman->ranknum );
         return -1;
      }
      // terminate the resource input
      if ( rman->gstate.rinput  &&  resourceinput_term( &(rman->gstate.rinput) ) ) {
         LOG( LOG_ERR, "Failed to terminate resourceinput\n" );
         snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to terminate resourceinput\n" );
         return -1;
      }
      char threaderror = 0;
      if ( rman->tq ) {
         // wait for the queue to be marked as FINISHED
         TQ_Control_Flags setflags = 0;
         if ( tq_wait_for_flags( rman->tq, 0, &(setflags) ) ) {
            LOG( LOG_ERR, "Failed to wait on ThreadQueue state flags\n" );
            snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to wait on ThreadQueue state flags\n" );
            return -1;
         }
         if ( setflags != TQ_FINISHED ) {
            LOG( LOG_WARNING, "Unexpected ( NON-FINISHED ) ThreadQueue state flags: %u\n", setflags );
            threaderror = 1;
         }
         else {
            // wait for TQ completion
            if ( tq_wait_for_completion( rman->tq ) ) {
               LOG( LOG_ERR, "Failed to wait for ThreadQueue completion\n" );
               snprintf( response->errorstr, MAX_ERROR_BUFFER, "Failed to wait for ThreadQueue completion\n" );
               return -1;
            }
         }
         // gather all thread status values
         rthread_state* tstate = NULL;
         int retval;
         while ( (retval = tq_next_thread_status( rman->tq, (void**)&(tstate) )) > 0 ) {
            LOG( LOG_INFO, "Got state for Thread %zu\n", tstate->tID );
            // verify thread status
            if ( tstate == NULL ) {
               LOG( LOG_ERR, "Rank %zu encountered NULL thread state when completing NS \"%s\"\n",
                    rman->ranknum, rman->gstate.pos.ns->idstr );
               snprintf( response->errorstr, MAX_ERROR_BUFFER,
                         "Rank %zu encountered NULL thread state when completing NS \"%s\"\n",
                         rman->ranknum, rman->gstate.pos.ns->idstr );
               free( tstate );
               return -1;
            }
            if ( tstate->fatalerror ) {
               LOG( LOG_ERR, "Fatal Error in NS \"%s\": \"%s\"\n",
                    rman->gstate.pos.ns->idstr, tstate->errorstr );
               snprintf( response->errorstr, MAX_ERROR_BUFFER, "Fatal Error in NS \"%s\": \"%s\"\n",
                         rman->gstate.pos.ns->idstr, tstate->errorstr );
               free( tstate );
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
            free( tstate );
         }
         if ( retval ) {
            LOG( LOG_ERR, "Failed to collect thread states during completion of work on NS \"%s\"\n",
                 rman->gstate.pos.ns->idstr );
            snprintf( response->errorstr, MAX_ERROR_BUFFER,
                      "Failed to collect thread status during completion of work on NS \"%s\"\n",
                      rman->gstate.pos.ns->idstr );
            return -1;
         }
         response->haveinfo = 1; // note that we have info for the manager to process
         // close our the thread queue, repack streamer, and logs
         if ( tq_close( rman->tq ) ) {
            LOG( LOG_ERR, "Failed to close ThreadQueue after completion of work on NS \"%s\"\n",
                 rman->gstate.pos.ns->idstr );
            snprintf( response->errorstr, MAX_ERROR_BUFFER,
                      "Failed to close ThreadQueue after completion of work on NS \"%s\"\n",
                      rman->gstate.pos.ns->idstr );
            return -1;
         }
         rman->tq = NULL;
      }
      // need to preserve our logfile, allowing the manager to remove it
      char* outlogpath = resourcelog_genlogpath( 0, rman->logroot, rman->iteration, rman->gstate.pos.ns, rman->ranknum );
      int rlogret;
      if ( (rlogret = resourcelog_term( &(rman->gstate.rlog), &(response->summary), 0 )) < 0 ) {
         LOG( LOG_ERR, "Failed to terminate log \"%s\" following completion of work on NS \"%s\"\n",
              outlogpath, rman->gstate.pos.ns->idstr );
         snprintf( response->errorstr, MAX_ERROR_BUFFER,
                   "Failed to terminate log \"%s\" following completion of work on NS \"%s\"\n",
                   outlogpath, rman->gstate.pos.ns->idstr );
         free( outlogpath );
         return -1;
      }
      free( outlogpath );
      if (rlogret) { response->errorlog = 1; } // note if our log was preserved due to errors being present
      if ( rman->gstate.rpst ) {
         if ( repackstreamer_complete( rman->gstate.rpst ) ) {
            LOG( LOG_ERR, "Failed to complete repack streamer during completion of NS \"%s\"\n", rman->gstate.pos.ns->idstr );
            snprintf( response->errorstr, MAX_ERROR_BUFFER,
                      "Failed to complete repack streamer during completion of NS \"%s\"\n", rman->gstate.pos.ns->idstr );
            return -1;
         }
         rman->gstate.rpst = NULL;
      }
      const char* nsidstr = rman->gstate.pos.ns->idstr;
      if ( config_abandonposition( &(rman->gstate.pos) ) ) {
         LOG( LOG_ERR, "Failed to abandon marfs position during completion of NS \"%s\"\n", nsidstr );
         snprintf( response->errorstr, MAX_ERROR_BUFFER,
                   "Failed to abandon marfs position during completion of NS \"%s\"\n", nsidstr );
         return -1;
      }
      if ( threaderror ) {
         snprintf( response->errorstr, MAX_ERROR_BUFFER, "ThreadQueue had unexpected termination flags\n" );
         return -1;
      }
   }
   else if ( request->type == TERMINATE_WORK ) {
      // simply note and terminate
      response->fatalerror = 0;
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
 * Process the given response
 * @param rmanstate* rman : Rank state
 * @param size_t ranknum : Responding rank number
 * @param workresponse* response : Response to process
 * @param workrequest* request : New request to populate
 * @return int : A positive value if a new request has been populated;
 *               0 if no request should be sent ( rank has exited, or hit a fatal error );
 *               -1 if a fatal error has occurred in this function itself ( invalid processing )
 */
int handleresponse( rmanstate* rman, size_t ranknum, workresponse* response, workrequest* request ) {
   // check for a fatal error condition, as this overrides all other behaviors
   if ( response->fatalerror ) {
      // note a fatalerror, and don't generate a request
      fprintf( stderr, "ERROR: Rank %zu%s hit a fatal error condition\n"
                       "       All ranks will now attempt to terminate\n"
                       "       Work Type --\n"
                       "         %s\n"
                       "       Error Description --\n"
                       "         \"%s\"\n",
                       ranknum, (ranknum == rman->ranknum) ? " ( this rank )" : "",
                       ( response->request.type == RLOG_WORK )      ? "Resource Log Processing" :
                       ( response->request.type == NS_WORK )        ? "Namespace Processing"    :
                       ( response->request.type == COMPLETE_WORK )  ? "Namespace Finalization"  :
                       ( response->request.type == TERMINATE_WORK ) ? "Termination"             :
                       ( response->request.type == ABORT_WORK )     ? "Abort Condition"         :
                       "UNKNOWN WORK TYPE",
                       response->errorstr );
      rman->terminatedworkers[ranknum] = 1;
      return 0;
   }
   if ( rman->fatalerror  &&  (response->request.type != ABORT_WORK) ) {
      // if we are in an error state, all ranks should be signaled to abort
      LOG( LOG_INFO, "Signaling Rank %zu to Abort\n", ranknum );
      request->type = ABORT_WORK;
      request->nsindex = 0;
      request->refdist = 0;
      request->iteration[0] = '\0';
      request->ranknum = 0;
      return 1;
   }
   // handle the response based on type
   if ( response->request.type == TERMINATE_WORK  ||  response->request.type == ABORT_WORK ) {
      // these simply indicate that the rank has terminated
      rman->terminatedworkers[ranknum] = 1;
      printf( "  Rank %zu is terminating\n", ranknum );
      return 0;
   }
   else if ( response->request.type == COMPLETE_WORK ) {
      if ( response->request.nsindex != rman->nscount ) { // only perform processing / cleanup for real completions
         printf( "  Rank %zu completed work on NS \"%s\"\n", ranknum, rman->nslist[response->request.nsindex]->idstr );
         // possibly process info from the rank
         if ( response->haveinfo ) {
            // incorporate walk report
            rman->walkreport[response->request.nsindex].fileusage   += response->report.fileusage;
            rman->walkreport[response->request.nsindex].byteusage   += response->report.byteusage;
            rman->walkreport[response->request.nsindex].filecount   += response->report.filecount;
            rman->walkreport[response->request.nsindex].objcount    += response->report.objcount;
            rman->walkreport[response->request.nsindex].bytecount   += response->report.bytecount;
            rman->walkreport[response->request.nsindex].streamcount += response->report.streamcount;
            rman->walkreport[response->request.nsindex].delobjs     += response->report.delobjs;
            rman->walkreport[response->request.nsindex].delfiles    += response->report.delfiles;
            rman->walkreport[response->request.nsindex].delstreams  += response->report.delstreams;
            rman->walkreport[response->request.nsindex].volfiles    += response->report.volfiles;
            rman->walkreport[response->request.nsindex].rpckfiles   += response->report.rpckfiles;
            rman->walkreport[response->request.nsindex].rpckbytes   += response->report.rpckbytes;
            rman->walkreport[response->request.nsindex].rbldobjs    += response->report.rbldobjs;
            rman->walkreport[response->request.nsindex].rbldbytes   += response->report.rbldbytes;
            // incorporate log summary
            rman->logsummary[response->request.nsindex].deletion_object_count += response->summary.deletion_object_count;
            rman->logsummary[response->request.nsindex].deletion_object_failures += response->summary.deletion_object_failures;
            rman->logsummary[response->request.nsindex].deletion_reference_count += response->summary.deletion_reference_count;
            rman->logsummary[response->request.nsindex].deletion_reference_failures += response->summary.deletion_reference_failures;
            rman->logsummary[response->request.nsindex].rebuild_count += response->summary.rebuild_count;
            rman->logsummary[response->request.nsindex].rebuild_failures += response->summary.rebuild_failures;
            rman->logsummary[response->request.nsindex].repack_count += response->summary.repack_count;
            rman->logsummary[response->request.nsindex].repack_failures += response->summary.repack_failures;
            // output all gathered info, prior to possible log deletion
            outputinfo( rman->summarylog, rman->nslist[response->request.nsindex], &(response->report) , &(response->summary) );
         }
         // identify the output log of the transmitting rank
         char* outlogpath = resourcelog_genlogpath( 0, rman->logroot, rman->iteration,
                                                    rman->nslist[response->request.nsindex], ranknum );
         if ( outlogpath == NULL ) {
            fprintf( stderr, "ERROR: Failed to identify the output log path of Rank %zu for NS \"%s\"\n",
                     ranknum, rman->nslist[response->request.nsindex]->idstr );
            rman->fatalerror = 1;
            return -1;
         }
         // potentially duplicate the logfile to a final location
         if ( rman->preservelogtgt ) {
            char* preslogpath = resourcelog_genlogpath( 1, rman->preservelogtgt, rman->iteration,
                                                        rman->nslist[response->request.nsindex], ranknum );
            if ( preslogpath == NULL ) {
               fprintf( stderr, "ERROR: Failed to identify the preserve log path of Rank %zu for NS \"%s\"\n",
                        ranknum, rman->nslist[response->request.nsindex]->idstr );
               free( outlogpath );
               rman->fatalerror = 1;
               return -1;
            }
            // simply use a hardlink for this purpose, no need to make a 'real' duplicate
            if ( link( outlogpath, preslogpath ) ) {
               fprintf( stderr, "ERROR: Failed to link logfile \"%s\" to new target path: \"%s\"\n",
                        outlogpath, preslogpath );
               free( preslogpath );
               free( outlogpath );
               rman->fatalerror = 1;
               return -1;
            }
            free( preslogpath );
         }
         // open the logfile for read
         RESOURCELOG ranklog = NULL;
         if ( resourcelog_init( &(ranklog), outlogpath, RESOURCE_READ_LOG, rman->nslist[response->request.nsindex] ) ) {
            fprintf( stderr, "ERROR: Failed to open the output log of Rank %zu for NS \"%s\": \"%s\"\n",
                     ranknum, rman->nslist[response->request.nsindex]->idstr, outlogpath );
            free( outlogpath );
            rman->fatalerror = 1;
            return -1;
         }
         // process the work log
         if ( response->errorlog ) {
            // identify the errorlog output location
            char* errlogpath = resourcelog_genlogpath( 1, rman->logroot, rman->iteration,
                                                        rman->nslist[response->request.nsindex], ranknum );
            if ( errlogpath == NULL ) {
               fprintf( stderr, "ERROR: Failed to identify the error log path of Rank %zu for NS \"%s\"\n",
                        ranknum, rman->nslist[response->request.nsindex]->idstr );
               resourcelog_term( &(ranklog), NULL, 0 );
               free( outlogpath );
               rman->fatalerror = 1;
               return -1;
            }

            // open the error log for write
            RESOURCELOG errlog = NULL;
            if ( resourcelog_init( &(errlog), errlogpath, RESOURCE_RECORD_LOG, rman->nslist[response->request.nsindex] ) ) {
               fprintf( stderr, "ERROR: Failed to open the error log of Rank %zu: \"%s\"\n", ranknum, errlogpath );
               resourcelog_term( &(ranklog), NULL, 0 );
               free( errlogpath );
               free( outlogpath );
               rman->fatalerror = 1;
               return -1;
            }
            // replay the logfile into the error log location
            if ( resourcelog_replay( &(ranklog), &(errlog), error_only_filter ) ) {
               fprintf( stderr, "ERROR: Failed to replay errors from logfile \"%s\" into \"%s\"\n", outlogpath, errlogpath );
               resourcelog_term( &(errlog), NULL, 1 );
               resourcelog_term( &(ranklog), NULL, 0 );
               free( errlogpath );
               free( outlogpath );
               rman->fatalerror = 1;
               return -1;
            }
            if ( resourcelog_term( &(errlog), NULL, 0 ) ) {
               fprintf( stderr, "ERROR: Failed to finalize error logfile: \"%s\"\n", errlogpath );
               free( errlogpath );
               free( outlogpath );
               rman->fatalerror = 1;
               return -1;
            }
            free( errlogpath );
         }
         else {
            // simply delete the output logfile of this rank
            if ( resourcelog_term( &(ranklog), NULL, 1 ) ) {
               fprintf( stderr, "ERROR: Failed to delete the output log of Rank %zu for NS \"%s\": \"%s\"\n",
                        ranknum, rman->nslist[response->request.nsindex]->idstr, outlogpath );
               free( outlogpath );
               rman->fatalerror = 1;
               return -1;
            }
         }
         free( outlogpath );
      } // end of response content processing
      // this rank needs work to process, with no preference for NS
      if ( rman->oldlogs ) {
         // start by checking for old resource logs to process
         HASH_NODE* resnode = NULL;
         if ( hash_lookup( rman->oldlogs, "irrelevant, as we just need a point to iterate from", &(resnode) ) < 0 ) {
            fprintf( stderr, "ERROR: Failure of hash_lookup() when trying to identify logfiles for rank %zu to process\n",
                             ranknum );
            rman->fatalerror = 1;
            return -1;
         }
         while ( resnode ) {
            // check for any nodes with log processing requests remaining
            loginfo* linfo = (loginfo*)( resnode->content );
            if ( linfo->logcount ) {
               // use the request at the tail of our list
               LOG( LOG_INFO, "Passing out log %zu for NS \"%s\" from iteration \"%s\"\n",
                    linfo->logcount,
                    rman->nslist[linfo->nsindex]->idstr,
                    (linfo->requests + (linfo->logcount - 1))->iteration );
               *request = *(linfo->requests + (linfo->logcount - 1));
               linfo->logcount--;
               return 1;
            }
            if ( hash_iterate( rman->oldlogs, &(resnode) ) < 0 ) {
               fprintf( stderr,
                        "ERROR: Failure of hash_iterate() when trying to identify logfiles for rank %zu to process\n",
                        ranknum );
               rman->fatalerror = 1;
               return -1;
            }
         }
         printf( "  -- All old logfiles have been handed out for processing --\n" );
         // it seems we've iterated over our entire oldlogs table, with no requests remaining
         // need to free our table
         size_t ncount = 0;
         if ( hash_term( rman->oldlogs, &(resnode), &(ncount) ) ) {
            fprintf( stderr, "ERROR: Failure of hash_term() after passing out all logfiles for processing\n" );
            rman->fatalerror = 1;
            return -1;
         }
         rman->oldlogs = NULL;
         // free all subnodes and requests
         size_t nindex = 0;
         for ( ; nindex < ncount; nindex++ ) {
            if ( (resnode+nindex)->name ) { free( (resnode+nindex)->name ); }
            loginfo* linfo = (loginfo*)( (resnode+nindex)->content );
            if ( linfo->requests ) { free( linfo->requests ); }
         }
         free( resnode ); // these were allocated in one block, and thus require only one free()
      }
      // no resource logs remain to process
      if ( rman->execprevroot ) {
         // if we are picking up a previous run, this means no more work remains at all
         LOG( LOG_INFO, "Signaling Rank %zu to terminate, as no resource logs remain\n", ranknum );
         request->type = TERMINATE_WORK;
         return 1;
      }
      // first, check specifically for NSs that have yet to be processed at all
      size_t nsindex = 0;
      for ( ; nsindex < rman->nscount; nsindex++ ) {
         if ( rman->distributed[nsindex] == 0 ) {
            // this NS still has yet to be worked on at all
            request->type = NS_WORK;
            request->nsindex = nsindex;
            request->refdist = rman->distributed[nsindex];
            request->iteration[0] = '\0';
            request->ranknum = ranknum;
            rman->distributed[nsindex]++; // note newly distributed range
            LOG( LOG_INFO, "Passing out reference range %zu of NS \"%s\" to Rank %zu\n",
                 rman->distributed[nsindex], rman->nslist[nsindex]->idstr, ranknum );
            printf( "  Rank %zu is beginning work on NS \"%s\" ( ref range %zu )\n",
                    ranknum, rman->nslist[nsindex]->idstr, rman->distributed[nsindex] );
            return 1;
         }
      }
      // next, check for NSs with ANY remaining work to distribute
      nsindex = 0;
      for ( ; nsindex < rman->nscount; nsindex++ ) {
         if ( rman->distributed[nsindex] < rman->workingranks ) {
            // this NS still has reference ranges to be scanned
            request->type = NS_WORK;
            request->nsindex = nsindex;
            request->refdist = rman->distributed[nsindex];
            request->iteration[0] = '\0';
            request->ranknum = ranknum;
            rman->distributed[nsindex]++; // note newly distributed range
            LOG( LOG_INFO, "Passing out reference range %zu of NS \"%s\" to Rank %zu\n",
                 rman->distributed[nsindex], rman->nslist[nsindex]->idstr, ranknum );
            printf( "  Rank %zu is picking up work on NS \"%s\" ( ref range %zu )\n",
                    ranknum, rman->nslist[nsindex]->idstr, rman->distributed[nsindex] );
            // check through remaining namespaces for any undistributed work
            for ( ; nsindex < rman->nscount; nsindex++ ) {
               if ( rman->distributed[nsindex] < rman->workingranks ) { break; }
            }
            if ( nsindex == rman->nscount ) {
               // just handed out the last NS ref range for processing
               printf( "  -- All NS reference ranges have been handed out for processing --\n" );
            }
            return 1;
         }
      }
      // no NS reference ranges remain to process, so we now need to signal termination
      LOG( LOG_INFO, "Signaling Rank %zu to terminate, as no NS reference ranges remain\n", ranknum );
      request->type = TERMINATE_WORK;
      return 1;
   }
   else if ( response->request.type == NS_WORK  ||  response->request.type == RLOG_WORK ) {
      // this rank needs work to process, specifically in the same NS
      if ( rman->oldlogs ) {
         // start by checking for old resource logs to process
         HASH_NODE* resnode = NULL;
         if ( hash_lookup( rman->oldlogs, rman->nslist[response->request.nsindex]->idstr, &(resnode) ) ) {
            fprintf( stderr, "ERROR: Failure of hash_lookup() when looking for logfiles in NS \"%s\" for rank %zu\n",
                             rman->nslist[response->request.nsindex]->idstr, ranknum );
            rman->fatalerror = 1;
            return -1;
         }
         // check for any nodes with log processing requests remaining
         loginfo* linfo = (loginfo*)( resnode->content );
         if ( linfo->logcount ) {
            // use the request at the tail of our list
            LOG( LOG_INFO, "Passing out log %zu for NS \"%s\" from iteration \"%s\"\n",
                 linfo->logcount,
                 rman->nslist[linfo->nsindex]->idstr,
                 (linfo->requests + (linfo->logcount - 1))->iteration );
            *request = *(linfo->requests + (linfo->logcount - 1));
            linfo->logcount--;
            return 1;
         }
      }
      // no resource logs remain to process in the active NS of this rank
      if ( rman->execprevroot ) {
         // if we are picking up a previous run, this means no more work remains for the active NS at all
         LOG( LOG_INFO, "Signaling Rank %zu to complete and quiesce, as no resource logs remain in NS \"%s\"\n",
              ranknum, rman->nslist[response->request.nsindex]->idstr );
         *request = response->request; // populate request with active NS values
         request->type = COMPLETE_WORK;
         return 1;
      }
      // check for any remaining work in the rank's active NS
      if ( rman->distributed[response->request.nsindex] < rman->workingranks ) {
         request->type = NS_WORK;
         request->nsindex = response->request.nsindex;
         request->refdist = rman->distributed[response->request.nsindex];
         request->iteration[0] = '\0';
         request->ranknum = ranknum;
         rman->distributed[response->request.nsindex]++; // note newly distributed range
         LOG( LOG_INFO, "Passing out reference range %zu of NS \"%s\" to Rank %zu\n",
              rman->distributed[response->request.nsindex], rman->nslist[response->request.nsindex]->idstr, ranknum );
         return 1;
      }
      // all work in the active NS has been completed
      LOG( LOG_INFO, "Signaling Rank %zu to complete and quiesce, as no reference ranges remain in NS \"%s\"\n",
           ranknum, rman->nslist[response->request.nsindex]->idstr );
      *request = response->request; // populate request with active NS values
      request->type = COMPLETE_WORK;
      return 1;
   }

   LOG( LOG_ERR, "Encountered unrecognized response type\n" );
   fprintf( stderr, "ERROR: Encountered response to an unknown request type\n" );
   rman->fatalerror = 1;
   return -1;
}


//   -------------   CORE BEHAVIOR LOOPS   -------------

/**
 * Manager rank behavior ( sending out requests, potentially processing them as well )
 * @param rmanstate* rman : Resource manager state
 * @return int : Zero on success, or -1 on failure
 */
int managerbehavior( rmanstate* rman ) {
   // setup out response and request structs
   workresponse response;
   bzero( &(response), sizeof( struct workresponse_struct ) );
   size_t respondingrank = 0;
   workrequest  request;
   bzero( &(request), sizeof( struct workrequest_struct ) );
   if ( rman->totalranks == 1 ) {
      // we need to fake our own 'startup' response
      response.request.type = COMPLETE_WORK;
      response.request.nsindex = rman->nscount;
   }
   // loop until all workers have terminated
   char workersrunning = 1;
   while ( workersrunning ) {
      // begin by getting a response from a worker via MPI
      if ( rman->totalranks > 1 ) {
         MPI_Status msgstatus;
         if ( MPI_Recv( &(response), sizeof( struct workresponse_struct), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &(msgstatus) ) ) {
            LOG( LOG_ERR, "Failed to recieve a response\n" );
            fprintf( stderr, "ERROR: Failed to receive an MPI response message\n" );
            return -1;
         }
         respondingrank = msgstatus.MPI_SOURCE;
      }
      // generate an appropriate request, based on response
      int handleres = handleresponse( rman, respondingrank, &(response), &(request) );
      if ( handleres < 0 ) {
         fprintf( stderr, "Fatal error detected during response handling.  Program will terminate.\n" );
         return -1;
      }
      // send out a new request, if appropriate
      if ( handleres ) {
         if ( rman->totalranks > 1 ) {
            // send out the request via MPI to the responding rank
            if ( MPI_Send( &(request), sizeof(struct workrequest_struct), MPI_BYTE, respondingrank, 0, MPI_COMM_WORLD ) ) {
               LOG( LOG_ERR, "Failed to send a request\n" );
               fprintf( stderr, "ERROR: Failed to send an MPI request message\n" );
               return -1;
            }
         }
         else {
            // just process the new request ourself
            if ( handlerequest( rman, &(request), &(response) ) < 0 ) {
               fprintf( stderr, "ERROR: %s\nFatal error detected during local request processing.  Program will terminate.\n",
                        response.errorstr );
               return -1;
            }
         }
      }
      // check worker states
      workersrunning = 0; // assume none, until found
      size_t windex = ( rman->totalranks > 1 ) ? 1 : 0;
      for ( ; windex < rman->totalranks; windex++ ) {
         if ( rman->terminatedworkers[windex] == 0 ) { workersrunning = 1; break; }
      }
   }
   printf( "\n" );
   // loop over all namespaces
   size_t nsindex = 0;
   for ( ; nsindex < rman->nscount; nsindex++ ) {
      marfs_ns* curns = rman->nslist[nsindex];
      // potentially set NS quota values
      if ( rman->quotas ) {
         // update position value
         if ( config_establishposition( &(rman->gstate.pos), rman->config ) ) {
            LOG( LOG_ERR, "Failed to establish a rootNS position\n" );
            fprintf( stderr, "ERROR: Failed to establish a rootNS position\n" );
            return -1;
         }
         char* tmpnspath = NULL;
         if ( config_nsinfo( curns->idstr, NULL, &(tmpnspath) ) ) {
            LOG( LOG_ERR, "Failed to identify NS path of NS \"%s\"\n", curns->idstr );
            fprintf( stderr, "ERROR: Failed to identify NS path of NS \"%s\"\n", curns->idstr );
            return -1;
         }
         char* nspath = strdup( tmpnspath+1 ); // strip off leading '/', to get a relative NS path
         free( tmpnspath );
         if ( config_traverse( rman->config, &(rman->gstate.pos), &(nspath), 0 ) ) {
            LOG( LOG_ERR, "Failed to traverse config to new NS path: \"%s\"\n", nspath );
            fprintf( stderr, "ERROR: Failed to traverse config to new NS path: \"%s\"\n", nspath );
            free( nspath );
            return -1;
         }
         free( nspath );
         if ( rman->gstate.pos.ctxt == NULL  &&  config_fortifyposition( &(rman->gstate.pos) ) ) {
            LOG( LOG_ERR, "Failed to fortify position for new NS: \"%s\"\n", curns->idstr );
            fprintf( stderr, "ERROR: Failed to fortify position for new NS: \"%s\"\n", curns->idstr );
            config_abandonposition( &(rman->gstate.pos) );
            return -1;
         }
         // update quota values based on report totals
         MDAL nsmdal = curns->prepo->metascheme.mdal;
         if ( nsmdal->setinodeusage( rman->gstate.pos.ctxt, rman->walkreport[nsindex].fileusage ) ) {
            fprintf( stderr, "WARNING: Failed to set inode usage for NS \"%s\"\n", curns->idstr );
         }
         if ( nsmdal->setdatausage( rman->gstate.pos.ctxt, rman->walkreport[nsindex].byteusage ) ) {
            fprintf( stderr, "WARNING: Failed to set data usage for NS \"%s\"\n", curns->idstr );
         }
         config_abandonposition( &(rman->gstate.pos) );
      }
      // print out NS info
      outputinfo( stdout, curns, rman->walkreport + nsindex, rman->logsummary + nsindex );
   }
   if ( rman->fatalerror ) { return -1; } // skip final log cleanup if we hit some crucial error
   // final cleanup
   char* iterationroot = resourcelog_genlogpath( 0, rman->logroot, rman->iteration, NULL, -1 );
   if ( iterationroot == NULL ) {
      fprintf( stderr, "ERROR: Failed to identify iteration path of this run for final cleanup\n" );
      return -1;
   }
   size_t sumstrlen = strlen( iterationroot ) + 1 + strlen( "summary.log" );
   char* sumlogpath = calloc( sizeof(char), sumstrlen + 1 );
   if ( sumlogpath == NULL ) {
      fprintf( stderr, "ERROR: Failed to identify summary log path of this run for final cleanup\n" );
      free( iterationroot );
      return -1;
   }
   snprintf( sumlogpath, sumstrlen + 1, "%s/%s", iterationroot, "summary.log" );
   // potentially duplicate our summary log to a final location
   if ( rman->preservelogtgt ) {
      char* presiterroot = resourcelog_genlogpath( 0, rman->preservelogtgt, rman->iteration, NULL, -1 );
      if ( presiterroot == NULL ) {
         fprintf( stderr, "ERROR: Failed to identify iteration preservation path of this run for final cleanup\n" );
         free( sumlogpath );
         free( iterationroot );
         return -1;
      }
      size_t presstrlen = strlen( presiterroot ) + 1 + strlen( "summary.log" );
      char* preslogpath = calloc( sizeof(char), presstrlen + 1 );
      if ( preslogpath == NULL ) {
         fprintf( stderr, "ERROR: Failed to identify summary log path of this run for final cleanup\n" );
         free( presiterroot );
         free( sumlogpath );
         free( iterationroot );
         return -1;
      }
      snprintf( preslogpath, presstrlen + 1, "%s/%s", presiterroot, "summary.log" );
      free( presiterroot );
      // simply use a hardlink for this purpose, no need to make a 'real' duplicate
      if ( link( sumlogpath, preslogpath ) ) {
         fprintf( stderr, "ERROR: Failed to link summary logfile \"%s\" to new target path: \"%s\"\n",
                  sumlogpath, preslogpath );
         free( preslogpath );
         free( sumlogpath );
         free( iterationroot );
         return -1;
      }
      free( preslogpath );
   }
   // unlink our summary logfile
   if ( unlink( sumlogpath ) ) {
      // just complain
      fprintf( stderr, "WARNING: Failed to unlink summary log path during final cleanup\n" );
   }
   free( sumlogpath );
   // attempt to unlink our iteration dir
   if ( rmdir( iterationroot ) ) {
      // just complain
      fprintf( stderr, "WARNING: Failed to unlink iteration root: \"%s\" (%s)\n", iterationroot, strerror(errno) );
   }
   free( iterationroot );
   // potentially cleanup the previous run's summary log and iteration dir
   if ( rman->execprevroot ) {
      iterationroot = resourcelog_genlogpath( 0, rman->execprevroot, rman->iteration, NULL, -1 );
      if ( iterationroot == NULL ) {
         fprintf( stderr, "ERROR: Failed to identify iteration path of the previous run for final cleanup\n" );
         return -1;
      }
      sumstrlen = strlen( iterationroot ) + 1 + strlen( "summary.log" );
      sumlogpath = calloc( sizeof(char), sumstrlen + 1 );
      if ( sumlogpath == NULL ) {
         fprintf( stderr, "ERROR: Failed to identify summary log path of the previous run for final cleanup\n" );
         free( iterationroot );
         return -1;
      }
      snprintf( sumlogpath, sumstrlen + 1, "%s/%s", iterationroot, "summary.log" );
      if ( unlink( sumlogpath ) ) {
         // just complain
         fprintf( stderr, "WARNING: Failed to unlink summary log path of previous run during final cleanup\n" );
      }
      free( sumlogpath );
      if ( rmdir( iterationroot ) ) {
         // just complain
         fprintf( stderr, "WARNING: Failed to unlink iteration root of previous run: \"%s\" (%s)\n", iterationroot, strerror(errno) );
      }
      free( iterationroot );
   }
   // all work should now be complete
   return 0;
}

/**
 * Worker rank behavior ( processing requests and sending responses )
 * @param rmanstate* rman : Resource manager state
 * @return int : Zero on success, or -1 on failure
 */
int workerbehavior( rmanstate* rman ) {
   // setup out response and request structs
   workresponse response;
   bzero( &(response), sizeof( struct workresponse_struct ) );
   workrequest  request;
   bzero( &(request), sizeof( struct workrequest_struct ) );
   // we need to fake a 'startup' response
   response.request.type = COMPLETE_WORK;
   response.request.nsindex = rman->nscount;
   // begin by sending a response
   if ( MPI_Send( &(response), sizeof(struct workresponse_struct), MPI_BYTE, 0, 0, MPI_COMM_WORLD ) ) {
      LOG( LOG_ERR, "Failed to send initial 'dummy' response\n" );
      fprintf( stderr, "ERROR: Failed to send an initial MPI response message\n" );
      return -1;
   }
   // loop until we process a TERMINATE request
   int handleres = 1;
   while ( handleres ) {
      // wait for a new request
      if ( MPI_Recv( &(request), sizeof( struct workrequest_struct), MPI_BYTE, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE ) ) {
         LOG( LOG_ERR, "Failed to recieve a new request\n" );
         fprintf( stderr, "ERROR: Failed to receive an MPI request message\n" );
         return -1;
      }
      // generate an appropriate response
      if ( (handleres = handlerequest( rman, &(request), &(response) )) < 0 ) {
         LOG( LOG_ERR, "Fatal error detected during request processing: \"%s\"\n", response.errorstr );
         // send out our response anyway, so the manger prints our error message
         MPI_Send( &(response), sizeof(struct workresponse_struct), MPI_BYTE, 0, 0, MPI_COMM_WORLD );
         return -1;
      }
      // send out our response
      if ( MPI_Send( &(response), sizeof(struct workresponse_struct), MPI_BYTE, 0, 0, MPI_COMM_WORLD ) ) {
         LOG( LOG_ERR, "Failed to send a response\n" );
         fprintf( stderr, "ERROR: Failed to send an MPI response message\n" );
         return -1;
      }
   }
   // all work should now be complete
   return 0;
}


//   -------------    STARTUP BEHAVIOR     -------------


int main(int argc, char** argv) {
   // Initialize MPI
   if ( MPI_Init(&argc,&argv) ) {
      fprintf( stderr, "ERROR: Failed to initialize MPI\n" );
      return -1;
   }

   errno = 0; // init to zero (apparently not guaranteed)
   char* config_path = getenv( "MARFS_CONFIG_PATH" ); // check for config env var
   char* ns_path = ".";
   char recurse = 0;
   rmanstate rman;
   bzero( &(rman), sizeof( struct rmanstate_struct ) );
   rman.gstate.numprodthreads = DEFAULT_PRODUCER_COUNT;
   rman.gstate.numconsthreads = DEFAULT_CONSUMER_COUNT;

   // get the initialization time of the program, to identify thresholds
   struct timeval currenttime;
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk\n" );
      return -1;
   }
   time_t gcthresh = currenttime.tv_sec - GC_THRESH;
   time_t rblthresh = currenttime.tv_sec - RB_L_THRESH;
   time_t rbmthresh = currenttime.tv_sec - RB_M_THRESH;
   time_t rpthresh = currenttime.tv_sec - RP_THRESH;
   time_t clthresh = currenttime.tv_sec - CL_THRESH;

   // parse all position-independent arguments
   char pr_usage = 0;
   int c;
   while ((c = getopt(argc, (char* const*)argv, "c:n:ri:l:p:dX:QGRPCT:L:h")) != -1) {
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
         if ( strlen(optarg) >= ITERATION_STRING_LEN ) {
            fprintf( stderr, "ERROR: Iteration string exceeds allocated length %u: \"%s\"\n", ITERATION_STRING_LEN, optarg );
            errno = ENAMETOOLONG;
            return -1;
         }
         snprintf( rman.iteration, ITERATION_STRING_LEN, "%s", optarg );
         break;
      case 'l':
         rman.logroot = optarg;
         break;
      case 'p':
         rman.preservelogtgt = optarg;
         break;
      case 'd':
         rman.gstate.dryrun = 1;
         break;
      case 'X':
         rman.execprevroot = optarg;
         break;
      case 'Q':
         rman.quotas = 1;
         break;
      case 'G':
         rman.gstate.thresh.gcthreshold = 1;
         break;
      case 'R':
         rman.gstate.thresh.rebuildthreshold = 1;
         break;
      case 'P':
         rman.gstate.thresh.repackthreshold = 1;
         break;
      case 'C':
         rman.gstate.thresh.cleanupthreshold = 1;
         break;
      case 'T':
         {
         char* threshparse = optarg;
         char foundtval = 0;
         char tflag = '\0';
         while ( threshparse  &&  *threshparse != ' '  &&  *threshparse != '\0' ) {
            if ( tflag == '\0' ) {
               // check for a Threshold type flag
               if ( *threshparse == 'G'  ||  *threshparse == 'R'  ||  *threshparse == 'P'  ||  *threshparse == 'C' ) {
                  tflag = *threshparse;
               }
               else if ( *threshparse != '-' ) { // ignore '-' chars
                  printf( "ERROR: Failed to parse '-T' argument value: \"%s\"\n", optarg );
                  pr_usage = 1;
                  break;
               }
            }
            else {
               // parse the expected numeric value trailing the type flag
               char* endptr = NULL;
               unsigned long long parseval = strtoull( threshparse, &(endptr), 10 );
               if ( parseval == ULLONG_MAX  ||  endptr == NULL  ||
                    ( *endptr != 's'  &&  *endptr != 'm'  &&  *endptr != 'h'  &&  *endptr != 'd'  &&
                      *endptr != '-'  &&  *endptr != '\0' ) ) {
                  printf( "ERROR: Failed to parse '%c' threshold from '-T' argument value: \"%s\"\n", tflag, optarg );
                  pr_usage = 1;
                  break;
               }
               if ( *endptr == 'm' ) { parseval *= 60; }
               else if ( *endptr == 'h' ) { parseval *= 60 * 60; }
               else if ( *endptr == 'd' ) { parseval *= 60 * 60 * 24; }
               // actually asign the parsed value to the appropriate threshold
               if ( tflag == 'G' ) {
                  gcthresh = currenttime.tv_sec - parseval;
               }
               else if ( tflag == 'R' ) {
                  rblthresh = currenttime.tv_sec - parseval;
                  rbmthresh = currenttime.tv_sec - parseval;
               }
               else if ( tflag == 'P' ) {
                  rpthresh = currenttime.tv_sec - parseval;
               }
               else if ( tflag == 'C' ) {
                  clthresh = currenttime.tv_sec - parseval;
               }
               foundtval = 1;
               tflag = '\0';
               // check for early termination
               if ( *endptr == '\0'  ||  *endptr == ' ' ) {
                  break;
               }
               threshparse = endptr;
            }
            threshparse++;
         }
         if ( pr_usage == 0  &&  (threshparse == NULL  ||  foundtval == 0) ) {
            printf( "ERROR: Failed to parse '-T' argument value: \"%s\"\n", optarg );
            pr_usage = 1;
            break;
         }
         break;
         }
      case '?':
         printf( "ERROR: Unrecognized cmdline argument: \'%c\'\n", optopt );
      case 'h': // note fallthrough from above
         pr_usage = 1;
         break;
      default:
         printf("ERROR: Failed to parse command line options\n");
         return -1;
      }
   }
   if ( pr_usage ) {
      print_usage_info();
      return -1;
   }

   // validate arguments
   if ( rman.execprevroot ) {
      // check if we were incorrectly passed any args
      if ( rman.gstate.thresh.gcthreshold  ||  rman.gstate.thresh.rebuildthreshold  ||
           rman.gstate.thresh.repackthreshold  ||  rman.gstate.thresh.cleanupthreshold  ||
           rman.iteration[0] != '\0' ) {
         fprintf( stderr, "ERROR: The '-G', '-R', '-P', and '-i' args are incompatible with '-X'\n" );
         return -1;
      }
      // parse over the specified path, looking for RECORD_ITERATION_PARENT
      char* prevdup = strdup( rman.execprevroot );
      char* pathelem = strtok( prevdup, "/" );
      while ( pathelem ) {
         if ( strcmp( RECORD_ITERATION_PARENT, pathelem ) == 0 ) { break; }
         pathelem = strtok( NULL, "/" );
      }
      if ( pathelem == NULL ) {
         fprintf( stderr, "ERROR: The specified previous run path is missing the expected '%s' path component: \"%s\"\n",
                  RECORD_ITERATION_PARENT, rman.execprevroot );
         free( prevdup );
         return -1;
      }
      size_t keepbytes = strlen(pathelem) + (pathelem - prevdup); // identify the strlen of the path up to this elem
      // get our iteration name from the subsequent path element
      pathelem = strtok( NULL, "/" );
      if ( pathelem == NULL ) {
         fprintf( stderr, "ERROR: Failed to identify iteration string from previous run path: \"%s\"\n", rman.execprevroot );
         free( prevdup );
         return -1;
      }
      if ( snprintf( rman.iteration, ITERATION_STRING_LEN, "%s", pathelem ) >= ITERATION_STRING_LEN ) {
         fprintf( stderr, "ERROR: Parsed invalid iteration string from previous run path: \"%s\"\n", rman.execprevroot );
         free( prevdup );
         return -1;
      }
      free( prevdup );
      // identify the log root we will be pulling from
      char* baseroot = rman.execprevroot;
      rman.execprevroot = malloc( sizeof(char) * ( keepbytes + 1 ) );
      if ( rman.execprevroot == NULL ) {
         fprintf( stderr, "ERROR: Failed to allocate execprev root path\n" );
         return -1;
      }
      snprintf( rman.execprevroot, keepbytes + 1, "%s", baseroot ); // use snprintf to truncate to appropriate length
   }
   char* newroot = NULL;
   if ( rman.logroot ) {
      if ( rman.gstate.dryrun ) {
         newroot = malloc( sizeof(char) * ( strlen(rman.logroot) + 1 + strlen( RECORD_ITERATION_PARENT ) + 1 ) );
         if ( newroot == NULL ) {
            fprintf( stderr, "ERROR: Failed to allocate log root path string\n" );
            return -1;
         }
         snprintf( newroot, strlen(rman.logroot) + 1 + strlen( RECORD_ITERATION_PARENT ) + 1, "%s/%s",
                   rman.logroot, RECORD_ITERATION_PARENT );
      }
      else {
         newroot = malloc( sizeof(char) * ( strlen(rman.logroot) + 1 + strlen( MODIFY_ITERATION_PARENT )  + 1) );
         if ( newroot == NULL ) {
            fprintf( stderr, "ERROR: Failed to allocate log root path string\n" );
            return -1;
         }
         snprintf( newroot, strlen(rman.logroot) + 1 + strlen( MODIFY_ITERATION_PARENT ) + 1, "%s/%s",
                   rman.logroot, MODIFY_ITERATION_PARENT );
      }
   }
   else {
      if ( rman.gstate.dryrun ) {
         newroot = malloc( sizeof(char) * ( strlen(DEFAULT_LOG_ROOT) + 1 + strlen( RECORD_ITERATION_PARENT ) + 1 ) );
         if ( newroot == NULL ) {
            fprintf( stderr, "ERROR: Failed to allocate log root path string\n" );
            return -1;
         }
         snprintf( newroot, strlen(DEFAULT_LOG_ROOT) + 1 + strlen( RECORD_ITERATION_PARENT ) + 1, "%s/%s",
                   DEFAULT_LOG_ROOT, RECORD_ITERATION_PARENT );
      }
      else {
         newroot = malloc( sizeof(char) * ( strlen(DEFAULT_LOG_ROOT) + 1 + strlen( MODIFY_ITERATION_PARENT ) + 1 ) );
         if ( newroot == NULL ) {
            fprintf( stderr, "ERROR: Failed to allocate log root path string\n" );
            return -1;
         }
         snprintf( newroot, strlen(DEFAULT_LOG_ROOT) + 1 + strlen( MODIFY_ITERATION_PARENT ) + 1, "%s/%s",
                   DEFAULT_LOG_ROOT, MODIFY_ITERATION_PARENT );
      }
   }
   errno = 0;
   if ( mkdir( newroot, 0700 )  &&  errno != EEXIST ) {
      fprintf( stderr, "ERROR: Failed to create logging root dir: \"%s\"\n", newroot );
      free( newroot );
      return -1;
   }
   rman.logroot = newroot;
   if ( rman.preservelogtgt ) {
      char* newpresroot;
      if ( rman.gstate.dryrun ) {
         newpresroot = malloc( sizeof(char) * ( strlen(rman.preservelogtgt) + 1 + strlen( RECORD_ITERATION_PARENT ) + 1 ) );
         if ( newpresroot == NULL ) {
            fprintf( stderr, "ERROR: Failed to allocate log preservation path string\n" );
            return -1;
         }
         snprintf( newpresroot, strlen(rman.preservelogtgt) + 1 + strlen( RECORD_ITERATION_PARENT ) + 1, "%s/%s",
                   rman.preservelogtgt, RECORD_ITERATION_PARENT );
      }
      else {
         newpresroot = malloc( sizeof(char) * ( strlen(rman.preservelogtgt) + 1 + strlen( MODIFY_ITERATION_PARENT ) + 1 ) );
         if ( newpresroot == NULL ) {
            fprintf( stderr, "ERROR: Failed to allocate log preservation path string\n" );
            return -1;
         }
         snprintf( newpresroot, strlen(rman.preservelogtgt) + 1 + strlen( MODIFY_ITERATION_PARENT ) + 1, "%s/%s",
                   rman.preservelogtgt, MODIFY_ITERATION_PARENT );
      }
      errno = 0;
      if ( mkdir( newpresroot, 0700 )  &&  errno != EEXIST ) {
         fprintf( stderr, "ERROR: Failed to create log preservation root dir: \"%s\"\n", newpresroot );
         free( newpresroot );
         return -1;
      }
      rman.preservelogtgt = newpresroot;
   }

   // populate an iteration string, if missing
   if ( rman.iteration[0] == '\0' ) {
      struct tm* timeinfo = localtime( &(currenttime.tv_sec) );
      strftime( rman.iteration, ITERATION_STRING_LEN, "%Y-%m-%d-%H:%M:%S", timeinfo );
   }

   // substitute in appropriate threshold values, if specified
   if ( rman.gstate.thresh.gcthreshold ) { rman.gstate.thresh.gcthreshold = gcthresh; }
   if ( rman.gstate.thresh.rebuildthreshold ) {
      if ( rman.gstate.lbrebuild ) { rman.gstate.thresh.rebuildthreshold = rblthresh; }
      else { rman.gstate.thresh.rebuildthreshold = rbmthresh; }
   }
   if ( rman.gstate.thresh.repackthreshold ) { rman.gstate.thresh.repackthreshold = rpthresh; }
   if ( rman.gstate.thresh.cleanupthreshold ) { rman.gstate.thresh.cleanupthreshold = clthresh; }

   // check how many ranks we have
   int rankcount = 0;
   int rank = 0;
   if ( MPI_Comm_size( MPI_COMM_WORLD, &(rankcount) ) ) {
      fprintf( stderr, "ERROR: Failed to identify rank count\n" );
      return -1;
   }
   if ( MPI_Comm_rank( MPI_COMM_WORLD, &(rank) ) ) {
      fprintf( stderr, "ERROR: Failed to identify process rank\n" );
      return -1;
   }
   rman.ranknum = (size_t)rank;
   rman.totalranks = (size_t)rankcount;
   rman.workingranks = 1;
   if ( rankcount > 1 ) { rman.workingranks = rman.totalranks - 1; }

   // Initialize the MarFS Config
   if ( (rman.config = config_init( config_path )) == NULL ) {
      fprintf( stderr, "ERROR: Failed to initialize MarFS config: \"%s\"\n", config_path );
      return -1;
   }

   // Identify our target NS
   marfs_position pos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if ( config_establishposition( &(pos), rman.config ) ) {
      fprintf( stderr, "ERROR: Failed to establish a MarFS root NS position\n" );
      config_term( rman.config );
      return -1;
   }
   char* nspathdup = strdup( ns_path );
   if ( nspathdup == NULL ) {
      fprintf( stderr, "ERROR: Failed to duplicate NS path string: \"%s\"\n", ns_path );
      config_term( rman.config );
      return -1;
   }
   int travret = config_traverse( rman.config, &(pos), &(nspathdup), 1 );
   if ( travret < 0 ) {
      fprintf( stderr, "ERROR: Failed to identify NS path target: \"%s\"\n", ns_path );
      free( nspathdup );
      config_term( rman.config );
      return -1;
   }
   if ( travret ) {
      fprintf( stderr, "ERROR: Path target is not a NS, but a subpath of depth %d: \"%s\"\n", travret, ns_path );
      free( nspathdup );
      config_term( rman.config );
      return -1;
   }
   free( nspathdup );
   // Generate our NS list
   marfs_ns* curns = pos.ns;
   rman.nscount = 1;
   rman.nslist = malloc( sizeof( marfs_ns* ) );
   if ( rman.nslist == NULL ) {
      fprintf( stderr, "ERROR: Failed to allocate NS list of length %zu\n", rman.nscount );
      config_term( rman.config );
      return -1;
   }
   *(rman.nslist) = pos.ns;
   while ( curns ) {
      // we can use hash_iterate, as this is guaranteed to be the only proc using this config struct
      HASH_NODE* subnode = NULL;
      int iterres = 0;
      if ( curns->subspaces  &&  recurse ) {
         iterres = hash_iterate( curns->subspaces, &(subnode) );
         if ( iterres < 0 ) {
            fprintf( stderr, "ERROR: Failed to iterate through subspaces of \"%s\"\n", curns->idstr );
            config_term( rman.config );
            return -1;
         }
         else if ( iterres ) {
            marfs_ns* subspace = (marfs_ns*)(subnode->content);
            // only process non-ghost subspaces
            if ( subspace->ghtarget == NULL ) {
               LOG( LOG_INFO, "Adding subspace \"%s\" to our NS list\n", subspace->idstr );
               // note and enter the subspace
               rman.nscount++;
               // yeah, this is very inefficient; but we're only expecting 10s to 1000s of elements
               marfs_ns** newlist = realloc( rman.nslist, sizeof( marfs_ns* ) * rman.nscount );
               if ( newlist == NULL ) {
                  fprintf( stderr, "ERROR: Failed to allocate NS list of length %zu\n", rman.nscount );
                  free( rman.nslist );
                  config_term( rman.config );
                  return -1;
               }
               rman.nslist = newlist;
               *(rman.nslist + rman.nscount - 1) = subspace;
               curns = subspace;
               continue;
            }
         }
      }
      if ( iterres == 0 ) {
         // check for completion condition
         if ( curns == pos.ns ) {
            // iteration over the original NS target means we're done
            curns = NULL;
         }
         else {
            curns = curns->pnamespace;
         }
      }
   }
   // abandon our current position
   if ( config_abandonposition( &(pos) ) ) {
      fprintf( stderr, "WARNING: Failed to abandon MarFS traversal position\n" );
      free( rman.nslist );
      config_term( rman.config );
      return -1;
   }
   // complete allocation of required state elements
   rman.distributed = calloc( sizeof(size_t), rman.nscount );
   if ( rman.distributed == NULL ) {
      fprintf( stderr, "ERROR: Failed to allocate a 'distributed' list of length %zu\n", rman.nscount );
      free( rman.nslist );
      config_term( rman.config );
      return -1;
   }
   rman.terminatedworkers = calloc( sizeof(char), rman.totalranks );
   if ( rman.terminatedworkers == NULL ) {
      fprintf( stderr, "ERROR: Failed to allocate a 'terminatedworkers' list of length %zu\n", rman.totalranks );
      free( rman.distributed );
      free( rman.nslist );
      config_term( rman.config );
      return -1;
   }
   rman.walkreport = calloc( sizeof( struct streamwalker_report_struct ), rman.nscount );
   if ( rman.walkreport == NULL ) {
      fprintf( stderr, "ERROR: Failed to allocate a 'walkreport' list of length %zu\n", rman.nscount );
      free( rman.terminatedworkers );
      free( rman.distributed );
      free( rman.nslist );
      config_term( rman.config );
      return -1;
   }
   rman.logsummary = calloc( sizeof( struct operation_summary_struct ), rman.nscount );
   if ( rman.logsummary == NULL ) {
      fprintf( stderr, "ERROR: Failed to allocate a 'logsummary' list of length %zu\n", rman.nscount );
      free( rman.walkreport );
      free( rman.terminatedworkers );
      free( rman.distributed );
      free( rman.nslist );
      config_term( rman.config );
      return -1;
   }

   // check for previous run execution
   if ( rman.execprevroot ) {
      // open the summary log of that run
      size_t alloclen = strlen(rman.execprevroot) + 1 + strlen( rman.iteration ) + 1 + strlen( "summary.log" ) + 1;
      char* sumlogpath = malloc( sizeof(char) * alloclen );
      if ( sumlogpath == NULL ) {
         fprintf( stderr, "ERROR: Failed to allocate summary logfile path\n" );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      snprintf( sumlogpath, alloclen, "%s/%s/%s", rman.execprevroot, rman.iteration, "summary.log" );
      FILE* sumlog = fopen( sumlogpath, "r" );
      if ( sumlog == NULL ) {
         fprintf( stderr, "ERROR: Failed to open previous run's summary log: \"%s\" ( %s )\n", sumlogpath, strerror(errno) );
         free( sumlogpath );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      if ( parse_program_args( &(rman), sumlog ) ) {
         fprintf( stderr, "ERROR: Failed to parse previous run info from summary log: \"%s\"\n", sumlogpath );
         fclose( sumlog );
         free( sumlogpath );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      if ( rman.quotas ) {
         fprintf( stderr, "WARNING: Ignoring quota processing for execution of previous run\n" );
         rman.quotas = 0;
      }
      fclose( sumlog );
      free( sumlogpath );
      if ( rman.gstate.dryrun == 0 ) {
         fprintf( stderr, "ERROR: Cannot pick up execution of a non-'dry-run'\n" );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      // incorporate logfiles from the previous run
      if ( rman.ranknum == 0  &&  findoldlogs( &(rman), rman.execprevroot ) ) {
         fprintf( stderr, "ERROR: Failed to identify previous run's logfiles: \"%s\"\n", rman.execprevroot );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
   }
   else if ( rman.gstate.dryrun == 0  &&  rman.ranknum == 0 ) {
      // otherwise, scan for and incorporate logs from previous modification runs
      if ( findoldlogs( &(rman), rman.logroot ) ) {
         fprintf( stderr, "ERROR: Failed to scan for previous iteration logs under \"%s\"\n", rman.logroot );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
   }

   // rank zero needs to output our summary header
   if ( rman.ranknum == 0 ) {
      // open our summary log
      size_t alloclen = strlen( rman.logroot ) + 1 + strlen( rman.iteration ) + 1 + strlen( "summary.log" ) + 1;
      char* sumlogpath = malloc( sizeof(char) * alloclen );
      if ( sumlogpath == NULL ) {
         fprintf( stderr, "ERROR: Failed to allocate summary logfile path\n" );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      size_t printres = snprintf( sumlogpath, alloclen, "%s/%s", rman.logroot, rman.iteration );
      errno = 0;
      if ( mkdir( sumlogpath, 0700 )  &&  errno != EEXIST ) {
         fprintf( stderr, "ERROR: Failed to create summary log parent dir: \"%s\"\n", sumlogpath );
         free( sumlogpath );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      printres += snprintf( sumlogpath + printres, alloclen - printres, "/summary.log" );
      int sumlog = open( sumlogpath, O_WRONLY | O_CREAT | O_EXCL, 0700 );
      if ( sumlog < 0 ) {
         fprintf( stderr, "ERROR: Failed to open summary logfile: \"%s\"\n", sumlogpath );
         free( sumlogpath );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      rman.summarylog = fdopen( sumlog, "w" );
      if ( rman.summarylog == NULL ) {
         fprintf( stderr, "ERROR: Failed to convert summary logfile to file stream: \"%s\"\n", sumlogpath );
         free( sumlogpath );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      // output our program arguments to the summary file
      if ( output_program_args( &(rman) ) ) {
         fprintf( stderr, "ERROR: Failed to output program arguments to summary log: \"%s\"\n", sumlogpath );
         free( sumlogpath );
         free( rman.logsummary );
         free( rman.walkreport );
         free( rman.terminatedworkers );
         free( rman.distributed );
         free( rman.nslist );
         config_term( rman.config );
         return -1;
      }
      free( sumlogpath );

      // print out run info
      printf( "Processing %zu Total Namespaces ( %sTarget NS \"%s\" )\n",
              rman.nscount, (recurse) ? "Recursing Below " : "", (rman.nslist[0])->idstr );
   }

   // actually perform core behavior loops
   int bres;
   if ( rman.ranknum == 0 ) {
      bres = managerbehavior( &(rman) );
   }
   else {
      bres = workerbehavior( &(rman) );
   }
   if ( bres ) {
      cleanupstate( &(rman), 1 );
   }
   else {
      bres = (int)rman.fatalerror;
      cleanupstate( &(rman), 0 );
   }

   return bres;
}


