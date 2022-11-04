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
#define RB_M_THRESH 120   // Age of files before they are rebuilt ( based on marker )
                          // Default to 2 minutes ago
#define RP_THRESH 259200  // Age of files before they are repacked
                          // Default to 3 days ago

#define MODIFY_ITERATION_PREFIX "MODIFY-RUN-"
#define RECORD_ITERATION_PREFIX "RECORD-RUN-"
#define ARGS_ITERATION_FILE "PROGRAM-ARGUMENTS"
#define ITERATION_STRING_LEN 1024

typedef struct rmanstate_struct {
   // Per-Run Rank State
   size_t        ranknum;
   size_t        totalranks;

   // Per-Run MarFS State
   marfs_config* config;

   // Per-NS Progress Tracking
   size_t        nscount;
   marfs_ns**    nslist;
   size_t*       distributed;
   size_t*       activeworkers;
   streamwalker_report* walkreport;
   operation_summary*   logsummary;

   // Thread State
   rthread_gstate gstate;
   ThreadQueue TQ;

   // arg reference vals
   char        iteration[ITERATION_STRING_LEN];
   char*       logroot;
   char*       errorlogtgt;
   char*       preservelogtgt;
} rmanstate;

typedef enum {
   RLOG_WORK,     // request to process an existing resource log ( either previous dry-run or dead run pickup )
   NS_WORK,       // request to process a portion of a NS
   COMPLETE_WORK, // request to complete outstanding work ( quiesce all threads and close all streams )
   ABORT_WORK     // request to abort all processing and terminate
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
   char                 fatalerror;
   char                 errorstr[MAX_STR_BUFFER];
} workresponse;

//   -------------   INTERNAL FUNCTIONS    -------------

int handlerequest( rmanstate* rman, workrequest* request, workresponse* response ) {
   // pre-populate response with a 'fatal error' condition, just in case
   response->request = *(request);
   response->haveinfo = 0;
   bzero( &(response->report), sizeof( struct streamwalker_report_struct ) );
   bzero( &(response->summary), sizeof( struct operation_summary_struct ) );
   response->fatalerror = 1;
   snprintf( response->errorstr, MAX_STR_BUFFER, "UNKNOWN-ERROR!\n" );
   // identify and process the request
}

/**
 * TODO
 * @param rmanstate* rman : 
 * @param workresponse* response : 
 * @param workrequest*  request : 
 * @return int : On success, positive count of exited workers ( may be zero );
 *               -1 if a fatal error has occurred
 */
int handleresponse( rmanstate* rman, workresponse* response, workrequest* request ) {
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


