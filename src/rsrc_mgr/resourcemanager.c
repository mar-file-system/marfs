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


typedef struct rmanprogress_struct {
   // per-NS Progress Tracking
   size_t     nscount;
   marfs_ns** nslist;
   size_t*    distributed;
   // per-NS Work Tracking
   streamwalker_report* walkreport;
   operation_summary*   logsummary;
   // rank state tracking
   size_t     activeworkers;
   // Per-Run MarFS State
   marfs_config*  config;
   // Per-NS MarFS State
   marfs_position pos;
   // arg reference vals
   char*       iteration;
   char*       logroot;
   char*       errorlogtgt;
   char*       preservelogtgt;
   thresholds  thresh;
   char        dryrun;
} rmanprogress;


//   -------------   INTERNAL FUNCTIONS    -------------




//   -------------   EXTERNAL FUNCTIONS    -------------


int main(int argc, const char** argv) {
   errno = 0; // init to zero (apparently not guaranteed)
   char* config_path = getenv( "MARFS_CONFIG_PATH" ); // check for config env var
   char* ns_path = ".";
   char recurse = 0;
   rmanprogress rmanprog = {0};

   // get the initialization time of the program, to identify thresholds
   struct timeval currenttime;
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk\n" );
      return -1;
   }
   time_t defgcthresh = currenttime.tv_nsec - GC_THRESH;
   time_t defrblthresh = currenttime.tv_nsec - RB_L_THRESH;
   time_t defrbmthresh = currenttime.tv_nsec - RB_M_THRESH;
   time_t defrpthresh = currenttime.tv_nsec - RP_THRESH;

   // parse all position-independent arguments
   char pr_usage = 0;
   int c;
   while ((c = getopt(argc, (char* const*)argv, "c:n:rilpqgrRdT:L:h")) != -1) {
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
         rmanprog.iteration = optarg;
         break;
      case 'l':
         rmanprog.logroot = optarg;
         break;
      case 'p':
         rmanprog.preservelogtgt = optarg;
         break;
      case 'q':
         rmanprog.quotas = optarg;
         break;
      case 'g':
         rmanprog.thresh.gcthreshold = optarg;
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



