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

#include <stdio.h>
#include <unistd.h>

#if RMAN_USE_MPI
#include <mpi.h>
#endif

#include "rsrc_mgr/manager.h"
#include "rsrc_mgr/outputinfo.h"
#include "rsrc_mgr/rmanstate.h"
#include "rsrc_mgr/work.h"

static int loop_namespaces(rmanstate* rman) {
   // loop over all namespaces
   for (size_t nsindex = 0; nsindex < rman->nscount; nsindex++) {
      marfs_ns* curns = rman->nslist[nsindex];

      // potentially set NS quota values
      if (rman->quotas) {
         // update position value
         if (config_establishposition(&rman->gstate.pos, rman->config)) {
            LOG(LOG_ERR, "Failed to establish a rootNS position\n");
            fprintf(stderr, "ERROR: Failed to establish a rootNS position\n");
            return -1;
         }

         char* tmpnspath = NULL;
         if (config_nsinfo(curns->idstr, NULL, &tmpnspath)) {
            LOG(LOG_ERR, "Failed to identify NS path of NS \"%s\"\n", curns->idstr);
            fprintf(stderr, "ERROR: Failed to identify NS path of NS \"%s\"\n", curns->idstr);
            return -1;
         }

         char* nspath = strdup(tmpnspath + 1); // strip off leading '/', to get a relative NS path
         free(tmpnspath);
         if (config_traverse(rman->config, &rman->gstate.pos, &nspath, 0)) {
            LOG(LOG_ERR, "Failed to traverse config to new NS path: \"%s\"\n", nspath);
            fprintf(stderr, "ERROR: Failed to traverse config to new NS path: \"%s\"\n", nspath);
            free(nspath);
            return -1;
         }
         free(nspath);

         if (rman->gstate.pos.ctxt == NULL && config_fortifyposition(&rman->gstate.pos)) {
            LOG(LOG_ERR, "Failed to fortify position for new NS: \"%s\"\n", curns->idstr);
            fprintf(stderr, "ERROR: Failed to fortify position for new NS: \"%s\"\n", curns->idstr);
            config_abandonposition(&rman->gstate.pos);
            return -1;
         }

         // update quota values based on report totals
         MDAL nsmdal = curns->prepo->metascheme.mdal;
         if (nsmdal->setinodeusage(rman->gstate.pos.ctxt, rman->walkreport[nsindex].fileusage)) {
            fprintf(stderr, "WARNING: Failed to set inode usage for NS \"%s\"\n", curns->idstr);
         }

         if (nsmdal->setdatausage(rman->gstate.pos.ctxt, rman->walkreport[nsindex].byteusage)) {
            fprintf(stderr, "WARNING: Failed to set data usage for NS \"%s\"\n", curns->idstr);
         }

         config_abandonposition(&rman->gstate.pos);
      }

      // print out NS info
      outputinfo(stdout, curns, rman->walkreport + nsindex, rman->logsummary + nsindex);
   }

   return 0;
}

static int cleanup_iteration_root(rmanstate* rman) {
   // trim logpath at the last '/' char, to get the parent dir path
   char* iterationroot = resourcelog_genlogpath(0, rman->logroot, rman->iteration, NULL, -1);
   if (iterationroot == NULL) {
      fprintf(stderr, "ERROR: Failed to identify iteration path of this run for final cleanup\n");
      return -1;
   }

   size_t sumstrlen = strlen(iterationroot) + 1 + strlen(SUMMARY_FILENAME);
   char* sumlogpath = calloc(sizeof(char), sumstrlen + 1);
   snprintf(sumlogpath, sumstrlen + 1, "%s/%s", iterationroot, SUMMARY_FILENAME);

   // potentially duplicate our summary log to a final location
   if (rman->preservelogtgt) {
      char* presiterroot = resourcelog_genlogpath(0, rman->preservelogtgt, rman->iteration, NULL, -1);
      if (presiterroot == NULL) {
         fprintf(stderr, "ERROR: Failed to identify iteration preservation path of this run for final cleanup\n");
         free(sumlogpath);
         free(iterationroot);
         return -1;
      }

      size_t presstrlen = strlen(presiterroot) + 1 + strlen(SUMMARY_FILENAME);
      char* preslogpath = calloc(sizeof(char), presstrlen + 1);
      snprintf(preslogpath, presstrlen + 1, "%s/%s", presiterroot, SUMMARY_FILENAME);
      free(presiterroot);

      // simply use a hardlink for this purpose, no need to make a 'real' duplicate
      if (link(sumlogpath, preslogpath)) {
         fprintf(stderr, "ERROR: Failed to link summary logfile \"%s\" to new target path: \"%s\"\n",
                  sumlogpath, preslogpath);
         free(preslogpath);
         free(sumlogpath);
         free(iterationroot);
         return -1;
      }

      free(preslogpath);
   }

   // unlink our summary logfile
   if (unlink(sumlogpath)) {
      // just complain
      fprintf(stderr, "WARNING: Failed to unlink summary log path during final cleanup\n");
   }

   free(sumlogpath);

   // attempt to unlink our iteration dir
   if (rmdir(iterationroot)) {
      // just complain
      const int err = errno;
      fprintf(stderr, "WARNING: Failed to unlink iteration root: \"%s\" (%s)\n", iterationroot, strerror(err));
   }

   free(iterationroot);

   return 0;
}

static int cleanup_previous(rmanstate* rman) {
   // potentially cleanup the previous run's summary log and iteration dir
   if (rman->execprevroot) {
      char *iterationroot = resourcelog_genlogpath(0, rman->execprevroot, rman->iteration, NULL, -1);
      if (iterationroot == NULL) {
         fprintf(stderr, "ERROR: Failed to identify iteration path of the previous run for final cleanup\n");
         return -1;
      }

      size_t sumstrlen = strlen(iterationroot) + 1 + strlen(SUMMARY_FILENAME);
      char *sumlogpath = malloc(sizeof(char) * sumstrlen + 1);
      snprintf(sumlogpath, sumstrlen + 1, "%s/%s", iterationroot, SUMMARY_FILENAME);
      if (unlink(sumlogpath)) {
         // just complain
         fprintf(stderr, "WARNING: Failed to unlink summary log path of previous run during final cleanup\n");
      }
      free(sumlogpath);

      if (rmdir(iterationroot)) {
         // just complain
         const int err = errno;
         fprintf(stderr, "WARNING: Failed to unlink iteration root of previous run: \"%s\" (%s)\n",
                 iterationroot, strerror(err));
      }

      free(iterationroot);
   }

   return 0;
}

/**
 * Manager rank behavior (sending out requests, potentially processing them as well)
 * @param rmanstate* rman : Resource manager state
 * @return int : Zero on success, or -1 on failure
 */
int managerbehavior(rmanstate* rman) {
   // setup out response and request structs
   size_t respondingrank = 0;

   workresponse response;
   workrequest  request;

   memset(&response, 0, sizeof(response));
   memset(&request,  0, sizeof(request));

   if (rman->totalranks == 1) {
      // we need to fake our own 'startup' response
      response.request.type = COMPLETE_WORK;
      response.request.nsindex = rman->nscount;
   }

   // loop until all workers have terminated
   int workersrunning = 1;
   while (workersrunning) {
      // begin by getting a response from a worker via MPI
      if (rman->totalranks > 1) {
#if RMAN_USE_MPI
         MPI_Status msgstatus;
         if (MPI_Recv(&response, sizeof(response), MPI_BYTE, MPI_ANY_SOURCE,
                      MPI_ANY_TAG, MPI_COMM_WORLD, &msgstatus)) {
            LOG(LOG_ERR, "Failed to recieve a response\n");
            fprintf(stderr, "ERROR: Failed to receive an MPI response message\n");
            return -1;
         }
         respondingrank = msgstatus.MPI_SOURCE;
#else
         fprintf(stderr, "Hit totalranks > 1 case with MPI disabled!\n");
         return -1;
#endif
      }

      // generate an appropriate request, based on response
      int handleres = handleresponse(rman, respondingrank, &response, &request);
      if (handleres < 0) {
         fprintf(stderr, "Fatal error detected during response handling. Program will terminate.\n");
         return -1;
      }

      // send out a new request, if appropriate
      if (handleres) {
         if (rman->totalranks > 1) {
#if RMAN_USE_MPI
            // send out the request via MPI to the responding rank
            if (MPI_Send(&request, sizeof(request), MPI_BYTE, respondingrank, 0, MPI_COMM_WORLD)) {
               LOG(LOG_ERR, "Failed to send a request\n");
               fprintf(stderr, "ERROR: Failed to send an MPI request message\n");
               return -1;
            }
#else
            fprintf(stderr, "Hit totalranks > 1 case with MPI disabled!\n");
            return -1;
#endif
         }
         else {
            // just process the new request ourself
            if (handlerequest(rman, &request, &response) < 0) {
               fprintf(stderr, "ERROR: %s\nFatal error detected during local request processing. Program will terminate.\n",
                       response.errorstr);
               return -1;
            }
         }
      }

      // check worker states
      workersrunning = 0; // assume none, until found
      for (size_t windex = (rman->totalranks > 1); windex < rman->totalranks; windex++) {
         if (rman->terminatedworkers[windex] == 0) {
             workersrunning = 1;
             break;
         }
      }
   }

   printf("\n");

   if (loop_namespaces(rman) != 0) {
       return -1;
   }

   // close our summary log file
   int cres = fclose(rman->summarylog);
   rman->summarylog = NULL; // avoid possible double close
   if (cres) {
      fprintf(stderr, "ERROR: Failed to close summary logfile for this iteration\n");
      return -1;
   }

   if (rman->fatalerror) { return -1; } // skip final log cleanup if we hit some crucial error

   // NOTE -- frustrates me greatly to put this sleep in, but a brief pause really seems to help any NFS-hosted
   //         log location to cleanup state, allowing us to delete the parent dir
   usleep(100000);

   // final cleanup
   if (cleanup_iteration_root(rman) != 0) {
       return -1;
   }

   if (cleanup_previous(rman) != 0) {
       return -1;
   }

   // all work should now be complete
   return 0;
}
