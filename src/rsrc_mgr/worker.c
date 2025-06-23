/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <stdio.h>
#include <unistd.h>

#if RMAN_USE_MPI
#include <mpi.h>
#endif

#include "rsrc_mgr/rmanstate.h"
#include "rsrc_mgr/work.h"
#include "rsrc_mgr/worker.h"

/**
 * Worker rank behavior (processing requests and sending responses)
 * @param rmanstate* rman : Resource manager state
 * @return int : Zero on success, or -1 on failure
 */
int workerbehavior(rmanstate* rman) {
#if RMAN_USE_MPI
   // setup out response and request structs
   workresponse response;
   workrequest  request;

   memset(&response, 0, sizeof(response));
   memset(&request,  0, sizeof(request));

   // we need to fake a 'startup' response
   response.request.type = COMPLETE_WORK;
   response.request.nsindex = rman->nscount;

   // begin by sending a response
   if (MPI_Send(&response, sizeof(response), MPI_BYTE, 0, 0, MPI_COMM_WORLD)) {
      LOG(LOG_ERR, "Failed to send initial 'dummy' response\n");
      fprintf(stderr, "ERROR: Failed to send an initial MPI response message\n");
      return -1;
   }

   // loop until we process a TERMINATE request
   int handleres = 1;
   while (handleres) {
      // wait for a new request
      if (MPI_Recv(&request, sizeof(request), MPI_BYTE, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE)) {
         LOG(LOG_ERR, "Failed to recieve a new request\n");
         fprintf(stderr, "ERROR: Failed to receive an MPI request message\n");
         return -1;
      }

      // generate an appropriate response
      if ((handleres = handlerequest(rman, &request, &response)) < 0) {
         LOG(LOG_ERR, "Fatal error detected during request processing: \"%s\"\n", response.errorstr);
         // send out our response anyway, so the manger prints our error message
         MPI_Send(&response, sizeof(response), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
         return -1;
      }

      // send out our response
      if (MPI_Send(&response, sizeof(response), MPI_BYTE, 0, 0, MPI_COMM_WORLD)) {
         LOG(LOG_ERR, "Failed to send a response\n");
         fprintf(stderr, "ERROR: Failed to send an MPI response message\n");
         return -1;
      }
   }

   // all work should now be complete
   return 0;
#else
   (void) rman;
   fprintf(stderr, "Hit workerbehavior() with MPI disabled!\n");
   return -1;
#endif
}
