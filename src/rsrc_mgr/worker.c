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

#include "rsrc_mgr/rmanstate.h"
#include "rsrc_mgr/work.h"
#include "rsrc_mgr/worker.h"

/**
 * Worker rank behavior (processing requests and sending responses)
 * @param rmanstate* rman : Resource manager state
 * @return int : Zero on success, or -1 on failure
 */
int workerbehavior(rmanstate* rman) {
#ifdef RMAN_USE_MPI
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
