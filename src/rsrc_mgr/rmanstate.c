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

#include "rmanstate.h"

#include "rsrc_mgr/work.h"
#include "rsrc_mgr/loginfo.h"

void rmanstate_init(rmanstate *rman, int rank, int rankcount) {
   memset(rman, 0, sizeof(*rman));
   rman->gstate.numprodthreads = DEFAULT_PRODUCER_COUNT;
   rman->gstate.numconsthreads = DEFAULT_CONSUMER_COUNT;
   rman->gstate.rebuildloc.pod = -1;
   rman->gstate.rebuildloc.cap = -1;
   rman->gstate.rebuildloc.scatter = -1;
   rman->ranknum = (size_t)rank;
   rman->totalranks = (size_t)rankcount;
   rman->workingranks = 1;
   if (rankcount > 1) {
       rman->workingranks = rman->totalranks - 1;
   }
}

void rmanstate_fini(rmanstate* rman, char abort) {
    free(rman->preservelogtgt);
    free(rman->logroot);
    free(rman->execprevroot);

    if (rman->summarylog) {
        fclose(rman->summarylog);
    }

    if (rman->tq) {
       if (!abort) {
          LOG(LOG_ERR, "Encountered active TQ with no abort condition specified\n");
          fprintf(stderr, "Encountered active TQ with no abort condition specified\n");
          rman->fatalerror = 1;
       }
       tq_set_flags(rman->tq, TQ_ABORT);
       if (rman->gstate.rinput) {
          resourceinput_purge(&rman->gstate.rinput);
          resourceinput_term(&rman->gstate.rinput);
       }

       // gather all thread status values
       rthread_state* tstate = NULL;
       while (tq_next_thread_status(rman->tq, (void**)&tstate) > 0) {
           // verify thread status
           free(tstate);
       }
       tq_close(rman->tq);
    }

    if (rman->gstate.rpst) {
       repackstreamer_abort(rman->gstate.rpst);
    }

    if (rman->gstate.rlog) {
       resourcelog_abort(&rman->gstate.rlog);
    }

    if (rman->gstate.rinput) {
       resourceinput_destroy(&rman->gstate.rinput);
    }

    if (rman->gstate.pos.ns) {
       config_abandonposition(&rman->gstate.pos);
    }

    free(rman->logsummary);
    free(rman->walkreport);
    free(rman->terminatedworkers);
    free(rman->distributed);
    free(rman->nslist);

    if (rman->oldlogs) {
       HASH_NODE* resnode = NULL;
       size_t ncount = 0;
       if (hash_term(rman->oldlogs, &resnode, &ncount) == 0) {
          // free all subnodes and requests
          for (size_t nindex = 0; nindex < ncount; nindex++) {
             loginfo* linfo = (loginfo*)resnode[nindex].content;
             if (linfo) {
                free(linfo->requests);
                free(linfo);
             }
             free(resnode[nindex].name);
          }

          free(resnode); // these were allocated in one block, and thus require only one free()
       }
    }

    if (rman->config) {
        config_term(rman->config);
    }
}
