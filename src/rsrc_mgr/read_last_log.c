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

#include "rsrc_mgr/findoldlogs.h"
#include "rsrc_mgr/parse_program_args.h"
#include "rsrc_mgr/read_last_log.h"
#include "rsrc_mgr/rmanstate.h"

int read_last_log(rmanstate *rman, const time_t skip) {
   // check for previous run execution
   if (rman->execprevroot) {
      // open the summary log of that run
      size_t alloclen = strlen(rman->execprevroot) + 1 + strlen(rman->iteration) + 1 + strlen(SUMMARY_FILENAME) + 1;
      char* sumlogpath = malloc(sizeof(char) * alloclen);
      snprintf(sumlogpath, alloclen, "%s/%s/%s", rman->execprevroot, rman->iteration, SUMMARY_FILENAME);

      FILE* sumlog = fopen(sumlogpath, "r");
      if (sumlog == NULL) {
         const int err = errno;
         fprintf(stderr, "ERROR: Failed to open previous run's summary log: \"%s\" (%s)\n",
                 sumlogpath, strerror(err));
         return -1;
      }

      const int ppa = parse_program_args(rman, sumlog);
      fclose(sumlog);

      if (ppa != 0) {
         fprintf(stderr, "ERROR: Failed to parse previous run info from summary log: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }
      free(sumlogpath);

      if (rman->quotas) {
         if (rman->ranknum == 0) {
            fprintf(stderr, "WARNING: Ignoring quota processing for execution of previous run\n");
         }
         rman->quotas = 0;
      }

      if (rman->gstate.dryrun == 0) {
         if (rman->ranknum == 0) {
            fprintf(stderr, "ERROR: Cannot pick up execution of a non-'dry-run'\n");
         }
         return -1;
      }

      // incorporate logfiles from the previous run
      if (rman->ranknum == 0 && findoldlogs(rman, rman->execprevroot, 0)) {
         fprintf(stderr, "ERROR: Failed to identify previous run's logfiles: \"%s\"\n",
                 rman->execprevroot);
         return -1;
      }
   }
   else if (rman->gstate.dryrun == 0 && rman->ranknum == 0) {
      // otherwise, scan for and incorporate logs from previous modification runs
      if (findoldlogs(rman, rman->logroot, skip)) {
         fprintf(stderr, "ERROR: Failed to scan for previous iteration logs under \"%s\"\n",
                 rman->logroot);
         return -1;
      }
   }

   return 0;
}
