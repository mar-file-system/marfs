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

#include <string.h>
#include <fcntl.h>
#include <stdlib.h>

#include "rsrc_mgr/summary_log_setup.h"
#include "rsrc_mgr/output_program_args.h"

int summary_log_setup(rmanstate *rman, const int recurse) {
   // rank zero needs to output our summary header
   if (rman->ranknum == 0) {
      // open our summary log
      size_t alloclen = strlen(rman->logroot) + 1 + strlen(rman->iteration) + 1 + strlen(SUMMARY_FILENAME) + 1;
      char* sumlogpath = malloc(sizeof(char) * alloclen);
      size_t printres = snprintf(sumlogpath, alloclen, "%s/%s", rman->logroot, rman->iteration);
      if (mkdir(sumlogpath, 0700) && errno != EEXIST) {
         fprintf(stderr, "ERROR: Failed to create summary log parent dir: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      printres += snprintf(sumlogpath + printres, alloclen - printres, "/" SUMMARY_FILENAME);
      int sumlog = open(sumlogpath, O_WRONLY | O_CREAT | O_EXCL, 0700);
      if (sumlog < 0) {
         fprintf(stderr, "ERROR: Failed to open summary logfile: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      rman->summarylog = fdopen(sumlog, "w");
      if (rman->summarylog == NULL) {
         fprintf(stderr, "ERROR: Failed to convert summary logfile to file stream: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      // output our program arguments to the summary file
      if (output_program_args(rman)) {
         fprintf(stderr, "ERROR: Failed to output program arguments to summary log: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      free(sumlogpath);

      // print out run info
      printf("Processing %zu Total Namespaces (%sTarget NS \"%s\")\n",
              rman->nscount, (recurse) ? "Recursing Below " : "", (rman->nslist[0])->idstr);
      printf("   Operation Summary:%s%s%s%s%s\n",
              (rman->gstate.dryrun) ? " DRY-RUN" : "", (rman->quotas) ? " QUOTAS" : "",
              (rman->gstate.thresh.gcthreshold) ? " GC" : "",
              (rman->gstate.thresh.repackthreshold) ? " REPACK" : "",
              (rman->gstate.thresh.rebuildthreshold) ?
               ((rman->gstate.lbrebuild) ? " REBUILD(LOCATION)" : " REBUILD(MARKER)") : "");
   }

   return 0;
}
