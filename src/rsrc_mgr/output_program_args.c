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

#include "rsrc_mgr/output_program_args.h"
#include "rsrc_mgr/rmanstate.h"

int output_program_args(rmanstate* rman) {
   // start with marfs config version (config changes could seriously break an attempt to re-execute this later)
   if (fprintf(rman->summarylog, "%s\n", rman->config->version) < 1) {
      fprintf(stderr, "ERROR: Failed to output config version to summary log\n");
      return -1;
   }

   // output operation types and threshold values
   if (rman->gstate.dryrun  &&  fprintf(rman->summarylog, "DRY-RUN\n") < 1) {
      fprintf(stderr, "ERROR: Failed to output dry-run flag to summary log\n");
      return -1;
   }

   if (rman->quotas  &&  fprintf(rman->summarylog, "QUOTAS\n") < 1) {
      fprintf(stderr, "ERROR: Failed to output quota flag to summary log\n");
      return -1;
   }

   if (rman->gstate.thresh.gcthreshold  &&
        fprintf(rman->summarylog, "GC=%llu\n", (unsigned long long) rman->gstate.thresh.gcthreshold) < 1) {
      fprintf(stderr, "ERROR: Failed to output GC threshold to summary log\n");
      return -1;
   }

   if (rman->gstate.thresh.repackthreshold  &&
        fprintf(rman->summarylog, "REPACK=%llu\n", (unsigned long long) rman->gstate.thresh.repackthreshold) < 1) {
      fprintf(stderr, "ERROR: Failed to output REPACK threshold to summary log\n");
      return -1;
   }

   if (rman->gstate.thresh.rebuildthreshold  &&
        fprintf(rman->summarylog, "REBUILD=%llu\n", (unsigned long long) rman->gstate.thresh.rebuildthreshold) < 1) {
      fprintf(stderr, "ERROR: Failed to output REBUILD threshold to summary log\n");
      return -1;
   }

   if (rman->gstate.lbrebuild  &&
        (
         fprintf(rman->summarylog, "REBUILD-LOCATION-POD=%d\n", rman->gstate.rebuildloc.pod) < 1  ||
         fprintf(rman->summarylog, "REBUILD-LOCATION-CAP=%d\n", rman->gstate.rebuildloc.cap) < 1  ||
         fprintf(rman->summarylog, "REBUILD-LOCATION-SCATTER=%d\n", rman->gstate.rebuildloc.scatter) < 1
       )) {
      fprintf(stderr, "ERROR: Failed to output REBUILD location to summary log\n");
      return -1;
   }

   if (fprintf(rman->summarylog, "\n") < 1) {
      fprintf(stderr, "ERROR: Failed to output header separator summary log\n");
      return -1;
   }

   return 0;
}
