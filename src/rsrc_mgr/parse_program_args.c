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

#include "parse_program_args.h"

int parse_program_args(rmanstate* rman, FILE* inputsummary) {
   // parse in a verify the config version
   char* readline = NULL;
   size_t linealloc = 0;
   ssize_t linelen = getline(&readline, &linealloc, inputsummary);
   if (linelen < 2) {
      fprintf(stderr, "WARNING: Failed to read MarFS config version string from previous run's summary log\n");
      return -1;
   }

   linelen--; // decrement length, to exclude newline

   if (strncmp(readline, rman->config->version, linelen)) {
      fprintf(stderr, "WARNING: Previous run is associated with a different MarFS config version: \"%s\"\n",
              readline);
      free(readline);
      return -1;
   }

   // parse in operation threshold values and set our state to match
   while ((linelen = getline(&readline, &linealloc, inputsummary)) > 1) {
      linelen--; // decrement length, to exclude newline
      if (strncmp(readline, "QUOTAS", linelen) == 0) {
         rman->quotas = 1;
      }
      else if (strncmp(readline, "DRY-RUN", linelen) == 0) {
         rman->gstate.dryrun = 1;
      }
      else {
         char *value = NULL;
         char *key = strtok_r(readline, "=", &value);

         if (value == (readline + linelen)) {
            fprintf(stderr, "ERROR: Failed to parse previous run's operation info (\"%s\")\n", readline);
            free(readline);
            return -1;
         }

         unsigned long long parseval = 0;
         if (sscanf(value, "%llu", &parseval) != 1) {
            fprintf(stderr, "ERROR: Failed to parse previous run's \"%s\" operation threshold: \"%s\"\n",
                    readline, value);
            free(readline);
            return -1;
         }

         // populate the appropriate value, based on string header
         if (strncmp(key, "GC", 3) == 0) {
            rman->gstate.thresh.gcthreshold = (time_t)parseval;
         }
         else if (strncmp(key, "REPACK", 7) == 0) {
            rman->gstate.thresh.repackthreshold = (time_t)parseval;
         }
         else if (strncmp(key, "REBUILD", 8) == 0) {
            rman->gstate.thresh.rebuildthreshold = (time_t)parseval;
         }
         else if (strncmp(key, "REBUILD-LOCATION-POD", 21) == 0) {
            rman->gstate.rebuildloc.pod = (int)parseval;
            rman->gstate.lbrebuild = 1;
         }
         else if (strncmp(key, "REBUILD-LOCATION-CAP", 21) == 0) {
            rman->gstate.rebuildloc.cap = (int)parseval;
            rman->gstate.lbrebuild = 1;
         }
         else if (strncmp(key, "REBUILD-LOCATION-SCATTER", 25) == 0) {
            rman->gstate.rebuildloc.scatter = (int)parseval;
            rman->gstate.lbrebuild = 1;
         }
         else {
            fprintf(stderr, "ERROR: Encountered unrecognized operation type in log of previous run: \"%s\"\n",
                    readline);
            free(readline);
            return -1;
         }
      }
   }

   free(readline);

   if (linelen < 1) {
      fprintf(stderr, "ERROR: Failed to read operation info from previous run's logfile\n");
      return -1;
   }

   return 0;
}
