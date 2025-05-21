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

#include "rsrc_mgr/common.h"
#include "rsrc_mgr/find_namespaces.h"
#include "rsrc_mgr/rmanstate.h"

int find_namespaces(rmanstate *rman, const char *ns_path, const int recurse) {
   // Identify our target NS
   marfs_position pos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if (config_establishposition(&pos, rman->config)) {
       fprintf(stderr, "ERROR: Failed to establish a MarFS root NS position\n");
       return -1;
   }

   char* nspathdup = strdup(ns_path); // this is neccessary due to how config_traverse works
   if (nspathdup == NULL) {
      fprintf(stderr, "ERROR: Failed to duplicate NS path string: \"%s\"\n",
              ns_path);
      return -1;
   }

   const int travret = config_traverse(rman->config, &pos, &nspathdup, 1);

   free(nspathdup);

   if (travret < 0) {
      if (rman->ranknum == 0)
         fprintf(stderr, "ERROR: Failed to identify NS path target: \"%s\"\n",
                 ns_path);
      return -1;
   }
   if (travret) {
      if (rman->ranknum == 0) {
         fprintf(stderr, "ERROR: Path target is not a NS, but a subpath of depth %d: \"%s\"\n",
                 travret, ns_path);
      }
      return -1;
   }

   // Generate our NS list
   marfs_ns* curns = pos.ns;
   rman->nscount = 1;
   rman->nslist = malloc(sizeof(marfs_ns*));
   *(rman->nslist) = pos.ns;
   while (curns) {
      // we can use hash_iterate, as this is guaranteed to be the only proc using this config struct
      HASH_NODE* subnode = NULL;
      int iterres = 0;
      if (curns->subspaces && recurse) {
         iterres = hash_iterate(curns->subspaces, &subnode);
         if (iterres < 0) {
             fprintf(stderr, "ERROR: Failed to iterate through subspaces of \"%s\"\n", curns->idstr);
             return -1;
         }
         else if (iterres) {
            marfs_ns* subspace = (marfs_ns*)(subnode->content);
            // only process non-ghost subspaces
            if (subspace->ghtarget == NULL) {
               LOG(LOG_INFO, "Adding subspace \"%s\" to our NS list\n", subspace->idstr);
               // note and enter the subspace
               rman->nscount++;
               // yeah, this is very inefficient; but we're only expecting 10s to 1000s of elements
               marfs_ns** newlist = realloc(rman->nslist, sizeof(marfs_ns*) * rman->nscount);
               if (newlist == NULL) {
                   fprintf(stderr, "ERROR: Failed to allocate NS list of length %zu\n", rman->nscount);
                   return -1;
               }
               rman->nslist = newlist;
               *(rman->nslist + rman->nscount - 1) = subspace;
               curns = subspace;
               continue;
            }
         }
      }
      if (iterres == 0) {
         // check for completion condition
         if (curns == pos.ns) {
            // iteration over the original NS target means we're done
            curns = NULL;
         }
         else {
            curns = curns->pnamespace;
         }
      }
   }

   // abandon our current position
   if (config_abandonposition(&pos)) {
       fprintf(stderr, "WARNING: Failed to abandon MarFS traversal position\n");
       return -1;
   }

   return 0;
}
