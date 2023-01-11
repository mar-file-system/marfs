/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include <unistd.h>
#include <stdio.h>
// directly including the C file allows more flexibility for these tests
#include "hash/hash.c"

int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error contions which result in immediate termination

   // Initialize a hash table for direct lookup
   size_t nodecount = 10;
   HASH_NODE* nodelist = malloc( sizeof(HASH_NODE) * nodecount );
   if ( nodelist == NULL ) {
      printf( "failed to allocate node list\n" );
      return -1;
   }
   int i = 0;
   for ( ; i < nodecount; i++ ) {
      nodelist[i].name = malloc( sizeof(char) * 60 );
      if ( nodelist[i].name == NULL ) {
         printf( "failed to allocate name string for node %d\n", i );
         return -1;
      }
      snprintf( nodelist[i].name, 60, "node%d", i );
      nodelist[i].weight = 0;
      nodelist[i].content = NULL;
   }
   HASH_TABLE lookuptable = hash_init( nodelist, nodecount, 1 );
   if ( lookuptable == NULL ) {
      printf( "failed to initialize direct lookup table with %d nodes\n", i );
      return -1;
   }

   // confirm that lookup succeeds for all existing nodenames, and fails for non-existent
   char* nodename = malloc( sizeof(char) * 60 );
   HASH_NODE* noderef = NULL;
   if ( nodename == NULL ) {
      printf( "failed to allocate nodename string\n" );
      return -1;
   }
   for( i = (nodecount + 2); i >= 0; i-- ) {
      snprintf( nodename, 60, "node%d", i );
      int lres = hash_lookup( lookuptable, nodename, &(noderef) );
      if ( i < nodecount ) {
         if ( lres  ||  strcmp( nodename, noderef->name ) ) {
            printf( "failed to directly match existing %s (res = %d, %s)\n", nodename, lres, noderef->name );
            return -1;
         }
      }
      else if ( lres == 0  ||  strcmp( nodename, noderef->name ) == 0 ) {
         printf( "non-existent node %d resulted matched lookup of %s (res = %d)\n", i, noderef->name, lres );
         return -1;
      }
      else if ( lres != 1 ) {
         printf( "lookup failure for node%d (res = %d)\n", i, lres );
         return -1;
      }
   }

   // we should have just performed a lookup of node zero
   // iterate from this position, confirming all other nodes
   for ( i = 0; i < (nodecount + 2); i++ ) {
      snprintf( nodename, 60, "node%d", i );
      int ires = hash_iterate( lookuptable, &(noderef) );
      if ( i >= nodecount ) {
         if ( ires ) {
            printf( "expected return of iteration completion: %d\n", ires );
            return -1;
         }
      }
      else if ( strcmp( nodename, noderef->name ) ) {
         printf( "expected to iterate to node%d, but recieved %s instead\n", i, noderef->name );
         return -1;
      }
   }

   // reach into the hash table structure itself, and manually force an ID collision case
   uint64_t oldid[2];
   oldid[0] = lookuptable->vnodes[1].id[0];
   oldid[1] = lookuptable->vnodes[1].id[1];
   lookuptable->vnodes[1].id[0] = lookuptable->vnodes[0].id[0];
   lookuptable->vnodes[1].id[1] = lookuptable->vnodes[0].id[1];
   const char* cname = lookuptable->nodes[ lookuptable->vnodes[0].nodenum ].name;

   // ensure that a lookup of the colliding name still succeeds
   if ( hash_lookup( lookuptable, cname, &(noderef) ) ) {
      printf( "lookup matching vnode0 failed, post ID collision\n" );
      return -1;
   }
   // ensure an actual name match as well
   if ( strcmp( cname, noderef->name ) ) {
      printf( "invalid node name of %s following collision case 1\n", noderef->name );
      return -1;
   }

   // force the reverse of the previous collision case
   lookuptable->vnodes[1].id[0] = oldid[0]; // restore vnode1's correct ID value
   lookuptable->vnodes[1].id[1] = oldid[1];
   oldid[0] = lookuptable->vnodes[0].id[0];
   oldid[1] = lookuptable->vnodes[0].id[1];
   lookuptable->vnodes[0].id[0] = lookuptable->vnodes[1].id[0];
   lookuptable->vnodes[0].id[1] = lookuptable->vnodes[1].id[1];
   cname = lookuptable->nodes[ lookuptable->vnodes[1].nodenum ].name;

   // ensure that a lookup of the colliding name still succeeds
   if ( hash_lookup( lookuptable, cname, &(noderef) ) ) {
      printf( "lookup matching vnode1 failed, post ID collision\n" );
      return -1;
   }
   // ensure an actual name match as well
   if ( strcmp( cname, noderef->name ) ) {
      printf( "invalid node name of %s following collision case 2\n", noderef->name );
      return -1;
   }

   lookuptable->vnodes[0].id[0] = oldid[0]; // restore vnode0's correct ID value
   lookuptable->vnodes[0].id[1] = oldid[1];

   // terminate the hash table
   size_t retcount = 0;
   if ( hash_term( lookuptable, &(noderef), &(retcount) ) ) {
      printf( "failed to terminate hash table\n" );
      return -1;
   }
   // ensure the nodelist length matches
   if ( retcount != nodecount ) {
      printf( "term nodelist has an unexpected length of %zu\n", retcount );
      return -1;
   }
   // ensure the actual pointer value matches
   if ( nodelist != noderef ) {
      printf( "term list is altered pointer\n" );
      return -1;
   }

   // iterate over all nodes, comparing to expected values and freeing mem
   for ( i = 0; i < nodecount; i++ ) {
      snprintf( nodename, 60, "node%d", i );
      if ( strcmp( nodename, noderef[i].name ) ) {
         printf( "term list: expected %s, but encountered %s\n", nodename, noderef[i].name );
         return -1;
      }
      free( noderef[i].name );
   }
   // free the nodelist itself
   free( noderef );
   free( nodename );

   return 0;
}
