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

struct node_content {
   size_t lcount;
   size_t num;
};

int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error contions which result in immediate termination

   // Initialize a hash table for distribution
   size_t nodecount = 10;
   size_t totalweight = 0;
   struct node_content* nc;
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
      nodelist[i].weight = i;
      totalweight += i;
      nodelist[i].content = malloc( sizeof(struct node_content) );
      if ( nodelist[i].content == NULL ) {
         printf( "failed to allocate content for node %d\n", i );
         return -1;
      }
      nc = nodelist[i].content;
      nc->lcount = 0;
      nc->num = i;
   }
   HASH_TABLE disttable = hash_init( nodelist, nodecount, 0 );
   if ( disttable == NULL ) {
      printf( "failed to initialize distribution table with %d nodes\n", i );
      return -1;
   }

   // perform a TON of lookups, incrementing the count of the node result each time
   char* nodename = malloc( sizeof(char) * 128 );
   if ( nodename == NULL ) {
      printf( "failed to allocate nodename string\n" );
      return -1;
   }
   HASH_NODE* noderef = NULL;
   size_t lperweight = 1000;
   size_t lookuptotal = totalweight * lperweight;
   size_t lnum = 0;
   for ( ; lnum < lookuptotal; lnum++ ) {
      snprintf( nodename, 128, "consistent-section-%zu-consistent-section", lnum );
      if ( hash_lookup( disttable, nodename, &(noderef) ) != 1 ) {
         printf( "failed to perform lookup %zu\n", lnum );
         return -1;
      }
      nc = noderef->content;
      nc->lcount++;
   }

   // iterate from the current position, determining if node counts seem sensible
   for ( i = 0; i < (nodecount + 2); i++ ) {
      if ( noderef ) {
         nc = noderef->content;
         size_t minlookup = (nc->num == 0) ? 0 : (nc->num - 1) * lperweight;
         size_t maxlookup = (nc->num == 0) ? 0 : (nc->num + 1) * lperweight;
         if ( nc->lcount > maxlookup ) {
            printf( "Excessive lookup count of %zu on node %zu\n", nc->lcount, nc->num ); 
            return -1;
         }
         if ( nc->lcount < minlookup ) {
            printf( "Minimal lookup count of %zu on node %zu\n", nc->lcount, nc->num );
            return -1;
         }
      }
      int ires = hash_iterate( disttable, &(noderef) );
      if ( i >= nodecount ) {
         if ( ires ) {
            printf( "expected return of iteration completion: %d\n", ires );
            return -1;
         }
      }
      else if ( ires != 1 ) {
         printf( "expected to iterate, but recieved %d\n", ires );
         return -1;
      }
   }

   // terminate the hash table
   size_t retcount = 0;
   if ( hash_term( disttable, &(noderef), &(retcount) ) ) {
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
      free( noderef[i].content );
   }
   // free the nodelist itself
   free( noderef );
   free( nodename );

   return 0;
}
