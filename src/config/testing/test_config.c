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
#include "config/config.c"


int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error conditions 
   //         which result in immediate termination


   // Initialize the libxml lib and check for API mismatches
   LIBXML_TEST_VERSION

   char xmlbuffer[1024];
   if ( snprintf( xmlbuffer, 1024, "%s", 
                  "<perms>\
                        <interactive>RM</interactive>\
                        <batch>RD,WD</batch>\
                   </perms>" ) < 1 ) {
      printf( "failed to generate initial perm string\n" );
      return -1;
   }

   // open the test config file and produce an XML tree
   xmlDoc* doc = xmlReadMemory(xmlbuffer, 1024, "noexist.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse initial perm xml string\n");
      return -1;
   }
   xmlNode* root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of initial perm doc\n" );
      return -1;
   }

   // test permission parsing
   ns_perms iperms = NS_NOACCESS;
   ns_perms bperms = NS_NOACCESS;
   if ( parse_perms( &(iperms), &(bperms), root_element->children ) ) {
      printf( "parse_perms() failed on initial perm doc\n" );
      return -1;
   }
   // verify expected perm values
   if ( iperms != NS_READMETA  ||  bperms != NS_RWDATA ) {
      printf( "permission values differ from initial doc\n" );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();


   // open a new xml doc, with size_t and int node content
   if ( snprintf( xmlbuffer, 1024, "%s", 
                  "<root><size>10T</size>\
                   <int>1048576</int></root>" ) < 1 ) {
      printf( "failed to generate initial perm string\n" );
      return -1;
   }
   doc = xmlReadMemory(xmlbuffer, 1024, "noexist.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse size/int xml string\n");
      return -1;
   }
   root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of size/int xml doc\n" );
      return -1;
   }

   // parse the size node and verify the result
   size_t sizeres = 0;
   if ( parse_size_node( &(sizeres), root_element->children )  ||  sizeres != 10995116277760ULL ) {
      printf( "failed parse of size node or invalid size result of %zu\n", sizeres );
      return -1;
   }
   // parse the int node and verify the result
   int intres = 0;
   if ( parse_int_node( &(intres), root_element->children->next )  ||  intres != 1048576ULL ) {
      printf( "failed parse of int node or invalid int result of %d\n", intres );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();


   // open a new xml doc, with size_t and int node content
   if ( snprintf( xmlbuffer, 1024, "%s", 
              "<quotas>\
                  <files>1M</files>\
                  <data>123M</data>\
               </quotas>" ) < 1 ) {
      printf( "failed to generate quota xml string\n" );
      return -1;
   }
   doc = xmlReadMemory(xmlbuffer, 1024, "noexist.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse quota xml string\n");
      return -1;
   }
   root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of quota xml doc\n" );
      return -1;
   }

   // parse the quota node and verify the result
   size_t fquota = 0;
   size_t dquota = 0;
   if ( parse_quotas( &(fquota), &(dquota), root_element->children )  ||
        fquota != 1048576  ||
        dquota != 128974848ULL ) {
      printf( "failed to parse quota node or invalid result (dquota=%zu,fquota=%zu)\n", dquota, fquota );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();


   // now, move on to the complete config
   doc = xmlReadFile("testing/config.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse quota xml string\n");
      return -1;
   }
   root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of quota xml doc\n" );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();


   return 0;
}


