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

#include "dal/dal.h"
#include <unistd.h>
#include <stdio.h>

int main(int argc, char **argv)
{

   xmlDoc *doc = NULL;
   xmlNode *root_element = NULL;

   /*
   * this initialize the library and check potential ABI mismatches
   * between the version it was compiled for and the actual shared
   * library used.
   */
   LIBXML_TEST_VERSION

   /*parse the file and get the DOM */
   doc = xmlReadFile("./testing/fuzzing_config.xml", NULL, XML_PARSE_NOBLANKS);

   if (doc == NULL)
   {
      printf("error: could not parse file %s\n", "./dal/testing/fuzzing_config.xml");
      return -1;
   }

   /*Get the root element node */
   root_element = xmlDocGetRootElement(doc);

   // Initialize a posix dal instance
   DAL_location maxloc = {.pod = 1, .block = 1, .cap = 1, .scatter = 1};
   DAL dal = init_dal(root_element, maxloc);

   /* Free the xml Doc */
   xmlFreeDoc(doc);
   /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
   xmlCleanupParser();

   // check that initialization succeeded
   if (dal == NULL)
   {
      printf("error: failed to initialize DAL: %s\n", strerror(errno));
      return -1;
   }

   // Open, write to, and set meta info for a specific block
   void *writebuffer = calloc(10, 1024);
   if (writebuffer == NULL)
   {
      printf("error: failed to allocate write buffer\n");
      return -1;
   }
   BLOCK_CTXT block = dal->open(dal->ctxt, DAL_WRITE, maxloc, "");
   if (block == NULL)
   {
      printf("error: failed to open block context for write: %s\n", strerror(errno));
      return -1;
   }
   if (dal->put(block, writebuffer, (10 * 1024)) == 0) // we EXPECT a failure here
   {
      printf("error: put did not return expected value\n");
      return -1;
   }
   meta_info meta_val = { .N = 3, .E = 1, .O = 3, .partsz = 4096, .versz = 1048576, .blocksz = 10485760, .crcsum = 1234567, .totsz = 7654321 };
   if (dal->set_meta(block, &meta_val))
   {
      printf("error: set_meta did not return expected value\n");
      return -1;
   }
   if (dal->close(block))
   {
      printf("error: failed to close block write context: %s\n", strerror(errno));
      return -1;
   }

   // Open the same block for read and verify all values
   void *readbuffer = malloc(sizeof(char) * 10 * 1024);
   if (readbuffer == NULL)
   {
      printf("error: failed to allocate read buffer\n");
      return -1;
   }
   block = dal->open(dal->ctxt, DAL_READ, maxloc, "");
   if (block == NULL)
   {
      printf("error: failed to open block context for read: %s\n", strerror(errno));
      return -1;
   }
   if (dal->get(block, readbuffer, (10 * 1024), 0) > 0) // we EXPECT a failure here
   {
      printf("error: get did not return expected value\n");
      return -1;
   }
   meta_info readmeta;
   if (dal->get_meta(block, &readmeta))
   {
      printf("error: get_meta returned an unexpected value\n");
      return -1;
   }
   else if (cmp_minfo(&meta_val, &readmeta))
   {
      printf("error: retrieved meta value does not match written!\n");
      return -1;
   }
   if (dal->close(block))
   {
      printf("error: failed to close block read context: %s\n", strerror(errno));
      return -1;
   }

   // Delete the block we created
   if (dal->del(dal->ctxt, maxloc, ""))
   {
      printf("error: del failed!\n");
      return -1;
   }

   // Free the DAL
   if (dal->cleanup(dal))
   {
      printf("error: failed to cleanup DAL\n");
      return -1;
   }

   /*free the document */
   free(writebuffer);
   free(readbuffer);

   return 0;
}
