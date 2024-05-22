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


#include "ne/ne.h"
#include <unistd.h>
#include <stdio.h>
#include <string.h>


int main( int argc, char** argv ) {

   // NOTE -- I am ignoring memory leaks for failure conditions

   xmlDoc *doc = NULL;
   xmlNode *root_element = NULL;

   // initialize libxml and check for api mismatch
   LIBXML_TEST_VERSION

   // parse our config file
   doc = xmlReadFile ( "./testing/test_libne_seek_config.xml", NULL, XML_PARSE_NOBLANKS );
   if ( doc == NULL ) {
      printf( "failed to parse config file\n" );
      return -1;
   }

   // get the root elem
   root_element = xmlDocGetRootElement(doc);

   // initialize libne
   ne_location maxloc = {
      .pod = 0,
      .cap = 0,
      .scatter = 0
   };
   ne_erasure erasure = {
      .N = 2,
      .E = 1,
      .O = 0,
      .partsz = 100
   };
   ne_ctxt ctxt = ne_init( root_element, maxloc, erasure.N + erasure.E , NULL);
   if ( ctxt == NULL ) {
      printf( "failed to initialize libne instance\n" );
      return -1;
   }

   /* Free the xml Doc */
   xmlFreeDoc(doc);
   /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
   xmlCleanupParser();

   // generate an object with known content
   void* databuf = calloc( 1, 1024 * erasure.N * 2 * sizeof(char) );
   if ( databuf == NULL ) {
      printf( "failed to allocate data buffer\n" );
      return -1;
   }
   size_t output = 0;
   char* curpos = (char*)databuf;
   while ( output < (1024 * erasure.N * 2) ) {
      *curpos = ((char)(output % (CHAR_MAX + 1)));
      curpos++;
      output++;
   }
   ne_handle handle = ne_open( ctxt, "seekobj", maxloc, erasure, NE_WRALL );
   if ( handle == NULL ) {
      printf( "Failed to open write handle for seekobj\n" );
      return -1;
   }
   if ( ne_write( handle, databuf, output * sizeof(char) ) != (output * sizeof(char)) ) {
      printf( "failed to output content of seekobj\n" );
      return -1;
   }
   if ( ne_close( handle, NULL, NULL ) ) {
      printf( "failed to close write handle\n" );
      return -1;
   }

   // validate entire data content
   handle = ne_open( ctxt, "seekobj", maxloc, erasure, NE_RDALL );
   if ( handle == NULL ) {
      printf( "Failed to open read handle for seekobj\n" );
      return -1;
   }
   char readbuf[123] = {0};
   size_t validated = 0;
   while ( validated < output ) {
      ssize_t readres = ne_read( handle, readbuf, 123 * sizeof(char) );
      if ( readres < 1 ) {
         printf( "failure of full read at offset %zu\n", validated );
         return -1;
      }
      readres /= sizeof(char); // pointless, I know
      size_t readval = 0;
      for ( ; readval < readres; readval++ ) {
         if ( *(((char*)databuf) + validated + readval) != readbuf[readval] ) {
            printf( "mismatch of full read on byte %zu\n", validated + readval );
            return -1;
         }
      }
      validated += readval;
   }

   // reseek to the beginning and revalidate
   if ( ne_seek( handle, 0 ) ) {
      printf( "failed to seek to 0\n" );
      return -1;
   }
   bzero( readbuf, 123 * sizeof(char) );
   validated = 0;
   while ( validated < output ) {
      ssize_t readres = ne_read( handle, readbuf, 123 * sizeof(char) );
      if ( readres < 1 ) {
         printf( "failure of 1st seek read at offset %zu\n", validated );
         return -1;
      }
      readres /= sizeof(char); // pointless, I know
      size_t readval = 0;
      for ( ; readval < readres; readval++ ) {
         if ( *(((char*)databuf) + validated + readval) != readbuf[readval] ) {
            printf( "mismatch of 1st seek read on byte %zu\n", validated + readval );
            return -1;
         }
      }
      validated += readval;
   }

   // reseek to the beginning plus one byte, and revalidate
   if ( ne_seek( handle, sizeof(char) ) != sizeof(char) ) {
      printf( "failed to seek to %zu\n", sizeof(char) );
      return -1;
   }
   bzero( readbuf, 123 * sizeof(char) );
   validated = 1;
   while ( validated < output ) {
      ssize_t readres = ne_read( handle, readbuf, 123 * sizeof(char) );
      if ( readres < 1 ) {
         printf( "failure of 2nd seek read at offset %zu\n", validated );
         return -1;
      }
      readres /= sizeof(char); // pointless, I know
      size_t readval = 0;
      for ( ; readval < readres; readval++ ) {
         if ( *(((char*)databuf) + validated + readval) != readbuf[readval] ) {
            printf( "mismatch of 2nd seek read on byte %zu\n", validated + readval );
            return -1;
         }
      }
      validated += readval;
   }

   // reseek to one stripe plus one byte and revalidate
   if ( ne_seek( handle, 101 * sizeof(char) ) != (101 * sizeof(char)) ) {
      printf( "failed to seek to %zu\n", (101 * sizeof(char)) );
      return -1;
   }
   bzero( readbuf, 123 * sizeof(char) );
   validated = 101;
   while ( validated < output ) {
      ssize_t readres = ne_read( handle, readbuf, 123 * sizeof(char) );
      if ( readres < 1 ) {
         printf( "failure of 3rd seek read at offset %zu\n", validated );
         return -1;
      }
      readres /= sizeof(char); // pointless, I know
      size_t readval = 0;
      for ( ; readval < readres; readval++ ) {
         if ( *(((char*)databuf) + validated + readval) != readbuf[readval] ) {
            printf( "mismatch of 3rd seek read on byte %zu\n", validated + readval );
            return -1;
         }
      }
      validated += readval;
   }

   // reseek to half an I/O boundary and revalidate
   if ( ne_seek( handle, 1024 * sizeof(char) ) != (1024 * sizeof(char)) ) {
      printf( "failed to seek to %zu\n", (1024 * sizeof(char)) );
      return -1;
   }
   bzero( readbuf, 123 * sizeof(char) );
   validated = 1024;
   while ( validated < output ) {
      ssize_t readres = ne_read( handle, readbuf, 123 * sizeof(char) );
      if ( readres < 1 ) {
         printf( "failure of 4th seek read at offset %zu\n", validated );
         return -1;
      }
      readres /= sizeof(char); // pointless, I know
      size_t readval = 0;
      for ( ; readval < readres; readval++ ) {
         if ( *(((char*)databuf) + validated + readval) != readbuf[readval] ) {
            printf( "mismatch of 4th seek read on byte %zu\n", validated + readval );
            return -1;
         }
      }
      validated += readval;
   }

   // reseek just beyond the first I/O boundary and revalidate
   if ( ne_seek( handle, 1024 * erasure.N * sizeof(char) ) != (1024 * erasure.N * sizeof(char)) ) {
      printf( "failed to seek to %zu\n", (1024 * erasure.N * sizeof(char)) );
      return -1;
   }
   bzero( readbuf, 123 * sizeof(char) );
   validated = 1024 * erasure.N;
   while ( validated < output ) {
      ssize_t readres = ne_read( handle, readbuf, 123 * sizeof(char) );
      if ( readres < 1 ) {
         printf( "failure of 4th seek read at offset %zu\n", validated );
         return -1;
      }
      readres /= sizeof(char); // pointless, I know
      size_t readval = 0;
      for ( ; readval < readres; readval++ ) {
         if ( *(((char*)databuf) + validated + readval) != readbuf[readval] ) {
            printf( "mismatch of 4th seek read on byte %zu\n", validated + readval );
            return -1;
         }
      }
      validated += readval;
   }

   // finally close out handle
   if ( ne_close( handle, NULL, NULL ) ) {
      printf( "failed to close read handle\n" );
      return -1;
   }

   // free our data buf
   free( databuf );

   // delete our data obj
   if ( ne_delete( ctxt, "seekobj", maxloc ) ) {
      printf( "failed to delete seekobj\n" );
      return -1;
   }

   // cleanup out ctxt
   if ( ne_term( ctxt ) ) {
      printf( "failed to term libne ctxt\n" );
      return -1;
   }


   return 0;
}


