/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
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


