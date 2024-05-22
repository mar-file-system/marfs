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


#include "io/io.h"
#include "dal/dal.h"
#include "io/testing/bufferfuncs.c"

#include <unistd.h>
#include <stdio.h>
#include <pthread.h>


int main( int argc, char** argv ) {
   xmlDoc *doc = NULL;
   xmlNode *root_element = NULL;


   /*
   * this initialize the library and check potential ABI mismatches
   * between the version it was compiled for and the actual shared
   * library used.
   */
   LIBXML_TEST_VERSION

   /*parse the file and get the DOM */
   doc = xmlReadFile("./testing/config.xml", NULL, XML_PARSE_NOBLANKS);

   if (doc == NULL) {
     printf("error: could not parse file %s\n", "./dal/testing/config.xml");
     return -1;
   }

   /*Get the root element node */
   root_element = xmlDocGetRootElement(doc);

   // Initialize a posix dal instance
   DAL_location maxloc = { .pod = 1, .block = 1, .cap = 1, .scatter = 1 };
   DAL dal = init_dal( root_element, maxloc );

   /* Free the xml Doc */
   xmlFreeDoc(doc);
   /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
   xmlCleanupParser();

   // check that initialization succeeded
   if ( dal == NULL ) {
      printf( "error: failed to initialize DAL: %s\n", strerror(errno) );
      return -1;
   }


   // Test WRITE thread logic
   
   // create a global state struct
   gthread_state gstate;
   pthread_mutex_t erasurelock;
   gstate.erasurelock = &erasurelock;
   if ( pthread_mutex_init( gstate.erasurelock, NULL ) ) {
      printf( "failed to initialize erasurelock\n" );
      return -1;
   }
   gstate.objID = "";
   gstate.location = maxloc;
   gstate.dmode = DAL_WRITE;
   gstate.dal = dal;
   gstate.offset = 0;
   gstate.minfo.N = 1;
   gstate.minfo.E = 0;
   gstate.minfo.O = 0;
   gstate.minfo.partsz = 4096;
   gstate.minfo.versz = 16388;
   gstate.minfo.blocksz = 0;
   gstate.minfo.crcsum = 0;
   gstate.minfo.totsz = 0;
   gstate.meta_error = 0;
   gstate.data_error = 0;

   // create an ioqueue for our data blocks
   gstate.ioq = create_ioqueue( gstate.minfo.versz, gstate.minfo.partsz, gstate.dmode );
   if ( gstate.ioq == NULL ) {
      printf( "Failed to create IOQueue for write thread!\n" );
      return -1;
   }

   // create a thread state reference
   void* tstate;

   // run our write_init function
   printf( "initializing write thread state..." );
   if ( write_init( 0, (void*)&gstate, &tstate ) ) {
      printf( "write_init() function failed!\n" );
      return -1;
   }
   printf( "done\n" );

   // loop through writing out our test data
   int partcnt = 0;
   int partlimit = 10;
   ioblock* iob = NULL;
   ioblock* pblock = NULL;
   while ( partcnt < partlimit ) {
      int resres = 0;
      while ( (resres = reserve_ioblock( &iob, &pblock, gstate.ioq )) == 0 ) {
         // get a target for our buffer
         void* fill_tgt = ioblock_write_target( iob );
         if ( fill_tgt == NULL ) {
            printf( "Failed to get fill target for ioblock!\n" );
            return -1;
         }

         // fill our ioblock
         size_t fill_sz = fill_buffer( partcnt * gstate.minfo.partsz, gstate.minfo.versz, gstate.minfo.partsz, fill_tgt, gstate.dmode );
         if ( fill_sz != gstate.minfo.partsz ) {
            printf( "Expected to fill %zu bytes, but instead filled %zu\n", gstate.minfo.partsz, fill_sz );
            return -1;
         }
         ioblock_update_fill( iob, fill_sz, 0 );
         partcnt++;
      }
      // check for an error condition forcing loop exit
      if ( resres < 0 ) {
         printf( "An error occured while trying to reserve a new IOBlock for writing\n" );
         return -1;
      }

      // otherwise, pblock should now be populated, run our write_consume function
      printf( "consuming ioblock..." );
      if ( write_consume( &tstate, (void**) &pblock ) ) {
         printf( "write_consume() function indicates an error!\n" );
         return -1;
      }
      printf( "done\n" );
      pblock = NULL;
   }

   // check if any data made it into our current ioblock
   if ( ioblock_get_fill( iob ) ) {
      // if so, consume it as well
      printf( "consuming final ioblock..." );
      if ( write_consume( &tstate, (void**) &pblock ) ) {
         printf( "write_consume() function indicates an error!\n" );
         return -1;
      }
      printf( "done\n" );
   }
   else {
      // otherwise, we'll need to release it
      release_ioblock( gstate.ioq );
   }

   // set some of our minfo values
   gstate.minfo.totsz = ( partcnt * gstate.minfo.partsz );

   // finally, call our write thread termination function
   printf( "terminating write state..." );
   write_term( &tstate, (void**) &pblock, 0 );
   printf( "done\n" );

   // check for any write errors
   if( gstate.meta_error ) {
      printf( "Write thread global state indicates a meta error was encountered!\n" );
   }
   if ( gstate.data_error ) {
      printf( "Write thread global state indicates a data error was encountered!\n" );
   }



   // Test READ thread logic

   // fix our state values
   gstate.dmode = DAL_READ;
   gstate.offset = 0;
   if ( destroy_ioqueue( gstate.ioq ) ) {
      printf( "Failed to destroy write IOQueue!\n" );
   }
   gstate.ioq = NULL;
   tstate = NULL;

   // call our read_init function
   printf( "Initializing our read thread state..." );
   if ( read_init( 0, (void**) &gstate, &tstate ) ) {
      printf( "Failure of the read_init() function!\n" );
      return -1;
   }
   printf( "done\n" );

   // create our ioqueue (based on minfo values gathered by the read thread)
   gstate.ioq = create_ioqueue( gstate.minfo.versz, gstate.minfo.partsz, gstate.dmode );
   if ( gstate.ioq == NULL ) {
      printf( "Failed to create ioqueue for read!\n" );
      return -1;
   }

   // NOTE -- here is where we'd signal the thread to begin work

   int prodres;
   int readparts = 0;
   iob = NULL;
   printf( "Producing a new read ioblock..." );
   while ( (prodres = read_produce( &tstate, (void**) &iob )) == 0 ) {
      printf( "done\n" );
      // get a read target
      size_t buffsz = 0;
      void* readtgt = ioblock_read_target( iob, &buffsz, NULL );
      if ( readtgt == NULL ) {
         printf( "Failed to get read target for produced ioblock!\n" );
         return -1;
      }
      // verify all data in our ioblock
      if ( verify_data( readparts * gstate.minfo.partsz, gstate.minfo.partsz, buffsz, readtgt, DAL_READ ) != 
            buffsz ) {
         printf( "Failed to verify all data from produced ioblock!\n" );
         return -1;
      }
      // release our produced ioblock
      if ( release_ioblock( gstate.ioq ) ) {
         printf( "Failed to release ioblock!\n" );
         return -1;
      }
      // NULL out our ioblock reference
      iob = NULL;
      // count how many parts we verified
      readparts += ( buffsz / gstate.minfo.partsz );
      if ( buffsz % gstate.minfo.partsz ) {
         printf( "WARNING: read buffer size (%zu) does not align with partsz (%zu)!\n", buffsz, gstate.minfo.partsz );
      }
      printf( "Producing a new read ioblock..." );
   }
   if ( prodres < 0 ) {
      printf( "read_produce indicates an error!\n" );
      return -1;
   }
   printf( "all reads complete\n" );

   // call our read term func
   printf( "Terminating read thread state..." );
   read_term( &tstate, (void**) &iob, 0 );
   printf( "done\n" );


   // CLEANUP

   // WE still need to destroy the read thread ioqueue
   if ( destroy_ioqueue( gstate.ioq ) ) {
      printf( "Failed to destroy read ioqueue!\n" );
      return -1;
   }

   // Delete the block we created
   if ( dal->del( dal->ctxt, maxloc, "" ) ) { printf( "warning: del failed!\n" ); }

   // destroy our erasurelock
   pthread_mutex_destroy( gstate.erasurelock );

   // Free the DAL
   if ( dal->cleanup( dal ) ) { printf( "error: failed to cleanup DAL\n" ); return -1; }

   // Finally, compare our structs
//   int retval=0;
//   if ( minfo_ref.N != minfo_fill.N ) {
//      printf( "error: set (%d) and retrieved (%d) meta info 'N' values do not match!\n", minfo_ref.N, minfo_fill.N );
//      retval=-1;
//   }
//   if ( minfo_ref.E != minfo_fill.E ) {
//      printf( "error: set (%d) and retrieved (%d) meta info 'E' values do not match!\n", minfo_ref.E, minfo_fill.E );
//      retval=-1;
//   }
//   if ( minfo_ref.O != minfo_fill.O ) {
//      printf( "error: set (%d) and retrieved (%d) meta info 'O' values do not match!\n", minfo_ref.O, minfo_fill.O );
//      retval=-1;
//   }
//   if ( minfo_ref.partsz != minfo_fill.partsz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'partsz' values do not match!\n", minfo_ref.partsz, minfo_fill.partsz );
//      retval=-1;
//   }
//   if ( minfo_ref.versz != minfo_fill.versz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'versz' values do not match!\n", minfo_ref.versz, minfo_fill.versz );
//      retval=-1;
//   }
//   if ( minfo_ref.blocksz != minfo_fill.blocksz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'blocksz' values do not match!\n", minfo_ref.blocksz, minfo_fill.blocksz );
//      retval=-1;
//   }
//   if ( minfo_ref.crcsum != minfo_fill.crcsum ) {
//      printf( "error: set (%lld) and retrieved (%lld) meta info 'crcsum' values do not match!\n", minfo_ref.crcsum, minfo_fill.crcsum );
//      retval=-1;
//   }
//   if ( minfo_ref.totsz != minfo_fill.totsz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'totsz' values do not match!\n", minfo_ref.totsz, minfo_fill.totsz );
//      retval=-1;
//   }

   return 0;
}


