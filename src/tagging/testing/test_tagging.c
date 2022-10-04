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
#include "tagging/tagging.c"

int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error conditions 
   //         which result in immediate termination

   // initialize an FTAG struct and output a string representation
   FTAG ftag;
   ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   ftag.ctag = "TEST_CLIENT";
   ftag.streamid = "teststreamidvalue";
   ftag.objfiles = 1024;
   ftag.objsize = 1073741824; // 1GiB
   ftag.refbreadth = 24;
   ftag.refdepth = 123;
   ftag.refdigits = 1;
   ftag.fileno = 0;
   ftag.objno = 0;
   ftag.offset = 234;
   ftag.endofstream = 0;
   ftag.protection.N = 10;
   ftag.protection.E = 2;
   ftag.protection.O = 1;
   ftag.protection.partsz = 1024;
   ftag.bytes = 1073741812; // 1GiB - 12 bytes
   ftag.availbytes = 1024; // 1KiB
   ftag.recoverybytes = 234;
   ftag.state = FTAG_SIZED | FTAG_WRITEABLE;
   size_t ftagstrlen = ftag_tostr( &(ftag), NULL, 0 );
   if ( ftagstrlen < 1 ) {
      printf( "failed to generate initial ftag string\n" );
      return -1;
   }
   char* ftagstr = malloc( sizeof(char) * (ftagstrlen + 1) );
   if ( ftagstr == NULL ) {
      printf( "failed to allocate space for initial ftag string\n" );
      return -1;
   }
   if ( ftag_tostr( &(ftag), ftagstr, ftagstrlen + 1 ) != ftagstrlen ) {
      printf( "inconsistent length of initial ftag string\n" );
      return -1;
   }
   // verify the ftag string
   FTAG oftag; // initialize to entirely different vals, ensuring all are changed
   oftag.majorversion = FTAG_CURRENT_MAJORVERSION + 12;
   oftag.minorversion = FTAG_CURRENT_MINORVERSION + 2;
   oftag.ctag = "TEST_OCLIENT";
   oftag.streamid = "testostreamid";
   oftag.objfiles = 10;
   oftag.objsize = 10737;
   oftag.refbreadth = 43;
   oftag.refdepth = 1;
   oftag.refdigits = 543;
   oftag.fileno = 1234;
   oftag.objno = 42;
   oftag.offset = 432;
   oftag.endofstream = 1;
   oftag.protection.N = 2;
   oftag.protection.E = 10;
   oftag.protection.O = 34;
   oftag.protection.partsz = 1;
   oftag.bytes = 1;
   oftag.availbytes = 1048576; // 1MiB
   oftag.recoverybytes = 5432;
   oftag.state = FTAG_INIT;
   if ( ftag_initstr( &(oftag), ftagstr ) ) {
      printf( "failed to init ftag from str: \"%s\"\n", ftagstr );
      return -1;
   }
   if ( ftag_cmp( &(ftag), &(oftag) ) ) {
      printf( "orig values differ from string vals: \"%s\"\n", ftagstr );
      return -1;
   }

   // output a meta tgt string
   char metatgtstr[1024] = {0};
   size_t metatgtstrlen = ftag_metatgt( &(ftag), metatgtstr, 1024 );
   if ( metatgtstrlen < 1  ||  metatgtstrlen >= 1024 ) {
      printf( "invalid length of metatgtstr: %zu\n", metatgtstrlen );
      return -1;
   }
   // identify the fileno value of the metatgtstr
   char enttype = -1;
   if ( ftag_metainfo( metatgtstr, &(enttype) ) ) {
      printf( "invalid fileno value from metatgtstr: \"%s\"\n", metatgtstr );
      return -1;
   }
   if ( enttype ) {
      printf( "unexpected type value for metatgtstr: %c\n", enttype );
      return -1;
   }

   // output a rebuild marker string
   metatgtstrlen = ftag_rebuildmarker( &(ftag), metatgtstr, 1024 );
   if ( metatgtstrlen < 1  ||  metatgtstrlen >= 1024 ) {
      printf( "invalid length of rebuild marker string: %zu\n", metatgtstrlen );
      return -1;
   }
   // identify the objno value of the rebuild marker str
   enttype = -1;
   if ( ftag_metainfo( metatgtstr, &(enttype) ) ) {
      printf( "invalid objno value from rebuild marker str: \"%s\"\n", metatgtstr );
      return -1;
   }
   if ( enttype != 1 ) {
      printf( "unexpected type value for rebuild marker str: %c\n", enttype );
      return -1;
   }

   // need to free strings allocated by us and the string initializer
   free( ftagstr );
   free( oftag.streamid );
   free( oftag.ctag );

   // test rebuild tag processing
   ne_state rtag = {
      .versz = 1234,
      .blocksz = 19744,
      .totsz = 59121,
      .meta_status = calloc( 5, sizeof(char) ),
      .data_status = calloc( 5, sizeof(char) ),
      .csum = NULL
   };
   if ( rtag.meta_status == NULL  ||  rtag.data_status == NULL ) {
      printf( "failed to allocate data/meta_status lists\n" );
      return -1;
   }
   rtag.meta_status[4] = 1;
   rtag.meta_status[1] = 1;
   rtag.data_status[0] = 1;
   size_t rtaglen = rtag_tostr( &(rtag), 5, NULL, 0 );
   if ( rtaglen < 1 ) {
      printf( "failed to indentify length of rebuild tag\n" );
      return -1;
   }
   char* rtagstr = malloc( sizeof(char) * (rtaglen + 1) );
   if ( rtagstr == NULL ) {
      printf( "failed to allocate rtag string\n" );
      return -1;
   }
   if ( rtag_tostr( &(rtag), 5, rtagstr, rtaglen + 1 ) != rtaglen ) {
      printf( "inconsistent length of rebuild tag string\n" );
      return -1;
   }
   ne_state newrtag = {
      .versz = 0,
      .blocksz = 0,
      .totsz = 0,
      .meta_status = calloc( 5, sizeof(char) ),
      .data_status = calloc( 5, sizeof(char) ),
      .csum = NULL
   };
   if ( newrtag.meta_status == NULL  ||  newrtag.data_status == NULL ) {
      printf( "failed to allocate new data/meta_status lists\n" );
      return -1;
   }
   if ( rtag_initstr( &(newrtag), 5, rtagstr ) ) {
      printf( "failed to parse rebuild tag string: \"%s\"\n", rtagstr );
      return -1;
   }
   if ( rtag.versz != newrtag.versz  ||  rtag.blocksz != newrtag.blocksz  ||
        rtag.totsz != newrtag.totsz ) {
      printf( "parsed rtag values do not match\n" );
      return -1;
   }
   int index = 0;
   for ( ; index < 5; index++ ) {
      if ( rtag.meta_status[index] != newrtag.meta_status[index] ) {
         printf( "meta status differs on index %d\n", index );
         return -1;
      }
      if ( rtag.data_status[index] != newrtag.data_status[index] ) {
         printf( "data status differs on index %d\n", index );
         return -1;
      }
   }
   free( rtagstr );
   free( rtag.data_status );
   free( rtag.meta_status );
   free( newrtag.data_status );
   free( newrtag.meta_status );

   return 0;
}


