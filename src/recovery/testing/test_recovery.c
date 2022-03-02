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
#include "recovery/recovery.c"

int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error conditions 
   //         which result in immediate termination

   // create a header structure
   RECOVERY_HEADER header = {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = strdup( "client-tag12345!" ),
      .streamid = strdup( "my-stream-id-987654321.1234456789" )
   };
   if ( header.ctag == NULL  ||  header.streamid == NULL ) {
      printf( "Failed to populate header ctag/streamid vals\n" );
      return -1;
   }

   // output header as string
   size_t headerstrlen = recovery_headertostr( &(header), NULL, 0 );
   if ( headerstrlen < 1 ) {
      printf( "Failed to generate the length of the recovheader string\n" );
      return -1;
   }
   char* headerstr = malloc( sizeof(char) * (headerstrlen + 1) );
   if ( headerstr == NULL ) {
      printf( "Failed to allocate space for header string of length %zu\n", headerstrlen );
      return -1;
   }
   if ( recovery_headertostr( &(header), headerstr, headerstrlen + 1) != headerstrlen ) {
      printf( "Inconsistent length of recovery header string\n" );
      return -1;
   }
   printf( "Recovery Header String: \n\"%s\"\n", headerstr );

   // parse header string and compare to orig
   RECOVERY_HEADER cmpheader;
   if ( parse_recov_header( headerstr, headerstrlen, &(cmpheader) ) !=
         (headerstr + (headerstrlen - 1)) ) {
      printf( "Failed to parse header string\n" );
      return -1;
   }
   if ( cmpheader.majorversion != header.majorversion  ||
        cmpheader.minorversion != header.minorversion ) {
      printf( "Parsed header has different version\n" );
      return -1;
   }
   if ( strcmp( cmpheader.ctag, header.ctag ) ) {
      printf( "Parsed header has different ctag: \"%s\"\n", cmpheader.ctag );
      return -1;
   }
   if ( strcmp( cmpheader.streamid, header.streamid ) ) {
      printf( "Parsed header has different streamid: \"%s\"\n", cmpheader.streamid );
      return -1;
   }

   // free cmpheader vals
   free( cmpheader.ctag );
   free( cmpheader.streamid );

   // create a new finfo struct
   RECOVERY_FINFO finfo = {
      .inode = 12345,
      .mode = 0724,
      .owner = 23456,
      .size = 10485760,
      .mtime.tv_sec = 1632428084,
      .mtime.tv_nsec = 123456789,
      .eof = 0,
      .path = strdup( "/gransom-allocation/read-only-data/subdir/tgtfile" )
   };
   if ( finfo.path == NULL ) {
      printf( "Failed to produce a new finfo struct\n" );
      return -1;
   }

   // output finfo as string
   size_t finfostrlen = recovery_finfotostr( &(finfo), NULL, 0 );
   if ( finfostrlen < 1 ) {
      printf( "Failed to generate the length of the recovfinfo string\n" );
      return -1;
   }
   char* finfostr = malloc( sizeof(char) * (finfostrlen + 1) );
   if ( finfostr == NULL ) {
      printf( "Failed to allocate space for finfo string of length %zu\n", finfostrlen );
      return -1;
   }
   if ( recovery_finfotostr( &(finfo), finfostr, finfostrlen + 1) != finfostrlen ) {
      printf( "Inconsistent length of recovery finfo string\n" );
      return -1;
   }
   printf( "Recovery Header String: \n\"%s\"\n", finfostr );

   // parse finfo string and compare to orig
   RECOVERY_FINFO cmpfinfo;
   if ( parse_recov_finfo( finfostr, finfostrlen, &(cmpfinfo) ) !=
         (finfostr + (finfostrlen - 1)) ) {
      printf( "Failed to parse finfo string\n" );
      return -1;
   }
   if ( cmpfinfo.inode != finfo.inode ) {
      printf( "Parsed finfo has different inode: %lu\n", cmpfinfo.inode );
      return -1;
   }
   if ( cmpfinfo.mode != finfo.mode ) {
      printf( "Parsed finfo has different mode: %o\n", cmpfinfo.mode );
      return -1;
   }
   if ( cmpfinfo.owner != finfo.owner ) {
      printf( "Parsed finfo has different owner: %u\n", cmpfinfo.owner );
      return -1;
   }
   if ( cmpfinfo.group != finfo.group ) {
      printf( "Parsed finfo has different group: %u\n", cmpfinfo.group );
      return -1;
   }
   if ( cmpfinfo.size != finfo.size ) {
      printf( "Parsed finfo has different size: %zu\n", cmpfinfo.size );
      return -1;
   }
   if ( cmpfinfo.mtime.tv_sec != finfo.mtime.tv_sec  ||
        cmpfinfo.mtime.tv_nsec != finfo.mtime.tv_nsec ) {
      printf( "Parsed finfo has different time: %lu.%ld\n",
              cmpfinfo.mtime.tv_sec, cmpfinfo.mtime.tv_nsec );
      return -1;
   }
   if ( cmpfinfo.eof != finfo.eof ) {
      printf( "Parsed finfo has different eof: %d\n", (int)cmpfinfo.eof );
      return -1;
   }
   if ( strcmp( cmpfinfo.path, finfo.path ) ) {
      printf( "Parsed finfo has different path: \"%s\"\n", cmpfinfo.path );
      return -1;
   }

   // free cmpfinfo vals
   free( cmpfinfo.path );
   cmpfinfo.inode = 0;
   cmpfinfo.mode = 0;
   cmpfinfo.owner = 0;
   cmpfinfo.group = 0;
   cmpfinfo.size = 0;
   cmpfinfo.mtime.tv_sec = 0;
   cmpfinfo.mtime.tv_nsec = 0;
   cmpfinfo.eof = 1;
   cmpfinfo.path = NULL;


   // parse finfo string via finfofromstr() and compare
   if ( recovery_finfofromstr( &(cmpfinfo), finfostr, finfostrlen ) ) {
      printf( "Failure of recovery_finfofromstr()\n" );
      return -1;
   }
   if ( cmpfinfo.inode != finfo.inode ) {
      printf( "Parsed finfo has different inode: %lu\n", cmpfinfo.inode );
      return -1;
   }
   if ( cmpfinfo.mode != finfo.mode ) {
      printf( "Parsed finfo has different mode: %o\n", cmpfinfo.mode );
      return -1;
   }
   if ( cmpfinfo.owner != finfo.owner ) {
      printf( "Parsed finfo has different owner: %u\n", cmpfinfo.owner );
      return -1;
   }
   if ( cmpfinfo.group != finfo.group ) {
      printf( "Parsed finfo has different group: %u\n", cmpfinfo.group );
      return -1;
   }
   if ( cmpfinfo.size != finfo.size ) {
      printf( "Parsed finfo has different size: %zu\n", cmpfinfo.size );
      return -1;
   }
   if ( cmpfinfo.mtime.tv_sec != finfo.mtime.tv_sec  ||
        cmpfinfo.mtime.tv_nsec != finfo.mtime.tv_nsec ) {
      printf( "Parsed finfo has different time: %lu.%ld\n",
              cmpfinfo.mtime.tv_sec, cmpfinfo.mtime.tv_nsec );
      return -1;
   }
   if ( cmpfinfo.eof != finfo.eof ) {
      printf( "Parsed finfo has different eof: %d\n", (int)cmpfinfo.eof );
      return -1;
   }
   if ( strcmp( cmpfinfo.path, finfo.path ) ) {
      printf( "Parsed finfo has different path: \"%s\"\n", cmpfinfo.path );
      return -1;
   }

   // free cmpfinfo vals
   free( cmpfinfo.path );

   // allocate a fake data object
   RECOVERY_FINFO finfo2 = {
      .inode = 654321,
      .mode = 01777,
      .owner = 12345,
      .size = 0,
      .mtime.tv_sec = 2632428084,
      .mtime.tv_nsec = 987654321,
      .eof = 1,
      .path = strdup( "/root-subdir/tgtfile2" )
   };
   if ( finfo2.path == NULL ) {
      printf( "Failed to allocate finfo2 string\n" );
      return -1;
   }
   RECOVERY_FINFO finfo3 = {
      .inode = 6543,
      .mode = 0027,
      .owner = 12345,
      .size = 102400,
      .mtime.tv_sec = 1632428081,
      .mtime.tv_nsec = 123,
      .eof = 1,
      .path = strdup( "/gransom-allocation/heavily-protected-data/tgtfile3" )
   };
   if ( finfo3.path == NULL ) {
      printf( "Failed to allocate finfo3 string\n" );
      return -1;
   }
   size_t finfo2strlen = recovery_finfotostr( &(finfo2), NULL, 0 );
   size_t finfo3strlen = recovery_finfotostr( &(finfo3), NULL, 0 );
   if ( finfo2strlen == 0  ||  finfo3strlen == 0 ) {
      printf( "Failed string length check for finfo2 and/or 3\n" );
      return -1;
   }
   size_t objlen = headerstrlen +
                   finfostrlen + 10485760 +
                   finfo2strlen + 0 +
                   finfo3strlen + 10240; // only including trailing 10240 bytes of finfo3
   void* objbuffer = malloc( objlen );
   if ( objbuffer == NULL ) {
      printf( "Failed to allocate object buffer of length %zu\n", objlen );
      return -1;
   }
   memcpy( objbuffer, headerstr, headerstrlen );
   size_t populated = headerstrlen;
   bzero( objbuffer + populated, 10240 );
   populated += 10240;
   if ( recovery_finfotostr( &(finfo3), objbuffer + populated, finfo3strlen + 1 ) != finfo3strlen ) {
      printf( "Inconsistent length of string for finfo3\n" );
      return -1;
   }
   printf( "Finfo3 String: \"%s\"\n", (char*)objbuffer + populated );
   populated += finfo3strlen;
   bzero( objbuffer + populated, 0 );
   populated += 0;
   if ( recovery_finfotostr( &(finfo2), objbuffer + populated, finfo2strlen + 1 ) != finfo2strlen ) {
      printf( "Inconsistent length of string for finfo2\n" );
      return -1;
   }
   printf( "Finfo2 String: \"%s\"\n", (char*)objbuffer + populated );
   populated += finfo2strlen;
   bzero( objbuffer + populated, 10485760 );
   populated += 10485760;
   memcpy( objbuffer + populated, finfostr, finfostrlen );
   populated += finfostrlen;
   if ( populated != objlen ) { printf( "Garrett can't do math!!!!\n" ); return -1; }

   // initialize a recovery object against this object
   RECOVERY recov = recovery_init( objbuffer, objlen, &(cmpheader) );
   if ( recov == NULL ) {
      printf( "Failed to init recov against fake object\n" );
      return -1;
   }

   // create a zero buff for comparision
   void* zerobuf = calloc( 1, 10485760 );
   if ( zerobuf == NULL ) {
      printf( "Failed to allocate zerobuf\n" );
      return -1;
   }

   // iterate over files, verifying all info
   void* databuf = NULL;
   size_t bufsize = 0;
   // FINFO3
   if ( recovery_nextfile( recov, &(cmpfinfo), &(databuf), &(bufsize) ) != 1 ) {
      printf( "Failed to retrieve finfo3 info\n" );
      return -1;
   }
   if ( bufsize != 10240 ) {
      printf( "Bufsize of retrieved finfo3 has an unexpected value: %zu\n", bufsize );
      return -1;
   }
   if ( memcmp( databuf, zerobuf, 10240 ) ) {
      printf( "Databuf of finfo3 has non-zero values\n" );
      return -1;
   }
   if ( cmpfinfo.inode != finfo3.inode ) {
      printf( "Recovered finfo3 has different inode: %lu\n", cmpfinfo.inode );
      return -1;
   }
   if ( cmpfinfo.mode != finfo3.mode ) {
      printf( "Recovered finfo3 has different mode: %o\n", cmpfinfo.mode );
      return -1;
   }
   if ( cmpfinfo.owner != finfo3.owner ) {
      printf( "Recovered finfo3 has different owner: %u\n", cmpfinfo.owner );
      return -1;
   }
   if ( cmpfinfo.group != finfo3.group ) {
      printf( "Recovered finfo3 has different group: %u\n", cmpfinfo.group );
      return -1;
   }
   if ( cmpfinfo.size != finfo3.size ) {
      printf( "Recovered finfo3 has different size: %zu\n", cmpfinfo.size );
      return -1;
   }
   if ( cmpfinfo.mtime.tv_sec != finfo3.mtime.tv_sec  ||
        cmpfinfo.mtime.tv_nsec != finfo3.mtime.tv_nsec ) {
      printf( "Recovered finfo3 has different time: %lu.%ld\n",
              cmpfinfo.mtime.tv_sec, cmpfinfo.mtime.tv_nsec );
      return -1;
   }
   if ( cmpfinfo.eof != finfo3.eof ) {
      printf( "Recovered finfo3 has different eof: %d\n", (int)cmpfinfo.eof );
      return -1;
   }
   if ( strcmp( cmpfinfo.path, finfo3.path ) ) {
      printf( "Recovered finfo3 has different path: \"%s\"\n", cmpfinfo.path );
      return -1;
   }
   free( cmpfinfo.path );
   // FINFO2
   if ( recovery_nextfile( recov, &(cmpfinfo), &(databuf), &(bufsize) ) != 1 ) {
      printf( "Failed to retrieve finfo2 info\n" );
      return -1;
   }
   if ( bufsize ) {
      printf( "Bufsize of retrieved finfo2 has an unexpected value: %zu\n", bufsize );
      return -1;
   }
   if ( cmpfinfo.inode != finfo2.inode ) {
      printf( "Recovered finfo2 has different inode: %lu\n", cmpfinfo.inode );
      return -1;
   }
   if ( cmpfinfo.mode != finfo2.mode ) {
      printf( "Recovered finfo2 has different mode: %o\n", cmpfinfo.mode );
      return -1;
   }
   if ( cmpfinfo.owner != finfo2.owner ) {
      printf( "Recovered finfo2 has different owner: %u\n", cmpfinfo.owner );
      return -1;
   }
   if ( cmpfinfo.group != finfo2.group ) {
      printf( "Recovered finfo2 has different group: %u\n", cmpfinfo.group );
      return -1;
   }
   if ( cmpfinfo.size != finfo2.size ) {
      printf( "Recovered finfo2 has different size: %zu\n", cmpfinfo.size );
      return -1;
   }
   if ( cmpfinfo.mtime.tv_sec != finfo2.mtime.tv_sec  ||
        cmpfinfo.mtime.tv_nsec != finfo2.mtime.tv_nsec ) {
      printf( "Recovered finfo2 has different time: %lu.%ld\n",
              cmpfinfo.mtime.tv_sec, cmpfinfo.mtime.tv_nsec );
      return -1;
   }
   if ( cmpfinfo.eof != finfo2.eof ) {
      printf( "Recovered finfo2 has different eof: %d\n", (int)cmpfinfo.eof );
      return -1;
   }
   if ( strcmp( cmpfinfo.path, finfo2.path ) ) {
      printf( "Recovered finfo2 has different path: \"%s\"\n", cmpfinfo.path );
      return -1;
   }
   free( cmpfinfo.path );
   // FINFO
   if ( recovery_nextfile( recov, &(cmpfinfo), &(databuf), &(bufsize) ) != 1 ) {
      printf( "Failed to retrieve finfo info\n" );
      return -1;
   }
   if ( bufsize != 10485760 ) {
      printf( "Bufsize of retrieved finfo3 has an unexpected value: %zu\n", bufsize );
      return -1;
   }
   if ( memcmp( databuf, zerobuf, 10485760 ) ) {
      printf( "Databuf of finfo3 has non-zero values\n" );
      return -1;
   }
   if ( cmpfinfo.inode != finfo.inode ) {
      printf( "Recovered finfo has different inode: %lu\n", cmpfinfo.inode );
      return -1;
   }
   if ( cmpfinfo.mode != finfo.mode ) {
      printf( "Recovered finfo has different mode: %o\n", cmpfinfo.mode );
      return -1;
   }
   if ( cmpfinfo.owner != finfo.owner ) {
      printf( "Recovered finfo has different owner: %u\n", cmpfinfo.owner );
      return -1;
   }
   if ( cmpfinfo.group != finfo.group ) {
      printf( "Recovered finfo has different group: %u\n", cmpfinfo.group );
      return -1;
   }
   if ( cmpfinfo.size != finfo.size ) {
      printf( "Recovered finfo has different size: %zu\n", cmpfinfo.size );
      return -1;
   }
   if ( cmpfinfo.mtime.tv_sec != finfo.mtime.tv_sec  ||
        cmpfinfo.mtime.tv_nsec != finfo.mtime.tv_nsec ) {
      printf( "Recovered finfo has different time: %lu.%ld\n",
              cmpfinfo.mtime.tv_sec, cmpfinfo.mtime.tv_nsec );
      return -1;
   }
   if ( cmpfinfo.eof != finfo.eof ) {
      printf( "Recovered finfo has different eof: %d\n", (int)cmpfinfo.eof );
      return -1;
   }
   if ( strcmp( cmpfinfo.path, finfo.path ) ) {
      printf( "Recovered finfo has different path: \"%s\"\n", cmpfinfo.path );
      return -1;
   }
   free( cmpfinfo.path );
   if ( recovery_nextfile( recov, &(cmpfinfo), &(databuf), &(bufsize) ) != 0 ) {
      printf( "4th nextfile call gave unexpected return value\n" );
      return -1;
   }

   // attempt a nextfile() call, just using the same buffer
   if ( recovery_cont( recov, objbuffer, objlen ) ) {
      printf( "Failed to continue current recov object\n" );
      return -1;
   }

   // close the recovery obj
   if ( recovery_close( recov ) ) {
      printf( "Failed to close recovery ref\n" );
      return -1;
   }

   // cleanup object refs
   free( finfo2.path );
   free( finfo3.path );
   free( cmpheader.ctag );
   free( cmpheader.streamid );
   free( objbuffer );
   free( zerobuf );

   // free the recovery header string
   free( headerstr );

   // free finfostr
   free( finfostr );

   // cleanup our finfo
   free( finfo.path );

   // cleanup our header
   free( header.ctag );
   free( header.streamid );

   return 0;
}


