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

// directly including the C file allows more flexibility for these tests
#include "datastream/datastream.c"

#include <ne.h>

#include <unistd.h>
#include <stdio.h>
#include <ftw.h>


// WARNING: error-prone and ugly method of deleting dir trees, written for simplicity only
//          don't replicate this junk into ANY production code paths!
size_t dirlistpos = 0;
char** dirlist = NULL;
int ftwnotedir( const char* fpath, const struct stat* sb, int typeflag ) {
   if ( typeflag != FTW_D ) {
      printf( "Encountered non-directory during tree deletion: \"%s\"\n", fpath );
      return -1;
   }
   dirlist[dirlistpos] = strdup( fpath );
   if ( dirlist[dirlistpos] == NULL ) {
      printf( "Failed to duplicate dir name: \"%s\"\n", fpath );
      return -1;
   }
   dirlistpos++;
   if ( dirlistpos >= 4096 ) { printf( "Dirlist has insufficient length! (curtgt = %s)\n", fpath ); return -1; }
   return 0;
}
int deletesubdirs( const char* basepath ) {
   dirlist = malloc( sizeof(char*) * 4096 );
   if ( dirlist == NULL ) {
      printf( "Failed to allocate dirlist\n" );
      return -1;
   }
   if ( ftw( basepath, ftwnotedir, 100 ) ) {
      printf( "Failed to identify reference dirs of \"%s\"\n", basepath );
      return -1;
   }
   int retval = 0;
   while ( dirlistpos ) {
      dirlistpos--;
      if ( strcmp( dirlist[dirlistpos], basepath ) ) {
         printf( "Deleting: \"%s\"\n", dirlist[dirlistpos] );
         if ( rmdir( dirlist[dirlistpos] ) ) {
            printf( "ERROR -- failed to delete \"%s\"\n", dirlist[dirlistpos] );
            retval = -1;
         }
      }
      free( dirlist[dirlistpos] );
   }
   free( dirlist );
   return retval;
}


int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error conditions 
   //         which result in immediate termination

   // Initialize the libxml lib and check for API mismatches
   LIBXML_TEST_VERSION

   // create the dirs necessary for DAL/MDAL initialization (ignore EEXIST)
   errno = 0;
   if ( mkdir( "./test_datastream_topdir", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_datastream_topdir\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_datastream_topdir/dal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_datastream_topdir/dal_root\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_datastream_topdir/mdal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_datastream_topdir/mdal_root\"\n" );
      return -1;
   }

   // establish a new marfs config
   marfs_config* config = config_init( "./testing/config.xml" );
   if ( config == NULL ) {
      printf( "Failed to initialize marfs config\n" );
      return -1;
   }

   // create all namespaces associated with the config
   if ( config_verify( config, "./.", 1, 1, 1, 1 ) ) {
      printf( "Failed to validate the marfs config\n" );
      return -1;
   }

   // establish a rootNS position
   MDAL rootmdal = config->rootns->prepo->metascheme.mdal;
   marfs_position pos = {
      .ns = config->rootns,
      .depth = 0,
      .ctxt = rootmdal->newctxt( "/.", rootmdal->ctxt )
   };
   if ( pos.ctxt == NULL ) {
      printf( "Failed to establish root MDAL_CTXT for position\n" );
      return -1;
   }
   // establish a data buffer, filled with arbitrary data content
   void* databuf = calloc( 10, 1024 * 1024 ); // 10MiB
   if ( databuf == NULL ) {
      printf( "Failed to allocate 10MiB data buffer\n" );
      return -1;
   }
   size_t numints = (1024 * 1024 * 10) / sizeof(int);
   int index = 0;
   for ( ; index < numints; index++ ) {
      *( ((int*)databuf) + index ) = (int)(numints - index);
   }
   char* readbuf[1048576] = {0};


// EXTERNAL FUNC TESTS
// CHUNKED OBJECT TESTING

   // create a new stream
   DATASTREAM stream = NULL;
   if ( datastream_create( &(stream), "file1", &(pos), 0744, "NO-PACK-repackCLIENT" ) ) {
      printf( "create failure for 'file1' of no-pack\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 4096 ) != 4096 ) {
      printf( "write failure for 'file1' of no-pack\n" );
      return -1;
   }
   struct timespec times[2];
   times[0].tv_sec = 123456;
   times[0].tv_nsec = 0;
   times[1].tv_sec = 7654321;
   times[1].tv_nsec = 123;
   if ( datastream_utimens( &(stream), times ) ) {
      printf( "failed to set times on 'file1' of no-pack\n" );
      return -1;
   }

   // keep track of this file's rpath
   char* rpath = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath == NULL ) {
      printf( "Failed to identify the rpath of no-pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   char* objname;
   ne_erasure objerasure;
   ne_location objlocation;
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname), &(objerasure), &(objlocation) ) ) {
      printf( "Failed to identify data object of 'file1' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a new file off the same stream
   if ( datastream_create( &(stream), "file2", &(pos), 0622, NULL ) ) {
      printf( "create failure for 'file2' of no-pack\n" );
      return -1;
   }
   if ( datastream_setrecoverypath( &(stream), "file2-recovset" ) ) {
      printf( "failed to set recovery path for 'file2' of no-pack\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 1006 ) != 1006 ) {
      printf( "write failure for 'file2' of no-pack\n" );
      return -1;
   }

   // validate that we have switched to a fresh data object
   if ( stream->objno != 1 ) {
      printf( "unexpected objno for 'file2' of no-pack: %zu\n", stream->objno );
      return -1;
   }
   if ( stream->curfile ) {
      printf( "unexpected curfile for 'file2' of no-pack: %zu\n", stream->curfile );
      return -1;
   }

   // keep track of this file's rpath
   char* rpath2 = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath2 == NULL ) {
      printf( "Failed to identify the rpath of no-pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   char* objname2 = NULL;
   ne_erasure objerasure2;
   ne_location objlocation2;
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      printf( "Failed to identify data object of no-pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a chunked file off the same stream
   if ( datastream_create( &(stream), "file3", &(pos), 0600, "NO-PACK-repackCLIENT" ) ) {
      printf( "create failure for 'file3' of no-pack\n" );
      return -1;
   }
   if ( stream->objno != 2 ) {
      printf( "unexpected objno after 'file3' create for no-pack: %zu\n", stream->objno );
      return -1;
   }
   if ( stream->curfile ) {
      printf( "unexpected curfile for 'file3' of no-pack: %zu\n", stream->curfile );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 1024 * 1024 * 3 ) != (1024 * 1024 * 3) ) {
      printf( "write failure for 'file3' of no-pack\n" );
      return -1;
   }
   if ( stream->objno != 5 ) {
      printf( "unexpected objno after write of 'file3' in no-pack: %zu\n", stream->objno );
      return -1;
   }

   // keep track of this file's rpath
   char* rpath3 = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath3 == NULL ) {
      printf( "Failed to identify the rpath of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data objects
   char* objname3 = NULL;
   ne_erasure objerasure3;
   ne_location objlocation3;
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname3), &(objerasure3), &(objlocation3) ) ) {
      printf( "Failed to identify data object 1 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   char* objname4 = NULL;
   ne_erasure objerasure4;
   ne_location objlocation4;
   FTAG tmptag = stream->files->ftag;
   tmptag.objno++;
   if ( datastream_objtarget( &(tmptag), &(stream->ns->prepo->datascheme), &(objname4), &(objerasure4), &(objlocation4) ) ) {
      printf( "Failed to identify data object 2 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   char* objname5 = NULL;
   ne_erasure objerasure5;
   ne_location objlocation5;
   tmptag.objno++;
   if ( datastream_objtarget( &(tmptag), &(stream->ns->prepo->datascheme), &(objname5), &(objerasure5), &(objlocation5) ) ) {
      printf( "Failed to identify data object 2 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   char* objname6 = NULL;
   ne_erasure objerasure6;
   ne_location objlocation6;
   tmptag.objno++;
   if ( datastream_objtarget( &(tmptag), &(stream->ns->prepo->datascheme), &(objname6), &(objerasure6), &(objlocation6) ) ) {
      printf( "Failed to identify data object 2 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }

   // close the stream
   if ( datastream_close( &(stream) ) ) {
      printf( "close failure for no-pack\n" );
      return -1;
   }


   // repack the third file
   DATASTREAM repackstream = NULL;
   if ( datastream_repack( &(repackstream), rpath3, &(pos), NULL ) ) {
      printf( "failed to open repack stream for target1 \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   // keep track of this file's rpath
   char* rpckpath1 = datastream_genrpath( &(repackstream->files->ftag), repackstream->ns->prepo->metascheme.reftable );
   if ( rpckpath1 == NULL ) {
      printf( "Failed to identify the repack path of 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data objects
   char* rpckobjname1 = NULL;
   ne_erasure rpckobjerasure1;
   ne_location rpckobjlocation1;
   if ( datastream_objtarget( &(repackstream->files->ftag), &(repackstream->ns->prepo->datascheme), &(rpckobjname1), &(rpckobjerasure1), &(rpckobjlocation1) ) ) {
      printf( "Failed to identify data object 1 of repacked 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   tmptag = repackstream->files->ftag;
   tmptag.objno++;
   char* rpckobjname2 = NULL;
   ne_erasure rpckobjerasure2;
   ne_location rpckobjlocation2;
   if ( datastream_objtarget( &(tmptag), &(repackstream->ns->prepo->datascheme), &(rpckobjname2), &(rpckobjerasure2), &(rpckobjlocation2) ) ) {
      printf( "Failed to identify data object 2 of repacked 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   tmptag.objno++;
   char* rpckobjname3 = NULL;
   ne_erasure rpckobjerasure3;
   ne_location rpckobjlocation3;
   if ( datastream_objtarget( &(tmptag), &(repackstream->ns->prepo->datascheme), &(rpckobjname3), &(rpckobjerasure3), &(rpckobjlocation3) ) ) {
      printf( "Failed to identify data object 3 of repacked 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   tmptag.objno++;
   char* rpckobjname4 = NULL;
   ne_erasure rpckobjerasure4;
   ne_location rpckobjlocation4;
   if ( datastream_objtarget( &(tmptag), &(repackstream->ns->prepo->datascheme), &(rpckobjname4), &(rpckobjerasure4), &(rpckobjlocation4) ) ) {
      printf( "Failed to identify data object 4 of repacked 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   if ( datastream_scan( &(stream), rpath3, &(pos) ) ) {
      printf( "failed to open scan/read stream for target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_read( &(stream), readbuf, 1048576 ) != 1048576 ) {
      printf( "failed to read/repack content of target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, 1048576 ) ) {
      printf( "unexpected read/repack content of file3 / target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_write( &(repackstream), readbuf, 1048576 ) != 1048576 ) {
      printf( "failed to write/repack content of file3 target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_read( &(stream), readbuf, 1048576 ) != 1048576 ) {
      printf( "failed to read/repack2 content of target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf + 1048576, 1048576 ) ) {
      printf( "unexpected read/repack2 content of file3 / target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_write( &(repackstream), readbuf, 1048576 ) != 1048576 ) {
      printf( "failed to write/repack2 content of file3 target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_read( &(stream), readbuf, 1048576 ) != 1048576 ) {
      printf( "failed to read/repack3 content of target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf + (2 * 1048576), 1048576 ) ) {
      printf( "unexpected read/repack3 content of file3 / target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_write( &(repackstream), readbuf, 1048576 ) != 1048576 ) {
      printf( "failed to write/repack3 content of file3 target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   // repack the first file
   if ( datastream_repack( &(repackstream), rpath, &(pos), NULL ) ) {
      printf( "failed to open repack stream for target2 \"%s\" (%s)\n", rpath, strerror(errno) );
      return -1;
   }
   if ( datastream_scan( &(stream), rpath, &(pos) ) ) {
      printf( "failed to open scan/read stream for target \"%s\" (%s)\n", rpath, strerror(errno) );
      return -1;
   }
   if ( datastream_read( &(stream), readbuf, 4096 ) != 4096 ) {
      printf( "failed to read/repack content of target \"%s\" (%s)\n", rpath, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, 4096 ) ) {
      printf( "unexpected read/repack content of file1 / target \"%s\" (%s)\n", rpath, strerror(errno) );
      return -1;
   }
   if ( datastream_write( &(repackstream), readbuf, 4096 ) != 4096 ) {
      printf( "failed to write/repack content of file1 target \"%s\" (%s)\n", rpath, strerror(errno) );
      return -1;
   }
   if ( repackstream->curfile ) {
      printf( "repack stream is unexpectedly packing file1 onto file3\n" );
      return -1;
   }
   // keep track of this file's rpath
   char* rpckpath2 = datastream_genrpath( &(repackstream->files->ftag), repackstream->ns->prepo->metascheme.reftable );
   if ( rpckpath2 == NULL ) {
      printf( "Failed to identify the repack path of 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data objects
   char* rpckobjname5 = NULL;
   ne_erasure rpckobjerasure5;
   ne_location rpckobjlocation5;
   if ( datastream_objtarget( &(repackstream->files->ftag), &(repackstream->ns->prepo->datascheme), &(rpckobjname5), &(rpckobjerasure5), &(rpckobjlocation5) ) ) {
      printf( "Failed to identify data object 5 of repacked 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // close our streams
   if ( datastream_release( &(stream) ) ) {
      printf( "failed to release read/repack stream\n" );
      return -1;
   }
   if ( datastream_close( &(repackstream) ) ) {
      printf( "failed to close write/repack stream\n" );
      return -1;
   }


   // cleanup 'file1' refs ( except file itself! )
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath ) ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath );
      return -1;
   }
   free( rpath );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname, objlocation ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname );
      return -1;
   }
   free( objname );
   // cleanup 'file3' refs ( except file itself! )
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath3 ) ) {
      printf( "Failed to unlink rpath3: \"%s\"\n", rpath3 );
      return -1;
   }
   free( rpath3 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname3, objlocation3 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname3 );
      return -1;
   }
   free( objname3 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname4, objlocation4 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname4 );
      return -1;
   }
   free( objname4 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname5, objlocation5 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname5 );
      return -1;
   }
   free( objname5 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname6, objlocation6 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname6 );
      return -1;
   }
   free( objname6 );


   // read back the written files
   // file1
   if ( datastream_open( &(stream), READ_STREAM, "file1", &(pos), NULL ) ) {
      printf( "failed to open 'file1' of no-pack for read\n" );
      return -1;
   }
   ssize_t iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 4096 ) {
      printf( "unexpected read res for 'file1' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, 4096 ) ) {
      printf( "unexpected content of 'file1' of no-pack\n" );
      return -1;
   }
   // file2
   if ( datastream_open( &(stream), READ_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 'file2' of no-pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 1006 ) {
      printf( "unexpected read res for 'file2' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, iores ) ) {
      printf( "unexpected content of 'file2' of no-pack\n" );
      return -1;
   }
   if ( datastream_close( &(stream) ) ) {
      printf( "failed to close no-pack read stream\n" );
      return -1;
   }
   // file3
   if ( datastream_open( &(stream), READ_STREAM, "file3", &(pos), NULL ) ) {
      printf( "failed to open 'file3' of no-pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 1048576 ) {
      printf( "unexpected res for read1 from 'file3' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, iores ) ) {
      printf( "unexpected content of read1 for 'file3' of no-pack\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 1048576 ) {
      printf( "unexpected res for read2 from 'file3' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf + 1048576, iores ) ) {
      printf( "unexpected content of read2 from 'file3' of no-pack\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 1048576 ) {
      printf( "unexpected res for read2 from 'file3' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf + (2 * 1048576), iores ) ) {
      printf( "unexpected content of read2 from 'file3' of no-pack\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores ) {
      printf( "unexpected res for read3 from 'file3' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( datastream_close( &(stream) ) ) {
      printf( "failed to close no-pack read stream\n" );
      return -1;
   }


   // cleanup 'file2' refs
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file2" ) ) {
      printf( "Failed to unlink \"file2\"\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath2 ) ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath2 );
      return -1;
   }
   free( rpath2 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname2, objlocation2 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname2 );
      return -1;
   }
   free( objname2 );
   // cleanup 'file1'
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file1" ) ) {
      printf( "Failed to unlink \"file1\"\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpckpath2 ) ) {
      printf( "Failed to unlink rpath-rpck2: \"%s\"\n", rpckpath2 );
      return -1;
   }
   free( rpckpath2 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, rpckobjname5, rpckobjlocation5 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", rpckobjname5 );
      return -1;
   }
   free( rpckobjname5 );
   // cleanup 'file3'
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file3" ) ) {
      printf( "Failed to unlink \"file3\"\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpckpath1 ) ) {
      printf( "Failed to unlink rpath-rpck1: \"%s\"\n", rpckpath1 );
      return -1;
   }
   free( rpckpath1 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, rpckobjname1, rpckobjlocation1 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", rpckobjname1 );
      return -1;
   }
   free( rpckobjname1 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, rpckobjname2, rpckobjlocation2 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", rpckobjname2 );
      return -1;
   }
   free( rpckobjname2 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, rpckobjname3, rpckobjlocation3 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", rpckobjname3 );
      return -1;
   }
   free( rpckobjname3 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, rpckobjname4, rpckobjlocation4 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", rpckobjname4 );
      return -1;
   }
   free( rpckobjname4 );


// PACKED OBJECT TESTING

   // transition to a NS that supports packing
   char* configtgt = strdup( "./gransom-allocation/nothin" );
   if ( configtgt == NULL ) {
      printf( "Failed to duplicate configtgt string\n" );
      return -1;
   }
   if ( config_traverse( config, &(pos), &(configtgt), 0 ) != 1 ) {
      printf( "failed to traverse config subpath: \"%s\"\n", configtgt );
      return -1;
   }
   free( configtgt );


   // create a new stream
   if ( datastream_create( &(stream), "file1", &(pos), 0744, "PACK-CLIENT" ) ) {
      printf( "create failure for 'file1' of pack\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 1024 * 2 ) != (1024 * 2) ) {
      printf( "write failure for 'file1' of pack\n" );
      return -1;
   }

   // keep track of this file's rpath
   rpath = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath == NULL ) {
      printf( "Failed to identify the rpath of pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname), &(objerasure), &(objlocation) ) ) {
      printf( "Failed to identify data object of pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a new file off of the same stream
   if ( datastream_create( &(stream), "file2", &(pos), 0600, "PACK-CLIENT" ) ) {
      printf( "create failure for 'file2' of pack\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 33 ) != 33 ) {
      printf( "write failure for 'file2' of pack\n" );
      return -1;
   }
   if ( stream->curfile != 1 ) {
      printf( "unexpected curfile value for file2 of pack: %zu\n", stream->curfile );
      return -1;
   }

   // keep track of this file's rpath
   rpath2 = datastream_genrpath( &(stream->files[1].ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath2 == NULL ) {
      printf( "Failed to identify the rpath of pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and identify the data object (should match previous)
   if ( datastream_objtarget( &(stream->files[1].ftag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      printf( "Failed to identify data object of pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   // validate that the data object matches the previous
   if ( strcmp( objname, objname2 ) ) {
      printf( "Object mismatch between file1 and file2 of pack:\n   obj1: \"%s\"\n   obj2: \"%s\"\n", objname, objname2 );
      return -1;
   }
   free( objname2 );
   if ( objlocation.pod != objlocation2.pod  ||
        objlocation.cap != objlocation2.cap  ||
        objlocation.scatter != objlocation2.scatter ) {
      printf( "Location mismatch between file1 and file2 of pack:\n   obj1.pod: %d\n   obj2.pod: %d\n   obj1.cap: %d\n   obj2.cap: %d\n   obj1.scat: %d\n   obj2.scat: %d\n", objlocation.pod, objlocation2.pod, objlocation.cap, objlocation2.cap, objlocation.scatter, objlocation2.scatter );
      return -1;
   }

   // create a new 'multi' file off of the same stream
   if ( datastream_create( &(stream), "file3", &(pos), 0777, "PACK-CLIENT" ) ) {
      printf( "create failure for 'file3' of pack\n" );
      return -1;
   }
   if ( stream->curfile != 2 ) {
      printf( "unexpected curfile value after 'file3' creation: %zu\n", stream->curfile );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 1024 * 3 ) != (1024 * 3) ) {
      printf( "write failure for 'file2' of pack\n" );
      return -1;
   }
   if ( stream->curfile ) {
      printf( "unexpected curfile value after write of 'file3': %zu\n", stream->curfile );
      return -1;
   }
   if ( stream->objno != 1 ) {
      printf( "unexpected objno value after write of 'file3': %zu\n", stream->objno );
      return -1;
   }

   // keep track of this file's rpath
   rpath3 = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath3 == NULL ) {
      printf( "Failed to identify the rpath of pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   FTAG tgttag = stream->files->ftag;
   tgttag.objno++;
   if ( datastream_objtarget( &(tgttag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      printf( "Failed to identify data object of pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }

   // close the stream
   if ( datastream_close( &(stream) ) ) {
      printf( "Failed to close pack create stream\n" );
      return -1;
   }


   // repack the second and last files
   if ( datastream_repack( &(repackstream), rpath3, &(pos), "repack-prog2" ) ) {
      printf( "failed to open repack stream for target3 \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   // keep track of this file's rpath
   rpckpath1 = datastream_genrpath( &(repackstream->files->ftag), repackstream->ns->prepo->metascheme.reftable );
   if ( rpckpath1 == NULL ) {
      printf( "Failed to identify the repack path of packed 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data objects
   rpckobjname1 = NULL;
   if ( datastream_objtarget( &(repackstream->files->ftag), &(repackstream->ns->prepo->datascheme), &(rpckobjname1), &(rpckobjerasure1), &(rpckobjlocation1) ) ) {
      printf( "Failed to identify data object 1 of repacked 'file3' of pack (%s)\n", strerror(errno) );
      return -1;
   }
   if ( datastream_scan( &(stream), rpath3, &(pos) ) ) {
      printf( "failed to open scan/read stream for target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   bzero( readbuf, 3 * 1024 );
   if ( datastream_read( &(stream), readbuf, 1048576 ) != 3 * 1024 ) {
      printf( "failed to read/repack content of target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, 3 * 1024 ) ) {
      printf( "unexpected read/repack content of file3 / target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_write( &(repackstream), readbuf, 3 * 1024 ) != 3 * 1024 ) {
      printf( "failed to write/repack content of file3 target \"%s\" (%s)\n", rpath3, strerror(errno) );
      return -1;
   }
   if ( datastream_repack( &(repackstream), rpath2, &(pos), NULL ) ) {
      printf( "failed to open repack stream for target4 \"%s\" (%s)\n", rpath2, strerror(errno) );
      return -1;
   }
   // keep track of this file's rpath
   rpckpath2 = datastream_genrpath( &(repackstream->files[1].ftag), repackstream->ns->prepo->metascheme.reftable );
   if ( rpckpath2 == NULL ) {
      printf( "Failed to identify the repack path of packed 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   if ( datastream_scan( &(stream), rpath2, &(pos) ) ) {
      printf( "failed to open scan/read stream for target \"%s\" (%s)\n", rpath2, strerror(errno) );
      return -1;
   }
   bzero( readbuf, 1024 );
   if ( datastream_read( &(stream), readbuf, 1048576 ) != 33 ) {
      printf( "failed to read/repack content of target \"%s\" (%s)\n", rpath2, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, 33 ) ) {
      printf( "unexpected read/repack content of file2 / target \"%s\" (%s)\n", rpath2, strerror(errno) );
      return -1;
   }
   if ( datastream_write( &(repackstream), readbuf, 33 ) != 33 ) {
      printf( "failed to write/repack content of file2 target \"%s\" (%s)\n", rpath2, strerror(errno) );
      return -1;
   }
   if ( repackstream->objno ) {
      printf( "unexpected objno of repackstream after writing out \"%s\" content: %zu\n", rpath2, repackstream->objno );
      return -1;
   }
   // close our streams
   if ( datastream_release( &(stream) ) ) {
      printf( "failed to close read/repack stream for packed files\n" );
      return -1;
   }
   if ( datastream_close( &(repackstream) ) ) {
      printf( "failed to close write/repack stream for packed files\n" );
      return -1;
   }


   // start, then abort, a repack of the first file
   if ( datastream_repack( &(repackstream), rpath, &(pos), "repack-prog2" ) ) {
      printf( "failed to open repack stream for target5 \"%s\" (%s)\n", rpath, strerror(errno) );
      return -1;
   }
   char* rpckpath3 = datastream_genrpath( &(repackstream->files->ftag), repackstream->ns->prepo->metascheme.reftable );
   if ( rpckpath3 == NULL ) {
      printf( "Failed to identify the repack path of packed 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   if ( datastream_scan( &(stream), rpath, &(pos) ) ) {
      printf( "failed to open scan/read stream for target \"%s\" (%s)\n", rpath, strerror(errno) );
      return -1;
   }
   char* rmarkerpath = repackmarkertgt( rpath, &(stream->files->ftag), &(stream->ns->prepo->metascheme) );
   if ( rmarkerpath == NULL ) {
      printf( "Failed to generate repack marker path of file \"%s\"\n", rpath );
      return -1;
   }
   freestream( repackstream );
   repackstream = NULL;
   if ( datastream_close( &(stream) ) ) {
      printf( "Failed to close read stream for partially repacked file \"%s\"\n", rpath );
      return -1;
   }
   if ( datastream_repack_cleanup( rmarkerpath, &(pos) ) != 1 ) {
      printf( "Repack cleanup failed for marker \"%s\"\n", rmarkerpath );
      return -1;
   }
   free( rmarkerpath );
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpckpath3 ) ) {
      printf( "Failed to unlink repack tgt of packed file1: \"%s\"\n", rpckpath3 );
      return -1;
   }
   free( rpckpath3 );


   // cleanup original 'file2' refs ( except file itself! )
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath2 ) ) {
      printf( "Failed to unlink rpath2: \"%s\"\n", rpath2 );
      return -1;
   }
   free( rpath2 );
   // cleanup original 'file3' refs ( except file itself! )
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath3 ) ) {
      printf( "Failed to unlink rpath3: \"%s\"\n", rpath3 );
      return -1;
   }
   free( rpath3 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname2, objlocation2 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname2 );
      return -1;
   }
   free( objname2 );


   // read back the written files
   // file1
   if ( datastream_open( &(stream), READ_STREAM, "file1", &(pos), NULL ) ) {
      printf( "failed to open 'file1' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 2 * 1024 ) {
      printf( "unexpected read res for 'file1' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, 2 * 1024 ) ) {
      printf( "unexpected content of 'file1' of pack\n" );
      return -1;
   }
   // file2
   if ( datastream_open( &(stream), READ_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 'file2' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 33 ) {
      printf( "unexpected read res for 'file2' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, iores ) ) {
      printf( "unexpected content of 'file2' of pack\n" );
      return -1;
   }
   // file3
   if ( datastream_open( &(stream), READ_STREAM, "file3", &(pos), NULL ) ) {
      printf( "failed to open 'file3' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 3 * 1024 ) {
      printf( "unexpected res for read from 'file3' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, iores ) ) {
      printf( "unexpected content of read for 'file3' of pack\n" );
      return -1;
   }
   if ( datastream_close( &(stream) ) ) {
      printf( "failed to close read stream of pack\n" );
      return -1;
   }


   // cleanup all remaining paths
   // cleanup 'file1' refs
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file1" ) ) {
      printf( "failed to unlink packed file1\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath ) ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath );
      return -1;
   }
   free( rpath );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname, objlocation ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname );
      return -1;
   }
   free( objname );
   // cleanup 'file2' refs
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file2" ) ) {
      printf( "failed to unlink packed file2\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpckpath2 ) ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpckpath2 );
      return -1;
   }
   free( rpckpath2 );
   // cleanup 'file3' refs
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file3" ) ) {
      printf( "failed to unlink packed file3\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpckpath1 ) ) {
      printf( "Failed to unlink rpath3: \"%s\"\n", rpckpath1 );
      return -1;
   }
   free( rpckpath1 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, rpckobjname1, rpckobjlocation1 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", rpckobjname2 );
      return -1;
   }
   free( rpckobjname1 );



   // cleanup our data buffer
   free( databuf );

   // cleanup our position struct
   MDAL posmdal = pos.ns->prepo->metascheme.mdal;
   if ( posmdal->destroyctxt( pos.ctxt ) ) {
      printf( "Failed to destory position MDAL_CTXT\n" );
      return -1;
   }

   // cleanup all created NSs
   if ( deletesubdirs( "./test_datastream_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/heavily-protected-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of heavily-protected-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/heavily-protected-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/heavily-protected-data NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_datastream_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/read-only-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of read-only-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/read-only-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/read-only-data NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_datastream_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of gransom-allocation\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation" ) ) {
      printf( "Failed to destroy /gransom-allocation NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_datastream_topdir/mdal_root/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of rootNS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/ghost-gransom/heavily-protected-data" ) ) {
      printf( "Failed to destroy /ghost-gransom/heavily-protected-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/ghost-gransom/read-only-data" ) ) {
      printf( "Failed to destroy /ghost-gransom/read-only-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/ghost-gransom" ) ) {
      printf( "Failed to destroy /ghost-gransom NS\n" );
      return -1;
   }
   rootmdal->destroynamespace( rootmdal->ctxt, "/." ); // TODO : fix MDAL edge case?

   // cleanup out config struct
   if ( config_term( config ) ) {
      printf( "Failed to destory our config reference\n" );
      return -1;
   }

   // cleanup DAL trees
   if ( deletesubdirs( "./test_datastream_topdir/dal_root" ) ) {
      printf( "Failed to delete subdirs of DAL root\n" );
      return -1;
   }

   // delete dal/mdal dir structure
   rmdir( "./test_datastream_topdir/dal_root" );
   rmdir( "./test_datastream_topdir/mdal_root" );
   rmdir( "./test_datastream_topdir" );

   return 0;
}


