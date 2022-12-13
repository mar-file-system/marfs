/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/


#include "marfs.c" // include C file directly, to allow traversal of all structures
#include "config/config.h" // for config validation, alone

#include <ftw.h>


// WARNING: error-prone and ugly method of deleting dir trees, written for simplicity only
//          don't replicate this junk into ANY production code paths!
size_t tgtlistpos = 0;
char** tgtlist = NULL;
int ftwnotetgt( const char* fpath, const struct stat* sb, int typeflag ) {
   tgtlist[tgtlistpos] = strdup( fpath );
   if ( tgtlist[tgtlistpos] == NULL ) {
      printf( "Failed to duplicate tgt name: \"%s\"\n", fpath );
      return -1;
   }
   tgtlistpos++;
   if ( tgtlistpos >= 1048576 ) { printf( "Dirlist has insufficient length! (curtgt = %s)\n", fpath ); return -1; }
   return 0;
}
int deletefstree( const char* basepath ) {
   tgtlist = malloc( sizeof(char*) * 1048576 );
   if ( tgtlist == NULL ) {
      printf( "Failed to allocate tgtlist\n" );
      return -1;
   }
   if ( ftw( basepath, ftwnotetgt, 100 ) ) {
      printf( "Failed to identify reference tgts of \"%s\"\n", basepath );
      return -1;
   }
   int retval = 0;
   while ( tgtlistpos ) {
      tgtlistpos--;
      if ( strcmp( tgtlist[tgtlistpos], basepath ) ) {
         //printf( "Deleting: \"%s\"\n", tgtlist[tgtlistpos] );
         errno = 0;
         if ( rmdir( tgtlist[tgtlistpos] ) ) {
            if ( errno != ENOTDIR  ||  unlink( tgtlist[tgtlistpos] ) ) {
               printf( "ERROR -- failed to delete \"%s\"\n", tgtlist[tgtlistpos] );
               retval = -1;
            }
         }
      }
      free( tgtlist[tgtlistpos] );
   }
   free( tgtlist );
   return retval;
}


int main( int argc, char** argv ) {

   // NOTE -- I'm ignoring memory leaks for error conditions
   //         which result in immediate termination

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

   // verify the marfs config
   marfs_config* verconf = config_init( "testing/config.xml" );
   if ( config_verify( verconf, ".", 1, 1, 1, 1 ) ) {
      printf( "failed to verify batch ctxt config\n" );
      return -1;
   }
   config_term( verconf );

   // initialize our BATCH marfs ctxt
   marfs_ctxt batchctxt = marfs_init( "testing/config.xml", MARFS_BATCH, 0 );
   if ( batchctxt == NULL ) {
      printf( "failed to initialize batch ctxt\n" );
      return -1;
   }

   // set a client tag for our batch ctxt
   if ( marfs_setctag( batchctxt, "BatchClientProgram" ) ) {
      printf( "failed to set client tag for batch ctxt\n" );
      return -1;
   }

   // initialize our INTERACTIVE marfs ctxt
   marfs_ctxt interctxt = marfs_init( "testing/config.xml", MARFS_INTERACTIVE, 1 );
   if ( interctxt == NULL ) {
      printf( "failed to initialize inter ctxt\n" );
      return -1;
   }

   // set a client tag for our interactive ctxt
   if ( marfs_setctag( interctxt, "InteractiveClientProgram" ) ) {
      printf( "failed to set client tag for inter ctxt\n" );
      return -1;
   }

   // check the batchctxt config version
   char onekstr[1024] = {0};
   ssize_t verstrlen = marfs_configver( batchctxt, onekstr, 1024 );
   if ( verstrlen < 0  ||  verstrlen >= 1024 ) {
      printf( "unexpected verstrlen of %zd\n", verstrlen );
      return -1;
   }
   if ( strcmp( onekstr, "0.0001-apitest-notarealversion" ) ) {
      printf( "unexpected config version string: \"%s\"\n", onekstr );
      return -1;
   }

   // shift our interactive ctxt down to '/gransom-allocation/heavily-protected-data'
   marfs_dhandle hpdhandle = marfs_opendir( interctxt, "/campaign/gransom-allocation/heavily-protected-data" );
   if ( hpdhandle == NULL ) {
      printf( "failed to open inter dir handle for hpd NS root\n" );
      return -1;
   }
   if ( marfs_chdir( interctxt, hpdhandle ) ) {
      printf( "failed to chdir inter ctxt to hpd dir\n" );
      return -1;
   }

   // create a data buffer
   void* oneMBbuffer = calloc( 1, 1048576 );
   if ( oneMBbuffer == NULL ) {
      printf( "failed to allocate oneMBbuffer\n" );
      return -1;
   }
   int index;
   for( index = 0; index < 1048576; index++ ) {
      *((char*)( oneMBbuffer + index )) = (char)index;
   }

   // create some new dirs
   if ( marfs_mkdir( interctxt, "../../gransom-allocation/gasubdir", 0776 ) ) {
      printf( "failed to create 'gransom-allocation/gasubdir'\n" );
      return -1;
   }
   if ( marfs_mkdir( batchctxt, "rootsubdir", 0776 ) ) {
      printf( "failed to create 'rootsubdir'\n" );
      return -1;
   }
   if ( marfs_mkdir( interctxt, "hpdsubdir", 0776 ) ) {
      printf( "failed to create 'hpdsubdir'\n" );
      return -1;
   }
   // create a symlink
   if ( marfs_symlink( batchctxt, "gransom-allocation/heavily-protected-data/", "hpdsymlinkfromroot" ) ) {
      printf( "failed to create 'hpdsymlinkfromroot'\n" );
      return -1;
   }
   // read the content of that symlink
   ssize_t linklen = marfs_readlink( interctxt, "../../hpdsymlinkfromroot", onekstr, 1024 );
   if ( linklen != 42 ) {
      printf( "Unexpected length of symlink \"hpdsymlinkfromroot\": %zd (%s)\n", linklen, strerror(errno) );
      return -1;
   }
   if ( strncmp( "gransom-allocation/heavily-protected-data/", onekstr, linklen + 1 ) ) {
      printf( "Unexpected content of symlink \"hpdsymlinkfromroot\": \"%s\"\n", onekstr );
      return -1;
   }
   // create a couple of larger files
   marfs_fhandle bgasubfhandle = marfs_creat( batchctxt, NULL, "gransom-allocation/gasubdir/file1", 0700 );
   if ( bgasubfhandle == NULL ) {
      printf( "failed to open 'bgasubfile1' for write\n" );
      return -1;
   }
   if ( marfs_write( bgasubfhandle, oneMBbuffer, 1048576 ) != 1048576 ) {
      printf( "failed to write 1MB to gasubdir/file1\n" );
      return -1;
   }
   bgasubfhandle = marfs_creat( batchctxt, bgasubfhandle, "gransom-allocation/gasubdir/file2", 0700 );
   if ( bgasubfhandle == NULL ) {
      printf( "failed to open 'bgasubfile2' for write\n" );
      return -1;
   }
   if ( marfs_write( bgasubfhandle, oneMBbuffer, 1028 ) != 1028 ) {
      printf( "failed to write 1028 bytes to gasubdir/file2\n" );
      return -1;
   }
   // create a large number of packed files
   if ( marfs_mkdir( batchctxt, "gransom-allocation/packed-files", 0776 ) ) {
      printf( "failed to create 'gransom-allocation/packed-files'\n" );
      return -1;
   }
   for( index = 0; index < 4096; index++ ) {
      char fname[1024];
      if ( snprintf( fname, 1024, "gransom-allocation/packed-files/pfile%d", index ) >= 1024 ) {
         printf( "failed to generate name of packed-files/pfile%d\n", index );
         return -1;
      }
      if ( (bgasubfhandle = marfs_creat( batchctxt, bgasubfhandle, fname, 0644 )) == NULL ) {
         printf( "failed to create \"%s\"\n", fname );
         return -1;
      }
      if ( marfs_write( bgasubfhandle, oneMBbuffer, 10 + (index % 100) ) != 10 + (index % 100) ) {
         printf( "failed to write %d bytes to \"%s\"\n", 10 + (index % 100), fname );
         return -1;
      }
   }
   // finally close this stream
   if ( marfs_close( bgasubfhandle ) ) {
      printf( "failed to close 'bgasubfilehandle'\n" );
      return -1;
   }
   // create a chunked file in a different NS
   marfs_fhandle hpdstream = marfs_creat( interctxt, NULL, "chunked", 0704 );
   if ( hpdstream == NULL ) {
      printf( "failed to create 'chunked' output file\n" );
      return -1;
   }
   if ( marfs_write( hpdstream, oneMBbuffer, 1048576 ) != 1048576 ) {
      printf( "failed to write 1MB to 'chunked'\n" );
      return -1;
   }
   if ( marfs_write( hpdstream, oneMBbuffer, 1048576 ) != 1048576 ) {
      printf( "failed to write second 1MB buffer to 'chunked'\n" );
      return -1;
   }
   if ( marfs_write( hpdstream, oneMBbuffer, 1048576 ) != 1048576 ) {
      printf( "failed to write third 1MB buffer to 'chunked'\n" );
      return -1;
   }
   if ( marfs_close( hpdstream ) ) {
      printf( "failed to close 'hpdstream'\n" );
      return -1;
   }

   // create a couple of files in the GhostNS
   marfs_fhandle ghoststream = marfs_creat( batchctxt, NULL, "ghost-gransom/gfile1", 0621 );
   if ( ghoststream == NULL ) {
      printf( "failed to create ghost 'gfile1'\n" );
      return -1;
   }
   if ( marfs_write( ghoststream, oneMBbuffer, 1024 ) != 1024 ) {
      printf( "failed to write ghost 'gfile1'\n" );
      return -1;
   }
   if ( (ghoststream = marfs_creat( batchctxt, ghoststream, "ghost-gransom/gfile2", 0744 ) ) == NULL ) {
      printf( "failed to create ghost 'gfile2'\n" );
      return -1;
   }
   if ( marfs_write( ghoststream, oneMBbuffer, 10 ) != 10 ) {
      printf( "failed to write ghost 'gfile1'\n" );
      return -1;
   }
   if ( marfs_close( ghoststream ) ) {
      printf( "failed to close 'ghoststream'\n" );
      return -1;
   }

   // test rename and link ( allowed between ghost and tgt )
   if ( marfs_rename( batchctxt, "ghost-gransom/gfile2", "gransom-allocation/gasubdir/ghost-file2" ) ) {
      printf( "failed to rename ghost 'gfile2'\n" );
      return -1;
   }
   if ( marfs_link( batchctxt, "ghost-gransom/gfile1", "gransom-allocation/gfile1-link", 0 ) ) {
      printf( "failed to link ghost 'gfile1'\n" );
      return -1;
   }

   // write out a parallel file
   marfs_fhandle phandle = marfs_creat( batchctxt, NULL, "gransom-allocation/parallelfile", 0600 );
   if ( phandle == NULL ) {
      printf( "failed to create 'parallelfile'\n" );
      return -1;
   }
   if ( marfs_extend( phandle, 712400 ) ) {
      printf( "failed to extend 'parallelfile' to 5MiB\n" );
      return -1;
   }
   if ( marfs_release( phandle ) ) {
      printf( "failed to release initial handle for 'parallelfile'\n" );
      return -1;
   }
   // chunk 0
   phandle = marfs_open( batchctxt, NULL, "/campaign/gransom-allocation/gasubdir/../parallelfile", MARFS_WRITE );
   if ( phandle == NULL ) {
      printf( "failed to open 'parallelfile' for write\n" );
      return -1;
   }
   if ( marfs_setrecoverypath( batchctxt, phandle, "gransom-allocation/parallelfile" ) ) {
      printf( "failed to update recovery path of 'parallelfile'\n" );
      return -1;
   }
   off_t chunkoffset = 0;
   size_t chunksize = 0;
   if ( marfs_chunkbounds( phandle, 0, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to indentify bounds of chunk 0 of 'parallelfile'\n" );
      return -1;
   }
   if ( chunkoffset + chunksize > 1048576 ) {
      printf( "bounds of chunk 0 exceed buffer limits\n" );
      return -1;
   }
   if ( marfs_seek( phandle, chunkoffset, SEEK_SET ) != chunkoffset ) {
      printf( "failed to seek to offset %zd of 'parallelfile'\n", chunkoffset );
      return -1;
   }
   if ( marfs_write( phandle, oneMBbuffer + chunkoffset, chunksize ) != chunksize ) {
      printf( "failed to write %zu bytes to offset %zd of 'parallelfile'\n", chunksize, chunkoffset );
      return -1;
   }
   // chunk 5
   if ( marfs_chunkbounds( phandle, 5, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to indentify bounds of chunk 5 of 'parallelfile'\n" );
      return -1;
   }
   if ( chunkoffset + chunksize > 1048576 ) {
      printf( "bounds of chunk 5 exceed buffer limits\n" );
      return -1;
   }
   if ( marfs_seek( phandle, chunkoffset, SEEK_SET ) != chunkoffset ) {
      printf( "failed to seek to offset %zd of 'parallelfile'\n", chunkoffset );
      return -1;
   }
   if ( marfs_write( phandle, oneMBbuffer + chunkoffset, chunksize ) != chunksize ) {
      printf( "failed to write %zu bytes to offset %zd of 'parallelfile'\n", chunksize, chunkoffset );
      return -1;
   }
   // chunk 6
   if ( marfs_chunkbounds( phandle, 6, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to indentify bounds of chunk 6 of 'parallelfile'\n" );
      return -1;
   }
   if ( chunkoffset + chunksize > 1048576 ) {
      printf( "bounds of chunk 6 exceed buffer limits\n" );
      return -1;
   }
   if ( marfs_seek( phandle, chunkoffset, SEEK_SET ) != chunkoffset ) {
      printf( "failed to seek to offset %zd of 'parallelfile'\n", chunkoffset );
      return -1;
   }
   if ( marfs_write( phandle, oneMBbuffer + chunkoffset, chunksize ) != chunksize ) {
      printf( "failed to write %zu bytes to offset %zd of 'parallelfile'\n", chunksize, chunkoffset );
      return -1;
   }
   // open a new handle, writing out chunk 3 from that
   marfs_fhandle phandle2 = marfs_open( batchctxt, NULL, "/campaign/gransom-allocation/gasubdir/../parallelfile", MARFS_WRITE );
   if ( phandle2 == NULL ) {
      printf( "failed to open 'parallelfile' for write via handle2\n" );
      return -1;
   }
   if ( marfs_setrecoverypath( batchctxt, phandle2, "gransom-allocation/parallelfile" ) ) {
      printf( "failed to update recovery path of 'parallelfile' handle2\n" );
      return -1;
   }
   if ( marfs_chunkbounds( phandle2, 3, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to indentify bounds of chunk 3 of 'parallelfile'\n" );
      return -1;
   }
   if ( chunkoffset + chunksize > 1048576 ) {
      printf( "bounds of chunk 3 exceed buffer limits\n" );
      return -1;
   }
   if ( marfs_seek( phandle2, chunkoffset, SEEK_SET ) != chunkoffset ) {
      printf( "failed to seek to offset %zd of 'parallelfile'\n", chunkoffset );
      return -1;
   }
   if ( marfs_write( phandle2, oneMBbuffer + chunkoffset, chunksize ) != chunksize ) {
      printf( "failed to write %zu bytes to offset %zd of 'parallelfile'\n", chunksize, chunkoffset );
      return -1;
   }
   size_t currentoffset_handle2 = chunkoffset + chunksize;
   // chunk 2
   if ( marfs_chunkbounds( phandle, 2, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to indentify bounds of chunk 2 of 'parallelfile'\n" );
      return -1;
   }
   if ( chunkoffset + chunksize > 1048576 ) {
      printf( "bounds of chunk 2 exceed buffer limits\n" );
      return -1;
   }
   if ( marfs_seek( phandle, chunkoffset, SEEK_SET ) != chunkoffset ) {
      printf( "failed to seek to offset %zd of 'parallelfile'\n", chunkoffset );
      return -1;
   }
   if ( marfs_write( phandle, oneMBbuffer + chunkoffset, chunksize ) != chunksize ) {
      printf( "failed to write %zu bytes to offset %zd of 'parallelfile'\n", chunksize, chunkoffset );
      return -1;
   }
   // chunk 4
   if ( marfs_chunkbounds( phandle2, 4, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to indentify bounds of chunk 4 of 'parallelfile'\n" );
      return -1;
   }
   if ( chunkoffset + chunksize > 1048576 ) {
      printf( "bounds of chunk 4 exceed buffer limits\n" );
      return -1;
   }
   if ( chunkoffset != currentoffset_handle2 ) {
      printf( "offset of chunk 4 (%zd) does not equal current handle2 offset (%zu)\n",
              chunkoffset, currentoffset_handle2 );
      return -1;
   }
   if ( marfs_write( phandle2, oneMBbuffer + chunkoffset, chunksize ) != chunksize ) {
      printf( "failed to write %zu bytes to offset %zd of 'parallelfile'\n", chunksize, chunkoffset );
      return -1;
   }
   // release our first handle
   if ( marfs_release( phandle ) ) {
      printf( "failed to release phandle after writing out chunk 2\n" );
      return -1;
   }
   // chunk 1
   if ( marfs_chunkbounds( phandle2, 1, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to indentify bounds of chunk 1 of 'parallelfile'\n" );
      return -1;
   }
   if ( chunkoffset + chunksize > 1048576 ) {
      printf( "bounds of chunk 1 exceed buffer limits\n" );
      return -1;
   }
   if ( marfs_seek( phandle2, chunkoffset, SEEK_SET ) != chunkoffset ) {
      printf( "failed to seek to offset %zd of 'parallelfile'\n", chunkoffset );
      return -1;
   }
   if ( marfs_write( phandle2, oneMBbuffer + chunkoffset, chunksize ) != chunksize ) {
      printf( "failed to write %zu bytes to offset %zd of 'parallelfile'\n", chunksize, chunkoffset );
      return -1;
   }
   // ensure that chunk 7 is out of bounds
   errno = 0;
   if ( marfs_chunkbounds( phandle2, 7, &(chunkoffset), &(chunksize) ) == 0  ||  errno != EINVAL ) {
      printf( "chunk 7 of 'parallelfile' unnexpectedly in bounds\n" );
      return -1;
   }
   // set time values and close the handle
   struct stat stval;
   if ( stat( "./test_datastream_topdir", &(stval) ) ) {
      printf( "failed to stat marfs topdir for time reference\n" );
      return -1;
   }
   struct timespec timevals[2];
   timevals[0] = stval.st_atim;
   timevals[1] = stval.st_mtim;
   if ( marfs_futimens( phandle2, timevals ) ) {
      printf( "failed to set mtime/atime values of 'parallelfile'\n" );
      return -1;
   }
   if ( marfs_close( phandle2 ) ) {
      printf( "failed to close phandle2 after setting time values\n" );
      return -1;
   }


   // validate stat info of parallelfile
   stval.st_atim.tv_sec = 0;
   stval.st_atim.tv_nsec = 0;
   stval.st_mtim.tv_sec = 0;
   stval.st_mtim.tv_nsec = 0;
   if ( marfs_stat( interctxt, "/campaign/gransom-allocation/parallelfile", &(stval), 0 ) ) {
      printf( "failed to stat 'parallelfile'\n" );
      return -1;
   }
   if ( stval.st_atim.tv_sec != timevals[0].tv_sec  ||
        stval.st_atim.tv_nsec != timevals[0].tv_nsec ) {
      printf( "unexpected atime values on 'parallelfile'\n" );
      return -1;
   }
   if ( stval.st_mtim.tv_sec != timevals[1].tv_sec  ||
        stval.st_mtim.tv_nsec != timevals[1].tv_nsec ) {
      printf( "unexpected mtime values on 'parallelfile'\n" );
      return -1;
   }
   if ( stval.st_size != 712400 ) {
      printf( "unexpected size of 'parallelfile'\n" );
      return -1;
   }


   // read back written files
   void* oneMBreadbuf = calloc( 1024, 1024 );
   if ( oneMBreadbuf == NULL ) {
      printf( "failed to allocate oneMBreadbuf\n" );
      return -1;
   }
   // parallelfile
   phandle = marfs_open( batchctxt, NULL, "gransom-allocation/parallelfile", MARFS_READ );
   if ( phandle == NULL ) {
      printf( "failed to open 'parallelfile' for read\n" );
      return -1;
   }
   if ( marfs_read( phandle, oneMBreadbuf, 712400 ) != 712400 ) {
      printf( "failed to read 712400 bytes from 'parallelfile'\n" );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer, 712400 ) ) {
      printf( "unexpected content of 'parallelfile'\n" );
      return -1;
   }
   // packed files, in reverse order ( just to force the most complex case )
   for ( index = 4095; index >= 0; index-- ) {
      char fname[1024];
      if ( snprintf( fname, 1024, "/campaign/gransom-allocation/packed-files/pfile%d", index ) >= 1024 ) {
         printf( "failed to generate name of packed-files/pfile%d\n", index );
         return -1;
      }
      phandle = marfs_open( batchctxt, phandle, fname, MARFS_READ );
      if ( phandle == NULL ) {
         printf( "failed to open packed-files/pfile%d for read\n", index );
         return -1;
      }
      bzero( oneMBreadbuf, 1048576 );
      if ( marfs_read( phandle, oneMBreadbuf, 10 + (index % 100) ) != 10 + (index % 100) ) {
         printf( "failed to read %d bytes from %s\n", 10 + (index % 100), fname );
         return -1;
      }
      if ( memcmp( oneMBreadbuf, oneMBbuffer, 10 + (index % 100) ) ) {
         printf( "unexpected content of %s\n", fname );
         return -1;
      }
   }
   // file1
   phandle = marfs_open( batchctxt, phandle, "gransom-allocation/gasubdir/file1", MARFS_READ );
   if ( phandle == NULL ) {
      printf( "failed to open 'file1' for read\n" );
      return -1;
   }
   bzero( oneMBreadbuf, 1048576 );
   if ( marfs_read( phandle, oneMBreadbuf, 1048576 - 1234 ) != 1048576 - 1234 ) {
      printf( "failed to read %d bytes from 'file1'\n", 1048576 - 1234 );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer, 1048576 - 1234 ) ) {
      printf( "first %d bytes of 'file1' do not match expectations\n", 1048576 - 1234 );
      return -1;
   }
   if ( marfs_read( phandle, oneMBreadbuf, 1234 ) != 1234 ) {
      printf( "failed to read next 1234 bytes from 'file1'\n" );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer + (1048576 - 1234), 1234 ) ) {
      printf( "last 1234 bytes of 'file1' do not match expectations\n" );
      return -1;
   }
   // seek around a bit, and check for expected content
   if ( marfs_seek( phandle, -4321, SEEK_END ) != 1048576 - 4321 ) {
      printf( "failed to seek to 4321 bytes from end of 'file1'\n" );
      return -1;
   }
   bzero( oneMBreadbuf, 1048576 );
   if ( marfs_read( phandle, oneMBreadbuf, 1048576 ) != 4321 ) {
      printf( "failed to read last 4321 bytes from 'file1'\n" );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer + (1048576 - 4321), 4321 ) ) {
      printf( "last 4321 bytes of 'file1' do not match expectations\n" );
      return -1;
   }
   if ( marfs_seek( phandle, -1048000, SEEK_CUR ) != 576 ) {
      printf( "failed to seek 1048000 bytes back from current position of 'file1'\n" );
      return -1;
   }
   bzero( oneMBreadbuf, 1048576 );
   if ( marfs_read( phandle, oneMBreadbuf, 1048576 ) != 1048000 ) {
      printf( "failed to read last 1048000 bytes from 'file1'\n" );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer + 576, 1048000 ) ) {
      printf( "last 1048000 bytes of 'file1' do not match expectations\n" );
      return -1;
   }
   if ( marfs_seek( phandle, 567890, SEEK_SET ) != 567890 ) {
      printf( "failed to seek to offset 567890 of 'file1'\n" );
      return -1;
   }
   bzero( oneMBreadbuf, 1048576 );
   if ( marfs_read( phandle, oneMBreadbuf, 1048 ) != 1048 ) {
      printf( "failed to read 1048 bytes from 'file1' @ offset 567890\n" );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer + 567890, 1048 ) ) {
      printf( "1048 bytes of 'file1' @ offset 567890 do not match expectations\n" );
      return -1;
   }
   if ( marfs_seek( phandle, 31062, SEEK_CUR ) != 600000 ) {
      printf( "failed to seek to offset 600000 of 'file1'\n" );
      return -1;
   }
   bzero( oneMBreadbuf, 1048576 );
   if ( marfs_read( phandle, oneMBreadbuf, 104857 ) != 104857 ) {
      printf( "failed to read 104857 bytes from 'file1' @ offset 600000\n" );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer + 600000, 104857 ) ) {
      printf( "104857 bytes of 'file1' @ offset 600000 do not match expectations\n" );
      return -1;
   }
   if ( marfs_close( phandle ) ) {
      printf( "failed to close 'file1' read handle\n" );
      return -1;
   }
   // open a new handle for file2
   phandle = marfs_open( interctxt, NULL, "../gasubdir/file2", MARFS_READ );
   if ( phandle == NULL ) {
      printf( "failed to open 'file2' for read\n" );
      return -1;
   }
   bzero( oneMBreadbuf, 1048576 );
   if ( marfs_read( phandle, oneMBreadbuf, 1028 ) != 1028 ) {
      printf( "failed to read 'file2'\n" );
      return -1;
   }
   if ( memcmp( oneMBreadbuf, oneMBbuffer, 1028 ) ) {
      printf( "unexpected content of 'file2'\n" );
      return -1;
   }
   if ( marfs_release( phandle ) ) {
      printf( "failed to close 'file2' read handle\n" );
      return -1;
   }


   // free buffers
   free( oneMBreadbuf );
   free( oneMBbuffer );

   // cleanup created files/dirs
   if ( marfs_unlink( batchctxt, "/campaign/../campaign/gransom-allocation/parallelfile" ) ) {
      printf( "failed to unlink 'parallelfile'\n" );
      return -1;
   }
   if ( marfs_unlink( interctxt, "../../gransom-allocation/heavily-protected-data/chunked" ) ) {
      printf( "failed to unlink 'chunked'\n" );
      return -1;
   }
   for( index = 0; index < 4096; index++ ) {
      char fname[1024];
      if ( snprintf( fname, 1024, "../packed-files/pfile%d", index ) >= 1024 ) {
         printf( "failed to generate name of packed-files/pfile%d\n", index );
         return -1;
      }
      if ( marfs_unlink( interctxt, fname ) ) {
         printf( "failed to unlink '%s'\n", fname );
         return -1;
      }
   }
   if ( marfs_rmdir( interctxt, "/campaign/gransom-allocation/packed-files" ) ) {
      printf( "failed to rmdir '/campaign/gransom-allocation/packed-files'\n" );
      return -1;
   }
   if ( marfs_unlink( batchctxt, "/campaign/gransom-allocation/gasubdir/file1" ) ) {
      printf( "failed to unlink '/campaign/gransom-allocation/gasubdir/file1'\n" );
      return -1;
   }
   if ( marfs_unlink( batchctxt, "gransom-allocation/gasubdir/file2" ) ) {
      printf( "failed to unlink 'gransom-allocation/gasubdir/file2'\n" );
      return -1;
   }
   if ( marfs_unlink( batchctxt, "ghost-gransom/gfile1" ) ) {
      printf( "failed to unlink 'ghost-gransom/gfile1'\n" );
      return -1;
   }
   if ( marfs_unlink( batchctxt, "gransom-allocation/gfile1-link" ) ) {
      printf( "failed to unlink 'gransom-allocation/gfile1-link'\n" );
      return -1;
   }
   if ( marfs_unlink( batchctxt, "gransom-allocation/gasubdir/ghost-file2" ) ) {
      printf( "failed to unlink 'gransom-allocation/gasubdir/ghost-file2'\n" );
      return -1;
   }
   if ( marfs_unlink( batchctxt, "/campaign/hpdsymlinkfromroot" ) ) {
      printf( "failed to unlink 'hpdsymlinkfromroot'\n" );
      return -1;
   }
   if ( marfs_rmdir( interctxt, "/campaign/gransom-allocation/heavily-protected-data/hpdsubdir" ) ) {
      printf( "failed to rmdir '/campaign/gransom-allocation/heavily-protected-data/hpdsubdir'\n" );
      return -1;
   }
   if ( marfs_rmdir( batchctxt, "/campaign/rootsubdir" ) ) {
      printf( "failed to rmdir 'gransom-allocation/rootsubdir'\n" );
      return -1;
   }
   if ( marfs_rmdir( batchctxt, "gransom-allocation/gasubdir" ) ) {
      printf( "failed to rmdir 'gransom-allocation/gasubdir'\n" );
      return -1;
   }

   // identify the root marfs MDAL and use this for all cleanup
   // NOTE -- shortcut.  Unsafe in most cases
   MDAL rootmdal = batchctxt->config->rootns->prepo->metascheme.mdal;

   // cleanup all created NSs
   if ( deletefstree( "./test_datastream_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/heavily-protected-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of heavily-protected-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/heavily-protected-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/heavily-protected-data NS\n" );
      return -1;
   }
   if ( deletefstree( "./test_datastream_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/read-only-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of read-only-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/read-only-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/read-only-data NS\n" );
      return -1;
   }
   if ( deletefstree( "./test_datastream_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of gransom-allocation\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation" ) ) {
      printf( "Failed to destroy /gransom-allocation NS\n" );
      return -1;
   }
   if ( deletefstree( "./test_datastream_topdir/mdal_root/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of rootNS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/ghost-gransom/read-only-data" ) ) {
      printf( "Failed to destroy /ghost-gransom/read-only-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/ghost-gransom/heavily-protected-data" ) ) {
      printf( "Failed to destroy /ghost-gransom/heavily-protected-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/ghost-gransom" ) ) {
      printf( "Failed to destroy /ghost-gransom NS\n" );
      return -1;
   }
   rootmdal->destroynamespace( rootmdal->ctxt, "/." ); // TODO : fix MDAL edge case?

   // cleanup our marfs_ctxt structs
   if ( marfs_term( batchctxt ) ) {
      printf( "Failed to destory our batch ctxt\n" );
      return -1;
   }
   if ( marfs_term( interctxt ) ) {
      printf( "Failed to destory our inter ctxt\n" );
      return -1;
   }

   // cleanup DAL trees
   if ( deletefstree( "./test_datastream_topdir/dal_root" ) ) {
      printf( "Failed to delete subdirs of DAL root\n" );
      return -1;
   }

   // delete dal/mdal dir structure
   rmdir( "./test_datastream_topdir/dal_root" );
   rmdir( "./test_datastream_topdir/mdal_root" );
   rmdir( "./test_datastream_topdir" );

   return 0;
}

