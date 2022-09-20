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

#include <unistd.h>
#include <stdio.h>
// directly including the C file allows more flexibility for these tests
#include "resourcelog.c"

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


int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error contions which result in immediate termination

   // create required config root dirs
   errno = 0;
   if ( mkdir( "./test_rmgr_topdir", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_rmgr_topdir\"\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_rmgr_topdir/dal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_rmgr_topdir/dal_root\"\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_rmgr_topdir/mdal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_rmgr_topdir/mdal_root\"\n" );
      return -1;
   }
   // initialize a fresh marfs config
   marfs_config* config = config_init( "./testing/config.xml" );
   if ( config == NULL ) {
      printf( "failed to initalize marfs config\n" );
      return -1;
   }
   if ( config_verify(config,"/campaign/",1,1,1,1) ) {
      printf( "Config validation failure\n" );
      return -1;
   }


   // terminate our config
   if ( config_term( config ) ) {
      printf( "failed to terminate config\n" );
      return -1;
   }

   // cleanup test trees
   if ( deletefstree( "./test_rmgr_topdir" ) ) {
      printf( "Failed to delete contents of test tree\n" );
      return -1;
   }
   rmdir( "./test_rmgr_topdir" );

   return 0;
}


