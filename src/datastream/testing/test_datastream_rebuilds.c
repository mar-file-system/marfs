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
#include <dirent.h>


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
   if ( mkdir( "./test_datastream_rebuilds_topdir", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_datastream_rebuilds_topdir\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_datastream_rebuilds_topdir/dal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_datastream_rebuilds_topdir/dal_root\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_datastream_rebuilds_topdir/mdal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_datastream_rebuilds_topdir/mdal_root\"\n" );
      return -1;
   }

   // establish a new marfs config
   marfs_config* config = config_init( "./testing/rebuilds_config.xml" );
   if ( config == NULL ) {
      printf( "Failed to initialize marfs config\n" );
      return -1;
   }

   // create all namespaces associated with the config
   if ( config_verify( config, "./.", 1, 1, 1, 1 ) ) {
      printf( "Failed to validate the marfs config\n" );
      return -1;
   }


//  TEST DEGRADED WRITE / READ


   // delete a specific block subtree, so that block is never properly written
   if ( deletesubdirs( "./test_datastream_rebuilds_topdir/dal_root/pod0/block2" ) ) {
      printf( "Failed to delete subdirs of block2\n" );
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

   // establish buffers to hold all data content
   void* databuf = malloc( 1024 * 1024 * 5 ); // 5MiB
   if ( databuf == NULL ) {
      printf( "Failed to allocate 5MiB data buffer\n" );
      return -1;
   }
   int randfd = open( "/dev/urandom", O_RDONLY );
   if ( randfd < 0 ) {
      printf( "failed to open /dev/urandom for read\n" );
      return -1;
   }
   if ( read( randfd, databuf, 1024*1024*5 ) != 1024*1024*5 ) {
      printf( "failed to read 5MiB from /dev/urandom\n" );
      return -1;
   }
   close( randfd ); // really don't care if this fails
   void* readbuf = calloc( 1024, 1024*5 ); // 5MiB
   if ( readbuf == NULL ) {
      printf( "Failed to allocate 5MiB read buffer\n" );
      return -1;
   }

   // create a new stream
   DATASTREAM stream = NULL;
   if ( datastream_create( &(stream), "file1", &(pos), 0744, "NO-PACK-CLIENT" ) ) {
      printf( "create failure for 'file1' of no-pack\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 1024 * 6 ) != 1024 * 6 ) {
      printf( "write failure for 'file1' of no-pack\n" );
      return -1;
   }

   // keep track of this file's rpath
   char* rpath = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable, NULL, NULL );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of no-pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   char* objname = NULL;
   ne_erasure objerasure = {0};
   ne_location objlocation = {0};
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname), &(objerasure), &(objlocation) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of no-pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a chunked file off the same stream
   if ( datastream_create( &(stream), "file2", &(pos), 0622, NULL ) ) {
      printf( "create failure for 'file2' of no-pack\n" );
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
   // write content and validate final obj target
   if ( datastream_write( &(stream), databuf, 1024 * 1024 * 3 ) != 1024 * 1024 * 3 ) {
      printf( "write failure for 'file2' of no-pack\n" );
      return -1;
   }
   if ( stream->objno != 4 ) {
      printf( "unexpected final objno for 'file2' of no-pack: %zu\n", stream->objno );
      return -1;
   }

   // keep track of this file's rpath
   char* rpath2 = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable, NULL, NULL );
   if ( rpath2 == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of no-pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data objects
   char* objname2 = NULL;
   ne_erasure objerasure2;
   ne_location objlocation2;
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of no-pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   char* objname3 = NULL;
   ne_erasure objerasure3;
   ne_location objlocation3;
   FTAG tmptag = stream->files->ftag;
   tmptag.objno++;
   if ( datastream_objtarget( &(tmptag), &(stream->ns->prepo->datascheme), &(objname3), &(objerasure3), &(objlocation3) ) ) {
      LOG( LOG_ERR, "Failed to identify data object 1 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   char* objname4 = NULL;
   ne_erasure objerasure4;
   ne_location objlocation4;
   tmptag.objno++;
   if ( datastream_objtarget( &(tmptag), &(stream->ns->prepo->datascheme), &(objname4), &(objerasure4), &(objlocation4) ) ) {
      LOG( LOG_ERR, "Failed to identify data object 2 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   char* objname5 = NULL;
   ne_erasure objerasure5;
   ne_location objlocation5;
   tmptag.objno++;
   if ( datastream_objtarget( &(tmptag), &(stream->ns->prepo->datascheme), &(objname5), &(objerasure5), &(objlocation5) ) ) {
      LOG( LOG_ERR, "Failed to identify data object 2 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }


   // close the stream
   if ( datastream_close( &(stream) ) ) {
      printf( "close failure for no-pack\n" );
      return -1;
   }


   // reverify the config, so we actually have a block location to rebuild into
   if ( config_verify( config, "./.", 1, 1, 1, 1 ) ) {
      printf( "Failed to re-validate the marfs config\n" );
      return -1;
   }


   // verify the existence of and cleanup rebuild markers for every object
   struct stat stval = {0};
   MDAL_SCANNER refscan = pos.ns->prepo->metascheme.mdal->openscanner( pos.ctxt, "0" );
   if ( refscan == NULL ) {
      printf( "failed to open reference scanner after first round of writes\n" );
      return -1;
   }
   int foundcnt = 0;
   char foundobjs[5] = {0};
   errno = 0; // only way to check for scan errors
   struct dirent* refent = pos.ns->prepo->metascheme.mdal->scan( refscan );
   while( refent ) {
      // parse through the dirent name, looking for the final '|' char
      printf( "parsing no-pack refpath: \"%s\"\n", refent->d_name );
      char* nameparse = refent->d_name;
      char* finsep = NULL;
      while ( *nameparse != '\0' ) { if ( *nameparse == '|' ) { finsep = nameparse; }; nameparse++; }
      if ( finsep ) {
         // parse the leading numeric value
         nameparse = NULL;
         unsigned long long parseval = strtoull( finsep + 1, &(nameparse), 10 );
         if ( nameparse  &&  *(nameparse) == 'r'  &&  strcmp( nameparse, "rebuild" ) == 0 ) {
            if ( parseval > 4 ) {
               printf( "found rebuild marker for no-pack object %llu ( max expected == 4 )\n", parseval );
               return -1;
            }
            if ( foundobjs[(int)parseval] ) {
               printf( "found duplicate rebuild marker for no-pack object %llu\n", parseval );
               return -1;
            }
            printf( "found no-pack rebuild marker: %llu\n", parseval );
            foundobjs[(int)parseval] = 1;
            foundcnt++;

            // stat the marker and check linkcnt
            if ( pos.ns->prepo->metascheme.mdal->sstat( refscan, refent->d_name, &(stval) ) ) {
               printf( "bizzarre stat failure for rebuild marker: \"%s\"\n", refent->d_name );
               return -1;
            }
            if ( parseval  &&  stval.st_nlink != 6 - ( foundcnt - ( foundobjs[0] + 1 ) ) ) { // hideous means of calculating remaining obj1 markers
               printf( "unexpected link cnt for no-pack rebuild marker %llu: %d\n", parseval, (int)stval.st_nlink );
               return -1;
            }
            else if ( parseval == 0  &&  stval.st_nlink != 3 ) {
               printf( "unexpected link cnt for no-pack rebuild marker %llu: %d\n", parseval, (int)stval.st_nlink );
               return -1;
            }

            // open a handle for the marker, for xattr manipulation
            MDAL_FHANDLE rhandle = pos.ns->prepo->metascheme.mdal->sopen( refscan, refent->d_name );
            if ( rhandle == NULL ) {
               printf( "failed to open a handle for no-pack rebuild marker \"%s\"\n", refent->d_name );
               return -1;
            }
            char* rtagname = rtag_getname( (size_t)parseval );

            // specifically rebuild object 2 ONLY
            if ( parseval == 2 ) {
               char rtagval[1024] = {0};
               if ( pos.ns->prepo->metascheme.mdal->fgetxattr( rhandle, 1, rtagname, rtagval, 1024 ) < 1 ) {
                  printf( "failed to retrieve \"%s\" xatrr value form rebuild marker \"%s\"\n", rtagname, refent->d_name );
                  return -1;
               }
               ne_state rtag = {0};
               rtag.data_status = calloc( 5, sizeof(char) );
               rtag.meta_status = calloc( 5, sizeof(char) );
               if ( rtag_initstr( &(rtag), 5, rtagval ) ) {
                  printf( "failed to parse \"%s\" xatrr value form rebuild marker \"%s\"\n", rtagname, refent->d_name );
                  return -1;
               }
               ne_handle rhandle = ne_open( pos.ns->prepo->datascheme.nectxt, objname3, objlocation3, objerasure3, NE_REBUILD );
               if ( rhandle == NULL ) {
                  printf( "failed to open for rebuild object 2 from marker \"%s\"\n", refent->d_name );
                  return -1;
               }
               if ( ne_seed_status( rhandle, &(rtag) ) ) {
                  printf( "failed to seed status for rebuild object 2 from marker \"%s\"\n", refent->d_name );
                  return -1;
               }
               free( rtag.data_status );
               free( rtag.meta_status );
               if ( ne_rebuild( rhandle, NULL, NULL ) ) {
                  printf( "failed to rebuild object 2 from marker \"%s\"\n", refent->d_name );
                  return -1;
               }
               if ( ne_close(rhandle,NULL,NULL) ) {
                  printf( "failed to close rebuild object 2 from marker \"%s\"\n", refent->d_name );
                  return -1;
               }
            }

            // remove the rebuild tag
            if ( pos.ns->prepo->metascheme.mdal->fremovexattr( rhandle, 1, rtagname ) ) {
               printf( "failed to remove \"%s\" xatrr value form rebuild marker \"%s\"\n", rtagname, refent->d_name );
               return -1;
            }
            pos.ns->prepo->metascheme.mdal->close( rhandle ); // ignore failure
            free( rtagname );

            // delete every encountered marker
            if ( pos.ns->prepo->metascheme.mdal->sunlink( refscan, refent->d_name ) ) {
               printf( "failed to unlink no-pack rebuild marker: \"%s\"\n", refent->d_name );
               return -1;
            }
         }
      }
      // get the next entry
      errno = 0;
      refent = pos.ns->prepo->metascheme.mdal->scan( refscan );
   }
   if ( errno ) {
      printf( "failed to scan reference scanner after first round of writes\n" );
      return -1;
   }
   if ( foundcnt != 5 ) {
      printf( "failed to find all expected rebuild markers after no-pack write ( found %d )\n", foundcnt );
      return -1;
   }
   pos.ns->prepo->metascheme.mdal->closescanner( refscan ); // don't care about failures


   // read back the written files
   // file1
   if ( datastream_open( &(stream), READ_STREAM, "file1", &(pos), NULL ) ) {
      printf( "failed to open 'file1' of no-pack for read\n" );
      return -1;
   }
   ssize_t iores = datastream_read( &(stream), readbuf, 1048576 );
   if ( iores != 1024 * 6 ) {
      printf( "unexpected read res for 'file1' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readbuf, databuf, iores ) ) {
      printf( "unexpected content of 'file1' of no-pack\n" );
      return -1;
   }
   // file2
   if ( datastream_open( &(stream), READ_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 'file2' of no-pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), readbuf, 1048576 * 5 );
   if ( iores != 1024*1024*3 ) {
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


   // this should have generated new rebuild markers
   // verify the existence of and cleanup rebuild markers for every object ( except 2 )
   bzero( &stval, sizeof(struct stat) );
   refscan = pos.ns->prepo->metascheme.mdal->openscanner( pos.ctxt, "0" );
   if ( refscan == NULL ) {
      printf( "failed to open reference scanner after first round of writes\n" );
      return -1;
   }
   while ( foundcnt ) {
      foundcnt--;
      foundobjs[foundcnt] = 0;
   }
   errno = 0; // only way to check for scan errors
   refent = pos.ns->prepo->metascheme.mdal->scan( refscan );
   while( refent ) {
      // parse through the dirent name, looking for the final '|' char
      printf( "parsing no-pack refpath: \"%s\"\n", refent->d_name );
      char* nameparse = refent->d_name;
      char* finsep = NULL;
      while ( *nameparse != '\0' ) { if ( *nameparse == '|' ) { finsep = nameparse; }; nameparse++; }
      if ( finsep ) {
         // parse the leading numeric value
         nameparse = NULL;
         unsigned long long parseval = strtoull( finsep + 1, &(nameparse), 10 );
         if ( nameparse  &&  *(nameparse) == 'r'  &&  strcmp( nameparse, "rebuild" ) == 0 ) {
            if ( parseval > 4 ) {
               printf( "found rebuild marker for no-pack object %llu ( max expected == 4 )\n", parseval );
               return -1;
            }
            if ( foundobjs[(int)parseval] ) {
               printf( "found duplicate rebuild marker for no-pack object %llu\n", parseval );
               return -1;
            }
            printf( "found no-pack rebuild marker: %llu\n", parseval );
            foundobjs[(int)parseval] = 1;
            foundcnt++;

            // stat the marker and check linkcnt
            if ( pos.ns->prepo->metascheme.mdal->sstat( refscan, refent->d_name, &(stval) ) ) {
               printf( "bizzarre stat failure for rebuild marker: \"%s\"\n", refent->d_name );
               return -1;
            }
            if ( parseval  &&  stval.st_nlink != 5 - ( foundcnt - ( foundobjs[0] + 1 ) ) ) { // hideous means of calculating remaining obj1 markers
               printf( "unexpected link cnt for no-pack rebuild marker %llu: %d\n", parseval, (int)stval.st_nlink );
               return -1;
            }
            else if ( parseval == 0  &&  stval.st_nlink != 3 ) {
               printf( "unexpected link cnt for no-pack rebuild marker %llu: %d\n", parseval, (int)stval.st_nlink );
               return -1;
            }

            // open a handle for the marker, for xattr manipulation
            MDAL_FHANDLE rhandle = pos.ns->prepo->metascheme.mdal->sopen( refscan, refent->d_name );
            if ( rhandle == NULL ) {
               printf( "failed to open a handle for no-pack rebuild marker \"%s\"\n", refent->d_name );
               return -1;
            }
            char* rtagname = rtag_getname( (size_t)parseval );

            // remove the rebuild tag
            if ( pos.ns->prepo->metascheme.mdal->fremovexattr( rhandle, 1, rtagname ) ) {
               printf( "failed to remove \"%s\" xatrr value form rebuild marker \"%s\"\n", rtagname, refent->d_name );
               return -1;
            }
            pos.ns->prepo->metascheme.mdal->close( rhandle ); // ignore failure
            free( rtagname );

            // delete every encountered marker
            if ( pos.ns->prepo->metascheme.mdal->sunlink( refscan, refent->d_name ) ) {
               printf( "failed to unlink no-pack rebuild marker: \"%s\"\n", refent->d_name );
               return -1;
            }
         }
      }
      // get the next entry
      errno = 0;
      refent = pos.ns->prepo->metascheme.mdal->scan( refscan );
   }
   if ( errno ) {
      printf( "failed to scan reference scanner after first round of writes\n" );
      return -1;
   }
   if ( foundcnt != 4 ) {
      printf( "failed to find all expected rebuild markers after no-pack write ( found %d )\n", foundcnt );
      return -1;
   }
   pos.ns->prepo->metascheme.mdal->closescanner( refscan ); // don't care about failures




   // cleanup 'file1' refs
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file1" ) ) {
      printf( "Failed to unlink \"file1\"\n" );
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


   // shift to a new NS, which has packing enabled
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

/*

// PACKED OBJECT TESTING


   // create a new stream
   if ( datastream_create( &(stream), "file1", &(pos), 0744, "PACK-CLIENT" ) ) {
      printf( "create failure for 'file1' of pack\n" );
      return -1;
   }
   bzero( databuf, 1024 * 2 );
   if ( datastream_write( &(stream), databuf, 1024 * 2 ) != (1024 * 2) ) {
      printf( "write failure for 'file1' of pack\n" );
      return -1;
   }

   // keep track of this file's rpath
   rpath = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable, NULL, NULL );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname), &(objerasure), &(objlocation) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a new file off of the same stream
   if ( datastream_create( &(stream), "file2", &(pos), 0600, "PACK-CLIENT" ) ) {
      printf( "create failure for 'file2' of pack\n" );
      return -1;
   }
   bzero( databuf, 10 );
   if ( datastream_write( &(stream), databuf, 10 ) != 10 ) {
      printf( "write failure for 'file2' of pack\n" );
      return -1;
   }
   if ( stream->curfile != 1 ) {
      printf( "unexpected curfile value for file2 of pack: %zu\n", stream->curfile );
      return -1;
   }

   // keep track of this file's rpath
   rpath2 = datastream_genrpath( &(stream->files[1].ftag), stream->ns->prepo->metascheme.reftable, NULL, NULL );
   if ( rpath2 == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and identify the data object (should match previous)
   if ( datastream_objtarget( &(stream->files[1].ftag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   // validate that the data object matches the previous
   if ( strcmp( objname, objname2 ) ) {
      printf( "Object mismatch between file1 and file2 of pack:\n   obj1: \"%s\"\n   obj2: \"%s\"\n", objname, objname2 );
      return -1;
   }
   if ( objerasure.N != objerasure2.N  ||
        objerasure.E != objerasure2.E  ||
        objerasure.O != objerasure2.O  ||
        objerasure.partsz  != objerasure2.partsz ) {
      printf( "Erasure mismatch between file1 and file2 of pack:\n   obj1.N: %d\n   obj2.N %d\n   obj1.E: %d\n   obj2.E: %d\n   obj1.O: %d\n   obj2.O: %d\n   obj1.partsz: %zu\n   obj2.partsz: %zu\n", objerasure.N, objerasure2.N, objerasure.E, objerasure2.E, objerasure.O, objerasure2.O, objerasure.partsz, objerasure2.partsz );
      return -1;
   }
   if ( objlocation.pod != objlocation2.pod  ||
        objlocation.cap != objlocation2.cap  ||
        objlocation.scatter != objlocation2.scatter ) {
      printf( "Location mismatch between file1 and file2 of pack:\n   obj1.pod: %d\n   obj2.pod: %d\n   obj1.cap: %d\n   obj2.cap: %d\n   obj1.scat: %d\n   obj2.scat: %d\n", objlocation.pod, objlocation2.pod, objlocation.cap, objlocation2.cap, objlocation.scatter, objlocation2.scatter );
      return -1;
   }
   free( objname2 );

   // write a bit more data
   bzero( databuf, 100 );
   if ( datastream_write( &(stream), databuf, 100 ) != 100 ) {
      printf( "write failure for second write to 'file2' of pack\n" );
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
   bzero( databuf, 1024 * 4 );
   if ( datastream_write( &(stream), databuf, 1024 * 4 ) != (1024 * 4) ) {
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
   char* rpath3 = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable, NULL, NULL );
   if ( rpath3 == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   FTAG tgttag = stream->files->ftag;
   tgttag.objno++;
   if ( datastream_objtarget( &(tgttag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }

   // close the stream
   if ( datastream_close( &(stream) ) ) {
      printf( "Failed to close pack create stream\n" );
      return -1;
   }


   // truncate file2 to an increased size
   if ( datastream_open( &(stream), EDIT_STREAM, "file2", &(pos), NULL ) ) {
      LOG( LOG_ERR, "Failed to open edit handle for 'file2' of pack\n" );
      return -1;
   }
   if ( datastream_truncate( &(stream), 1024 ) ) {
      LOG( LOG_ERR, "Failed to truncate 'file2' of pack to %zu bytes\n", 1024 );
      return -1;
   }
   // truncate file3 to a reduced size
   if ( datastream_open( &(stream), EDIT_STREAM, "file3", &(pos), NULL ) ) {
      LOG( LOG_ERR, "Failed to open edit handle for 'file3' of pack\n" );
      return -1;
   }
   if ( datastream_truncate( &(stream), 1024*3 ) ) {
      LOG( LOG_ERR, "Failed to truncate 'file3' of pack to %zu bytes\n", (1024*3) );
      return -1;
   }
   if ( datastream_release( &(stream) ) ) {
      LOG( LOG_ERR, "Failed to release edit stream for 'file3' of pack\n" );
      return -1;
   }


   // read back the written PACK files
   // file1
   if ( datastream_open( &(stream), READ_STREAM, "file1", &(pos), NULL ) ) {
      printf( "failed to open 'file1' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), databuf, 1048576 );
   if ( iores != (1024 * 2) ) {
      printf( "unexpected read res for 'file1' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( zeroarray, databuf, (1024 * 2) ) ) {
      printf( "unexpected content of 'file1' of pack\n" );
      return -1;
   }
   // file2
   if ( datastream_open( &(stream), READ_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 'file2' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), databuf, 1048576 );
   if ( iores != 1024 ) {
      printf( "unexpected read res for 'file2' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( zeroarray, databuf, iores ) ) {
      printf( "unexpected content of 'file2' of pack\n" );
      return -1;
   }
   if ( datastream_release( &(stream) ) ) {
      printf( "failed to close pack read stream1\n" );
      return -1;
   }
   // file3
   if ( datastream_open( &(stream), READ_STREAM, "file3", &(pos), NULL ) ) {
      printf( "failed to open 'file3' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), databuf, 1048576 );
   if ( iores != (1024 * 3) ) {
      printf( "unexpected read res for 'file3' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( zeroarray, databuf, iores ) ) {
      printf( "unexpected content of 'file3' of pack\n" );
      return -1;
   }
   char* packctag = strdup( stream->ctag );
   char* packstreamid = strdup( stream->streamid );
   if ( packctag == NULL  ||  packstreamid == NULL ) {
      printf( "failed to duplicate stream ctag/id\n" );
      return -1;
   }
   if ( datastream_close( &(stream) ) ) {
      printf( "failed to close pack read stream2\n" );
      return -1;
   }


   // validate recovery info in the first obj
   datahandle = ne_open( pos.ns->prepo->datascheme.nectxt, objname, objlocation, objerasure, NE_RDALL );
   if ( datahandle == NULL ) {
      printf( "Failed to open a read handle for data object: \"%s\" (%s)\n", objname, strerror(errno) );
      return -1;
   }
   datasize = ne_read( datahandle, databuf, 1024 * 1024 * 10 );
   if ( datasize < 1 ) {
      printf( "Failed to read from data object: \"%s\" (%s)\n", objname, strerror(errno) );
      return -1;
   }
   printf( "Read %zd bytes from data object: \"%s\"\n", datasize, objname );
   if ( ne_close( datahandle, NULL, NULL ) ) {
      printf( "Failed to close handle for data object(%s)\n", strerror(errno) );
      return -1;
   }
   RECOVERY_HEADER rheader = {
      .majorversion = 0,
      .minorversion = 0,
      .ctag = NULL,
      .streamid = NULL
   };
   recov = recovery_init( databuf, datasize, &(rheader) );
   if ( recov == NULL ) {
      printf( "Failed to initialize recovery stream for data object: \"%s\" (%s)\n", objname, strerror(errno) );
      return -1;
   }
   if ( strcmp( rheader.ctag, packctag )  ||  strcmp( rheader.streamid, packstreamid ) ) {
      printf( "Recovery header has unexpcted stream values: ctag=%s, sid=%s\n",
              rheader.ctag, rheader.streamid );
      return -1;
   }
   free( packctag );
   free( packstreamid );
   free( rheader.ctag );
   free( rheader.streamid );
   if ( recovery_nextfile( recov, &(rfinfo), NULL, &(bufsize) ) != 1 ) {
      printf( "Failed to retrieve recov info for file1 of pack\n" );
      return -1;
   }
   if ( strcmp( rfinfo.path, "file1" )  ||
         rfinfo.size != 1024 * 2  ||
         rfinfo.size != bufsize  ||
         rfinfo.eof != 1 ) {
      printf( "Unexpected recov info for file1 of pack\n" );
      return -1;
   }
   free( rfinfo.path );
   if ( recovery_nextfile( recov, &(rfinfo), NULL, &(bufsize) ) != 1 ) {
      printf( "Failed to retrieve recov info for file2 of pack\n" );
      return -1;
   }
   if ( strcmp( rfinfo.path, "file2" )  ||
         rfinfo.size != 110  ||
         rfinfo.size != bufsize  ||
         rfinfo.eof != 1 ) {
      printf( "Unexpected recov info for file2 of pack\n" );
      return -1;
   }
   free( rfinfo.path );
   if ( recovery_nextfile( recov, &(rfinfo), NULL, &(bufsize) ) != 1 ) {
      printf( "Failed to retrieve recov info for file3 of pack\n" );
      return -1;
   }
   if ( strcmp( rfinfo.path, "file3" )  ||
         rfinfo.size >= 1024 * 4  ||
         rfinfo.size != bufsize  ||
         rfinfo.eof ) {
      printf( "Unexpected recov info for file3 of pack\n" );
      return -1;
   }
   size_t origrfinfosize = rfinfo.size; // stash this for later check
   free( rfinfo.path );
   if ( recovery_nextfile( recov, NULL, NULL, NULL ) ) {
      printf( "Unexpected trailing file in obj0 of pack\n" );
      return -1;
   }
   // continue to object2
   datahandle = ne_open( pos.ns->prepo->datascheme.nectxt, objname2, objlocation2, objerasure2, NE_RDALL );
   if ( datahandle == NULL ) {
      printf( "Failed to open a read handle for data object: \"%s\" (%s)\n", objname, strerror(errno) );
      return -1;
   }
   datasize = ne_read( datahandle, databuf, 1024 * 1024 * 10 );
   if ( datasize < 1 ) {
      printf( "Failed to read from data object: \"%s\" (%s)\n", objname, strerror(errno) );
      return -1;
   }
   printf( "Read %zd bytes from data object: \"%s\"\n", datasize, objname );
   if ( ne_close( datahandle, NULL, NULL ) ) {
      printf( "Failed to close handle for data object(%s)\n", strerror(errno) );
      return -1;
   }
   if ( recovery_cont( recov, databuf, datasize ) ) {
      printf( "Failed to continue pack recovery process into object1 data\n" );
      return -1;
   }
   if ( recovery_nextfile( recov, &(rfinfo), NULL, &(bufsize) ) != 1 ) {
      printf( "Failed to retrieve recov info for file3 of pack object2\n" );
      return -1;
   }
   if ( strcmp( rfinfo.path, "file3" )  ||
         rfinfo.size != 1024 * 4  ||
         bufsize != (1024 * 4) - origrfinfosize  ||
         rfinfo.eof != 1 ) {
      printf( "Unexpected recov info for file3 of pack object1\n" );
      return -1;
   }
   free( rfinfo.path );
   if ( recovery_nextfile( recov, NULL, NULL, NULL ) ) {
      printf( "Unexpected trailing file in obj1 of pack\n" );
      return -1;
   }
   if ( recovery_close( recov ) ) {
      printf( "Failed to close recovery stream of pack\n" );
      return -1;
   }


   // cleanup 'file1' refs
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file1" ) ) {
      printf( "Failed to unlink pack \"file1\"\n" );
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
      printf( "Failed to unlink pack \"file2\"\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath2 ) ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath2 );
      return -1;
   }
   free( rpath2 );
   // cleanup 'file3' refs
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file3" ) ) {
      printf( "Failed to unlink pack \"file3\"\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath3 ) ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath3 );
      return -1;
   }
   free( rpath3 );
   if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname2, objlocation2 ) ) {
      printf( "Failed to delete data object: \"%s\"\n", objname2 );
      return -1;
   }
   free( objname2 );


*/

   // cleanup our data buffers
   free( databuf );
   free( readbuf );

   // cleanup our position struct
   MDAL posmdal = pos.ns->prepo->metascheme.mdal;
   if ( posmdal->destroyctxt( pos.ctxt ) ) {
      printf( "Failed to destory position MDAL_CTXT\n" );
      return -1;
   }

   // cleanup all created NSs
   if ( deletesubdirs( "./test_datastream_rebuilds_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/heavily-protected-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of heavily-protected-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/heavily-protected-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/heavily-protected-data NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_datastream_rebuilds_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/read-only-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of read-only-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/read-only-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/read-only-data NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_datastream_rebuilds_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of gransom-allocation\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation" ) ) {
      printf( "Failed to destroy /gransom-allocation NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_datastream_rebuilds_topdir/mdal_root/MDAL_reference" ) ) {
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
   if ( deletesubdirs( "./test_datastream_rebuilds_topdir/dal_root" ) ) {
      printf( "Failed to delete subdirs of DAL root\n" );
      return -1;
   }

   // delete dal/mdal dir structure
   rmdir( "./test_datastream_rebuilds_topdir/dal_root" );
   rmdir( "./test_datastream_rebuilds_topdir/mdal_root" );
   rmdir( "./test_datastream_rebuilds_topdir" );

   return 0;
}


