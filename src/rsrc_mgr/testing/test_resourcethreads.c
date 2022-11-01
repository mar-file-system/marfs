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
#include "resourcethreads.c"

#include <unistd.h>
#include <stdio.h>
#include <ftw.h>
#include <sys/time.h>
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

   // get a start of run time
   struct timeval starttime;
   if ( gettimeofday( &starttime, NULL ) ) {
      printf( "failed to get start time\n" );
      return -1;
   }

   // Initialize the libxml lib and check for API mismatches
   LIBXML_TEST_VERSION

   // create the dirs necessary for DAL/MDAL initialization (ignore EEXIST)
   errno = 0;
   if ( mkdir( "./test_rman_topdir", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_rman_topdir\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_rman_topdir/dal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_rman_topdir/dal_root\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_rman_topdir/mdal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_rman_topdir/mdal_root\"\n" );
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

   // establish a data buffer to hold all data content
   void* databuf = malloc( 1024 * 1024 * 10 * sizeof(char) ); // 10MiB
   if ( databuf == NULL ) {
      printf( "Failed to allocate 10MiB data buffer\n" );
      return -1;
   }
   int tmpcnt = 0;
   for ( ; tmpcnt < (1024 * 1024 * 10); tmpcnt++ ) {
      // populate databuf
      *((char*)databuf + tmpcnt) = (char)tmpcnt;
   }
   char readarray[1048576] = {0}; // all zero 1MiB buffer

   // shift to a new NS, which has chunking and packing enabled
   char* configtgt = strdup( "./pack" );
   if ( configtgt == NULL ) {
      printf( "Failed to duplicate configtgt string\n" );
      return -1;
   }
   if ( config_traverse( config, &(pos), &(configtgt), 1 ) ) {
      printf( "failed to traverse config subpath: \"%s\"\n", configtgt );
      return -1;
   }
   free( configtgt );
   if ( pos.ctxt == NULL  &&  config_fortifyposition( &(pos) ) ) {
      printf( "failed to fortify nopack position\n" );
      return -1;
   }
   MDAL curmdal = pos.ns->prepo->metascheme.mdal;



   // create a randomized, but deterministic, set of marfs files
   size_t totalfiles = 0;
   size_t totalbytes = 0;
   size_t totalstreams = 0;
   char filepath[1024] = {0};
   srand( 42 );
   size_t streamcount = (rand() % 10) + 3;
   int streamnum = 0;
   // create each stream in a new subdir
   for ( ; streamnum < streamcount; streamnum++ ) {
      int printres = snprintf( filepath, 1024, "stream%.2d", streamnum );
      if ( printres >= 1024 ) {
         printf( "subsized filepath for dir %d\n", streamnum );
         return -1;
      }
      if ( curmdal->mkdir( pos.ctxt, filepath, 0770 ) ) {
         printf( "failed to mkdir \"%s\"\n", filepath );
         return -1;
      }
      // create all files for the stream
      DATASTREAM stream = NULL;
      DATASTREAM editstream = NULL;
      int filenum = 0;
      size_t filecount = (rand() % 10) + 1;
      for ( ; filenum < filecount; filenum++ ) {
         if ( snprintf( filepath + printres, 1024 - printres, "/file-%.2d", filenum ) >= 1024 ) {
            printf( "subsized filepath for file %d\n", filenum );
            return -1;
         }
         if ( datastream_create( &(stream), filepath, &(pos), 0744, "Thread-Client-1" ) ) {
            printf( "failed to create marfs file \"%s\"\n", filepath );
            return -1;
         }
         size_t filebytes = (rand() % 20480); // sizes range from 0 to 20K
         // randomly extend some portion of files
         if ( rand() % 10 == 0 ) {
            printf( "extending file \"%s\"\n", filepath );
            if ( datastream_extend( &(stream), filebytes ) ) {
               printf( "failed to extend file \"%s\"\n", filepath );
               return -1;
            }
            if ( datastream_release( &(stream) ) ) {
               printf( "failed to realease stream for extended file \"%s\"\n", filepath );
               return -1;
            }
            totalstreams++;
            if ( datastream_open( &(editstream), EDIT_STREAM, filepath, &(pos), NULL ) ) {
               printf( "failed to open edit stream for extend file \"%s\"\n", filepath );
               return -1;
            }
            if ( datastream_write( &(editstream), databuf, filebytes ) != filebytes ) {
               printf( "failed to write %zu bytes to extended file \"%s\"\n", filebytes, filepath );
               return -1;
            }
            if ( datastream_close( &(editstream) ) ) {
               printf( "failed to close edit stream for extended file \"%s\"\n", filepath );
               return -1;
            }
         }
         else {
            if ( datastream_write( &(stream), databuf, filebytes ) != filebytes ) {
               printf( "failed to write %zu bytes to file \"%s\"\n", filebytes, filepath );
               return -1;
            }
         }
         totalfiles++;
         totalbytes+=filebytes;
      }
      // close the stream
      if ( datastream_close( &(stream) ) ) {
         printf( "close failure after file \"%s\"\n", filepath );
         return -1;
      }
      totalstreams++;
   }

   // read back all written files, and perform random deletions
   size_t delfiles = 0;
   size_t delbytes = 0;
   for ( streamnum = 0; streamnum < streamcount; streamnum++ ) {
      int printres = snprintf( filepath, 1024, "stream%.2d", streamnum );
      if ( printres >= 1024 ) {
         printf( "subsized filepath for dir %d\n", streamnum );
         return -1;
      }
      MDAL_DHANDLE dir = curmdal->opendir( pos.ctxt, filepath );
      if ( dir == NULL ) {
         printf( "failed to open dir \"%s\"\n", filepath );
         return -1;
      }
      struct dirent* dirent;
      DATASTREAM stream = NULL;
      errno = 0;
      while ( (dirent = curmdal->readdir( dir )) ) {
         if ( strncmp( dirent->d_name, ".", 1 ) == 0 ) { continue; }
         if ( snprintf( filepath + printres, 1024 - printres, "/%s", dirent->d_name ) >= 1024 ) {
            printf( "subsized filepath for file %s\n", dirent->d_name );
            return -1;
         }
         printf( "reading content of \"%s\"\n", filepath );
         if ( datastream_open( &(stream), READ_STREAM, filepath, &(pos), NULL ) ) {
            printf( "failed to open marfs file \"%s\" for read\n", filepath );
            return -1;
         }
         ssize_t readbytes = datastream_read( &(stream), readarray, 1048576 );
         if ( readbytes < 0 ) {
            printf( "failed to read from file \"%s\"\n", filepath );
            return -1;
         }
         if ( readbytes > 0  &&  memcmp( readarray, databuf, readbytes ) ) {
            printf( "invalid content of file \"%s\"\n", filepath );
            return -1;
         }
         // potentially delete this file (20% chance)
         if ( (rand() % 5) == 0 ) {
            printf( "deleting \"%s\"\n", filepath );
            if ( curmdal->unlink( pos.ctxt, filepath ) ) {
               printf( "failed to delete \"%s\"\n", filepath );
               return -1;
            }
            delfiles++;
            delbytes+=readbytes;
         }
         errno = 0;
      }
      if ( errno ) {
         *(filepath + printres) = '\0';
         printf( "failed readdir for \"%s\"\n", filepath );
         return -1;
      }
      // close the stream
      if ( stream ) {
         if ( datastream_close( &(stream) ) ) {
            printf( "close failure of read stream after file \"%s\"\n", filepath );
            return -1;
         }
      }
   }

   // get a post-delete time
   struct timeval currenttime;
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk\n" );
      return -1;
   }
   thresholds thresh = {
      .gcthreshold = currenttime.tv_sec + 120, // +1, to ensure we actually get *all* deleted files
      .repackthreshold = 0, // no repacks for now
      .rebuildthreshold = 0, // no rebuilds for now
      .cleanupthreshold = currenttime.tv_sec + 120
   };

   // set up resource threads
   rthread_global_state gstate;
   bzero( &(gstate), sizeof( struct rthread_global_state_struct ) );
   gstate.pos = pos;
   gstate.thresh = thresh;
   if ( resourceinput_init( &(gstate.rinput), &(gstate.pos), 2 ) ) {
      printf( "failed to initialize resourceinput for first run\n" );
      return -1;
   }
   if ( resourcelog_init( &(gstate.rlog), "./test_rman_topdir/logfile1", RESOURCE_MODIFY_LOG, pos.ns ) ) {
      printf( "failed to initialize resourcelog for first run\n" );
      return -1;
   }
   gstate.rpst = repackstreamer_init();
   if ( gstate.rpst == NULL ) {
      printf( "failed to initalize repackstreamer for first run\n" );
      return -1;
   }
   gstate.numprodthreads = 2;
   gstate.numconsthreads = 3;
   TQ_Init_Opts tqopts = {
      .log_prefix = "Run1",
      .init_flags = 0,
      .max_qdepth = 10,
      .global_state = &(gstate),
      .num_threads = gstate.numprodthreads + gstate.numconsthreads,
      .num_prod_threads = gstate.numprodthreads,
      .thread_init_func = rthread_init_func,
      .thread_consumer_func = rthread_consumer_func,
      .thread_producer_func = rthread_producer_func,
      .thread_pause_func = NULL,
      .thread_resume_func = NULL,
      .thread_term_func = rthread_term_func
   };
   printf( "starting up threads for first walk\n" );

   ThreadQueue tq = tq_init( &(tqopts) );
   if ( tq == NULL ) {
      printf( "failed to start up threads for first walk\n" );
      return -1;
   }

   // set input to the first ref range
   size_t refcnt = (rand() % (pos.ns->prepo->metascheme.refnodecount)) + 1;
   printf( "setting first walk input to 0 - %zu\n", refcnt );
   if ( resourceinput_setrange( &(gstate.rinput), 0, refcnt ) ) {
      printf( "failed to set first walk input 1\n" );
      return -1;
   }
   printf( "waiting for first walk, first input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for first walk, first input completion\n" );
      return -1;
   }
   // set input to second ref range
   printf( "setting second walk input to %zu - %zu\n", refcnt, pos.ns->prepo->metascheme.refnodecount );
   if ( resourceinput_setrange( &(gstate.rinput), refcnt, pos.ns->prepo->metascheme.refnodecount ) ) {
      printf( "failed to set first walk input 2\n" );
      return -1;
   }
   printf( "waiting for first walk, second input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for first walk, first input completion\n" );
      return -1;
   }
   // terminate thread input
   if ( resourceinput_term( &(gstate.rinput) ) ) {
      printf( "failed to term first walk input\n" );
      return -1;
   }
   // wait for the queue to be marked as FINISHED
   TQ_Control_Flags setflags = 0;
   if ( tq_wait_for_flags( tq, 0, &(setflags) ) ) {
      printf( "failed to wait for TQ flags on first walk\n" );
      return -1;
   }
   if ( setflags != TQ_FINISHED ) {
      printf( "unexpected flags following first walk\n" );
      return -1;
   }
   // wait for TQ completion
   if ( tq_wait_for_completion( tq ) ) {
      printf( "failed to wait for completion of first walk TQ\n" );
      return -1;
   }
   // gather all thread status values
   rthread_state* tstate = NULL;
   int retval;
   streamwalker_report report = {0};
   size_t reportedstreams = 0;
   size_t origdelstreams = report.delstreams;
   while ( (retval = tq_next_thread_status( tq, (void**)&(tstate) )) > 0 ) {
      if ( tstate == NULL ) {
         printf( "received a NULL thread status\n" );
         return -1;
      }
      if ( tstate->fatalerror ) {
         printf( "thread %u indicates a fatal error\n", tstate->tID );
         return -1;
      }
      if ( tstate->scanner  ||  tstate->rdirpath  ||  tstate->walker  ||  tstate->gcops  ||  tstate->repackops ) {
         printf( "thread %u has remaining state values\n", tstate->tID );
         return -1;
      }
      // note all thread report values
      reportedstreams   += tstate->streamcount;
      report.fileusage  += tstate->report.fileusage;
      report.byteusage  += tstate->report.byteusage;
      report.filecount  += tstate->report.filecount;
      report.objcount   += tstate->report.objcount;
      report.bytecount  += tstate->report.bytecount;
      report.streamcount+= tstate->report.streamcount;
      report.delobjs    += tstate->report.delobjs;
      report.delfiles   += tstate->report.delfiles;
      report.delstreams += tstate->report.delstreams;
      report.volfiles   += tstate->report.volfiles;
      report.rpckfiles  += tstate->report.rpckfiles;
      report.rpckbytes  += tstate->report.rpckbytes;
      report.rbldobjs   += tstate->report.rbldobjs;
      report.rbldbytes  += tstate->report.rbldbytes;
   }
   if ( retval ) {
      printf( "failed to collect all thread status values\n" );
      return -1;
   }
   // close our the thread queue, logs, etc.
   if ( tq_close( tq ) ) {
      printf( "failed to close first thread queue\n" );
      return -1;
   }
   operation_summary summary = {0};
   if ( resourcelog_term( &(gstate.rlog), &(summary), NULL ) ) {
      printf( "failed to terminate resource log following first walk\n" );
      return -1;
   }
   if ( repackstreamer_complete( gstate.rpst ) ) {
      printf( "failed to complete repack streamer  following first walk\n" );
      return -1;
   }
   gstate.rpst = NULL;


   // check report totals against expected values
   if ( reportedstreams != totalstreams ) {
      printf( "First walk reported streams (%zu) does not match expected total (%zu)\n", reportedstreams, totalstreams );
      return -1;
   }
   if ( report.filecount != totalfiles ) {
      printf( "First walk reported files (%zu) does not match expected total (%zu)\n", report.filecount, totalfiles );
      return -1;
   }
   if ( report.byteusage + delbytes != totalbytes ) {
      printf( "First walk reported bytes ( %zu used + %zu deleted ) does not match expected total (%zu)\n",
              report.byteusage, delbytes, totalbytes );
      return -1;
   }
   if ( report.bytecount != totalbytes ) {
      printf( "First walk reported bytes ( %zu ) does not match expected total (%zu)\n", report.bytecount, totalbytes );
      return -1;
   }
   // note deleteion values
   size_t gcfiles = report.delfiles;


   // re-read all files, and delete more
   for ( streamnum = 0; streamnum < streamcount; streamnum++ ) {
      int printres = snprintf( filepath, 1024, "stream%.2d", streamnum );
      if ( printres >= 1024 ) {
         printf( "subsized filepath for dir %d\n", streamnum );
         return -1;
      }
      MDAL_DHANDLE dir = curmdal->opendir( pos.ctxt, filepath );
      if ( dir == NULL ) {
         printf( "failed to open dir \"%s\"\n", filepath );
         return -1;
      }
      struct dirent* dirent;
      DATASTREAM stream = NULL;
      errno = 0;
      while ( (dirent = curmdal->readdir( dir )) ) {
         if ( strncmp( dirent->d_name, ".", 1 ) == 0 ) { continue; }
         if ( snprintf( filepath + printres, 1024 - printres, "/%s", dirent->d_name ) >= 1024 ) {
            printf( "subsized filepath for file %s\n", dirent->d_name );
            return -1;
         }
         printf( "reading content of \"%s\"\n", filepath );
         if ( datastream_open( &(stream), READ_STREAM, filepath, &(pos), NULL ) ) {
            printf( "failed to open marfs file \"%s\" for read\n", filepath );
            return -1;
         }
         ssize_t readbytes = datastream_read( &(stream), readarray, 1048576 );
         if ( readbytes < 0 ) {
            printf( "failed to read from file \"%s\"\n", filepath );
            return -1;
         }
         if ( readbytes > 0  &&  memcmp( readarray, databuf, readbytes ) ) {
            printf( "invalid content of file \"%s\"\n", filepath );
            return -1;
         }
         // potentially delete this file (50% chance)
         if ( (rand() % 2) == 0 ) {
            printf( "deleting \"%s\"\n", filepath );
            if ( curmdal->unlink( pos.ctxt, filepath ) ) {
               printf( "failed to delete \"%s\"\n", filepath );
               return -1;
            }
            delfiles++;
            delbytes+=readbytes;
         }
         errno = 0;
      }
      if ( errno ) {
         *(filepath + printres) = '\0';
         printf( "failed readdir for \"%s\"\n", filepath );
         return -1;
      }
      // close the stream
      if ( stream ) {
         if ( datastream_close( &(stream) ) ) {
            printf( "close failure of read stream after file \"%s\"\n", filepath );
            return -1;
         }
      }
   }


   // rewalk, cleaning up new objs/files
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for second walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120; // +1, to ensure we actually get *all* deleted files
   // no repacks for now
   // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;

   // set up resource threads
   if ( resourceinput_init( &(gstate.rinput), &(gstate.pos), 3 ) ) {
      printf( "failed to initialize resourceinput for second run\n" );
      return -1;
   }
   if ( resourcelog_init( &(gstate.rlog), "./test_rman_topdir/logfile2", RESOURCE_MODIFY_LOG, pos.ns ) ) {
      printf( "failed to initialize resourcelog for second run\n" );
      return -1;
   }
   gstate.rpst = repackstreamer_init();
   if ( gstate.rpst == NULL ) {
      printf( "failed to initalize repackstreamer for second run\n" );
      return -1;
   }
   gstate.numprodthreads = 3;
   gstate.numconsthreads = 6;
   tqopts.log_prefix = "Run2";
   tqopts.num_threads = gstate.numprodthreads + gstate.numconsthreads;
   tqopts.num_prod_threads = gstate.numprodthreads;
   printf( "starting up threads for second walk\n" );

   tq = tq_init( &(tqopts) );
   if ( tq == NULL ) {
      printf( "failed to start up threads for second walk\n" );
      return -1;
   }

   // set input to the first ref range
   refcnt = (rand() % (pos.ns->prepo->metascheme.refnodecount)) + 1;
   printf( "setting second walk input to 0 - %zu\n", refcnt );
   if ( resourceinput_setrange( &(gstate.rinput), 0, refcnt ) ) {
      printf( "failed to set second walk input 1\n" );
      return -1;
   }
   printf( "waiting for second walk, first input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for second walk, first input completion\n" );
      return -1;
   }
   // set input to second ref range
   printf( "setting second walk input to %zu - %zu\n", refcnt, pos.ns->prepo->metascheme.refnodecount );
   if ( resourceinput_setrange( &(gstate.rinput), refcnt, pos.ns->prepo->metascheme.refnodecount ) ) {
      printf( "failed to set second walk input 2\n" );
      return -1;
   }
   printf( "waiting for second walk, second input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for second walk, second input completion\n" );
      return -1;
   }
   // terminate our input
   if ( resourceinput_term( &(gstate.rinput) ) ) {
      printf( "failed to terminate second walk input\n" );
      return -1;
   }
   // wait for the queue to be marked as FINISHED
   setflags = 0;
   if ( tq_wait_for_flags( tq, 0, &(setflags) ) ) {
      printf( "failed to wait for TQ flags on second walk\n" );
      return -1;
   }
   if ( setflags != TQ_FINISHED ) {
      printf( "unexpected flags following second walk\n" );
      return -1;
   }
   // wait for TQ completion
   if ( tq_wait_for_completion( tq ) ) {
      printf( "failed to wait for completion of second walk TQ\n" );
      return -1;
   }
   // gather all thread status values
   tstate = NULL;
   origdelstreams += report.delstreams;
   printf( "ODELSTREAMS = %zu\n", origdelstreams );
   reportedstreams = origdelstreams; // include any previously deleted streams
   bzero( &(report), sizeof( struct streamwalker_report_struct ) );
   while ( (retval = tq_next_thread_status( tq, (void**)&(tstate) )) > 0 ) {
      if ( tstate == NULL ) {
         printf( "received a NULL thread status for second walk\n" );
         return -1;
      }
      if ( tstate->fatalerror ) {
         printf( "second walk thread %u indicates a fatal error\n", tstate->tID );
         return -1;
      }
      if ( tstate->scanner  ||  tstate->rdirpath  ||  tstate->walker  ||  tstate->gcops  ||  tstate->repackops ) {
         printf( "second walk thread %u has remaining state values\n", tstate->tID );
         return -1;
      }
      // note all thread report values
      reportedstreams   += tstate->streamcount;
      report.fileusage  += tstate->report.fileusage;
      report.byteusage  += tstate->report.byteusage;
      report.filecount  += tstate->report.filecount;
      report.objcount   += tstate->report.objcount;
      report.bytecount  += tstate->report.bytecount;
      report.streamcount+= tstate->report.streamcount;
      report.delobjs    += tstate->report.delobjs;
      report.delfiles   += tstate->report.delfiles;
      report.delstreams += tstate->report.delstreams;
      report.volfiles   += tstate->report.volfiles;
      report.rpckfiles  += tstate->report.rpckfiles;
      report.rpckbytes  += tstate->report.rpckbytes;
      report.rbldobjs   += tstate->report.rbldobjs;
      report.rbldbytes  += tstate->report.rbldbytes;
   }
   if ( retval ) {
      printf( "failed to collect all second walk thread status values\n" );
      return -1;
   }
   // close our the thread queue, logs, etc.
   if ( tq_close( tq ) ) {
      printf( "failed to close second thread queue\n" );
      return -1;
   }
   bzero( &(summary), sizeof( struct operation_summary_struct ) );
   if ( resourcelog_term( &(gstate.rlog), &(summary), NULL ) ) {
      printf( "failed to terminate resource log following second walk\n" );
      return -1;
   }
   if ( repackstreamer_complete( gstate.rpst ) ) {
      printf( "failed to complete repack streamer  following second walk\n" );
      return -1;
   }
   gstate.rpst = NULL;


   // check report totals against expected values
   printf( "second walk delstreams = %zu\n", report.delstreams );
   if ( reportedstreams != totalstreams ) {
      printf( "second walk reported streams (%zu) does not match expected total (%zu)\n", reportedstreams, totalstreams );
      return -1;
   }
   if ( report.filecount + gcfiles != totalfiles ) {
      printf( "second walk reported files (%zu) does not match expected total (%zu)\n", report.filecount, totalfiles );
      return -1;
   }
   if ( report.byteusage + delbytes != totalbytes ) {
      printf( "second walk reported bytes ( %zu used + %zu deleted ) does not match expected total (%zu)\n",
              report.byteusage, delbytes, totalbytes );
      return -1;
   }
   if ( report.bytecount + delbytes < totalbytes ) {
      printf( "second walk reported bytes ( %zu ) does not match/exceed expected total (%zu)\n",
              report.bytecount + delbytes, totalbytes );
      return -1;
   }
   // note deleteion values
   gcfiles += report.delfiles;


   // re-read and delete all remaining files
   for ( streamnum = 0; streamnum < streamcount; streamnum++ ) {
      int printres = snprintf( filepath, 1024, "stream%.2d", streamnum );
      if ( printres >= 1024 ) {
         printf( "subsized filepath for dir %d\n", streamnum );
         return -1;
      }
      MDAL_DHANDLE dir = curmdal->opendir( pos.ctxt, filepath );
      if ( dir == NULL ) {
         printf( "failed to open dir \"%s\"\n", filepath );
         return -1;
      }
      struct dirent* dirent;
      DATASTREAM stream = NULL;
      errno = 0;
      while ( (dirent = curmdal->readdir( dir )) ) {
         if ( strncmp( dirent->d_name, ".", 1 ) == 0 ) { continue; }
         if ( snprintf( filepath + printres, 1024 - printres, "/%s", dirent->d_name ) >= 1024 ) {
            printf( "subsized filepath for file %s\n", dirent->d_name );
            return -1;
         }
         printf( "reading content of \"%s\"\n", filepath );
         if ( datastream_open( &(stream), READ_STREAM, filepath, &(pos), NULL ) ) {
            printf( "failed to open marfs file \"%s\" for read\n", filepath );
            return -1;
         }
         ssize_t readbytes = datastream_read( &(stream), readarray, 1048576 );
         if ( readbytes < 0 ) {
            printf( "failed to read from file \"%s\"\n", filepath );
            return -1;
         }
         if ( readbytes > 0  &&  memcmp( readarray, databuf, readbytes ) ) {
            printf( "invalid content of file \"%s\"\n", filepath );
            return -1;
         }
         // delete this file
         printf( "deleting \"%s\"\n", filepath );
         if ( curmdal->unlink( pos.ctxt, filepath ) ) {
            printf( "failed to delete \"%s\"\n", filepath );
            return -1;
         }
         delfiles++;
         delbytes+=readbytes;
         errno = 0;
      }
      if ( errno ) {
         *(filepath + printres) = '\0';
         printf( "failed readdir for \"%s\"\n", filepath );
         return -1;
      }
      // close the stream
      if ( stream ) {
         if ( datastream_close( &(stream) ) ) {
            printf( "close failure of read stream after file \"%s\"\n", filepath );
            return -1;
         }
      }
      // should be safe to remove the parent dir as well
      snprintf( filepath, 1024, "stream%.2d", streamnum );
      if ( curmdal->rmdir( pos.ctxt, filepath ) ) {
         printf( "failed to rmdir \"%s\"\n", filepath );
         return -1;
      }
   }

   // walk for a third time, cleaning up all objs/files
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for third walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120; // +1, to ensure we actually get *all* deleted files
   // no repacks for now
   // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;

   // set up resource threads
   if ( resourceinput_init( &(gstate.rinput), &(gstate.pos), 3 ) ) {
      printf( "failed to initialize resourceinput for third run\n" );
      return -1;
   }
   if ( resourcelog_init( &(gstate.rlog), "./test_rman_topdir/logfile3", RESOURCE_MODIFY_LOG, pos.ns ) ) {
      printf( "failed to initialize resourcelog for third run\n" );
      return -1;
   }
   gstate.rpst = repackstreamer_init();
   if ( gstate.rpst == NULL ) {
      printf( "failed to initalize repackstreamer for third run\n" );
      return -1;
   }
   gstate.numprodthreads = 3;
   gstate.numconsthreads = 6;
   tqopts.log_prefix = "Run3+F";
   tqopts.num_threads = gstate.numprodthreads + gstate.numconsthreads;
   tqopts.num_prod_threads = gstate.numprodthreads;
   printf( "starting up threads for third walk\n" );

   tq = tq_init( &(tqopts) );
   if ( tq == NULL ) {
      printf( "failed to start up threads for third walk\n" );
      return -1;
   }

   // set input to the first ref range
   refcnt = (rand() % (pos.ns->prepo->metascheme.refnodecount)) + 1;
   printf( "setting third walk input to 0 - %zu\n", refcnt );
   if ( resourceinput_setrange( &(gstate.rinput), 0, refcnt ) ) {
      printf( "failed to set third walk input 1\n" );
      return -1;
   }
   printf( "waiting for third walk, first input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for third walk, first input completion\n" );
      return -1;
   }
   // set input to second ref range
   printf( "setting third walk input to %zu - %zu\n", refcnt, pos.ns->prepo->metascheme.refnodecount );
   if ( resourceinput_setrange( &(gstate.rinput), refcnt, pos.ns->prepo->metascheme.refnodecount ) ) {
      printf( "failed to set third walk input 2\n" );
      return -1;
   }
   printf( "waiting for third walk, second input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for third walk, second input completion\n" );
      return -1;
   }
   // terminate our input
   if ( resourceinput_term( &(gstate.rinput) ) ) {
      printf( "failed to terminate third walk input\n" );
      return -1;
   }
   // wait for the queue to be marked as FINISHED
   setflags = 0;
   if ( tq_wait_for_flags( tq, 0, &(setflags) ) ) {
      printf( "failed to wait for TQ flags on third walk\n" );
      return -1;
   }
   if ( setflags != TQ_FINISHED ) {
      printf( "unexpected flags following third walk\n" );
      return -1;
   }
   // wait for TQ completion
   if ( tq_wait_for_completion( tq ) ) {
      printf( "failed to wait for completion of third walk TQ\n" );
      return -1;
   }
   // gather all thread status values
   tstate = NULL;
   origdelstreams += report.delstreams;
   printf( "ODELSTREAMS = %zu\n", origdelstreams );
   reportedstreams = origdelstreams; // include any previously deleted streams
   bzero( &(report), sizeof( struct streamwalker_report_struct ) );
   while ( (retval = tq_next_thread_status( tq, (void**)&(tstate) )) > 0 ) {
      if ( tstate == NULL ) {
         printf( "received a NULL thread status for third walk\n" );
         return -1;
      }
      if ( tstate->fatalerror ) {
         printf( "third walk thread %u indicates a fatal error\n", tstate->tID );
         return -1;
      }
      if ( tstate->scanner  ||  tstate->rdirpath  ||  tstate->walker  ||  tstate->gcops  ||  tstate->repackops ) {
         printf( "third walk thread %u has remaining state values\n", tstate->tID );
         return -1;
      }
      // note all thread report values
      reportedstreams   += tstate->streamcount;
      report.fileusage  += tstate->report.fileusage;
      report.byteusage  += tstate->report.byteusage;
      report.filecount  += tstate->report.filecount;
      report.objcount   += tstate->report.objcount;
      report.bytecount  += tstate->report.bytecount;
      report.streamcount+= tstate->report.streamcount;
      report.delobjs    += tstate->report.delobjs;
      report.delfiles   += tstate->report.delfiles;
      report.delstreams += tstate->report.delstreams;
      report.volfiles   += tstate->report.volfiles;
      report.rpckfiles  += tstate->report.rpckfiles;
      report.rpckbytes  += tstate->report.rpckbytes;
      report.rbldobjs   += tstate->report.rbldobjs;
      report.rbldbytes  += tstate->report.rbldbytes;
   }
   if ( retval ) {
      printf( "failed to collect all third walk thread status values\n" );
      return -1;
   }
   // close our the thread queue, logs, etc.
   if ( tq_close( tq ) ) {
      printf( "failed to close third thread queue\n" );
      return -1;
   }
   bzero( &(summary), sizeof( struct operation_summary_struct ) );
   if ( resourcelog_term( &(gstate.rlog), &(summary), NULL ) ) {
      printf( "failed to terminate resource log following third walk\n" );
      return -1;
   }
   if ( repackstreamer_complete( gstate.rpst ) ) {
      printf( "failed to complete repack streamer  following third walk\n" );
      return -1;
   }
   gstate.rpst = NULL;


   // check report totals against expected values
   printf( "third walk delstreams = %zu\n", report.delstreams );
   if ( reportedstreams != totalstreams ) {
      printf( "third walk reported streams (Cnt%zu + Del%zu) does not match expected total (%zu)\n", reportedstreams - origdelstreams, origdelstreams, totalstreams );
      return -1;
   }
   if ( report.filecount + gcfiles != totalfiles ) {
      printf( "third walk reported files (%zu) does not match expected total (%zu)\n", report.filecount, totalfiles );
      return -1;
   }
   if ( report.byteusage + delbytes != totalbytes ) {
      printf( "third walk reported bytes ( %zu used + %zu deleted ) does not match expected total (%zu)\n",
              report.byteusage, delbytes, totalbytes );
      return -1;
   }
   if ( report.bytecount + delbytes < totalbytes ) {
      printf( "third walk reported bytes ( %zu ) does not match/exceed expected total (%zu)\n",
              report.bytecount + delbytes, totalbytes );
      return -1;
   }
   // note deleteion values
   gcfiles += report.delfiles;



   // walk for a final time, cleaning up all objs/files
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for final walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120; // +1, to ensure we actually get *all* deleted files
   // no repacks for now
   // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;

   // set up resource threads
   if ( resourceinput_init( &(gstate.rinput), &(gstate.pos), 5 ) ) {
      printf( "failed to initialize resourceinput for final run\n" );
      return -1;
   }
   if ( resourcelog_init( &(gstate.rlog), "./test_rman_topdir/logfileF", RESOURCE_MODIFY_LOG, pos.ns ) ) {
      printf( "failed to initialize resourcelog for final run\n" );
      return -1;
   }
   gstate.rpst = repackstreamer_init();
   if ( gstate.rpst == NULL ) {
      printf( "failed to initalize repackstreamer for final run\n" );
      return -1;
   }
   gstate.numprodthreads = 5;
   gstate.numconsthreads = 1;
   tqopts.log_prefix = "RunF";
   tqopts.num_threads = gstate.numprodthreads + gstate.numconsthreads;
   tqopts.num_prod_threads = gstate.numprodthreads;
   printf( "starting up threads for final walk\n" );

   tq = tq_init( &(tqopts) );
   if ( tq == NULL ) {
      printf( "failed to start up threads for final walk\n" );
      return -1;
   }

   // set input to the first ref range
   refcnt = (rand() % (pos.ns->prepo->metascheme.refnodecount)) + 1;
   printf( "setting final walk input to 0 - %zu\n", refcnt );
   if ( resourceinput_setrange( &(gstate.rinput), 0, refcnt ) ) {
      printf( "failed to set final walk input 1\n" );
      return -1;
   }
   printf( "waiting for final walk, first input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for final walk, first input completion\n" );
      return -1;
   }
   // set input to second ref range
   printf( "setting final walk input to %zu - %zu\n", refcnt, pos.ns->prepo->metascheme.refnodecount );
   if ( resourceinput_setrange( &(gstate.rinput), refcnt, pos.ns->prepo->metascheme.refnodecount ) ) {
      printf( "failed to set final walk input 2\n" );
      return -1;
   }
   printf( "waiting for final walk, second input completion\n" );
   if ( resourceinput_waitforcomp( &(gstate.rinput) ) ) {
      printf( "failed to wait for final walk, second input completion\n" );
      return -1;
   }
   // terminate our input
   if ( resourceinput_term( &(gstate.rinput) ) ) {
      printf( "failed to terminate final walk input\n" );
      return -1;
   }
   // wait for the queue to be marked as FINISHED
   setflags = 0;
   if ( tq_wait_for_flags( tq, 0, &(setflags) ) ) {
      printf( "failed to wait for TQ flags on final walk\n" );
      return -1;
   }
   if ( setflags != TQ_FINISHED ) {
      printf( "unexpected flags following final walk\n" );
      return -1;
   }
   // wait for TQ completion
   if ( tq_wait_for_completion( tq ) ) {
      printf( "failed to wait for completion of final walk TQ\n" );
      return -1;
   }
   // gather all thread status values
   tstate = NULL;
   origdelstreams += report.delstreams;
   printf( "ODELSTREAMS = %zu\n", origdelstreams );
   reportedstreams = origdelstreams; // include any previously deleted streams
   bzero( &(report), sizeof( struct streamwalker_report_struct ) );
   while ( (retval = tq_next_thread_status( tq, (void**)&(tstate) )) > 0 ) {
      if ( tstate == NULL ) {
         printf( "received a NULL thread status for final walk\n" );
         return -1;
      }
      if ( tstate->fatalerror ) {
         printf( "final walk thread %u indicates a fatal error\n", tstate->tID );
         return -1;
      }
      if ( tstate->scanner  ||  tstate->rdirpath  ||  tstate->walker  ||  tstate->gcops  ||  tstate->repackops ) {
         printf( "final walk thread %u has remaining state values\n", tstate->tID );
         return -1;
      }
      // note all thread report values
      reportedstreams   += tstate->streamcount;
      report.fileusage  += tstate->report.fileusage;
      report.byteusage  += tstate->report.byteusage;
      report.filecount  += tstate->report.filecount;
      report.objcount   += tstate->report.objcount;
      report.bytecount  += tstate->report.bytecount;
      report.streamcount+= tstate->report.streamcount;
      report.delobjs    += tstate->report.delobjs;
      report.delfiles   += tstate->report.delfiles;
      report.delstreams += tstate->report.delstreams;
      report.volfiles   += tstate->report.volfiles;
      report.rpckfiles  += tstate->report.rpckfiles;
      report.rpckbytes  += tstate->report.rpckbytes;
      report.rbldobjs   += tstate->report.rbldobjs;
      report.rbldbytes  += tstate->report.rbldbytes;
   }
   if ( retval ) {
      printf( "failed to collect all final walk thread status values\n" );
      return -1;
   }
   // close our the thread queue, logs, etc.
   if ( tq_close( tq ) ) {
      printf( "failed to close final thread queue\n" );
      return -1;
   }
   bzero( &(summary), sizeof( struct operation_summary_struct ) );
   if ( resourcelog_term( &(gstate.rlog), &(summary), NULL ) ) {
      printf( "failed to terminate resource log following final walk\n" );
      return -1;
   }
   if ( repackstreamer_complete( gstate.rpst ) ) {
      printf( "failed to complete repack streamer  following final walk\n" );
      return -1;
   }
   gstate.rpst = NULL;


   // check report totals against expected values
   printf( "final walk delstreams = %zu\n", report.delstreams );
   if ( reportedstreams != totalstreams ) {
      printf( "final walk reported streams (Cnt%zu + Del%zu) does not match expected total (%zu)\n", reportedstreams - origdelstreams, origdelstreams, totalstreams );
      return -1;
   }
   if ( report.filecount + gcfiles != totalfiles ) {
      printf( "final walk reported files (%zu) does not match expected total (%zu)\n", report.filecount, totalfiles );
      return -1;
   }
   if ( report.byteusage  ||  delbytes != totalbytes ) {
      printf( "final walk reported bytes ( %zu used + %zu deleted ) does not match expected total (%zu)\n",
              report.byteusage, delbytes, totalbytes );
      return -1;
   }
   if ( report.bytecount + delbytes < totalbytes ) {
      printf( "final walk reported bytes ( %zu ) does not match/exceed expected total (%zu)\n",
              report.bytecount + delbytes, totalbytes );
      return -1;
   }




   // cleanup our data buffer
   free( databuf );

   // cleanup our position struct
   MDAL posmdal = pos.ns->prepo->metascheme.mdal;
   if ( posmdal->destroyctxt( pos.ctxt ) ) {
      printf( "Failed to destory position MDAL_CTXT\n" );
      return -1;
   }

   // cleanup all created NSs
   if ( deletesubdirs( "./test_rman_topdir/mdal_root/MDAL_subspaces/pack/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of pack NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/pack" ) ) {
      printf( "Failed to destroy /pack NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_rman_topdir/mdal_root/MDAL_subspaces/nopack/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of nopack\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/nopack" ) ) {
      printf( "Failed to destroy /nopack NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/pack-ghost" ) ) {
      printf( "Failed to destroy /pack-ghost NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_rman_topdir/mdal_root/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of rootNS\n" );
      return -1;
   }
   rootmdal->destroynamespace( rootmdal->ctxt, "/." ); // TODO : fix MDAL edge case?

   // cleanup out config struct
   if ( config_term( config ) ) {
      printf( "Failed to destory our config reference\n" );
      return -1;
   }

   // cleanup DAL trees
   if ( deletesubdirs( "./test_rman_topdir/dal_root" ) ) {
      printf( "Failed to delete subdirs of DAL root\n" );
      return -1;
   }

   // delete dal/mdal dir structure
   rmdir( "./test_rman_topdir/dal_root" );
   rmdir( "./test_rman_topdir/mdal_root" );
   rmdir( "./test_rman_topdir" );

   return 0;
}


