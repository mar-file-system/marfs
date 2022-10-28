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
   char zerobuf[10240] = {0}; // all zero 10KiB buffer

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
      if ( datastream_close( &(stream) ) ) {
         printf( "close failure of read stream after file \"%s\"\n", filepath );
         return -1;
      }
   }

   // get a post-delete time
   struct timeval currenttime;
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk\n" );
      return -1;
   }
   thresholds thresh = {
      .gcthreshold = currenttime.tv_sec + 1, // +1, to ensure we actually get *all* deleted files
      .repackthreshold = 0, // no repacks for now
      .rebuildthreshold = 0, // no rebuilds for now
      .cleanupthreshold = currenttime.tv_sec + 1
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
      if ( datastream_close( &(stream) ) ) {
         printf( "close failure of read stream after file \"%s\"\n", filepath );
         return -1;
      }
   }


   // rewalk, cleaning up new objs/files
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for second walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 1; // +1, to ensure we actually get *all* deleted files
   // no repacks for now
   // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 1;

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
   reportedstreams = report.delstreams; // include any previously deleted streams
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


   return 0;

   // create a new stream
   DATASTREAM stream = NULL;
   if ( datastream_create( &(stream), "file1", &(pos), 0744, "NO-PACK-CLIENT" ) ) {
      printf( "create failure for 'file1' of no-pack\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 1024 ) != 1024 ) {
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
      LOG( LOG_ERR, "Failed to identify the rpath of no-pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   char* objname;
   ne_erasure objerasure;
   ne_location objlocation;
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname), &(objerasure), &(objlocation) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of no-pack 'file1' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a new file off the same stream
   if ( datastream_create( &(stream), "file2", &(pos), 0622, "NO-PACK-CLIENT" ) ) {
      printf( "create failure for 'file2' of no-pack\n" );
      return -1;
   }
   if ( datastream_setrecoverypath( &(stream), "file2-recovset" ) ) {
      printf( "failed to set recovery path for 'file2' of no-pack\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 100 ) != 100 ) {
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
      LOG( LOG_ERR, "Failed to identify the rpath of no-pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   char* objname2 = NULL;
   ne_erasure objerasure2;
   ne_location objlocation2;
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of no-pack 'file2' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a chunked file off the same stream
   if ( datastream_create( &(stream), "file3", &(pos), 0600, "NO-PACK-CLIENT" ) ) {
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
   if ( datastream_write( &(stream), databuf, 1024 * 1024 * 2 ) != (1024 * 1024 * 2) ) {
      printf( "write failure for 'file3' of no-pack\n" );
      return -1;
   }
   if ( stream->objno != 22 ) {
      printf( "unexpected objno after write of 'file3' in no-pack: %zu\n", stream->objno );
      return -1;
   }

   // keep track of this file's rpath
   char* rpath3 = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath3 == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data objects
   char* objname3 = NULL;
   ne_erasure objerasure3;
   ne_location objlocation3;
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname3), &(objerasure3), &(objlocation3) ) ) {
      LOG( LOG_ERR, "Failed to identify data object 1 of no-pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   char* objname4 = NULL;
   ne_erasure objerasure4;
   ne_location objlocation4;
   FTAG tmptag = stream->files->ftag;
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


   // walk the produced datastream
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   streamwalker walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker for \"%s\"\n", rpath );
      return -1;
   }
   opinfo* gcops = NULL;
   opinfo* repackops = NULL;
   opinfo* rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected result of first iteration from \"%s\"\n", rpath );
      return -1;
   }
   // not expecting any ops, at present
   if ( gcops ) {
      printf( "unexpected GCOPS following first traversal of no-pack stream\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following first traversal of no-pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following first traversal of no-pack stream\n" );
      return -1;
   }
   streamwalker_report walkreport;
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close first streamwalker\n" );
      return -1;
   }

   // validate count values
   if ( walkreport.fileusage != 3 ) {
      printf( "improper first walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage != 2098276 ) {
      printf( "improper first walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }
   if ( walkreport.filecount != 3 ) {
      printf( "improper first walk filecount: %zu\n", walkreport.filecount );
      return -1;
   }
   if ( walkreport.objcount != 23 ) {
      printf( "improper first walk objcount: %zu\n", walkreport.objcount );
      return -1;
   }
   if ( walkreport.delobjs ) {
      printf( "improper first walk delobjs: %zu\n", walkreport.delobjs );
      return -1;
   }
   if ( walkreport.delfiles ) {
      printf( "improper first walk delfiles: %zu\n", walkreport.delfiles );
      return -1;
   }
   if ( walkreport.delstreams ) {
      printf( "improper first walk delstreams: %zu\n", walkreport.delstreams );
      return -1;
   }
   if ( walkreport.volfiles ) {
      printf( "improper first walk volfiles: %zu\n", walkreport.volfiles );
      return -1;
   }
   if ( walkreport.rpckfiles ) {
      printf( "improper first walk rpckfiles: %zu\n", walkreport.rpckfiles );
      return -1;
   }
   if ( walkreport.rpckbytes ) {
      printf( "improper first walk rpckbytes: %zu\n", walkreport.rpckbytes );
      return -1;
   }
   if ( walkreport.rbldobjs ) {
      printf( "improper first walk rbldobjs: %zu\n", walkreport.rbldobjs );
      return -1;
   }
   if ( walkreport.rbldbytes ) {
      printf( "improper first walk rbldbytes: %zu\n", walkreport.rbldbytes );
      return -1;
   }


   // perform a minimal-impact, quota-only walk
   thresh.gcthreshold = 0;
   thresh.repackthreshold = 0;
   thresh.rebuildthreshold = 0;
   thresh.cleanupthreshold = 0;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker3 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected result of first quota-only iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have no ops at all
   if ( gcops ) {
      printf( "unexpcted gcops following first quota-only iteration of no-pack stream\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following traversal3 of no-pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following traversal3 of no-pack stream\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close walker after second gc iteration\n" );
      return -1;
   }
   // validate count values
   if ( walkreport.fileusage != 3 ) {
      printf( "improper first walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage != 2098276 ) {
      printf( "improper first walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }



   // start up a resourcelog
   RESOURCELOG logfile = NULL;
   if ( resourcelog_init( &(logfile), "./test_rman_topdir/logfile", RESOURCE_MODIFY_LOG, pos.ns ) ) {
      printf( "failed to initialize resourcelog\n" );
      return -1;
   }

   // delete file2
   if ( curmdal->unlink( pos.ctxt, "file2" ) ) {
      printf( "failed to delete file2 of nopack\n" );
      return -1;
   }

   // walk the stream again
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker2 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) < 1 ) {
      printf( "unexpected result of second iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops == NULL ) {
      printf( "no gcops following deletion of file2 in nopack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following first traversal of no-pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following first traversal of no-pack stream\n" );
      return -1;
   }
   // log gcops
   if ( resourcelog_processop( &(logfile), gcops, NULL ) ) {
      printf( "failed to log GCops for nopack1\n" );
      return -1;
   }
   // process the gcops
   // TODO repackstreamer
   if ( process_executeoperation( &(pos), gcops, &(logfile), NULL ) ) {
      printf( "failed to process first gc op of nopack\n" );
      return -1;
   }
   // continue iteration, which should produce no further ops
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected walker iteration after first gc from nopack\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, NULL ) ) {
      printf( "failed to close walker after second gc iteration\n" );
      return -1;
   }

   // read back the written files
   // file1
   if ( datastream_open( &(stream), READ_STREAM, "file1", &(pos), NULL ) ) {
      printf( "failed to open 'file1' of no-pack for read\n" );
      return -1;
   }
   ssize_t iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != 1024 ) {
      printf( "unexpected read res for 'file1' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, 1024 ) ) {
      printf( "unexpected content of 'file1' of no-pack\n" );
      return -1;
   }
   // file3
   if ( datastream_open( &(stream), READ_STREAM, "file3", &(pos), NULL ) ) {
      printf( "failed to open 'file3' of no-pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != 1048576 ) {
      printf( "unexpected res for read1 from 'file3' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, iores ) ) {
      printf( "unexpected content of read1 for 'file3' of no-pack\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != 1048576 ) {
      printf( "unexpected res for read2 from 'file3' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, iores ) ) {
      printf( "unexpected content of read2 from 'file3' of no-pack\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores ) {
      printf( "unexpected res for read3 from 'file3' of no-pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( datastream_close( &(stream) ) ) {
      printf( "failed to close no-pack read stream\n" );
      return -1;
   }

   // validate absence of file2 ref and object
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath2 ) == 0  ||  errno != ENOENT ) {
      printf( "Success of unlink in nopack after gc of \"%s\"\n", rpath2 );
      return -1;
   }
   free( rpath2 );
//      errno = 0;
//      if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname2, objlocation2 ) == 0  ||  errno != ENOENT ) {
//         printf( "Success of delete of data object: \"%s\"\n", objname2 );
//         return -1;
//      }
   free( objname2 );


   // delete file3
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file3" ) ) {
      printf( "Failed to unlink \"file3\"\n" );
      return -1;
   }


   // rewalk, and delete ref/objs for file3
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for third walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker3 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) < 1 ) {
      printf( "unexpected result of third iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops == NULL ) {
      printf( "no gcops following deletion of file3 in nopack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following traversal3 of no-pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following traversal3 of no-pack stream\n" );
      return -1;
   }
   // continue iteration, which should produce no further ops
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected walker iteration after second gc from nopack\n" );
      return -1;
   }
   // log gcops
   if ( resourcelog_processop( &(logfile), gcops, NULL ) ) {
      printf( "failed to log GCops for nopack2\n" );
      return -1;
   }
   // process the gcops
   // TODO repackstreamer
   if ( process_executeoperation( &(pos), gcops, &(logfile), NULL ) ) {
      printf( "failed to process second gc op of nopack\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, NULL ) ) {
      printf( "failed to close walker after second gc iteration\n" );
      return -1;
   }


   // verify absence of file3 components
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath3 ) == 0  ||  errno != ENOENT ) {
      printf( "DID unlink rpath: \"%s\"\n", rpath3 );
      return -1;
   }
   free( rpath3 );
//      errno = 0;
//      if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname3, objlocation3 ) == 0  ||  errno != ENOENT ) {
//         printf( "DID delete data object: \"%s\"\n", objname3 );
//         return -1;
//      }
   free( objname3 );
//      errno = 0;
//      if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname4, objlocation4 ) == 0  ||  errno != ENOENT ) {
//         printf( "DID delete data object: \"%s\"\n", objname4 );
//         return -1;
//      }
   free( objname4 );
//      errno = 0;
//      if ( ne_delete( pos.ns->prepo->datascheme.nectxt, objname5, objlocation5 ) == 0  ||  errno != ENOENT ) {
//         printf( "DID delete data object: \"%s\"\n", objname5 );
//         return -1;
//      }
   free( objname5 );

   // delete and cleanup file1
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file1" ) ) {
      printf( "Failed to unlink \"file1\"\n" );
      return -1;
   }
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for third walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker3 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) < 1 ) {
      printf( "unexpected result of third iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops == NULL ) {
      printf( "no gcops following deletion of file1 in nopack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following traversal3 of no-pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following traversal3 of no-pack stream\n" );
      return -1;
   }
   // continue iteration, which should produce no further ops
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected walker iteration after third gc from nopack\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, NULL ) ) {
      printf( "failed to close walker after second gc iteration\n" );
      return -1;
   }
   // log gcops
   if ( resourcelog_processop( &(logfile), gcops, NULL ) ) {
      printf( "failed to log GCops for nopack3\n" );
      return -1;
   }
   // process the gcops
   // TODO repackstreamer
   if ( process_executeoperation( &(pos), gcops, &(logfile), NULL ) ) {
      printf( "failed to process third gc op of nopack\n" );
      return -1;
   }


   // verify absence of 'file1' refs
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath ) == 0  ||  errno != ENOENT ) {
      printf( "DID unlink rpath: \"%s\"\n", rpath );
      return -1;
   }
   free( rpath );
   free( objname );


   // shift to a new NS, which has packing enabled
   configtgt = strdup( "../pack/nothin" );
   if ( configtgt == NULL ) {
      printf( "Failed to duplicate configtgt string\n" );
      return -1;
   }
   if ( config_traverse( config, &(pos), &(configtgt), 0 ) != 1 ) {
      printf( "failed to traverse config subpath: \"%s\"\n", configtgt );
      return -1;
   }
   free( configtgt );
   if ( pos.ctxt == NULL ) { printf( "ded ctxt\n" ); }


// PACKED OBJECT TESTING


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
   if ( datastream_write( &(stream), databuf, 10 ) != 10 ) {
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
   if ( datastream_write( &(stream), databuf + 10, 100 ) != 100 ) {
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
   if ( datastream_write( &(stream), databuf, 1024 * 4 ) != (1024 * 4) ) {
      printf( "write failure for 'file3' of pack\n" );
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
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != (1024 * 2) ) {
      printf( "unexpected read res for 'file1' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, (1024 * 2) ) ) {
      printf( "unexpected content of 'file1' of pack\n" );
      return -1;
   }
   // file2
   if ( datastream_open( &(stream), READ_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 'file2' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != 1024 ) {
      printf( "unexpected read res for 'file2' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, 110 ) ) {
      printf( "unexpected content ( check 1 ) of 'file2' of pack\n" );
      return -1;
   }
   if ( memcmp( readarray + 110, zerobuf, iores - 110 ) ) {
      printf( "unexpected content ( check 2 ) of 'file2' of pack\n" );
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
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != (1024 * 3) ) {
      printf( "unexpected read res for 'file3' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, iores ) ) {
      printf( "unexpected content of 'file3' of pack\n" );
      return -1;
   }
   if ( datastream_close( &(stream) ) ) {
      printf( "failed to close pack read stream2\n" );
      return -1;
   }


   // delete file1
   if ( curmdal->unlink( pos.ctxt, "file1" ) ) {
      printf( "failed to delete file1 of nopack\n" );
      return -1;
   }

   // do a quick walk of the stream, verifying expected values
   thresh.gcthreshold = 0;
   thresh.repackthreshold = 0;
   thresh.rebuildthreshold = 0;
   thresh.cleanupthreshold = 0;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open quick-streamwalker1 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected result of first quota-only iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have no ops at all
   if ( gcops ) {
      printf( "unexpcted gcops following first quota-only iteration of pack stream\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following first quota-only iteration of pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following first quota-only iteration of pack stream\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close walker after first quick iteration of pack\n" );
      return -1;
   }
   // validate count values
   if ( walkreport.fileusage != 2 ) {
      printf( "improper first pack walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage != 4096 ) {
      printf( "improper first pack walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }

   // walk the stream again, actually executing the GC
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for first walk of pack\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker2 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected result of second iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops ) {
      printf( "no gcops following deletion of file1 in pack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following first gc traversal of pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following first gc traversal of pack stream\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close walker after second gc iteration\n" );
      return -1;
   }
   // validate count values
   if ( walkreport.fileusage != 2 ) {
      printf( "improper second pack walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage != 4206 ) {
      printf( "improper second pack walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }
   if ( walkreport.filecount != 3 ) {
      printf( "improper second pack walk filecount: %zu\n", walkreport.filecount );
      return -1;
   }
   if ( walkreport.objcount != 2 ) {
      printf( "improper second pack walk objcount: %zu\n", walkreport.objcount );
      return -1;
   }
   if ( walkreport.delobjs ) {
      printf( "improper second pack walk delobjs: %zu\n", walkreport.delobjs );
      return -1;
   }
   if ( walkreport.delfiles ) {
      printf( "improper second pack walk delfiles: %zu\n", walkreport.delfiles );
      return -1;
   }
   if ( walkreport.delstreams ) {
      printf( "improper second pack walk delstreams: %zu\n", walkreport.delstreams );
      return -1;
   }
   if ( walkreport.volfiles ) {
      printf( "improper second pack walk volfiles: %zu\n", walkreport.volfiles );
      return -1;
   }
   if ( walkreport.rpckfiles ) {
      printf( "improper second pack walk rpckfiles: %zu\n", walkreport.rpckfiles );
      return -1;
   }
   if ( walkreport.rpckbytes ) {
      printf( "improper second pack walk rpckbytes: %zu\n", walkreport.rpckbytes );
      return -1;
   }
   if ( walkreport.rbldobjs ) {
      printf( "improper second pack walk rbldobjs: %zu\n", walkreport.rbldobjs );
      return -1;
   }
   if ( walkreport.rbldbytes ) {
      printf( "improper second pack walk rbldbytes: %zu\n", walkreport.rbldbytes );
      return -1;
   }


   // read back the written PACK files
   // file2
   if ( datastream_open( &(stream), READ_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 'file2' of pack for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != 1024 ) {
      printf( "unexpected read res for 'file2' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, 110 ) ) {
      printf( "unexpected content ( check 1 ) of 'file2' of pack\n" );
      return -1;
   }
   if ( memcmp( readarray + 110, zerobuf, iores - 110 ) ) {
      printf( "unexpected content ( check 2 ) of 'file2' of pack\n" );
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
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != (1024 * 3) ) {
      printf( "unexpected read res for 'file3' of pack: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, iores ) ) {
      printf( "unexpected content of 'file3' of pack\n" );
      return -1;
   }
   if ( datastream_close( &(stream) ) ) {
      printf( "failed to close pack read stream2\n" );
      return -1;
   }


   // delete and cleanup file2 and 3
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file2" ) ) {
      printf( "Failed to unlink \"file2\"\n" );
      return -1;
   }
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file3" ) ) {
      printf( "Failed to unlink \"file3\"\n" );
      return -1;
   }
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for third walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker3 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) < 1 ) {
      printf( "unexpected result of third iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops == NULL ) {
      printf( "no gcops following deletion of file2 and 3 in pack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following traversal3 of pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following traversal3 of pack stream\n" );
      return -1;
   }
   // continue iteration, which should produce no further ops
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected walker iteration after third gc from pack\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close walker after third pack gc iteration\n" );
      return -1;
   }
   // validate count values
   if ( walkreport.fileusage ) {
      printf( "improper third pack walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage ) {
      printf( "improper third pack walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }
   if ( walkreport.filecount != 3 ) {
      printf( "improper third pack walk filecount: %zu\n", walkreport.filecount );
      return -1;
   }
   if ( walkreport.objcount != 2 ) {
      printf( "improper third pack walk objcount: %zu\n", walkreport.objcount );
      return -1;
   }
   if ( walkreport.delobjs != 2 ) {
      printf( "improper third pack walk delobjs: %zu\n", walkreport.delobjs );
      return -1;
   }
   if ( walkreport.delfiles != 2 ) {
      printf( "improper third pack walk delfiles: %zu\n", walkreport.delfiles );
      return -1;
   }
   if ( walkreport.delstreams ) {
      printf( "improper third pack walk delstreams: %zu\n", walkreport.delstreams );
      return -1;
   }
   if ( walkreport.volfiles ) {
      printf( "improper third pack walk volfiles: %zu\n", walkreport.volfiles );
      return -1;
   }
   if ( walkreport.rpckfiles ) {
      printf( "improper third pack walk rpckfiles: %zu\n", walkreport.rpckfiles );
      return -1;
   }
   if ( walkreport.rpckbytes ) {
      printf( "improper third pack walk rpckbytes: %zu\n", walkreport.rpckbytes );
      return -1;
   }
   if ( walkreport.rbldobjs ) {
      printf( "improper third pack walk rbldobjs: %zu\n", walkreport.rbldobjs );
      return -1;
   }
   if ( walkreport.rbldbytes ) {
      printf( "improper third pack walk rbldbytes: %zu\n", walkreport.rbldbytes );
      return -1;
   }
   // log gcops
   if ( resourcelog_processop( &(logfile), gcops, NULL ) ) {
      printf( "failed to log GCops for pack3\n" );
      return -1;
   }
   // process the gcops
   // TODO repackstreamer
   if ( process_executeoperation( &(pos), gcops, &(logfile), NULL ) ) {
      printf( "failed to process third gc op of pack\n" );
      return -1;
   }

   // verify absence of 'file2' refs
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath2 ) == 0  ||  errno != ENOENT ) {
      printf( "DID unlink rpath: \"%s\"\n", rpath2 );
      return -1;
   }
   free( rpath2 );
   // verify absence 'file3' refs
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath3 ) == 0  ||  errno != ENOENT ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath3 );
      return -1;
   }
   free( rpath3 );
   free( objname2 );


   // walk a final time, to delete the entire stream
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker4 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) < 1 ) {
      printf( "unexpected result of fourth iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops == NULL ) {
      printf( "no gcops for final walk of pack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following traversal4 of pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following traversal4 of pack stream\n" );
      return -1;
   }
   // continue iteration, which should produce no further ops
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected walker iteration after fourth gc from pack\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close walker after fourth pack gc iteration\n" );
      return -1;
   }
   // validate count values
   if ( walkreport.fileusage ) {
      printf( "improper fourth pack walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage ) {
      printf( "improper fourth pack walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }
   if ( walkreport.filecount != 1 ) {
      printf( "improper fourth pack walk filecount: %zu\n", walkreport.filecount );
      return -1;
   }
   if ( walkreport.objcount ) {
      printf( "improper fourth pack walk objcount: %zu\n", walkreport.objcount );
      return -1;
   }
   if ( walkreport.delobjs ) {
      printf( "improper fourth pack walk delobjs: %zu\n", walkreport.delobjs );
      return -1;
   }
   if ( walkreport.delfiles != 1 ) {
      printf( "improper fourth pack walk delfiles: %zu\n", walkreport.delfiles );
      return -1;
   }
   if ( walkreport.delstreams != 1 ) {
      printf( "improper fourth pack walk delstreams: %zu\n", walkreport.delstreams );
      return -1;
   }
   if ( walkreport.volfiles ) {
      printf( "improper fourth pack walk volfiles: %zu\n", walkreport.volfiles );
      return -1;
   }
   if ( walkreport.rpckfiles ) {
      printf( "improper fourth pack walk rpckfiles: %zu\n", walkreport.rpckfiles );
      return -1;
   }
   if ( walkreport.rpckbytes ) {
      printf( "improper fourth pack walk rpckbytes: %zu\n", walkreport.rpckbytes );
      return -1;
   }
   if ( walkreport.rbldobjs ) {
      printf( "improper fourth pack walk rbldobjs: %zu\n", walkreport.rbldobjs );
      return -1;
   }
   if ( walkreport.rbldbytes ) {
      printf( "improper fourth pack walk rbldbytes: %zu\n", walkreport.rbldbytes );
      return -1;
   }
   // log gcops
   if ( resourcelog_processop( &(logfile), gcops, NULL ) ) {
      printf( "failed to log GCops for pack4\n" );
      return -1;
   }
   // process the gcops
   // TODO repackstreamer
   if ( process_executeoperation( &(pos), gcops, &(logfile), NULL ) ) {
      printf( "failed to process fourth gc op of pack\n" );
      return -1;
   }

   // verify absence of 'file1' refs
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath ) == 0  ||  errno != ENOENT ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath );
      return -1;
   }
   free( rpath );
   free( objname );



// PARALLEL WRITE TEST
   // create a new stream
   if ( datastream_create( &(stream), "file1", &(pos), 0700, "PARLLEL-CLIENT" ) ) {
      printf( "create failure for 'file1' of pwrite\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf, 34 ) != 34 ) {
      printf( "write1 failure for 'file1' of pwrite\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf + 34, 1000 ) != 1000 ) {
      printf( "write2 failure for 'file1' of pwrite\n" );
      return -1;
   }
   if ( datastream_write( &(stream), databuf + 1034, 200 ) != 200 ) {
      printf( "write3 failure for 'file1' of pwrite\n" );
      return -1;
   }

   // keep track of this file's rpath
   rpath = datastream_genrpath( &(stream->files->ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of pwrite 'file1' (%s)\n", strerror(errno) );
      return -1;
   }
   // ...and the data object
   if ( datastream_objtarget( &(stream->files->ftag), &(stream->ns->prepo->datascheme), &(objname), &(objerasure), &(objlocation) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of pwrite 'file1' (%s)\n", strerror(errno) );
      return -1;
   }

   // create a new file off of the same stream
   if ( datastream_create( &(stream), "file2", &(pos), 0666, NULL ) ) {
      printf( "create failure for 'file2' of pwrite\n" );
      return -1;
   }
   if ( stream->curfile != 1 ) {
      printf( "unexpected curfile following creation of 'file2' of pwrite: %zu\n", stream->curfile );
      return -1;
   }
   if ( stream->objno ) {
      printf( "unexpected objno following creation of 'file2' of pwrite: %zu\n", stream->objno );
      return -1;
   }

   // keep track of this file's rpath
   rpath2 = datastream_genrpath( &(stream->files[1].ftag), stream->ns->prepo->metascheme.reftable );
   if ( rpath2 == NULL ) {
      LOG( LOG_ERR, "Failed to identify the rpath of pwrite 'file2' (%s)\n", strerror(errno) );
      return -1;
   }

   // extend the file, making it available for parallel write
   if ( datastream_extend( &(stream), 5 * 1024 ) ) {
      printf( "extend failure for 'file2' of pwrite\n" );
      return -1;
   }
   if ( stream->curfile ) {
      printf( "unexpected curfile following extension of 'file2' of pwrite: %zu\n", stream->curfile );
      return -1;
   }
   if ( stream->objno != 1 ) {
      printf( "unexpected objno following extension of 'file2' of pwrite: %zu\n", stream->objno );
      return -1;
   }

   // open a second stream
   DATASTREAM pstream = NULL;
   if ( datastream_open( &(pstream), EDIT_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 1st edit stream for 'file2' of pwrite\n" );
      return -1;
   }
   off_t  chunkoffset = 0;
   size_t chunksize = 0;
   if ( datastream_chunkbounds( &(pstream), 0, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to identify bounds of chunk 0 for 1st edit stream of 'file2' of pwrite\n" );
      return -1;
   }
   if ( chunkoffset  ||  chunksize != 4096 - (stream->recoveryheaderlen + stream->files->ftag.recoverybytes) ) {
      printf( "unexpected bounds of chunk 0 for 1st edit stream of 'file2' of pwrite: o=%zd, s=%zu\n", chunkoffset, chunksize );
      return -1;
   }

   // keep track of this data object and the next
   tgttag = pstream->files->ftag;
   if ( datastream_objtarget( &(tgttag), &(stream->ns->prepo->datascheme), &(objname2), &(objerasure2), &(objlocation2) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }
   tgttag.objno++;
   if ( datastream_objtarget( &(tgttag), &(stream->ns->prepo->datascheme), &(objname3), &(objerasure3), &(objlocation3) ) ) {
      LOG( LOG_ERR, "Failed to identify data object of pack 'file3' (%s)\n", strerror(errno) );
      return -1;
   }

   // write to chunk 0 of the file
   if ( datastream_write( &(pstream), databuf, chunksize ) != chunksize ) {
      printf( "paralell write failure for chunk 0 of 'file2'\n" );
      return -1;
   }
   if ( datastream_release( &(pstream) ) ) {
      printf( "release failure for 1st edit stream for 'file2' of pwrite\n" );
      return -1;
   }

   // release the original stream
   if ( datastream_release( &(stream) ) ) {
      printf( "release failure for create stream of 'file2' of pwrite\n" );
      return -1;
   }

   // reopen an edit stream to write out the final data obj and complete the file
   if ( datastream_open( &(pstream), EDIT_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 1st edit stream for 'file2' of pwrite\n" );
      return -1;
   }
   if ( datastream_chunkbounds( &(pstream), 1, &(chunkoffset), &(chunksize) ) ) {
      printf( "failed to identify bounds of chunk 0 for 1st edit stream of 'file2' of pwrite\n" );
      return -1;
   }
   if ( chunkoffset != 4096 - (pstream->recoveryheaderlen + pstream->files->ftag.recoverybytes)  ||  chunksize != ( (5 * 1024) - chunkoffset ) ) {
      printf( "unexpected bounds of chunk 1 for 2nd edit stream of 'file2' of pwrite: o=%zd, s=%zu\n", chunkoffset, chunksize );
      return -1;
   }
   if ( datastream_seek( &(pstream), chunkoffset, SEEK_SET ) != chunkoffset ) {
      printf( "failed to seek to offset %zu of 2nd edit stream for 'file2' of pwrite\n", chunkoffset );
      return -1;
   }
   if ( datastream_write( &(pstream), databuf + chunkoffset, chunksize ) != chunksize ) {
      printf( "paralell write failure for chunk 1 of 'file2'\n" );
      return -1;
   }
   if ( datastream_close( &(pstream) ) ) {
      printf( "release failure for 2nd edit stream for 'file2' of pwrite\n" );
      return -1;
   }


   // read back the written pwrite files
   // file1
   if ( datastream_open( &(stream), READ_STREAM, "file1", &(pos), NULL ) ) {
      printf( "failed to open 'file1' of pwrite for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 12345 );
   if ( iores != 1234 ) {
      printf( "unexpected read res for 'file1' of pwrite: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, 1234 ) ) {
      printf( "unexpected content of 'file1' of pwrite\n" );
      return -1;
   }
   // file2
   if ( datastream_open( &(stream), READ_STREAM, "file2", &(pos), NULL ) ) {
      printf( "failed to open 'file2' of pwrite for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 1048576 );
   if ( iores != 1024 * 5 ) {
      printf( "unexpected read res for 'file2' of pwrite: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, iores ) ) {
      printf( "unexpected content of 'file2' of pwrite\n" );
      return -1;
   }
   if ( datastream_release( &(stream) ) ) {
      printf( "failed to close pwrite read stream1\n" );
      return -1;
   }

   // delete file2
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file2" ) ) {
      printf( "Failed to unlink pwrite \"file2\"\n" );
      return -1;
   }

   // walk the stream and process deletions
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for third walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker1 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) < 1 ) {
      printf( "unexpected result of first iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops == NULL ) {
      printf( "no gcops for final walk of pack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following traversal4 of pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following traversal4 of pack stream\n" );
      return -1;
   }
   // continue iteration, which should produce no further ops
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected walker iteration after first gc from parallel-write\n" );
      return -1;
   }
   // log gcops
   if ( resourcelog_processop( &(logfile), gcops, NULL ) ) {
      printf( "failed to log GCops for parallel1\n" );
      return -1;
   }
   // process the gcops
   // TODO reparallel-writestreamer
   if ( process_executeoperation( &(pos), gcops, &(logfile), NULL ) ) {
      printf( "failed to process first gc op of parallel-write\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close walker after first parallel-write gc iteration\n" );
      return -1;
   }
   // validate count values
   if ( walkreport.fileusage != 1 ) {
      printf( "improper first parallel-write walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage != 1234 ) {
      printf( "improper first parallel-write walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }
   if ( walkreport.filecount != 2 ) {
      printf( "improper first parallel-write walk filecount: %zu\n", walkreport.filecount );
      return -1;
   }
   if ( walkreport.objcount != 3 ) {
      printf( "improper first parallel-write walk objcount: %zu\n", walkreport.objcount );
      return -1;
   }
   if ( walkreport.delobjs != 2 ) {
      printf( "improper first parallel-write walk delobjs: %zu\n", walkreport.delobjs );
      return -1;
   }
   if ( walkreport.delfiles != 1 ) {
      printf( "improper first parallel-write walk delfiles: %zu\n", walkreport.delfiles );
      return -1;
   }
   if ( walkreport.delstreams ) {
      printf( "improper first parallel-write walk delstreams: %zu\n", walkreport.delstreams );
      return -1;
   }
   if ( walkreport.volfiles ) {
      printf( "improper first parallel-write walk volfiles: %zu\n", walkreport.volfiles );
      return -1;
   }
   if ( walkreport.rpckfiles ) {
      printf( "improper first parallel-write walk rpckfiles: %zu\n", walkreport.rpckfiles );
      return -1;
   }
   if ( walkreport.rpckbytes ) {
      printf( "improper first parallel-write walk rpckbytes: %zu\n", walkreport.rpckbytes );
      return -1;
   }
   if ( walkreport.rbldobjs ) {
      printf( "improper first parallel-write walk rbldobjs: %zu\n", walkreport.rbldobjs );
      return -1;
   }
   if ( walkreport.rbldbytes ) {
      printf( "improper first parallel-write walk rbldbytes: %zu\n", walkreport.rbldbytes );
      return -1;
   }

   // verify absence of 'file2' refs
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath2 ) == 0  ||  errno != ENOENT ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath2 );
      return -1;
   }
   free( rpath2 );
   free( objname2 );
   free( objname3 );


   // reread file1
   if ( datastream_open( &(stream), READ_STREAM, "file1", &(pos), NULL ) ) {
      printf( "failed to open 'file1' of pwrite for read\n" );
      return -1;
   }
   iores = datastream_read( &(stream), &(readarray), 12345 );
   if ( iores != 1234 ) {
      printf( "unexpected read res for 'file1' of pwrite: %zd (%s)\n", iores, strerror(errno) );
      return -1;
   }
   if ( memcmp( readarray, databuf, 1234 ) ) {
      printf( "unexpected content of 'file1' of pwrite\n" );
      return -1;
   }
   if ( datastream_release( &(stream) ) ) {
      printf( "failed to close pwrite read stream1\n" );
      return -1;
   }


   // delete file1
   if ( pos.ns->prepo->metascheme.mdal->unlink( pos.ctxt, "file1" ) ) {
      printf( "Failed to unlink pwrite \"file1\"\n" );
      return -1;
   }

   // walk the stream and process deletions
   if ( gettimeofday( &currenttime, NULL ) ) {
      printf( "failed to get current time for final walk\n" );
      return -1;
   }
   thresh.gcthreshold = currenttime.tv_sec + 120;
   thresh.repackthreshold = currenttime.tv_sec + 120;
   thresh.rebuildthreshold = 0; // no rebuilds for now
   thresh.cleanupthreshold = currenttime.tv_sec + 120;
   walker = process_openstreamwalker( &pos, rpath, thresh, NULL );
   if ( walker == NULL ) {
      printf( "failed to open streamwalker1 for \"%s\"\n", rpath );
      return -1;
   }
   gcops = NULL;
   repackops = NULL;
   rebuildops = NULL;
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) < 1 ) {
      printf( "unexpected result of final iteration from \"%s\"\n", rpath );
      return -1;
   }
   // we should have some gcops, and nothing else
   if ( gcops == NULL ) {
      printf( "no gcops for final walk of pack\n" );
      return -1;
   }
   if ( repackops ) {
      printf( "unexpected REPACKOPS following traversal4 of pack stream\n" );
      return -1;
   }
   if ( rebuildops ) {
      printf( "unexpected REBUILDOPS following traversal4 of pack stream\n" );
      return -1;
   }
   // continue iteration, which should produce no further ops
   if ( process_iteratestreamwalker( walker, &(gcops), &(repackops), &(rebuildops) ) ) {
      printf( "unexpected walker iteration after final gc from parallel-write\n" );
      return -1;
   }
   // log gcops
   if ( resourcelog_processop( &(logfile), gcops, NULL ) ) {
      printf( "failed to log GCops for parallel final\n" );
      return -1;
   }
   // process the gcops
   // TODO reparallel-writestreamer
   if ( process_executeoperation( &(pos), gcops, &(logfile), NULL ) ) {
      printf( "failed to process final gc op of parallel-write\n" );
      return -1;
   }
   if ( process_closestreamwalker( walker, &(walkreport) ) ) {
      printf( "failed to close walker after final parallel-write gc iteration\n" );
      return -1;
   }
   // validate count values
   if ( walkreport.fileusage ) {
      printf( "improper final parallel-write walk fileusage: %zu\n", walkreport.fileusage );
      return -1;
   }
   if ( walkreport.byteusage ) {
      printf( "improper final parallel-write walk byteusage: %zu\n", walkreport.byteusage );
      return -1;
   }
   if ( walkreport.filecount != 1 ) {
      printf( "improper final parallel-write walk filecount: %zu\n", walkreport.filecount );
      return -1;
   }
   if ( walkreport.objcount != 1 ) {
      printf( "improper final parallel-write walk objcount: %zu\n", walkreport.objcount );
      return -1;
   }
   if ( walkreport.delobjs != 1 ) {
      printf( "improper final parallel-write walk delobjs: %zu\n", walkreport.delobjs );
      return -1;
   }
   if ( walkreport.delfiles != 1 ) {
      printf( "improper final parallel-write walk delfiles: %zu\n", walkreport.delfiles );
      return -1;
   }
   if ( walkreport.delstreams != 1 ) {
      printf( "improper final parallel-write walk delstreams: %zu\n", walkreport.delstreams );
      return -1;
   }
   if ( walkreport.volfiles ) {
      printf( "improper final parallel-write walk volfiles: %zu\n", walkreport.volfiles );
      return -1;
   }
   if ( walkreport.rpckfiles ) {
      printf( "improper final parallel-write walk rpckfiles: %zu\n", walkreport.rpckfiles );
      return -1;
   }
   if ( walkreport.rpckbytes ) {
      printf( "improper final parallel-write walk rpckbytes: %zu\n", walkreport.rpckbytes );
      return -1;
   }
   if ( walkreport.rbldobjs ) {
      printf( "improper final parallel-write walk rbldobjs: %zu\n", walkreport.rbldobjs );
      return -1;
   }
   if ( walkreport.rbldbytes ) {
      printf( "improper final parallel-write walk rbldbytes: %zu\n", walkreport.rbldbytes );
      return -1;
   }


   // verify absence of 'file1' refs
   errno = 0;
   if ( pos.ns->prepo->metascheme.mdal->unlinkref( pos.ctxt, rpath ) == 0  ||  errno != ENOENT ) {
      printf( "Failed to unlink rpath: \"%s\"\n", rpath );
      return -1;
   }
   free( rpath );
   free( objname );


   // cleanup our resourcelog
   if ( resourcelog_term( &(logfile), NULL, NULL ) ) {
      printf( "failed to terminate resourcelog\n" );
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


