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
   if ( mkdir( "./test_rman_topdir", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_rman_topdir\"\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_rman_topdir/dal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_rman_topdir/dal_root\"\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_rman_topdir/mdal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_rman_topdir/mdal_root\"\n" );
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


   // generate all parent paths of a new logfile
   char* logpath = resourcelog_genlogpath( 1, "./test_rman_topdir", "test-resourcelog-iteration123456", config->rootns, 0 );
   if ( logpath == NULL ) {
      printf( "failed to generate inital logpath\n" );
      return -1;
   }
   // initaialize that new logfile
   RESOURCELOG rlog = NULL;
   if ( resourcelog_init( &(rlog), logpath, RESOURCE_RECORD_LOG, config->rootns ) ) {
      printf( "failed to initialize first logfile: \"%s\"\n", logpath );
      return -1;
   }

   // create a set of operations
   opinfo* opset = malloc( sizeof( struct opinfo_struct ) * 4 );
   opinfo* opparse = opset;
   // start of an object deletion op
   opparse->type = MARFS_DELETE_OBJ_OP;
   opparse->extendedinfo = calloc( 1, sizeof( struct delobj_info_struct ) );
   if ( opparse->extendedinfo == NULL ) {
      printf( "failed to allocate delobj extended info\n" );
      return -1;
   }
   delobj_info* delobjinf = (delobj_info*)opparse->extendedinfo;
   delobjinf->offset = 3;
   opparse->start = 1;
   opparse->count = 4;
   opparse->errval = 0;
   opparse->ftag.ctag = strdup( "someclient" );
   opparse->ftag.streamid = strdup( "initstreamID123forme" );
   if ( opparse->ftag.ctag == NULL  ||  opparse->ftag.streamid == NULL ) {
      printf( "failed to allocate ctag / streamid for inital operation\n" );
      return -1;
   }
   opparse->ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   opparse->ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   opparse->ftag.objfiles = 4;
   opparse->ftag.objsize = 1024;
   opparse->ftag.refbreadth = 10;
   opparse->ftag.refdepth = 9;
   opparse->ftag.refdigits = 32;
   opparse->ftag.fileno = 0;
   opparse->ftag.objno = 0;
   opparse->ftag.offset = 12;
   opparse->ftag.endofstream = 0;
   opparse->ftag.protection.N = 5;
   opparse->ftag.protection.E = 1;
   opparse->ftag.protection.O = 0;
   opparse->ftag.protection.partsz = 123;
   opparse->ftag.bytes = 4096;
   opparse->ftag.availbytes = 4096;
   opparse->ftag.recoverybytes = 23;
   opparse->ftag.state = FTAG_COMP | FTAG_READABLE;
   opparse->next = opparse + 1;
   opparse++;
   // start of a ref deletion op
   opparse->type = MARFS_DELETE_REF_OP;
   opparse->extendedinfo = malloc( sizeof( struct delref_info_struct ) );
   if ( opparse->extendedinfo == NULL ) {
      printf( "failed to allocate delref extended info\n" );
      return -1;
   }
   delref_info* delrefinf = (delref_info*)(opparse->extendedinfo);
   delrefinf->prev_active_index = 0;
   delrefinf->delzero = 1;
   delrefinf->eos = 0;
   opparse->start = 1;
   opparse->count = 1;
   opparse->errval = 0;
   opparse->ftag = (opparse-1)->ftag; // just inherit the same FTAG
   opparse->next = NULL;
   opparse++;
   // start of a rebuild op
   opparse->type = MARFS_REBUILD_OP;
   opparse->extendedinfo = malloc( sizeof( struct rebuild_info_struct ) );
   if ( opparse->extendedinfo == NULL ) {
      printf( "failed to allocate rebuild extended info\n" );
      return -1;
   }
   rebuild_info* rebuildinf = (rebuild_info*)(opparse->extendedinfo);
   rebuildinf->markerpath = strdup( "nothinmarker" );
   if( rebuildinf->markerpath == NULL ) {
      printf( "failed to allocate rebuild info markerpath\n" );
      return -1;
   }
   rebuildinf->rtag.versz = 0;
   rebuildinf->rtag.blocksz = 0;
   rebuildinf->rtag.totsz = 0;
   rebuildinf->rtag.meta_status = calloc( sizeof(char), 7 );
   rebuildinf->rtag.meta_status[2] = 1;
   rebuildinf->rtag.data_status = calloc( sizeof(char), 7 );
   rebuildinf->rtag.data_status[0] = 1;
   rebuildinf->rtag.data_status[5] = 1;
   rebuildinf->rtag.csum = NULL;
   opparse->start = 1;
   opparse->count = 1;
   opparse->errval = 0;
   opparse->ftag = (opparse-1)->ftag; // just inherit the same FTAG
   opparse->next = NULL;
   opparse++;
   // start of a repack op
   opparse->type = MARFS_REPACK_OP;
   opparse->extendedinfo = malloc( sizeof( struct repack_info_struct ) );
   if ( opparse->extendedinfo == NULL ) {
      printf( "failed to allocate repack extended info\n" );
      return -1;
   }
   repack_info* repackinf = (repack_info*)(opparse->extendedinfo);
   repackinf->totalbytes = 4096;
   opparse->start = 1;
   opparse->count = 1;
   opparse->errval = 0;
   opparse->ftag = (opparse-1)->ftag; // just inherit the same FTAG
   opparse->next = NULL;


   // insert deletion op chain
   char progress = 0;
   if ( resourcelog_processop( &(rlog), opset, &(progress) ) ) {
      printf( "failed to insert initial deletion op chain\n" );
      return -1;
   }
   if ( progress ) {
      printf( "insertion of inital deletion chain incorrectly set 'progress' flag\n" );
      return -1;
   }
   // insert rebuild op
   if ( resourcelog_processop( &(rlog), opset + 2, &(progress) ) ) {
      printf( "failed to insert initial rebuild op\n" );
      return -1;
   }
   if ( progress ) {
      printf( "insertion of inital rebuild op incorrectly set 'progress' flag\n" );
      return -1;
   }
   // insert repack op
   if ( resourcelog_processop( &(rlog), opset + 3, &(progress) ) ) {
      printf( "failed to insert initial repack op\n" );
      return -1;
   }
   if ( progress ) {
      printf( "insertion of inital repack op incorrectly set 'progress' flag\n" );
      return -1;
   }

   // attempt to insert completion of an op ( expected to fail )
   opset->start = 0;
   if ( resourcelog_processop( &(rlog), opset, &(progress) ) == 0 ) {
      printf( "unexpected success for insert of op completion into a RECORD log\n" );
      return -1;
   }
   opset->start = 1; //revert change


   // terminate the log, keeping the copy
   operation_summary summary;
   if ( resourcelog_term( &(rlog), &(summary), logpath ) ) {
      printf( "failed to terminate initial logfile\n" );
      return -1;
   }

   // generate a new logfile path
   char* wlogpath = resourcelog_genlogpath( 1, "./test_rman_topdir", "test-resourcelog-iteration654321", config->rootns, 10 );
   if ( wlogpath == NULL ) {
      printf( "failed to generate write logpath\n" );
      return -1;
   }
   // initaialize that new MODIFY logfile
   RESOURCELOG wlog = NULL;
   if ( resourcelog_init( &(wlog), wlogpath, RESOURCE_MODIFY_LOG, config->rootns ) ) {
      printf( "failed to initialize write logfile: \"%s\"\n", wlogpath );
      return -1;
   }


   // read and validate the content of our original log, adding each op to our MODIFY log
   if ( resourcelog_init( &(rlog), logpath, RESOURCE_READ_LOG, NULL ) ) {
      printf( "failed to initialize first read log\n" );
      return -1;
   }
   opparse = NULL;
   printf( "readingopset1\n" );
   if ( resourcelog_readop( &(rlog), &(opparse) ) ) {
      printf( "failed to read first operation set from first read log\n" );
      return -1;
   }
   opinfo* readops = opparse;
   printf( "valop1\n" );
   delobj_info* delobjparse = (delobj_info*)opparse->extendedinfo;
   if ( opparse->type != opset->type  ||
        delobjparse->offset != delobjinf->offset  ||
        opparse->start != opset->start  ||
        opparse->count != opset->count  ||
        opparse->errval != opset->errval  ||
        opparse->ftag.majorversion != opset->ftag.majorversion  ||
        opparse->ftag.minorversion != opset->ftag.minorversion  ||
        strcmp( opparse->ftag.ctag, opset->ftag.ctag )  ||
        strcmp( opparse->ftag.streamid, opset->ftag.streamid )  ||
        opparse->ftag.objfiles != opset->ftag.objfiles  ||
        opparse->ftag.objsize != opset->ftag.objsize  ||
        opparse->ftag.refbreadth != opset->ftag.refbreadth  ||
        opparse->ftag.refdepth != opset->ftag.refdepth  ||
        opparse->ftag.refdigits != opset->ftag.refdigits  ||
        opparse->ftag.fileno != opset->ftag.fileno  ||
        opparse->ftag.objno != opset->ftag.objno  ||
        opparse->ftag.offset != opset->ftag.offset  ||
        opparse->ftag.endofstream != opset->ftag.endofstream  ||
        opparse->ftag.protection.N != opset->ftag.protection.N  ||
        opparse->ftag.protection.E != opset->ftag.protection.E  ||
        opparse->ftag.protection.O != opset->ftag.protection.O  ||
        opparse->ftag.protection.partsz != opset->ftag.protection.partsz  ||
        opparse->ftag.bytes != opset->ftag.bytes  ||
        opparse->ftag.availbytes != opset->ftag.availbytes  ||
        opparse->ftag.recoverybytes != opset->ftag.recoverybytes  ||
        opparse->ftag.state != opset->ftag.state ) {
      printf( "read op 1 differs from original\n" );
      if ( opparse->type != opset->type ) { printf( "type\n" ); }
      if ( delobjparse->offset != delobjinf->offset ) { printf( "extendedinfo\n" ); }
      if ( opparse->start != opset->start ) { printf( "start\n" ); }
      if ( opparse->count != opset->count ) { printf( "count\n" ); }
      if ( opparse->errval != opset->errval ) { printf( "errval\n" ); }
      return -1;
   }
   opparse = opparse->next;
   printf( "valop2\n" );
   delref_info* delrefparse = (delref_info*)opparse->extendedinfo;
   if ( opparse->type != (opset+1)->type  ||
        delrefparse->prev_active_index != delrefinf->prev_active_index  ||
        delrefparse->delzero != delrefinf->delzero  ||
        delrefparse->eos != delrefinf->eos  ||
        opparse->start != (opset+1)->start  ||
        opparse->count != (opset+1)->count  ||
        opparse->errval != (opset+1)->errval  ||
        opparse->next   != (opset+1)->next  ||
        opparse->ftag.majorversion != (opset+1)->ftag.majorversion  ||
        opparse->ftag.minorversion != (opset+1)->ftag.minorversion  ||
        strcmp( opparse->ftag.ctag, (opset+1)->ftag.ctag )  ||
        strcmp( opparse->ftag.streamid, (opset+1)->ftag.streamid )  ||
        opparse->ftag.objfiles != (opset+1)->ftag.objfiles  ||
        opparse->ftag.objsize != (opset+1)->ftag.objsize  ||
        opparse->ftag.refbreadth != (opset+1)->ftag.refbreadth  ||
        opparse->ftag.refdepth != (opset+1)->ftag.refdepth  ||
        opparse->ftag.refdigits != (opset+1)->ftag.refdigits  ||
        opparse->ftag.fileno != (opset+1)->ftag.fileno  ||
        opparse->ftag.objno != (opset+1)->ftag.objno  ||
        opparse->ftag.offset != (opset+1)->ftag.offset  ||
        opparse->ftag.endofstream != (opset+1)->ftag.endofstream  ||
        opparse->ftag.protection.N != (opset+1)->ftag.protection.N  ||
        opparse->ftag.protection.E != (opset+1)->ftag.protection.E  ||
        opparse->ftag.protection.O != (opset+1)->ftag.protection.O  ||
        opparse->ftag.protection.partsz != (opset+1)->ftag.protection.partsz  ||
        opparse->ftag.bytes != (opset+1)->ftag.bytes  ||
        opparse->ftag.availbytes != (opset+1)->ftag.availbytes  ||
        opparse->ftag.recoverybytes != (opset+1)->ftag.recoverybytes  ||
        opparse->ftag.state != (opset+1)->ftag.state ) {
      printf( "read op 2 differs from original\n" );
      if ( opparse->type != (opset+1)->type ) { printf( "type\n" ); }
      if ( delrefparse->prev_active_index != delrefinf->prev_active_index ) { printf( "prevactive\n" ); }
      if ( delrefparse->delzero != delrefinf->delzero ) { printf( "delzero\n" ); }
      if ( delrefparse->eos != delrefinf->eos ) { printf( "eos\n" ); }
      if ( opparse->start != (opset+1)->start ) { printf( "start\n" ); }
      if ( opparse->count != (opset+1)->count ) { printf( "count\n" ); }
      if ( opparse->errval != (opset+1)->errval ) { printf( "errval\n" ); }
      return -1;
   }
   // output the same ops to our modify log
   if ( resourcelog_processop( &(wlog), readops, NULL ) ) {
      printf( "failed to output first read op set to MODIFY log\n" );
      return -1;
   }
   resourcelog_freeopinfo( readops );
   // validate the next operation
   if ( resourcelog_readop( &(rlog), &(opparse) ) ) {
      printf( "failed to read second operation set from first read log\n" );
      return -1;
   }
   rebuild_info* rebuildparse = (rebuild_info*)opparse->extendedinfo;
   if ( opparse->type != (opset+2)->type  ||
        strcmp( rebuildparse->markerpath, rebuildinf->markerpath )  ||
        rebuildparse->rtag.versz != rebuildinf->rtag.versz  ||
        rebuildparse->rtag.blocksz != rebuildinf->rtag.blocksz  ||
        rebuildparse->rtag.totsz != rebuildinf->rtag.totsz  ||
        rebuildparse->rtag.meta_status[0] != rebuildinf->rtag.meta_status[0]  ||
        rebuildparse->rtag.meta_status[1] != rebuildinf->rtag.meta_status[1]  ||
        rebuildparse->rtag.meta_status[2] != rebuildinf->rtag.meta_status[2]  ||
        rebuildparse->rtag.meta_status[3] != rebuildinf->rtag.meta_status[3]  ||
        rebuildparse->rtag.meta_status[4] != rebuildinf->rtag.meta_status[4]  ||
        rebuildparse->rtag.meta_status[5] != rebuildinf->rtag.meta_status[5]  ||
        rebuildparse->rtag.data_status[0] != rebuildinf->rtag.data_status[0]  ||
        rebuildparse->rtag.data_status[1] != rebuildinf->rtag.data_status[1]  ||
        rebuildparse->rtag.data_status[2] != rebuildinf->rtag.data_status[2]  ||
        rebuildparse->rtag.data_status[3] != rebuildinf->rtag.data_status[3]  ||
        rebuildparse->rtag.data_status[4] != rebuildinf->rtag.data_status[4]  ||
        rebuildparse->rtag.data_status[5] != rebuildinf->rtag.data_status[5]  ||
        rebuildparse->rtag.csum != rebuildinf->rtag.csum  ||
        opparse->start != (opset+2)->start  ||
        opparse->count != (opset+2)->count  ||
        opparse->errval != (opset+2)->errval  ||
        opparse->next   != (opset+2)->next  ||
        opparse->ftag.majorversion != (opset+2)->ftag.majorversion  ||
        opparse->ftag.minorversion != (opset+2)->ftag.minorversion  ||
        strcmp( opparse->ftag.ctag, (opset+2)->ftag.ctag )  ||
        strcmp( opparse->ftag.streamid, (opset+2)->ftag.streamid )  ||
        opparse->ftag.objfiles != (opset+2)->ftag.objfiles  ||
        opparse->ftag.objsize != (opset+2)->ftag.objsize  ||
        opparse->ftag.refbreadth != (opset+2)->ftag.refbreadth  ||
        opparse->ftag.refdepth != (opset+2)->ftag.refdepth  ||
        opparse->ftag.refdigits != (opset+2)->ftag.refdigits  ||
        opparse->ftag.fileno != (opset+2)->ftag.fileno  ||
        opparse->ftag.objno != (opset+2)->ftag.objno  ||
        opparse->ftag.offset != (opset+2)->ftag.offset  ||
        opparse->ftag.endofstream != (opset+2)->ftag.endofstream  ||
        opparse->ftag.protection.N != (opset+2)->ftag.protection.N  ||
        opparse->ftag.protection.E != (opset+2)->ftag.protection.E  ||
        opparse->ftag.protection.O != (opset+2)->ftag.protection.O  ||
        opparse->ftag.protection.partsz != (opset+2)->ftag.protection.partsz  ||
        opparse->ftag.bytes != (opset+2)->ftag.bytes  ||
        opparse->ftag.availbytes != (opset+2)->ftag.availbytes  ||
        opparse->ftag.recoverybytes != (opset+2)->ftag.recoverybytes  ||
        opparse->ftag.state != (opset+2)->ftag.state ) {
      printf( "read op 3 differs from original\n" );
      return -1;
   }
   // output the same ops to our modify log
   if ( resourcelog_processop( &(wlog), opparse, NULL ) ) {
      printf( "failed to output first read op set to MODIFY log\n" );
      return -1;
   }
   resourcelog_freeopinfo( opparse );
   // validate the next operation
   if ( resourcelog_readop( &(rlog), &(opparse) ) ) {
      printf( "failed to read third operation set from first read log\n" );
      return -1;
   }
   repack_info* repackparse = (repack_info*)opparse->extendedinfo;
   if ( opparse->type != (opset+3)->type  ||
        repackparse->totalbytes != repackinf->totalbytes  ||
        opparse->start != (opset+3)->start  ||
        opparse->count != (opset+3)->count  ||
        opparse->errval != (opset+3)->errval  ||
        opparse->next   != (opset+3)->next  ||
        opparse->ftag.majorversion != (opset+3)->ftag.majorversion  ||
        opparse->ftag.minorversion != (opset+3)->ftag.minorversion  ||
        strcmp( opparse->ftag.ctag, (opset+3)->ftag.ctag )  ||
        strcmp( opparse->ftag.streamid, (opset+3)->ftag.streamid )  ||
        opparse->ftag.objfiles != (opset+3)->ftag.objfiles  ||
        opparse->ftag.objsize != (opset+3)->ftag.objsize  ||
        opparse->ftag.refbreadth != (opset+3)->ftag.refbreadth  ||
        opparse->ftag.refdepth != (opset+3)->ftag.refdepth  ||
        opparse->ftag.refdigits != (opset+3)->ftag.refdigits  ||
        opparse->ftag.fileno != (opset+3)->ftag.fileno  ||
        opparse->ftag.objno != (opset+3)->ftag.objno  ||
        opparse->ftag.offset != (opset+3)->ftag.offset  ||
        opparse->ftag.endofstream != (opset+3)->ftag.endofstream  ||
        opparse->ftag.protection.N != (opset+3)->ftag.protection.N  ||
        opparse->ftag.protection.E != (opset+3)->ftag.protection.E  ||
        opparse->ftag.protection.O != (opset+3)->ftag.protection.O  ||
        opparse->ftag.protection.partsz != (opset+3)->ftag.protection.partsz  ||
        opparse->ftag.bytes != (opset+3)->ftag.bytes  ||
        opparse->ftag.availbytes != (opset+3)->ftag.availbytes  ||
        opparse->ftag.recoverybytes != (opset+3)->ftag.recoverybytes  ||
        opparse->ftag.state != (opset+3)->ftag.state ) {
      printf( "read op 4 differs from original\n" );
      return -1;
   }
   // output the same ops to our modify log
   if ( resourcelog_processop( &(wlog), opparse, NULL ) ) {
      printf( "failed to output first read op set to MODIFY log\n" );
      return -1;
   }
   resourcelog_freeopinfo( opparse );


   // terminate our reading log
   if ( resourcelog_term( &(rlog), &(summary), NULL ) ) {
      printf( "failed to terminate inital reading resourcelog\n" );
      return -1;
   }
   // and abort our writing log
   if ( resourcelog_abort( &(wlog) ) ) {
      printf( "failed to abort inital modify log\n" );
      return -1;
   }

   // open our writen modify log for read
   if ( resourcelog_init( &(rlog), wlogpath, RESOURCE_READ_LOG, NULL ) ) {
      printf( "failed to initialize first read log\n" );
      return -1;
   }
   free( wlogpath );
   // open a new output modify log
   wlogpath = resourcelog_genlogpath( 1, "./test_rman_topdir", "test-resourcelog-iteration654321", config->rootns, 1 );
   if ( wlogpath == NULL ) {
      printf( "failed to generate second modify logfile path\n" );
      return -1;
   }
   wlog = NULL;
   if ( resourcelog_init( &(wlog), wlogpath, RESOURCE_MODIFY_LOG, config->rootns ) ) {
      printf( "failed to initialize write logfile: \"%s\"\n", wlogpath );
      return -1;
   }


   // replay previous info from previous modify log into the new one
   if ( resourcelog_replay( &(rlog), &(wlog) ) ) {
      printf( "failed to replay old logfile\n" );
      return -1;
   }

   // insert first deletion op completion with decreased count
   progress = 0;
   opset->next = NULL;
   opset->count -= 2;
   delobjinf->offset = 2;
   opset->start = 0;
   if ( resourcelog_processop( &(wlog), opset, &(progress) ) ) {
      printf( "failed to insert initial deletion op completion\n" );
      return -1;
   }
   if ( progress ) {
      printf( "insertion of inital deletion completion incorrectly set 'progress' flag\n" );
      return -1;
   }
   // reinsert the same op, indicating completion
   if ( resourcelog_processop( &(wlog), opset, &(progress) ) ) {
      printf( "failed to insert full deletion op completion\n" );
      return -1;
   }
   if ( !(progress) ) {
      printf( "insertion of full deletion completion failed to set 'progress' flag\n" );
      return -1;
   }
   // insert rebuild op completion
   (opset + 2)->start = 0;
   progress = 0;
   if ( resourcelog_processop( &(wlog), opset + 2, &(progress) ) ) {
      printf( "failed to insert rebuild op completion\n" );
      return -1;
   }
   if ( !(progress) ) {
      printf( "insertion of rebuild op completion failed to set 'progress' flag\n" );
      return -1;
   }
   // insert repack op completion
   (opset + 3)->start = 0;
   progress = 0;
   if ( resourcelog_processop( &(wlog), opset + 3, &(progress) ) ) {
      printf( "failed to insert repack op completion\n" );
      return -1;
   }
   if ( !(progress) ) {
      printf( "insertion of repack op completion failed to set 'progress' flag\n" );
      return -1;
   }
   // insert final completion of inital deletion op
   (opset + 1)->start = 0;
   progress = 0;
   if ( resourcelog_processop( &(wlog), opset + 1, &(progress) ) ) {
      printf( "failed to insert final deletion op completion\n" );
      return -1;
   }
   if ( !(progress) ) {
      printf( "insertion of final deletion completion failed to set 'progress' flag\n" );
      return -1;
   }



//   // open another readlog for this same file
//   if ( resourcelog_init( &(rlog), logpath, RESOURCE_READ_LOG, NULL ) ) {
//      printf( "failed to initialize first read log\n" );
//      return -1;
//   }


   // replay our record log into the new modify log
//   if ( resourcelog_replay( &(rlog), &(wlog) ) ) {
//      printf( "failed to replay source log \"%s\" into dest log \"%s\"\n", logpath, wlogpath );
//      return -1;
//   }

   free( wlogpath );

   // free all operations
   opparse = opset;
   free( opparse->ftag.ctag );
   free( opparse->ftag.streamid );
   free( opparse->extendedinfo );
   opparse++;
   free( opparse->extendedinfo );
   opparse++;
   rebuildinf = (rebuild_info*)(opparse->extendedinfo);
   free( rebuildinf->markerpath );
   free( rebuildinf->rtag.meta_status );
   free( rebuildinf->rtag.data_status );
   free( opparse->extendedinfo );
   opparse++;
   free( opparse->extendedinfo );
   free( opset );
   // terminate the logfile
   printf( "terminating\n" );
   if ( resourcelog_term( &(wlog), &(summary), NULL ) ) {
      printf( "failed to terminate resourcelog\n" );
      return -1;
   }

   // free logpath
   free( logpath );
   // terminate our config
   if ( config_term( config ) ) {
      printf( "failed to terminate config\n" );
      return -1;
   }

   // cleanup test trees
   if ( deletefstree( "./test_rman_topdir" ) ) {
      printf( "Failed to delete contents of test tree\n" );
      return -1;
   }
   rmdir( "./test_rman_topdir" );

   return 0;
}


