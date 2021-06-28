#ifndef _STREAM_H
#define _STREAM_H
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

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/



#include <time.h>

#include "generic/numdigits.h"

typedef enum
{
   WRITE_STREAM,
   READ_STREAM
} STREAM_TYPE;

#define RECOVERY_CURRENT_MAJORVERSION 0
#define RECOVERY_CURRENT_MINORVERSION 1
#define RECOVERY_MINORVERSION_PADDING 3


// ALTERING HEADER OR MSG STRUCTURE MAY HORRIBLY BREAK PREVIOUS RECOVERY INFO AND STREAM LOGIC
#define RECOVERY_MSGHEAD "RECOV( "
#define RECOVERY_MSGTAIL " )\n"
typedef struct recovery_header_struct {
   unsigned int majorversion;
   unsigned int minorversion;
} RECOVERY_HEADER;
#define RECOVERY_HEADER "HEADER : "
#define RECOVERY_HEADER_SIZE ( 7 + 9 + UINT_DIGITS + 1 + UINT_DIGITS + 3 )


// ALTERING PER-FILE INFO IS SAFEISH, SO LONG AS YOU INCREMENT HEADER VERSIONS AND ADJUST PARSING
typedef struct recovery_finfo_struct {
   ino_t  inode;
   mode_t mode;
   uid_t  owner;
   gid_t  group;
   size_t size;
   struct timespec mtime;
   char   eof;
   char*  path;
} RECOVERY_FINFO;
#define RECOVERY_FINFO "FINFO : "


typedef struct streamfile_struct {
   MDAL_FHANDLE    metahandle;
   FTAG            ftag;
   struct timespec times[2];
}* STREAMFILE;


typedef struct datastream_struct {
   STREAM_TYPE     type;
   const marfs_ns  ns;
   MDAL_CTXT       mdalctxt;
   ne_handle   datahandle;
   RECOVERY_FINFO finfo;
   char*       ftagstr;
   size_t      ftagstrsize;
   STREAMFILE  files;
   size_t      filecount;
   size_t      filealloc;
}* DATASTREAM;




#define INTIAL_FILE_ALLOC 64
#define FILE_ALLOC_MULT    2


size_t allocfiles( STREAMFILE files, size_t current, size_t max ) {
   // calculate the target size of the file list
   size_t allocsize = 0;
   if ( current < INITIAL_FILE_ALLOC ) {
      allocsize = INITIAL_FILE_ALLOC;
   }
   else {
      allocsize = current * FILE_ALLOC_MULT;
   }
   if ( max  &&  allocsize > max ) { allocsize = max; }
   // realloc the list ( this is much simpler than allocating large linked list blocks )
   files = realloc( files, allocsize * sizeof( struct streamfile_struct ) );
   if ( files == NULL ) {
      LOG( LOG_ERR, "Failed to allocate stream filelist of size %zu\n", allocsize );
      return 0;
   }
   return allocsize;
}


char* genrpath( DATASTREAM stream ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // determine the lengths of rpath componenets
   size_t rnamelen = ftag_metaname( &(stream->files[0].ftag), NULL, 0 );
   size_t rdirlen = ( ms->rdigits + 1 ) * ms->rdepth;
   char* rpath = malloc( sizeof(char) * (rnamelen + rdirlen + 1) );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a reference path\n" );
      return NULL;
   }
   prres = ftag_metaname( &(stream->files[0].ftag), rpath + rdirlen, rnamelen + 1 ); // append rname
   if ( prres != rnamelen  ||  rnamelen == 0 ) {
      LOG( LOG_ERR, "Unexpected ftag metaname length\n" );
      errno = EBADFD;
      free( rpath );
      return NULL;
   }
   HASH_NODE* noderef = NULL;
   if ( hash_lookup( ms->reftable, rpath + rdirlen, &(noderef) ) < 0 ) {
      LOG( LOG_ERR, "Failed to identify reference tree for metaname \"%s\"\n", rpath + rdirlen );
      errno = EBADFD;
      free( rpath );
      return NULL;
   }
   prres = snprintf( rpath, rdirlen, "%s", noderef->name ); // prepend rdir
   if ( prres != rdirlen ) {
      LOG( LOG_ERR, "Reference dir len does not match expected value of %d: \"%s\"\n", rdirlen, rpath );
      errno = EBADFD;
      free( rpath );
      return NULL;
   }
   *( rpath + rdirlen - 1 ) = '/'; // snprintf will have left a NULL seperator between rdir and rname
   return rpath;
}


int updateftag( DATASTREAM stream, size_t filenum ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // populate the ftag string format
   size_t prres = ftag_tostr( &(stream->files[filenum].ftag), stream->ftagstr, stream->ftagstrsize );
   if ( prres >= stream->ftagstrsize ) {
      stream->ftagstrsize = 0;
      free( stream->ftagstr );
      stream->ftagstr = malloc( sizeof(char) * (prres + 1) );
      if ( stream->ftagstr == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for ftag string\n" );
         return -1;
      }
      stream->ftagstrsize = prres + 1;
      // reattempt, with a longer target string
      prres = ftag_tostr( &(stream->files[filenum].ftag), stream->ftagstr, stream->ftagstrsize );
      if ( prres >= stream->ftagstrsize ) {
         LOG( LOG_ERR, "Ftag string has an inconsistent length\n" );
         return -1;
      }
   }
   if ( prres <= 0 ) {
      LOG( LOG_ERR, "Failed to populate ftag string for stream\n" );
      return -1;
   }
   if ( ms->mdal->fsetxattr( stream->files[filenum].metahandle, 1, FTAG_NAME, stream->ftagstr, strlen(stream->ftagstr ) ) {
      LOG( LOG_ERR, "Failed to attach marfs ftag value: \"%s\"\n", stream->ftagstr );
      return -1;
   }
}


int linkfile( DATASTREAM stream, size_t filenum, const char* tgtpath ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // attempt to link the specified file to the specified user path
   if ( ms->mdal->flink( stream->files[filenum].metahandle, tgtpath ) ) {
      // if we got EEXIST, attempt to unlink the existing target and retry
      if ( errno != EEXIST ) {
         LOG( LOG_ERR, "Failed to link reference file to final location\n" );
         return -1;
      }
      else if ( ms->mdal->unlink( stream->mdalctxt, tgtpath ) ) {
         LOG( LOG_ERR, "Failed to unlink existing file: \"%s\"\n", tgtpath );
         return -1;
      }
      else if ( ms->mdal->flink( stream->files[filenum].metahandle, tgtpath ) ) {
         LOG( LOG_ERR, "Failed to link reference file to final location after retry\n" );
         return -1;
      }
   }
   return 0;
}


void freestream( DATASTREAM stream ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // abort any data handle
   if ( stream->datahandle  &&  ne_abort( stream->datahandle ) ) {
      LOG( LOG_WARNING, "Failed to abort stream datahandle\n" );
   }
   // free any string elements
   if ( stream->finfo.path ) { free( stream->finfo.path ); }
   if ( stream->ftagstr ) { free( stream->ftagstr ); }
   // iterate over all file references and clean them up
   size_t curfile = 0;
   for ( ; curfile < stream->filecount; curfile++ ) {
      if ( stream->files[curfile].metahandle  &&  ms->mdal->close( stream->files[curfile].metahandle ) ) {
         LOG( LOG_WARNING, "Failed to close meta handle for file %zu\n", curfile );
      }
      // all ftags share string references, so only free the first set 
      if ( curfile == 0 ) {
         if ( stream->files[0].ftag.streamid ) { free( stream->files[0].ftag.streamid ); }
         if ( stream->files[0].ftag.ctag ) { free( stream->files[0].ftag.ctag ); }
      }
   }
   // free the file list itself
   if ( stream->files ) { free( stream->files ); }
   // free mdal ctxt only AFTER all meta handles have been closed
   if ( stream->mdalctxt  &&  ms->mdal->destroyctxt( stream->mdalctxt ) ) {
      LOG( LOG_WARNING, "Failed to destroy stream MDAL ctxt\n" );
   }
   // finally, free the stream itself
   free( stream );
}


DATASTREAM datastream_creat( const char* path, mode_t mode, const marfs_ns* ns, const marfs_config* config ) {
   // create some shorthand references
   marfs_ms* ms = &(ns->prepo->metascheme);
   marfs_ds* ds = &(ns->prepo->datascheme);

   // allocate the new datastream and check for success
   DATASTREAM stream = malloc( sizeof( struct datastream_struct ) );
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a datastream\n" );
      return NULL;
   }
   stream->type = WRITE_STREAM;
   stream->ns = ns;
   stream->mdalctxt = ms->mdal->dupctxt( ms->mdal->ctxt );
   stream->datahandle = NULL;
   stream->finfo.inode = 0;
   stream->finfo.mode = mode;
   stream->finfo.owner = geteuid();
   stream->finfo.group = getegid();
   stream->finfo.size = 0;
   stream->finfo.mtime.tv_sec = 0;
   stream->finfo.mtime.tv_nsec = 0;
   stream->finfo.eof = 0;
   stream->finfo.path = strdup( path );
   stream->ftagstr = malloc( sizeof(char) * 1024 );
   stream->ftagstrsize = 1024;
   // skipping files, defined below
   stream->filecount = 0;
   if ( stream->ftagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for ftag string\n" );
      freestream( stream );
      return NULL;
   }

   // shift to the appropriate namespace
   if ( ms->mdal->setnamespace( stream->mdalctxt, ns->idstr ) ) {
      LOG( LOG_ERR, "Failed to shift stream context to \"%s\" namespace\n", ns->idstr );
      freestream( stream );
      return NULL;
   }

   // allocate our first file reference(s)
   stream->filealloc = allocfiles( stream->files, stream->filecount, ds->objfiles );
   if ( stream->filealloc == 0 ) {
      LOG( LOG_ERR, "Failed to allocate space for streamfiles\n" );
      freestream( stream );
      return NULL;
   }
   stream->filecount = 1;
   stream->files[0].metahandle = NULL;
   stream->files[0].ftag.editable = 1;
   stream->files[0].ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   stream->files[0].ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   stream->files[0].ftag.ctag = strdup( config->ctag );
   // skipping streamid, defined below
   stream->files[0].ftag.objfiles = ds.objfiles;
   stream->files[0].ftag.objsize = ds.objsize;
   stream->files[0].ftag.fileno = 0;
   stream->files[0].ftag.objno = 0;
   stream->files[0].ftag.endofstream = 0;
   stream->files[0].ftag.offset = 0;
   stream->files[0].ftag.protection = ds.protection;
   stream->files[0].ftag.bytes = 0;
   stream->files[0].ftag.availbytes = 0;
   stream->files[0].ftag.recoverybytes = 0;
   stream->files[0].ftag.directbytes = 0;
   stream->files[0].ftag.stat = FTAG_INIT;
   stream->files[0].times[0].tv_sec = 0;
   stream->files[0].times[0].tv_nsec = 0;
   stream->files[0].times[1].tv_sec = 0;
   stream->files[0].times[1].tv_nsec = 0;

   // generate a new streamID
   struct timespec curtime;
   if ( clock_gettime( CLOCK_REALTIME, &curtime ) ) {
      LOG( LOG_ERR, "Failed to determine the current time\n" );
      freestream( stream );
      return NULL;
   }
   size_t streamidlen = ( sizeof(time_t) > sizeof(int) ) ? SIZE_DIGITS : UINT_DIGITS;  // see numdigits.h
   streamidlen += SIZE_DIGITS; // to account for nanosecond precision
   streamidlen += strlen( ns->idstr ); // to include repo+NS info
   streamidlen += 3; // for '.' seperators and null terminator
   if ( (stream->files[0].ftag.streamid = malloc( sizeof(char) * streamidlen )) == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for streamID\n" );
      freestream( stream );
      return NULL;
   }
   ssize_t prres = snprintf( stream->files[0].ftag.streamid, streamidlen, "%s.%zd.%ld", ns->idstr, curtime.tv_sec, curtime.tv_nsec );
   if ( prres <= 0  ||  prres >= streamidlen ) {
      LOG( LOG_ERR, "Failed to generate streamID value\n" );
      freestream( stream );
      return NULL;
   }

   // identify a reference path
   char* rpath = genrpath( stream );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify reference path for stream\n" );
      freestream( stream );
      return NULL;
   }

   // create the reference file
   stream->files[0].metahandle = ms->mdal->openref( stream->mdalctxt, rpath, O_CREAT | O_EXCL | O_WRONLY, mode );
   if ( stream->files[0].metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to create reference meta file: \"%s\"\n", rpath );
      if ( errno = EEXIST ) { errno = EBUSY; } // a BUSY error is more indicative of the real problem
      free( rpath );
      freestream( stream );
      return NULL;
   }
   // still need to hang onto 'rpath', in case something goes wrong later

   // attach our initial MARFS ftag value
   if ( updateftag( stream, 0 ) ) {
      LOG( LOG_ERR, "Failed to update FTAG value on target file\n" );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      freestream( stream );
      return NULL;
   }

   // link the file into the user namespace
   if ( linkfile( stream, 0, path ) ) {
      LOG( LOG_ERR, "Failed to link reference file to target user path: \"%s\"\n", path );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      freestream( stream );
      return NULL;
   }

   // all done
   free( rpath );
   return stream;
}



DATASTREAM datastream_edit( const char* path, MDAL mdal, MDAL_CTXT mdalctxt );
DATASTREAM datastream_cont( DATASTREAM stream, const char* path, const char* rdir, mode_t mode );
DATASTREAM datastream_init( DATASTREAM stream, const char* path, MDAL mdal, MDAL_CTXT mdalctxt );
int datastream_setrecoverypath( DATASTREAM stream, const char* recovpath );
int datastream_chunkbounds( DATASTREAM stream, int chunknum, off_t* offset, size_t* size );
off_t datastream_seek( DATASTREAM stream, off_t offset, int whence );
int datastream_truncate( DATASTREAM stream, off_t length );
int datastream_utimens( DATASTREAM stream, const struct timespec times[2] );
size_t datastream_write( DATASTREAM stream, const void* buff, size_t size );
size_t datastream_read( DATASTREAM stream, void* buffer, size_t size );
int datastream_close( DATASTREAM stream );


RECOVSTREAM recovstream_init( void* objectbuffer, size_t objectsize, RECOVSTREAM_HEADER* header );
RECOVSTREAM recovstream_cont( RECOVSTREAM recovery, void* objectbuffer, size_t objectsize, RECOVERY_HEADER* header );
const void* recovstream_nextfile( RECOVSTREAM recovery, RECOVERY_FINFO* finfo, size_t* buffsize );
int recovstream_close( RECOVSTREAM recovery );


#endif // _STREAM_H

