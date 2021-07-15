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

#include <time.h>

#include "generic/numdigits.h"


//   -------------   INTERNAL DEFINITIONS    -------------


#define INTIAL_FILE_ALLOC 64
#define FILE_ALLOC_MULT    2


//   -------------   INTERNAL FUNCTIONS    -------------


DATASTREAM genstream( STREAM_TYPE type, const marfs_ns* ns, const marfs_config* config ) {
   // create some shorthand references
   marfs_ms* ms = &(ns->prepo->metascheme);
   marfs_ds* ds = &(ns->prepo->datascheme);

   // allocate the new datastream and check for success
   DATASTREAM stream = malloc( sizeof( struct datastream_struct ) );
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a datastream\n" );
      return NULL;
   }
   stream->type = type;
   stream->ns = ns;
   stream->mdalctxt = ms->mdal->dupctxt( ms->mdal->ctxt );
   stream->datahandle = NULL;
   // skipping files, defined below
   stream->filecount = 0;
   // skipping filealloc, defined below
   stream->ftagstr = malloc( sizeof(char) * 512 );
   stream->ftagstrsize = 512;
   stream->objno = 0;
   stream->offset = 0;
   // zero out all recovery finfo values; those will be populated properly later
   stream->finfo.inode = 0;
   stream->finfo.mode = 0;
   stream->finfo.owner = 0;
   stream->finfo.group = 0;
   stream->finfo.size = 0;
   stream->finfo.mtime.tv_sec = 0;
   stream->finfo.mtime.tv_nsec = 0;
   stream->finfo.eof = 0;
   stream->finfo.path = NULL;

   // shift to the appropriate namespace
   if ( ms->mdal->setnamespace( stream->mdalctxt, ns->idstr ) ) {
      LOG( LOG_ERR, "Failed to shift stream context to \"%s\" namespace\n", ns->idstr );
      freestream( stream );
      return NULL;
   }

   // allocate our first file reference(s)
   if ( type == READ_STREAM  ||  type == EDIT_STREAM ) {
      // Read streams should only ever expect a single file to be referenced at a time
      // Edit streams will *likely* only reference a single file, but may reference more later
      stream->filealloc = allocfiles( &(stream->files), stream->filecount, 1 );
   }
   else {
      // Create streams are only restriced by the object packing limits
      stream->filealloc = allocfiles( &(stream->files), stream->filecount, ds->objfiles );
   }
   if ( stream->filealloc == 0 ) {
      LOG( LOG_ERR, "Failed to allocate space for streamfiles\n" );
      freestream( stream );
      return NULL;
   }
   stream->filecount = 1;
   // skipping metahandle, defined later
   stream->files[0].ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   stream->files[0].ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   // skipping ctag, defined later
   // skipping streamid, defined later
   stream->files[0].ftag.objfiles = ds.objfiles;
   stream->files[0].ftag.objsize = ds.objsize;
   stream->files[0].ftag.fileno = 0;
   stream->files[0].ftag.objno = 0;
   stream->files[0].ftag.endofstream = 0;
   stream->files[0].ftag.offset = 0;
   stream->files[0].ftag.location.pod = -1;
   stream->files[0].ftag.location.cap = -1;
   stream->files[0].ftag.location.scatter = -1;
   stream->files[0].ftag.protection = ds.protection;
   stream->files[0].ftag.bytes = 0;
   stream->files[0].ftag.availbytes = 0;
   stream->files[0].ftag.recoverybytes = 0;
   stream->files[0].ftag.directbytes = 0;
   stream->files[0].ftag.state = FTAG_INIT; // no data written and no access by anyone else
   stream->files[0].times[0].tv_sec = 0;
   stream->files[0].times[0].tv_nsec = 0;
   stream->files[0].times[1].tv_sec = 0;
   stream->files[0].times[1].tv_nsec = 0;

   // verify that all string allocations succeeded
   if ( stream->ftagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for stream string elements\n" );
      freestream( stream );
      return NULL;
   }

   return stream;
}

size_t gettargets( DATASTREAM stream, off_t offset, int whence, size_t* tgtobjno, size_t* tgtoffest, size_t* remaining ) {
   // establish some bounds values that we'll need later
   FTAG curtag = stream->files[stream->filecount - 1].ftag;
   size_t dataperobj = ( curtag.objsize - (curtag.recoverybytes + stream->recoveryheaderlen) );
   size_t minobj = curtag.objno;
   size_t minoffset = curtag.offset - stream->recoveryheaderlen; // data space already occupied in first obj

   // convert all values to a seek from the start of the file ( convert to a whence == SEEK_SET offset )
   if ( whence == SEEK_END ) {
      offset += curtag.bytes; // just add the total file size
   }
   else if ( whence == SEEK_CUR ) {
      if ( stream->objno > minobj ) {
         offset += (dataperobj - minoffset); // add the data in the first obj
         offset += ( stream->objno - (minobj + 1) ) * dataperobj; // add the data of all complete objs
         offset += ( stream->offset - stream->recoveryheaderlen ); // add the data in the current obj
      }
      else {
         offset += ( (stream->offset - stream->recoveryheaderlen) - minoffset );
      }
   }
   else if ( whence != SEEK_SET ) {
      // catch an unknown 'whence' value
      LOG( LOG_ERR, "Invalid value of 'whence'\n" );
      errno = EINVAL;
      return 0;
   }
   // regardless of 'whence', we are now seeking from the min values ( beginning of file )
   if ( offset > curtag.bytes ) {
      // make sure we aren't seeking beyond the end of the file
      LOG( LOG_ERR, "Offset value extends beyond end of file\n" );
      errno = EINVAL;
      return 0;
   }
   if ( offset < 0 ) {
      // make sure we aren't seeking before the start of the file
      LOG( LOG_ERR, "Offset value extends prior to beginning of file\n" );
      errno = EINVAL;
      return 0;
   }
   size_t tgtobj = minobj;
   size_t tgtoff = minoffset;
   size_t remain = curtag.bytes - offset;
   if ( (offset + minoffset) > dataperobj ) {
      // this offset will cross object boundaries
      offset -= (dataperobj - minoffset);
      tgtobj += ( offset / dataperobj ) + 1;
      tgtoff = offset % dataperobj;
   }
   else {
      tgtoff += offset;
   }

   // finally, populate our final values, accounting for recovery info
   if ( tgtobjno ) { *tgtobjno = tgtobj; }
   if ( tgtoffset ) { *tgtoffset = tgtoff + stream->recoveryheaderlen; }
   if ( remaining ) { *remaining = remain; }
   return dataperobj;
}

         

size_t allocfiles( STREAMFILE* files, size_t current, size_t max ) {
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
   *files = realloc( *files, allocsize * sizeof( struct streamfile_struct ) );
   if ( *files == NULL ) {
      LOG( LOG_ERR, "Failed to allocate stream filelist of size %zu\n", allocsize );
      return 0;
   }
   return allocsize;
}

char* genrpath( DATASTREAM stream, STREAMFILE file ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // determine the lengths of rpath componenets
   size_t rnamelen = ftag_metaname( &(file->ftag), NULL, 0 );
   size_t rdirlen = ( ms->rdigits + 1 ) * ms->rdepth;
   char* rpath = malloc( sizeof(char) * (rnamelen + rdirlen + 1) );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a reference path\n" );
      return NULL;
   }
   prres = ftag_metaname( &(file->ftag), rpath + rdirlen, rnamelen + 1 ); // append rname
   if ( prres != rnamelen  ||  rnamelen == 0 ) {
      LOG( LOG_ERR, "Unexpected ftag metaname length\n" );
      free( rpath );
      return NULL;
   }
   HASH_NODE* noderef = NULL;
   if ( hash_lookup( ms->reftable, rpath + rdirlen, &(noderef) ) < 0 ) {
      LOG( LOG_ERR, "Failed to identify reference tree for metaname \"%s\"\n", rpath + rdirlen );
      free( rpath );
      return NULL;
   }
   prres = snprintf( rpath, rdirlen, "%s", noderef->name ); // prepend rdir
   if ( prres != rdirlen ) {
      LOG( LOG_ERR, "Reference dir len does not match expected value of %d: \"%s\"\n", rdirlen, rpath );
      errno = EDOM;
      free( rpath );
      return NULL;
   }
   *( rpath + rdirlen - 1 ) = '/'; // snprintf will have left a NULL seperator between rdir and rname
   return rpath;
}

int opencurrentobj( DATASTREAM stream ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);

   // find the length of the current object name
   FTAG tgttag = stream->files[stream->filecount - 1].ftag;
   tgttag.objno = stream->objno; // we actually want the stream object number
   tgttag.offset = stream->offset;
   ssize_t objnamelen = ftag_objectname( &(tgttag), NULL, 0 );
   if ( objnamelen <= 0 ) {
      LOG( LOG_ERR, "Failed to determine object path from current ftag\n" );
      return -1;
   }
   // allocate a new string, and populate it with the object name
   char* objname = malloc( sizeof(char) * (objnamelen + 1) );
   if ( objname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for new object name\n" );
      return -1;
   }
   if ( objnamelen != ftag_objectname( &(tgttag), objname, objnamelen + 1 ) ) {
      LOG( LOG_ERR, "Ftag producing inconsistent object name string\n" );
      free( objname );
      return -1;
   }

   // identify the pod/cap/scatter values for the current object
   ne_location location = { .pod = -1, .cap = -1, .scatter = -1 };
   int iteration = 0;
   for( ; iteration < 3; iteration++ ) {
      // determine which table we are currently pulling from
      char* itername = "scatter";
      HASH_TABLE curtable = ds->scattertable;
      int* tgtval = &(location.scatter);
      if ( iteration < 1 ) {
         itername = "pod";
         curtable = ds->podtable;
         tgtval = &(location.pod);
      }
      else if ( iteration < 2 ) {
         itername = "cap";
         curtable = ds->captable;
         tgtval = &(location.cap);
      }
      // hash our object name, to identify a target node
      HASH_NODE* node = NULL;
      if ( hash_lookup( curtable, objname, &node ) < 0 ) {
         LOG( LOG_ERR, "Failed to lookup %s location for new object \"%s\"\n", itername, objname );
         free( objname );
         return -1;
      } 
      // parse our nodename, to produce an integer value
      char* endptr = NULL;
      unsigned long long parseval = strtoull( node->name, &(endptr), 10 );
      if ( *endptr != '\0'  ||  parseval >= INT_MAX ) {
         LOG( LOG_ERR, "Failed to parse %s value of \"%s\" for new object \"%s\"\n", itername, node->name, objname );
         free( objname );
         return -1;
      }
      // assign the parsed value to the appropriate var
      *tgtval = (int)parseval;
   }

   // identify the erasure scheme
   ne_erasure erasure = tgttag.protection;
   erasure.O = hash_rangevalue( objname, erasure.N + erasure.E ); // produce erasure offset value

   // open a handle for the new object
   if ( stream->type == READ_STREAM ) {
      stream->datahandle = ne_open( ds->nectxt, objname, location, erasure, NE_RDONLY );
   }
   else {
      stream->datahandle = ne_open( ds->nectxt, objname, location, erasure, NE_WRALL );
   }
   if ( stream->datahandle == NULL ) {
      LOG( LOG_ERR, "Failed to open object \"%s\" at pod%d, cap%d, scatter%d\n", objname, location.pod, location.cap, location.scatter );
      free( objname );
      return -1;
   }
   free( objname ); // done with object name

   if ( stream->type == READ_STREAM ) {
      // if we're reading, we may need to seek to a specific offset
      if ( stream->offset ) {
         if ( stream->offset != ne_seek( stream->datastream, stream->offset ) ) {
            LOG( LOG_ERR, "Failed to seek to offset %zu of object %zu\n", stream->offset, stream->objno );
            return -1;
         }
      }
   }
   else {
      // if we're writing out a new object, output a recovery header
      RECOVERY_HEADER header = 
      {
         .majorversion = RECOVERY_CURRENT_MAJORVERSION,
         .minorversion = RECOVERY_CURRENT_MINORVERSION,
         .ctag = stream->files[stream->filecount - 1].ftag.ctag,
         .streamid = stream->files[stream->filecount - 1].ftag.streamid
      };
      char* recovheader = malloc( sizeof(char) * (stream->recoveryheaderlen + 1) );
      if ( recovheader == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for recovery header string\n" );
         ne_abort( stream->datahandle );
         stream->datahandle == NULL;
         return -1;
      }
      if ( recovery_headertostr( &(header), recovheader, stream->recoveryheaderlen + 1 ) != stream->recoveryheaderlen ) {
         LOG( LOG_ERR, "Recovery header string has inconsistent length\n" );
         ne_abort( stream->datahandle );
         stream->datahandle == NULL;
         free( recovheader );
         return -1;
      }
      if ( ne_write( stream->datahandle, recovheader, stream->recoveryheaderlen ) != stream->recoveryheaderlen ) {
         LOG( LOG_ERR, "Failed to write recovery header to new data object\n" );
         ne_abort( stream->datahandle );
         stream->datahandle == NULL;
         free( recovheader );
         return -1;
      }
      free( recovheader ); // done with recovery header string

      // our offset value is now however much recovery header info we stored
      stream->offset = stream->recoveryheaderlen;
   }

   return 0;
}

int putftag( DATASTREAM stream, STREAMFILE file ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // populate the ftag string format
   size_t prres = ftag_tostr( &(file->ftag), stream->ftagstr, stream->ftagstrsize );
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
      prres = ftag_tostr( &(file->ftag), stream->ftagstr, stream->ftagstrsize );
      if ( prres >= stream->ftagstrsize ) {
         LOG( LOG_ERR, "Ftag string has an inconsistent length\n" );
         return -1;
      }
   }
   if ( prres <= 0 ) {
      LOG( LOG_ERR, "Failed to populate ftag string for stream\n" );
      return -1;
   }
   if ( ms->mdal->fsetxattr( file->metahandle, 1, FTAG_NAME, stream->ftagstr, prres ) ) {
      LOG( LOG_ERR, "Failed to attach marfs ftag value: \"%s\"\n", stream->ftagstr );
      return -1;
   }
}

int getftag( DATASTREAM stream, STREAMFILE file ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // attempt to retrieve the ftag attr value ( leaving room for NULL terminator )
   size_t getres = ms->mdal->fgetxattr( file->metahandle, 1, FTAG_NAME, stream->ftagstr, stream->ftagstrsize - 1 );
   if ( getres >= stream->ftagstrsize ) {
      stream->ftagstrsize = 0;
      free( stream->ftagstr );
      stream->ftagstr = malloc( sizeof(char) * (getres + 1) );
      if ( stream->ftagstr == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for ftag string buffer\n" );
         return -1;
      }
      stream->ftagstrsize = getres + 1;
      // reattempt, with a longer target string
      getres = ms->mdal->fgetxattr( file->metahandle, 1, FTAG_NAME, stream->ftagstr, stream->ftagstrsize - 1 );
      if ( getres >= stream->ftagstrsize ) {
         LOG( LOG_ERR, "Ftag value of file %zu has an inconsistent length\n", filenum );
         return -1;
      }
   }
   if ( getres <= 0 ) {
      LOG( LOG_ERR, "Failed to retrieve ftag value for stream file %zu\n", filenum );
      return -1;
   }
   // ensure our string is NULL terminated
   *( stream->ftagstr + getres ) = '\0';
   // attempt to set struct values based on the ftag string
   if ( ftag_initstr( stream->ftagstr, &(file->ftag) ) ) {
      LOG( LOG_ERR, "Failed to initialize ftag values for file %zu of stream\n", filenum );
      return -1;
   }
   return 0;
}

int linkfile( DATASTREAM stream, STREAMFILE file, const char* tgtpath ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // attempt to link the specified file to the specified user path
   if ( ms->mdal->flink( file->metahandle, tgtpath ) ) {
      // if we got EEXIST, attempt to unlink the existing target and retry
      if ( errno != EEXIST ) {
         // any non-EEXIST error is fatal
         LOG( LOG_ERR, "Failed to link reference file to final location\n" );
         return -1;
      }
      else if ( ms->mdal->unlink( stream->mdalctxt, tgtpath )  &&  errno != ENOENT ) {
         // ENOENT would indicate that another proc has unlinked the conflicting file for us
         //   Otherwise, we have to fail
         LOG( LOG_ERR, "Failed to unlink existing file: \"%s\"\n", tgtpath );
         return -1;
      }
      else if ( ms->mdal->flink( file->metahandle, tgtpath ) ) {
         // This indicates either we're racing with another proc, or something more unusual
         //   Just fail out with whatever errno we get from flink()
         LOG( LOG_ERR, "Failed to link reference file to final location after retry\n" );
         return -1;
      }
   }
   return 0;
}

int genrecoveryinfo( RECOVERY_FINFO* finfo, STREAMFILE file, const char* path ) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // identify file attributes, for recovery info
   struct stat stval;
   if ( ms->mdal->fstat( file->metahandle, &(stval) ) ) {
      LOG( LOG_ERR, "Failed to stat meta file for recovery info values\n" );
      return -1;
   }

   // populate recovery info inode/mtime and calculate recovery finfo length
   finfo->inode = stval.st_ino;
   finfo->mode = stval.st_mode;
   finfo->owner = stval.st_uid;
   finfo->group = stval.st_gid;
   finfo->size = 0;
   finfo->mtime.tv_sec = stval.st_mtim.tv_sec;
   finfo->mtime.tv_nsec = stval.st_mtim.tv_nsec;
   finfo->eof = 0;
   finfo->path = strdup( path );
   if ( finfo->path == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate file path into recovery info\n" );
      return -1;
   }

   // align our finalized file times with those we will be using in recovery info
   file->times[0] = stval.st_atim;
   file->times[1] = stval.st_mtim;

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


//   -------------   EXTERNAL FUNCTIONS    -------------


DATASTREAM datastream_creat( const char* path, mode_t mode, const marfs_ns* ns, const marfs_config* config ) {
   // create some shorthand references
   marfs_ms* ms = &(ns->prepo->metascheme);
   marfs_ds* ds = &(ns->prepo->datascheme);

   // allocate the new datastream and check for success
   DATASTREAM stream = genstream( CREATE_STREAM, ns, config );
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new datastream\n" );
      return NULL;
   }

   // explicitly set the ctag value ( this is not done by genstream() )
   stream->files[0].ctag = strdup( config->ctag );
   if ( stream->files[0].ftag.ctag == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for stream string elements\n" );
      freestream( stream );
      return NULL;
   }

   // generate a new streamID ( this is the ONLY func that generates a streamid; all other funcs take existing )
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
   ssize_t prres = snprintf( stream->files[0].ftag.streamid, streamidlen, "%s#%zd#%ld", ns->idstr, curtime.tv_sec, curtime.tv_nsec );
   if ( prres <= 0  ||  prres >= streamidlen ) {
      LOG( LOG_ERR, "Failed to generate streamID value\n" );
      freestream( stream );
      return NULL;
   }

   // identify a reference path
   char* rpath = genrpath( stream, &(stream->files[0]) );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify reference path for stream\n" );
      freestream( stream );
      return NULL;
   }

   // create the reference file, ensuring we don't collide with an existing reference
   stream->files[0].metahandle = ms->mdal->openref( stream->mdalctxt, rpath, O_CREAT | O_EXCL | O_WRONLY, mode );
   if ( stream->files[0].metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to create reference meta file: \"%s\"\n", rpath );
      if ( errno = EEXIST ) { errno = EBUSY; } // a BUSY error is more indicative of the real problem
      free( rpath );
      freestream( stream );
      return NULL;
   }
   // still need to hang onto 'rpath', in case something goes wrong later

   // identify file recovery info
   if ( genrecoveryinfo( &(stream->finfo), &(stream->files[0]), path ) ) {
      LOG( LOG_ERR, "Failed to populate recovery info for file: \"%s\"\n", path );
      errno = EBADFD; // a bit vague, but hopefully at least not misleading
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      freestream( stream );
      return NULL;
   }
   stream->files[0].ftag.recoverybytes = recovery_finfotostr( &(stream->finfo), NULL, 0 );
   if ( stream->files[0].ftag.recoverybytes == 0 ) {
      LOG( LOG_ERR, "Failed to calculate recovery info size for created file\n" );
      errno = EBADFD; // a bit vague, but hopefully at least not misleading
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      freestream( stream );
      return NULL;
   }
   RECOVERY_HEADER header = 
   {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = stream->files[0].ftag.ctag,
      .streamid = stream->files[0].ftag.streamid
   };
   stream->recoveryheaderlen = recovery_headertostr( &(header), NULL, 0 );

   // verify that our object can fit recovery info, plus at least *some* data
   if ( (stream->recoveryheaderlen + stream->files[0].ftag.recoverybytes) >= stream->files[0].ftag.objsize ) {
      LOG( LOG_ERR, "Object size of %zu is insufficient for recovery header of %zu plus file recovery info of %zu\n", stream->files[0].ftag.objsize, stream->recoveryheaderlen, stream->files[0].ftag.recoverybytes );
      errno = EBADFD; // a bit vague, but hopefully at least not misleading
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      freestream( stream );
      return NULL;
   } 

   // attach our initial MARFS ftag value
   if ( putftag( stream, &(stream->files[0]) ) ) {
      LOG( LOG_ERR, "Failed to update FTAG value on target file\n" );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      freestream( stream );
      return NULL;
   }

   // link the file into the user namespace
   if ( linkfile( stream, &(stream->files[0]), path ) ) {
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

DATASTREAM datastream_edit( const char* path, const marfs_ns* ns, const marfs_config* config ) {
   // create some shorthand references
   marfs_ms* ms = &(ns->prepo->metascheme);
   marfs_ds* ds = &(ns->prepo->datascheme);

   // allocate the new datastream and check for success
   DATASTREAM stream = genstream( EDIT_STREAM, ns, config );
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new datastream\n" );
      return NULL;
   }

   // open a metadata handle for the target file
   stream->files[0].metahandle = ms->mdal->open( stream->mdalctxt, path, O_RDWR );
   if( stream->files[0].metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to open metadata handle for target path: \"%s\"\n", path );
      freestream( stream );
      return NULL;
   }

   // retrieve ftag values for the existing file
   if ( getftag( stream, &(stream->files[0]) ) ) {
      LOG( LOG_ERR, "Failed to retrieve ftag value for target file: \"%s\"\n", path );
      freestream( stream );
      return NULL;
   }

   // set stream offsets to align with the start of the file
   stream->objno = stream->files[0].ftag.objno;
   stream->offset = stream->files[0].ftag.offset;

   // verify that this file is writable via a new handle
   if ( ( stream->files[0].ftag.state & FTAG_WRITABLE ) == 0 ) {
      LOG( LOG_ERR, "Attempting to edit file before original handle has been released\n" );
      freestream( stream );
      return NULL;
   }

   // identify file attributes, for recovery info
   if ( genrecoveryinfo( &(stream->finfo), &(stream->files[0]), path ) ) {
      LOG( LOG_ERR, "Failed to populate recovery info for file: \"%s\"\n", path );
      freestream( stream );
      return NULL;
   }
   RECOVERY_HEADER header = 
   {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = stream->files[0].ftag.ctag,
      .streamid = stream->files[0].ftag.streamid
   };
   stream->recoveryheaderlen = recovery_headertostr( &(header), NULL, 0 );

   return stream;
}

DATASTREAM datastream_wcont( DATASTREAM stream, const char* path, mode_t mode ) {
   // verify we haven't just been given a NULL stream
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL stream value\n" );
      errno = EINVAL;
      return NULL;
   }

   // ensure our stream handle is in an appropriate mode
   if ( stream->type == READ_STREAM ) {
      LOG( LOG_ERR, "Cannot continue a READ datastream\n" );
      errno = EINVAL;
      return NULL;
   }

   // if this is an EDIT stream, ensure that this handle can finalize the current file
   if ( stream->type == EDIT_STREAM ) {
      FTAG_STATE curfilestate = stream->files[stream->filecount - 1].ftag.state;
      if ( (curfilestate & FTAG_WRITABLE) == 0 ) {
         LOG( LOG_ERR, "Current file cannot be modified by the given handle\n" );
         errno = EINVAL;
         return NULL;
      }
      if ( (curfilestate & FTAG_DATASTATE) != FTAG_FIN ) {
         LOG( LOG_ERR, "Current file does not have a finalized data size\n" );
         errno = EINVAL;
         return NULL;
      }
   }

   // create some shorthand references
   marfs_ms* ms = &(ns->prepo->metascheme);
   marfs_ds* ds = &(ns->prepo->datascheme);
   STREAMFILE curfile = stream->files + (stream->filecount - 1);

   // Note -- This function has two distinct phases: creation of a new file, and completion of the previous
   //         As completion of the previous file may involve irreversible modification of the stream, it is 
   //         safest to begin with creation of the new file.  That way, if file creation proves impossible, 
   //         for whatever reason, we still have the opportunity to roll-back the entire op.

   // NEW FILE CREATION

   // construct a reference struct for our new file
   struct streamfile_struct newfile = 
   {
      .metahandle = NULL,
      .ftag = curfile->ftag,  // copy our current, working ftag
      .times[0].tv_sec = 0,
      .times[0].tv_nsec = 0,
      .times[1].tv_sec = 0,
      .times[1].tv_nsec = 0
   };
   // update our ftag values to reflect the new file, rather than the old
   newfile.ftag.fileno++;
   if ( stream->type == CREATE_STREAM ) {
      // use the current stream object, for now
      newfile.ftag.objno = stream->objno;
      // use the current stream offset, accounting for the recovery info we still need to store
      newfile.ftag.offset = stream->offset + newfile.ftag.recoverybytes;
   }
   else {
   }
   newfile.ftag.bytes = 0;
   newfile.ftag.availbytes = 0;
   newfile.ftag.directbytes = 0;
   newfile.ftag.state = FTAG_INIT;

   // establish a reference path for the new file
   char* rpath = genrpath( stream, &(newfile) );
   if ( rpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify reference path for stream\n" );
      if ( errno == EBADFD ) { errno = ENOMSG; } // don't allow our reserved EBADFD value
      return NULL;
   }

   // create the reference file, ensuring we don't collide with an existing reference
   newfile.metahandle = ms->mdal->openref( stream->mdalctxt, rpath, O_CREAT | O_EXCL | O_WRONLY, mode );
   if ( newfile.metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to create reference meta file: \"%s\"\n", rpath );
      if ( errno = EEXIST ) { errno = EBUSY; } // a BUSY error is more indicative of the real problem
      else if ( errno == EBADFD ) { errno = ENOMSG; } // don't allow our reserved EBADFD value
      free( rpath );
      return NULL;
   }
   // still need to hang onto 'rpath', in case something goes wrong later

   // identify file recovery info
   RECOVERY_FINFO newfinfo;
   if ( genrecoveryinfo( &(newfinfo), &(newfile), path ) ) {
      LOG( LOG_ERR, "Failed to populate recovery info for file: \"%s\"\n", path );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      if ( errno == EBADFD ) { errno = ENOMSG; } // don't allow our reserved EBADFD value
      return NULL;
   }
   newfile.ftag.recoverybytes = recovery_finfotostr( &(newfinfo), NULL, 0 );
   if ( newfile.ftag.recoverybytes == 0 ) {
      LOG( LOG_ERR, "Failed to calculate recovery info size for created file\n" );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      if ( errno == EBADFD ) { errno = ENOMSG; } // don't allow our reserved EBADFD value
      return NULL;
   }

   // ensure the recovery info size is compatible with the current object size
   if ( (stream->recoveryheaderlen + newfile.ftag.recoverybytes) >= newfile.ftag.objsize ) {
      LOG( LOG_ERR, "Recovery info size of new file is incompatible with current object size\n" );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      errno = ENAMETOOLONG; // as the prev file succeeded, it must be a longer name issue
      return NULL;
   }

   // identify if we must move on to a new data object
   // Note -- we *must* skip to the next object if this is an EDIT stream
   if ( stream->type != CREATE_STREAM  ||
        stream->filecount >= newfile.ftag.objfiles  ||
        ( newfile.ftag.offset + newfile.ftag.recoverybytes) >= newfile.ftag.objsize ) {
      // we'll need to start outputting this file to a new data object
      newfile.ftag.objno++;
      newfile.ftag.offset = stream->recoveryheaderlen;  // data will begin after the recov object header
   }

   // attach updated ftag value to the new file
   if ( putftag( stream, &(newfile) ) ) {
      LOG( LOG_ERR, "Failed to initialize FTAG value on target file\n" );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      if ( errno == EBADFD ) { errno = ENOMSG; } // don't allow our reserved EBADFD value
      return NULL;
   }

   // link the new file into the user namespace
   if ( linkfile( stream, &(newfile), path ) ) {
      LOG( LOG_ERR, "Failed to link reference file to target user path: \"%s\"\n", path );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      if ( errno == EBADFD ) { errno = ENOMSG; } // don't allow our reserved EBADFD value
      return NULL;
   }
   free( rpath ); // finally done with rpath

   // PREVIOUS FILE COMPLETION

   // Note -- We are now beyond the point of no return.
   //         Any errors now require us to 'fail forward', progressing the stream regardless.
   char errorflag = 0;  // just report errors, don't return

   // if this is a create stream, and either...
   //    we ARE already writing to an active data object
   //  or
   //    we ARE NOT relying on the metadata FS for data storage
   // ...then we must store recovery info for the previous file
   if ( stream->type == CREATE_STREAM  &&
         ( stream->datahandle != NULL  ||  ms->directchunks == 0 ) ) {

      stream->finfo.eof = 1; // this is the end of the previous file

      // if we haven't ever opened the current data object, we now need to
      if ( stream->datahandle == NULL ) {
         if ( opencurrentobj( stream ) ) {
            LOG( LOG_ERR, "Failed to create initial data object\n" );
            errorflag = 1;
         }
      }
      if ( errorflag == 0 ) {
         // allocate a recovery info string
         size_t recoverybytes = curfile->ftag.recoverybytes;
         char* recovinfo = malloc( recoverybytes + 1 );
         if ( recovinfo == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for recovery info string\n" );
            errorflag = 1;
         }
         // populate recovery info string
         else if ( recovery_finfotostr( &(stream->finfo), recovinfo, recoverybytes + 1 ) != recoverybytes ) {
            LOG( LOG_ERR, "File recovery info has an inconsistent length\n" );
            errorflag = 1;
            free( recovinfo );
         }
         // Note -- previous writes should have ensured we have at least 'recoverybytes' of available obj space
         // write out the recovery string
         else if ( ne_write( stream->datahandle, recovinfo, recoverybytes ) != recoverybytes ) {
            LOG( LOG_ERR, "Failed to store file recovery info to data object\n" );
            errorflag = 1;
            free( recovinfo );
         }
         else {
            free( recovinfo ); // done with recovery info string
            stream->offest += recoverybytes; // update our object offset
         }
      }
   }

   // mark the current file as FINALIZED
   curfile->ftag.state = FTAG_FIN | ( curfile->ftag.state & ~(FTAG_DATASTATE) );

   // check if we need to finalize our current files
   if ( newfile.ftag.objno != stream->objno  ||  errorflag != 0 ) {
      char globalerror = errorflag; // failure to write recov info is unrecoverable
      // close the data object, if we have one
      if ( stream->datahandle != NULL  &&  ne_close( stream->datahandle ) ) {
         LOG( LOG_ERR, "Failed to sync current data object\n" );
         errorflag = 1;
         globalerror = 1; // don't indicate success for ANY files
      }
      stream->datahandle == NULL;
      // update all currently referenced files
      for( ; stream->filecount > 0; stream->filecount-- ) {
         STREAMFILE tmpfile = stream->files + ( stream->filecount - 1);
         if ( globalerror == 0 ) {
            // set ftag to readable and complete state
            tmpfile->ftag.state = ( FTAG_COMP & FTAG_READABLE ) | ( tmpfile->ftag.state & ~(FTAG_DATASTATE) );
            // truncate the file to an appropriate length
            if ( ms->mdal->ftruncate( tmpfile->metahandle, tmpfile->ftag.bytes ) ) {
               LOG( LOG_ERR, "Failed to truncate file %zu to proper size\n", stream->filecount );
               errorflag = 1;
            }
            // assuming no errors on this file, set an updated ftag value
            else if ( putftag( stream, tmpfile ) ) {
               LOG( LOG_ERR, "Failed to update FTAG on file %zu to complete state\n", stream->filecount );
               errorflag = 1;
            }
            // assuming no errors on this file, set atime/mtime values
            else if ( ms->mdal->futimens( tmpfile->metahandle, tmpfile->times ) ) {
               LOG( LOG_ERR, "Failed to update time values on file %zu\n", stream->filecount );
               errorflag = 1;
            }
         }
         // close the metadata handle for the file
         if ( ms->mdal->close( tmpfile->metahandle ) ) {
            // worth warning about, but probably not a serious issue
            LOG( LOG_WARNING, "Failed to close metahandle for file %zu\n", stream->filecount );
         }
      }
   }
   else {
      // at least update the ftag value of our current file
      if ( putftag( stream, curfile ) ) {
         LOG( LOG_ERR, "Failed to update FTAG state of previous file\n" );
         errorflag = 1;
      }
   }

   // update our stream values to match new ones
   stream->objno = newfile.ftag.objno;
   stream->offset = newfile.ftag.offset;
   if ( stream->finfo.path ) { free( stream->finfo.path ); } // free old recov path
   stream->finfo = newfinfo;
   if ( stream->filealloc < stream->filecount + 1 ) {
      // allocate more file references, if necessary
      stream->filealloc = allocfiles( &(stream->files), stream->filecount, ds->objfiles );
      if ( stream->filealloc == 0 ) {
         LOG( LOG_ERR, "Failed to allocate space for additional file reference\n" );
         // can't store new file info, so it must be freed
         ms->mdal->close( newfile.metahandle );
         if ( newfile.ftag.ctag ) { free( newfile.ftag.ctag ); }
         if ( newfile.ftag.streamid ) { free( newfile.ftag.streamid); }
         stream->filecount = 0; // ensure we never attempt to reference other files again
         errorflag = 1;
      }
   }
   if ( stream->filealloc ) {
      // so long as we didn't lose this allocate, store our new file reference
      stream->files[stream->filecount] = newfile;
      stream->filecount++;
   }

   // catch any errors with the stream itself
   if ( errorflag ) {
      stream->type = ERROR_STREAM; // render the stream unusable
      errno = EBADFD; // produce a special errno value
      return NULL;
   }

   // now that it references a new file, this can be considered a CREAT stream
   stream->type = CREAT_STREAM;

   // otherwise, return the same stream reference
   return stream;
}


DATASTREAM datastream_init( const char* path, const marfs_ns* ns, const marfs_config* config ) {
   // create some shorthand references
   marfs_ms* ms = &(ns->prepo->metascheme);
   marfs_ds* ds = &(ns->prepo->datascheme);

   // allocate the new datastream and check for success
   DATASTREAM stream = genstream( READ_STREAM, ns, config );
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new datastream\n" );
      return NULL;
   }

   // open a metadata handle for the target file
   stream->files[0].metahandle = ms->mdal->open( stream->mdalctxt, path, O_RDONLY );
   if( stream->files[0].metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to open metadata handle for target path: \"%s\"\n", path );
      freestream( stream );
      return NULL;
   }

   // retrieve ftag values for the existing file
   if ( getftag( stream, &(stream->files[0]) ) ) {
      LOG( LOG_ERR, "Failed to retrieve ftag value for target file: \"%s\"\n", path );
      freestream( stream );
      return NULL;
   }

   // set stream offsets to align with the start of the file
   stream->objno = stream->files[0].ftag.objno;
   stream->offset = stream->files[0].ftag.offset;

   // verify that this file is readable via a new handle
   if ( ( stream->files[0].ftag.state & FTAG_READABLE ) == 0 ) {
      LOG( LOG_ERR, "Cannot read from a file before it has been completed\n" );
      freestream( stream );
      return NULL;
   }

   // identify per-object recovery header info
   RECOVERY_HEADER header = 
   {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = stream->files[0].ftag.ctag,
      .streamid = stream->files[0].ftag.streamid
   };
   stream->recoveryheaderlen = recovery_headertostr( &(header), NULL, 0 );

   return stream;
}


DATASTREAM datastream_rcont( DATASTREAM stream, const char* path ) {
   // check for NULL stream or improper stream type
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL stream reference\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( stream->type != READ_STREAM ) {
      LOG( LOG_ERR, "Recieved a non-READ stream reference\n" );
      errno = EINVAL;
      return NULL;
   }

   // create some shorthand references
   marfs_ms* ms = &(stream->ns->prepo->metascheme);
   marfs_ds* ds = &(stream->ns->prepo->datascheme);

   // open a metadata handle for the target file
   struct streamfile_struct oldfile = stream->files[0]; // track the old file reference
   stream->files[0].metahandle = ms->mdal->open( stream->mdalctxt, path, O_RDONLY );
   if( stream->files[0].metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to open metadata handle for target path: \"%s\"\n", path );
      stream->files[0] = oldfile; // restore old references
      return NULL;
   }

   // retrieve ftag values for the existing file
   if ( getftag( stream, &(stream->files[0]) ) ) {
      LOG( LOG_ERR, "Failed to retrieve ftag value for target file: \"%s\"\n", path );
      stream->files[0] = oldfile; // restore old references
      return NULL;
   }

   // verify that this file is writable via a new handle
   if ( ( stream->files[0].ftag.state & FTAG_READABLE ) == 0 ) {
      LOG( LOG_ERR, "Attempting to edit file before original handle has been released\n" );
      stream->files[0] = oldfile; // restore old references
      return NULL;
   }

   // if we have an data handle, we need to check if it is still valid
   if ( stream->datahandle ) {
      char resethandle = 0;
      // check stream identifiers
      if ( strcmp( stream->files[0].ftag.streamid, oldfile.ftag.streamid )  ||
           strcmp( stream->files[0].ftag.ctag, oldfile.ftag.ctag ) ) {
         resethandle = 1;
      }
      // check object number
      if ( stream->objno != oldfile.ftag.objno ) {
         resethandle = 1;
      }
      // destroy the object reference, if it is outdated
      if ( resethandle ) {
         if ( ne_close( stream->datahandle ) ) {
            LOG( LOG_WARNING, "Failed to close previous data handle\n" );
         }
         stream->datahandle = NULL;
      }
   }

   // free old file reference info
   free( oldfile.ftag.streamid );
   if ( oldfile.ftag.ctag ) { free( oldfile.ftag.ctag ); }

   // set stream offsets to align with the start of the file
   stream->objno = stream->files[0].ftag.objno;
   stream->offset = stream->files[0].ftag.offset;


   // identify per-object recovery header info
   RECOVERY_HEADER header = 
   {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = stream->files[0].ftag.ctag,
      .streamid = stream->files[0].ftag.streamid
   };
   stream->recoveryheaderlen = recovery_headertostr( &(header), NULL, 0 );

   return stream;
}


int datastream_setrecoverypath( DATASTREAM stream, const char* recovpath ) {
   // check for NULL stream or improper stream type
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL stream reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( stream->type != CREAT_STREAM  &&  stream->type != EDIT_STREAM ) {
      LOG( LOG_ERR, "Recieved a non-CREAT/EDIT stream reference\n" );
      errno = EINVAL;
      return -1;
   }

   // we don't allow an edit of the recovery path after data output has begun
   if( stream->datahandle ) {
      LOG( LOG_ERR, "Recovery path cannot be altered after data output has begun\n" );
      errno = EINVAL;
      return -1;
   }

   // update the recovery path value
   char* oldpath = stream->finfo.path;
   stream->finfo.path = strdup( recovpath );
   if ( stream->finfo.path == NULL ) {
      LOG( LOG_ERR, "Failed to update recovery path value\n" );
      stream->finfo.path = oldpath;
      return -1;
   }

   // identify the new recovery info size
   size_t recovsize = recovery_finfotostr( &(stream->finfo), NULL, 0 );
   if ( recovsize == 0 ) {
      LOG( LOG_ERR, "Failed to calculate size of new recovery info\n" );
      free( stream->finfo.path );
      stream->finfo.path = oldpath;
      return -1;
   }

   // ensure the recovery info size is compatible with the current object size
   if ( (stream->recoveryheaderlen + recovsize) >= stream->files[stream->filecount - 1].ftag.objsize ) {
      LOG( LOG_ERR, "Resulting recovery info size is incompatible with current object size\n" );
      free( stream->finfo.path );
      stream->finfo.path = oldpath;
      return -1;
   }

   // ensure that the new recovery info size does not contradict the existing value
   if ( stream->files[stream->filecount - 1].ftag.recoverybytes != recovsize ) {
      // for EDIT streams, this is a hard error
      if ( stream->type == EDIT_STREAM ) {
         LOG( LOG_ERR, "Resulting recovery info size contradicts FTAG value\n" );
         free( stream->finfo.path );
         stream->finfo.path = oldpath;
         return -1;
      }
      // for CREAT streams, we will actually adjust the FTAG value
      else {
         size_t origrecovbytes = stream->files[stream->filecount - 1].ftag.recoverybytes;
         stream->files[stream->filecount - 1].ftag.recoverybytes = recovsize;
         if ( putftag( stream, &(stream->files[stream->filecount - 1]) ) ) {
            LOG( LOG_ERR, "Failed to update FTAG to reflect new recovery info size\n" );
            stream->files[stream->filecount - 1].ftag.recoverybytes = origrecovbytes;
            free( stream->finfo.path );
            stream->finfo.path = oldpath;
            return -1;
         }
      }
   }

   // all done
   free( oldpath );
   return 0;
}


int datastream_extend( DATASTREAM stream, off_t length ) {
   // check for NULL stream or improper stream type
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL stream reference\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( stream->type != CREAT_STREAM ) {
      // only CREAT streams can modify the size of a file
      LOG( LOG_ERR, "Recieved a non-CREAT stream reference\n" );
      errno = EINVAL;
      return NULL;
   }

   // create some shorthand references
   marfs_ms* ms = &(stream->ns->prepo->metascheme);
   marfs_ds* ds = &(stream->ns->prepo->datascheme);
}


int datastream_chunkbounds( DATASTREAM stream, int chunknum, off_t* offset, size_t* size );
off_t datastream_seek( DATASTREAM stream, off_t offset, int whence );
int datastream_truncate( DATASTREAM stream, off_t length );
int datastream_utimens( DATASTREAM stream, const struct timespec times[2] );
size_t datastream_write( DATASTREAM stream, const void* buff, size_t size );
size_t datastream_read( DATASTREAM stream, void* buffer, size_t size );
int datastream_flush( DATASTREAM stream );
int datastream_close( DATASTREAM stream );
int datastream_release( DATASTREAM stream );


