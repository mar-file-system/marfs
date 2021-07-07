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

typedef enum
{
   CREATE_STREAM,
   EDIT_STREAM,
   READ_STREAM
} STREAM_TYPE;



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
   STREAMFILE  files;
   size_t      filecount;
   size_t      filealloc;
   char*       ftagstr;
   size_t      ftagstrsize;
   size_t      objno;
   size_t      offset;
   RECOVERY_FINFO finfo;
}* DATASTREAM;

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
      stream->filealloc = allocfiles( stream->files, stream->filecount, 1 );
   }
   else {
      // Create streams are only restriced by the object packing limits
      stream->filealloc = allocfiles( stream->files, stream->filecount, ds->objfiles );
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

int genrecoveryinfo( DATASTREAM stream, STREAMFILE file, const char* path ) {
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
   stream->finfo.inode = stval.st_ino;
   stream->finfo.mode = stval.st_mode;
   stream->finfo.owner = stval.st_uid;
   stream->finfo.group = stval.st_gid;
   stream->finfo.size = 0;
   stream->finfo.mtime.tv_sec = stval.st_mtim.tv_sec;
   stream->finfo.mtime.tv_nsec = stval.st_mtim.tv_nsec;
   stream->finfo.eof = 0;
   stream->finfo.path = strdup( path );
   if ( stream->finfo.path == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate file path into stream recovery info\n" );
      return -1;
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

   // generate a new streamID ( the ONLY func that generates a streamid; all other funcs take existing )
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
   if ( genrecoveryinfo( stream, &(stream->files[0]), path ) ) {
      LOG( LOG_ERR, "Failed to populate recovery info for file: \"%s\"\n", path );
      ms->mdal->unlinkref( stream->mdalctxt, rpath );
      free( rpath );
      freestream( stream );
      return NULL;
   }
   stream->files[0].ftag.recoverybytes = recovery_finfotostr( &(stream->finfo), NULL, 0 );
   if ( stream->files[0].ftag.recoverybytes == 0 ) {
      LOG( LOG_ERR, "Failed to calculate recovery info size for created file\n" );
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
   stream->files[0].metahandle = ms->mdal->open( stream->mdalctxt, path, O_WRONLY );
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

   // verify that this file is writable via a new handle
   if ( ( stream->files[0].ftag.state & FTAG_WRITABLE ) == 0 ) {
      LOG( LOG_ERR, "Attempting to edit file before original handle has been released\n" );
      freestream( stream );
      return NULL;
   }

   // identify file attributes, for recovery info
   if ( genrecoveryinfo( stream, &(stream->files[0]), path ) ) {
      LOG( LOG_ERR, "Failed to populate recovery info for file: \"%s\"\n", path );
      freestream( stream );
      return NULL;
   }

   return stream;
}

DATASTREAM datastream_cont( DATASTREAM stream, const char* path, mode_t mode ) {
   // verify we haven't just been given a NULL stream
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL stream value\n" );
      return NULL;
   }

   // create some shorthand references
   marfs_ms* ms = &(ns->prepo->metascheme);
   marfs_ds* ds = &(ns->prepo->datascheme);

   // ensure our stream handle is in an appropriate mode
   if ( stream->type == READ_STREAM ) {
      LOG( LOG_ERR, "Cannot continue a READ datastream\n" );
      return NULL;
   }

   // if this is a create stream, we must append recovery info for the previous file
   if ( stream->type == CREATE_STREAM ) {
      // if we haven't ever opened the current data object, we now need to
      if ( stream->datahandle == NULL ) {
         // find the length of the current object name
         ssize_t objnamelen = ftag_objectname( &(stream->files[stream->filecount - 1]), NULL, 0 );
         if ( objnamelen <= 0 ) {
            LOG( LOG_ERR, "Failed to determine object path from current ftag\n" );
            return NULL;
         }
         // allocate a new string, and populate it with the object name
         char* objname = malloc( sizeof(char) * (objnamelen + 1) );
         if ( objname == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for new object name\n" );
            return NULL;
         }
         if ( objnamelen != ftag_objectname( &(stream->files[stream->filecount - 1]), objname, objnamelen + 1 ) ) {
            LOG( LOG_ERR, "Ftag producing inconsistent object name string\n" );
            free( objname );
            return NULL;
         }
         // identify the pod/cap/scatter values for the current object
         int pod = -1;
         int cap = -1;
         int scatter = -1;
         int iteration = 0;
         for( ; iteration < 3; iteration++ ) {
            // determine which table we are currently pulling from
            char* itername = "scatter";
            HASH_TABLE curtable = ds->scattertable;
            int* tgtval = &scatter;
            if ( iteration < 1 ) {
               itername = "pod";
               curtable = ds->podtable;
               tgtval = &pod;
            }
            else if ( iteration < 2 ) {
               itername = "cap";
               curtable = ds->captable;
               tgtval = &cap;
            }
            // hash our object name, to identify a target node
            HASH_NODE* node = NULL;
            if ( hash_lookup( curtable, objname, &node ) ) {
               LOG( LOG_ERR, "Failed to lookup %s location for new object \"%s\"\n", itername, objname );
               free( objname );
               return NULL;
            } 
            // parse our nodename, to produce an integer value
            char* endptr = NULL;
            unsigned long long parseval = strtoull( node->name, &(endptr), 10 );
            if ( *endptr != '\0'  ||  parseval >= INT_MAX ) {
               LOG( LOG_ERR, "Failed to parse %s value of \"%s\" for new object \"%s\"\n", itername, node->name, objname );
               free( objname );
               return NULL;
            }
            // assign the parsed value to the appropriate var
            *tgtval = (int)parseval;
         }
      }
   }


