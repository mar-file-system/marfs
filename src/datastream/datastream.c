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

#include "marfs_auto_config.h"
#ifdef DEBUG_DS
#define DEBUG DEBUG_DS
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "datastream"

#include <logging.h>
#include "datastream.h"
#include "general_include/numdigits.h"

#include <time.h>


//   -------------   INTERNAL DEFINITIONS    -------------


#define INITIAL_FILE_ALLOC 64
#define FILE_ALLOC_MULT     2


typedef struct datastream_position_struct {
   size_t totaloffset;      // offset from beginning of file ( SEEK_SET w/ this val would be no-op; includes 'fake' data )
   size_t dataremaining;    // amount of actual data ( data objects, not truncated ) beyond this position
   size_t excessremaining;  // amount of 'fake' data ( zero-fill, from truncate beyond EOF ) beyond this position
   size_t objno;            // currently referenced data object number
   size_t offset;           // current offset within the referenced data object
   size_t excessoffset;     // current offset within 'fake' data ( zero-fill, from truncate ), beyond end of actual data
   size_t dataperobj;       // maximum amount of this file's data than can be stored in each data object
                            //   ( ftag->objsize - recovHeaderLength ) - recovFinfoLength
                            //   Note -- this changes per-file, within the same object ( recovFinfoLength differs )
} DATASTREAM_POSITION;


//   -------------   INTERNAL FUNCTIONS    -------------

/**
 * Generate a new Stream ID string and recovery header size based on that ID
 * @param char** streamid : Reference to be populated with the Stream ID string
 *                          ( client is responsible for freeing this string )
 * @param size_t* rheadersize : Reference to be populated with the recovery header size
 * @return int : Zero on success, or -1 on failure
 */
int genstreamid(char* ctag, const marfs_ns* ns, char** streamid, size_t* rheadersize) {
   struct timespec curtime;
   if (clock_gettime(CLOCK_REALTIME, &curtime)) {
      LOG(LOG_ERR, "Failed to determine the current time\n");
      return -1;
   }
   char* nsrepo = NULL;
   char* nspath = NULL;
   if (config_nsinfo(ns->idstr, &(nsrepo), &(nspath))) {
      LOG(LOG_ERR, "Failed to retrieve path/repo info for NS: \"%s\"\n", ns->idstr);
      return -1;
   }
   char* nsparse = nspath;
   size_t nspathlen = 0;
   for (; *nsparse != '\0'; nsparse++) {
      // replace all '/' chars in a NS path with '#'
      if (*nsparse == '/') {
         *nsparse = '#';
      }
      nspathlen++;
   }
   size_t streamidlen = SIZE_DIGITS;  // to account for tv_sec (see numdigits.h)
   streamidlen += SIZE_DIGITS; // to account for tv_nsec
   streamidlen += strlen(nsrepo) + nspathlen; // to include NS/Repo info
   streamidlen += 4; // for '#'/'.' seperators and null terminator
   char* newstreamid = NULL;
   if ((newstreamid = malloc(sizeof(char) * streamidlen)) == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for streamID\n");
      free(nsrepo);
      free(nspath);
      return -1;
   }
   ssize_t prres = snprintf(newstreamid, streamidlen, "%s#%s#%zd.%ld",
      nsrepo, nspath, curtime.tv_sec, curtime.tv_nsec);
   if (prres <= 0 || prres >= streamidlen) {
      LOG(LOG_ERR, "Failed to generate streamID value\n");
      free(nsrepo);
      free(nspath);
      free(newstreamid);
      return -1;
   }
   free(nsrepo);
   free(nspath);

   // establish our recovery header length
   RECOVERY_HEADER header = {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = ctag,
      .streamid = newstreamid
   };
   size_t newrecoveryheaderlen = recovery_headertostr(&(header), NULL, 0);
   if (newrecoveryheaderlen < 1) {
      LOG(LOG_ERR, "Failed to identify length of create stream recov header\n");
      free(newstreamid);
      return -1;
   }

   // populate our return values
   *streamid = newstreamid;
   *rheadersize = newrecoveryheaderlen;
   return 0;
}

/**
 * Generate a repack marker path for a file
 * @param const char* rpath : Reference path of the repacked file ( can be left NULL, if unknown )
 * @param FTAG* ftag : FTAG value of the repacked file
 * @param marfs_ms* ms : Current marfs metascheme
 * @return char* : String path of the repack marker file
 */
char* repackmarkertgt( const char* rpath, FTAG* ftag, const marfs_ms* ms ) {
   // check for an rpath arg
   char* refpath = NULL;
   if ( rpath == NULL ) {
      // gen an rpath if we didn't get one
      refpath = datastream_genrpath( ftag, ms );
   }
   else {
      refpath = strdup( rpath );
   }
   if ( refpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify/duplicate rpath of the repacked file\n" );
      return NULL;
   }
   // identify the parent dir of the refpath
   const char* rparse = refpath;
   const char* rparent = refpath;
   while ( *rparse != '\0' ) {
      if ( *rparse == '/' ) { rparent = rparse + 1; }
      rparse++;
   }
   size_t rparentlen = rparent - refpath; // pointer arithmetic to calc length of parent dir string
   // identify the repack marker name for this file
   size_t rmarklen = ftag_repackmarker( ftag, NULL, 0 );
   if ( rmarklen == 0 ) {
      LOG( LOG_ERR, "Failed to identify repack marker for reference target: \"%s\"\n", refpath );
      free( refpath );
      return NULL;
   }
   // construct the final string
   char* rmarkstr = malloc( sizeof(char) * (rparentlen + rmarklen + 1) );
   if ( rmarkstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a repack marker path\n" );
      free( refpath );
      return NULL;
   }
   snprintf( rmarkstr, rparentlen + 1, "%s", refpath );
   free( refpath );
   if ( ftag_repackmarker( ftag, rmarkstr + rparentlen, rmarklen + 1 ) != rmarklen ) {
      LOG( LOG_ERR, "Repack marker has an inconsistent length\n" );
      free(rmarkstr);
      errno = EFAULT;
      return NULL;
   }
   return rmarkstr;
}

/**
 * Frees the provided stream, aborting the datahandle and closing all metahandles
 * @param DATASTREAM stream : DATASTREAM to be freed
 */
void freestream(DATASTREAM stream) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   // abort any data handle
   if (stream->datahandle && ne_abort(stream->datahandle)) {
      LOG(LOG_WARNING, "Failed to abort stream datahandle\n");
   }
   // free any string elements
   if (stream->ctag) {
      free(stream->ctag);
   }
   if (stream->streamid) {
      free(stream->streamid);
   }
   if (stream->ftagstr) {
      free(stream->ftagstr);
   }
   if (stream->finfostr) {
      free(stream->finfostr);
   }
   if (stream->finfo.path) {
      free(stream->finfo.path);
   }
   // iterate over all file references and clean them up
   if (stream->files) {
      size_t curfile = 0;
      for (; curfile < stream->curfile + 1; curfile++) {
         LOG(LOG_INFO, "Closing file %d\n", curfile);
         if (stream->files[curfile].metahandle && ms->mdal->close(stream->files[curfile].metahandle)) {
            LOG(LOG_WARNING, "Failed to close meta handle for file %zu\n", curfile);
         }
      }
      // free the file list itself
      free(stream->files);
   }
   // finally, free the stream itself
   free(stream);
}

/**
 * Extends the current STREAMFILE list allocation
 * @param STREAMFILE** files : Reference to the STREAMFILE list to be extended
 * @param size_t current : Current length of the STREAMFILE list
 * @param size_t max : Maximum allowable length of the list ( ignored if zero )
 * @return size_t : Resulting length of the extended list, or zero if a failure occurred
 */
size_t allocfiles(STREAMFILE** files, size_t current, size_t max) {
   // calculate the target size of the file list
   size_t allocsize = 0;
   if (current < INITIAL_FILE_ALLOC) {
      allocsize = INITIAL_FILE_ALLOC;
   }
   else {
      allocsize = current * FILE_ALLOC_MULT;
   }
   if (max && allocsize > max) {
      allocsize = max;
   }
   // realloc the list ( this is much simpler than allocating large linked list blocks )
   STREAMFILE* newfiles = realloc(*files, allocsize * sizeof(struct streamfile_struct));
   if (newfiles == NULL) {
      LOG(LOG_ERR, "Failed to allocate stream filelist of size %zu\n", allocsize);
      return 0;
   }
   // NULL out all metahandles, so that we never try to close() them
   for (; current < allocsize; current++) {
      newfiles[current].metahandle = NULL;
   }
   *files = newfiles;
   return allocsize;
}

/**
 * Attach the given STREAMFILE's FTAG attribute
 * @param DATASTREAM stream : Current DATASTREAM
 * @param STREAMFILE* file : Reference to the STREAMFILE to have its FTAG updated
 * @return int : Zero on success, -1 if a failure occurred
 */
int putftag(DATASTREAM stream, STREAMFILE* file) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   // populate the ftag string format
   ssize_t prres = ftag_tostr(&(file->ftag), stream->ftagstr, stream->ftagstrsize);
   if (prres <= 0) {
      LOG(LOG_ERR, "Failed to populate ftag string for stream\n");
      return -1;
   }
   if (prres >= stream->ftagstrsize) {
      stream->ftagstrsize = 0;
      free(stream->ftagstr);
      stream->ftagstr = malloc(sizeof(char) * (prres + 1));
      if (stream->ftagstr == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for ftag string\n");
         return -1;
      }
      stream->ftagstrsize = prres + 1;
      // reattempt, with a longer target string
      prres = ftag_tostr(&(file->ftag), stream->ftagstr, stream->ftagstrsize);
      if (prres >= stream->ftagstrsize) {
         LOG(LOG_ERR, "Ftag string has an inconsistent length\n");
         errno = EFAULT;
         return -1;
      }
   }
   if ( stream->type == REPACK_STREAM ) {
      if (ms->mdal->fsetxattr(file->metahandle, 1, TREPACK_TAG_NAME, stream->ftagstr, prres, 0)) {
         LOG(LOG_ERR, "Failed to attach marfs repack target ftag value: \"%s\"\n", stream->ftagstr);
         return -1;
      }
   }
   else {
      if (ms->mdal->fsetxattr(file->metahandle, 1, FTAG_NAME, stream->ftagstr, prres, 0)) {
         LOG(LOG_ERR, "Failed to attach marfs ftag value: \"%s\"\n", stream->ftagstr);
         return -1;
      }
   }

   return 0;
}

/**
 * Retrieve a given STREAMFILE's FTAG attribute
 * @param DATASTREAM stream : Current DATASTREAM
 * @param STREAMFILE* file : Reference to the STREAMFILE to have its FTAG retrieved
 * @return int : Zero on success;
 *               One if no FTAG value exists;
 *               -1 if a failure occurred
 */
int getftag(DATASTREAM stream, STREAMFILE* file) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   // attempt to retrieve the ftag attr value ( leaving room for NULL terminator )
   ssize_t getres = ms->mdal->fgetxattr(file->metahandle, 1, FTAG_NAME, stream->ftagstr, stream->ftagstrsize - 1);
   if (getres <= 0) {
      LOG(LOG_ERR, "Failed to retrieve ftag value for stream file\n");
      if (errno == ENODATA) {
         LOG(LOG_INFO, "Preserving meta handle for potential direct read\n");
         return 1;
      }
      return -1;
   }
   if (getres >= stream->ftagstrsize) {
      stream->ftagstrsize = 0;
      free(stream->ftagstr);
      LOG(LOG_INFO, "Expanding FTAG-String to size of %zd\n", getres + 1);
      stream->ftagstr = malloc(sizeof(char) * (getres + 1));
      if (stream->ftagstr == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for ftag string buffer\n");
         return -1;
      }
      stream->ftagstrsize = getres + 1;
      // reattempt, with a longer target string
      getres = ms->mdal->fgetxattr(file->metahandle, 1, FTAG_NAME, stream->ftagstr, stream->ftagstrsize - 1);
      if (getres >= stream->ftagstrsize) {
         LOG(LOG_ERR, "Ftag value of file has an inconsistent length\n");
         errno = EBUSY;
         return -1;
      }
   }
   // ensure our string is NULL terminated
   *(stream->ftagstr + getres) = '\0';
   // attempt to set struct values based on the ftag string
   if (ftag_initstr(&(file->ftag), stream->ftagstr)) {
      LOG(LOG_ERR, "Failed to initialize ftag values for file\n");
      return -1;
   }
   return 0;
}

/**
 * Link the given reference path to the given target path, potentially unlinking an
 * existing target
 * @param DATASTREAM stream : Current DATASTREAM
 * @param const char* refpath : Reference path to be linked from
 * @param const char* tgtpath : Target path to be linked to
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT
 * @return int : Zero on success, -1 on failure
 */
int linkfile(DATASTREAM stream, const char* refpath, const char* tgtpath, MDAL_CTXT ctxt) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   // attempt to link the specified file to the specified user path
   if (ms->mdal->linkref(ctxt, 0, refpath, tgtpath)) {
      // if we got EEXIST, attempt to unlink the existing target and retry
      if (errno != EEXIST) {
         // any non-EEXIST error is fatal
         LOG(LOG_ERR, "Failed to link reference file to final location\n");
         return -1;
      }
      else if (ms->mdal->unlink(ctxt, tgtpath) && errno != ENOENT) {
         // ENOENT would indicate that another proc has unlinked the conflicting file for us
         //   Otherwise, we have to fail
         LOG(LOG_ERR, "Failed to unlink existing file: \"%s\"\n", tgtpath);
         return -1;
      }
      else if (ms->mdal->linkref(ctxt, 0, refpath, tgtpath)) {
         // This indicates either we're racing with another proc, or something more unusual
         //   Just fail out with whatever errno we get from flink()
         LOG(LOG_ERR, "Failed to link reference file to final location after retry\n");
         return -1;
      }
   }
   return 0;
}

/**
 * Populate the given RECOVERY_FINFO struct with values based on the given STREAMFILE
 * @param DATASTREAM stream : Current DATASTREAM
 * @param RECOVERY_FINFO* finfo : Reference to the RECOVERY_FINFO struct to be populated
 * @param STREAMFILE* file : Reference to the STREAMFILE to generate values for
 * @param const char* path : Path of the STREAMFILE ( to be included in RECOVERY_FINFO )
 * @return int : Zero on success, -1 if a failure occurred
 */
int genrecoveryinfo(DATASTREAM stream, RECOVERY_FINFO* finfo, STREAMFILE* file, const char* path) {
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   // identify file attributes, for recovery info
   struct stat stval;
   if (ms->mdal->fstat(file->metahandle, &(stval))) {
      LOG(LOG_ERR, "Failed to stat meta file for recovery info values\n");
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
   if (stream->type == READ_STREAM) {
      // read streams can terminate early
      // we just need to track the size of the metadata file ( this defines logical file bounds )
      finfo->size = stval.st_size;
      return 0;
   }

   // align our finalized file times with those we will be using in recovery info
   file->times[0] = stval.st_atim;
   file->times[1] = stval.st_mtim;

   if ( stream->type == REPACK_STREAM ) {
      // repack streams terminate early, as we don't yet have a recovery path
      return 0;
   }

   // store the recovery path
   finfo->path = strdup(path);
   if (finfo->path == NULL) {
      LOG(LOG_ERR, "Failed to duplicate file path into recovery info\n");
      return -1;
   }

   // calculate the length of the recovery info
   size_t recoverybytes = recovery_finfotostr(finfo, NULL, 0);
   if (recoverybytes == 0) {
      LOG(LOG_ERR, "Failed to calculate recovery info size for \"%s\"\n", path);
      free(finfo->path);
      finfo->path = NULL;
      return -1;
   }

   // populate the recovery size, if absent
   // NOTE -- if the recovery size is inconsistent with the existing size, this will
   //         be caught when attempting to write out recovery info
   if (file->ftag.recoverybytes == 0) {
      file->ftag.recoverybytes = recoverybytes;
   }

   return 0;
}

/**
 * Create a new file at the current ( 'curfile' ) STREAMFILE reference position
 * @param DATASTREAM stream : Current DATASTREAM
 * @param const char* path : Path of the file to be created
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT
 * @param mode_t mode : Mode of the file to be created
 * @return int : Zero on success, or -1 on failure
 */
int create_new_file(DATASTREAM stream, const char* path, MDAL_CTXT ctxt, mode_t mode) {
   // NOTE -- it is the responsibility of the caller to set curfile/fileno/objno/offset
   //         values to the appropraite start positions prior to calling
   // populate shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   // construct a reference struct for our new file
   struct streamfile_struct newfile =
   {
      .metahandle = NULL,
      .ftag.majorversion = FTAG_CURRENT_MAJORVERSION,
      .ftag.minorversion = FTAG_CURRENT_MINORVERSION,
      .ftag.ctag = stream->ctag,
      .ftag.streamid = stream->streamid,
      .ftag.objfiles = ds->objfiles,
      .ftag.objsize = ds->objsize,
      .ftag.fileno = stream->fileno,
      .ftag.objno = stream->objno,   // potentially modified below
      .ftag.offset = stream->offset, // potentially modified below
      .ftag.endofstream = 0,
      .ftag.protection = ds->protection,
      .ftag.bytes = 0,
      .ftag.availbytes = 0,
      .ftag.recoverybytes = 0, // modified below
      .ftag.state = FTAG_INIT,
      .times[0].tv_sec = 0,
      .times[0].tv_nsec = 0,
      .times[1].tv_sec = 0,
      .times[1].tv_nsec = 0,
      .dotimes = 1
   };

   // establish a reference path for the new file
   char* newrpath = datastream_genrpath(&(newfile.ftag), &(stream->ns->prepo->metascheme));
   if (newrpath == NULL) {
      LOG(LOG_ERR, "Failed to identify reference path for stream\n");
      if (errno == EBADFD) {
         errno = ENOMSG;
      } // don't allow our reserved EBADFD value
      return -1;
   }

   // create the reference file, ensuring we don't collide with an existing reference
   newfile.metahandle = ms->mdal->openref(ctxt, newrpath, O_CREAT | O_EXCL | O_WRONLY, mode);
   if (newfile.metahandle == NULL) {
      LOG(LOG_ERR, "Failed to create reference meta file: \"%s\"\n", newrpath);
      // a BUSY error is more indicative of the real problem
      if (errno == EEXIST) {
         errno = EBUSY;
      }
      // don't allow our reserved EBADFD value
      else if (errno == EBADFD) {
         errno = ENOMSG;
      }
      free(newrpath);
      return -1;
   }

   // identify file recovery info
   RECOVERY_FINFO newfinfo;
   if (genrecoveryinfo(stream, &(newfinfo), &(newfile), path)) {
      LOG(LOG_ERR, "Failed to populate recovery info for file: \"%s\"\n", path);
      ms->mdal->unlinkref(ctxt, newrpath);
      free(newrpath);
      if (errno == EBADFD) {
         errno = ENOMSG;
      } // don't allow our reserved EBADFD value
      return -1;
   }

   // ensure the recovery info size is compatible with the current object size
   if (newfile.ftag.objsize && (stream->recoveryheaderlen + newfile.ftag.recoverybytes) >= newfile.ftag.objsize) {
      LOG(LOG_ERR, "Recovery info size of new file is incompatible with current object size\n");
      ms->mdal->unlinkref(ctxt, newrpath);
      free(newrpath);
      errno = ENAMETOOLONG; // this is most likely an issue with path length
      return -1;
   }

   // ensure that the current object still has space remaining for this file
   if (newfile.ftag.objsize && (newfile.ftag.objsize - stream->offset) < newfile.ftag.recoverybytes) {
      // we're too far into the current obj to fit any more data
      LOG(LOG_INFO, "Shifting to new object, as current can't hold recovery info\n");
      newfile.ftag.objno++;
      newfile.ftag.offset = stream->recoveryheaderlen;
   }
   else if (newfile.ftag.objfiles && stream->curfile >= newfile.ftag.objfiles) {
      // there are too many files in the current obj to fit this one
      LOG(LOG_INFO, "Shifting to new object, as current can't hold another file\n");
      newfile.ftag.objno++;
      newfile.ftag.offset = stream->recoveryheaderlen;
   }

   // attach updated ftag value to the new file
   if (putftag(stream, &(newfile))) {
      LOG(LOG_ERR, "Failed to initialize FTAG value on target file\n");
      ms->mdal->unlinkref(ctxt, newrpath);
      free(newrpath);
      if (errno == EBADFD) {
         errno = ENOMSG;
      } // don't allow our reserved EBADFD value
      return -1;
   }

   // link the new file into the user namespace
   if (linkfile(stream, newrpath, path, ctxt)) {
      LOG(LOG_ERR, "Failed to link reference file to target user path: \"%s\"\n", path);
      ms->mdal->unlinkref(ctxt, newrpath);
      free(newrpath);
      if (errno == EBADFD) {
         errno = ENOMSG;
      } // don't allow our reserved EBADFD value
      return -1;
   }

   // check if the current stream has space for this new file ref
   if (stream->curfile >= stream->filealloc) {
      stream->filealloc = allocfiles(&(stream->files), stream->filealloc, ds->objfiles + 1);
      if (stream->filealloc == 0) {
         LOG(LOG_ERR, "Failed to expand file list allocation\n");
         stream->filealloc = stream->curfile - 1;
         ms->mdal->unlinkref(ctxt, newrpath);
         free(newrpath);
         if (errno == EBADFD) {
            errno = ENOMSG;
         } // don't allow our reserved EBADFD value
         return -1;
      }
   }
   free(newrpath); // finally done with rpath

   // update the stream with new file information
   stream->files[stream->curfile] = newfile;
   if (stream->finfo.path) {
      free(stream->finfo.path);
   }
   stream->finfo = newfinfo;
   stream->fileno = newfile.ftag.fileno;
   stream->objno = newfile.ftag.objno;
   stream->offset = newfile.ftag.offset;

   return 0;
}

/**
 * Open the specified file at the current ( 'curfile' ) STREAMFILE reference position
 * @param DATASTREAM stream : Current DATASTREAM
 * @param const char* path : Path of the file to be opened
 * @param char rpathflag : If zero, treat 'path' as a user path
 *                         If non-zero AND stream is a READ_STREAM, treat 'path' as a reference path
 *                         NOTE -- ALL datastream_*() functions should use USER PATHS ONLY
 *                                 This flag is intended to provide options to specific
 *                                 administrator programs ( see streamwalker.c, for example )
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT
 * @return int : Zero on success;
 *               One if no FTAG was found, but the meta handle has been preserved;
 *               or -1 on failure
 */
int open_existing_file(DATASTREAM stream, const char* path, char rpathflag, MDAL_CTXT ctxt) {

   // open a handle for the target file
   MDAL curmdal = stream->ns->prepo->metascheme.mdal;
   STREAMFILE* curfile = stream->files + stream->curfile;
   if (stream->type == READ_STREAM) {
      if (rpathflag) {
         LOG(LOG_INFO, "Opening BY REFERENCE PATH: \"%s\"\n", path);
         curfile->metahandle = curmdal->openref(ctxt, path, O_RDONLY, 0);
      }
      else {
         curfile->metahandle = curmdal->open(ctxt, path, O_RDONLY);
      }
   }
   else {
      curfile->metahandle = curmdal->open(ctxt, path, O_WRONLY);
   }
   if (curfile->metahandle == NULL) {
      LOG(LOG_ERR, "Failed to open metahandle for target file: \"%s\"\n", path);
      return -1;
   }

   // retrieve the file's FTAG info
   int ftagres = getftag(stream, stream->files + stream->curfile);
   if (ftagres) {
      LOG(LOG_ERR, "Failed to retrieve FTAG value of target file: \"%s\"\n", path);
      if (ftagres > 0 && stream->type == READ_STREAM) {
         // no FTAG value exists, but preserve the meta handle for potential direct read
         return 1;
      }
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   curfile->dotimes = 0; // make sure our time flag is cleared

   // check if it is safe to access this file
   if (stream->type == EDIT_STREAM &&
      !(curfile->ftag.state & FTAG_WRITEABLE) &&
      (curfile->ftag.state & FTAG_DATASTATE) != FTAG_COMP) {
      LOG(LOG_ERR, "Cannot edit a non-complete, non-extended file\n");
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      errno = EINVAL;
      return -1;
   }
   if ( stream->type == READ_STREAM &&
        !(curfile->ftag.state & FTAG_READABLE)) {
      LOG(LOG_ERR, "Target file is not yet readable\n");
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      errno = EINVAL;
      return -1;
   }

   // populate RECOVERY_FINFO
   if (genrecoveryinfo(stream, &(stream->finfo), curfile, path)) {
      LOG(LOG_ERR, "Failed to identify recovery info for target file: \"%s\"\n", path);
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      return -1;
   }

   // calculate the recovery header length
   RECOVERY_HEADER header =
   {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = curfile->ftag.ctag,
      .streamid = curfile->ftag.streamid
   };
   size_t recoveryheaderlen = recovery_headertostr(&(header), NULL, 0);
   if (recoveryheaderlen < 1) {
      LOG(LOG_ERR, "Failed to identify length of stream recov header\n");
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      return -1;
   }

   // the stream inherits string values from the FTAG
   stream->ctag = curfile->ftag.ctag;
   stream->streamid = curfile->ftag.streamid;
   stream->recoveryheaderlen = recoveryheaderlen; // make sure to set the header length
   // the stream also inherits position values from the FTAG
   stream->fileno = curfile->ftag.fileno;
   stream->objno = curfile->ftag.objno;
   stream->offset = curfile->ftag.offset;
   stream->excessoffset = 0; // always zero out this offset to start
   return 0;
}

/**
 * Prep the specified file for repack at the current ( 'curfile' ) STREAMFILE reference position
 * @param DATASTREAM stream : Current DATASTREAM
 * @param const char* path : Reference path of the file to be opened
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT
 * @return int : Zero on success;
 *               One if no FTAG was found, but the meta handle has been preserved;
 *               or -1 on failure
 */
int open_repack_file(DATASTREAM stream, const char* path, MDAL_CTXT ctxt) {

   // populate shorthand references
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);

   // check if the current stream has space for this new file ref
   if (stream->curfile >= stream->filealloc) {
      stream->filealloc = allocfiles(&(stream->files), stream->filealloc, ds->objfiles + 1);
      if (stream->filealloc == 0) {
         LOG(LOG_ERR, "Failed to expand file list allocation\n");
         stream->filealloc = stream->curfile - 1;
         if (errno == EBADFD) {
            errno = ENOMSG;
         } // don't allow our reserved EBADFD value
         return -1;
      }
   }

   // more shorthand refs
   MDAL curmdal = stream->ns->prepo->metascheme.mdal;
   STREAMFILE* curfile = stream->files + stream->curfile;
   // get file info ( need to stash times prior to any modification )
   struct stat stval;
   if (curmdal->statref(ctxt, path, &(stval))) {
      LOG(LOG_ERR, "Failed to stat meta file for initial time values: \"%s\"\n", path);
      return -1;
   }
   // open a handle for the tgt file
   curfile->metahandle = curmdal->openref( ctxt, path, O_RDWR, 0 );
   if ( curfile->metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to open target reference path: \"%s\"\n", path );
      return -1;
   }
   LOG( LOG_INFO, "Successfully opened repack reference target: \"%s\"\n", path );
   // record times and note to reset them at completion
   curfile->times[0] = stval.st_atim;
   curfile->times[1] = stval.st_mtim;
   curfile->dotimes = 1;
   // retrieve the FTAG of our target file
   if ( getftag( stream, curfile ) ) {
      LOG( LOG_ERR, "Failed to retrieve FTAG of reference target: \"%s\"\n", path );
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   // check if it is safe to access this file
   if ( !(curfile->ftag.state & FTAG_READABLE) ) {
      LOG(LOG_ERR, "Target file is not yet readable\n");
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      errno = EINVAL;
      return -1;
   }
   // identify our repack marker
   char* rmarkstr = repackmarkertgt( path, &(curfile->ftag), ms );
   if ( rmarkstr == NULL ) {
      LOG( LOG_ERR, "Failed to identify repack marker path of file \"%s\"\n", path );
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   // create the repack marker
   MDAL_FHANDLE rmarker = curmdal->openref( ctxt, rmarkstr, O_WRONLY | O_EXCL | O_CREAT, 0700 );
   if ( rmarker == NULL ) {
      LOG( LOG_ERR, "Failed to create repack marker file: \"%s\"\n", rmarkstr );
      free(rmarkstr);
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   LOG( LOG_INFO, "Created repack marker file: \"%s\"\n", rmarkstr );
   // align marker file time values with tgt file times
   if ( curmdal->futimens( rmarker, curfile->times ) ) {
      LOG( LOG_ERR, "Failed to set time values on repack marker file: \"%s\"\n", rmarkstr );
      curmdal->close( rmarker );
      free(rmarkstr);
      free(curfile->ftag.ctag);
      curfile->ftag.ctag = NULL;
      free(curfile->ftag.streamid);
      curfile->ftag.streamid = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   free( rmarkstr );
   // establish a new FTAG value
   curfile->ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   curfile->ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   free(curfile->ftag.ctag);
   curfile->ftag.ctag = stream->ctag;
   free(curfile->ftag.streamid);
   curfile->ftag.streamid = stream->streamid;
   curfile->ftag.objfiles = ds->objfiles;
   curfile->ftag.objsize = ds->objsize;
   curfile->ftag.fileno = stream->fileno;
   curfile->ftag.objno = stream->objno;   // potentially modified below
   curfile->ftag.offset = stream->offset; // potentially modified below
   curfile->ftag.endofstream = 0;
   curfile->ftag.protection = ds->protection;
   curfile->ftag.bytes = 0;
   curfile->ftag.availbytes = 0;
   curfile->ftag.recoverybytes = 0; // modified below
   curfile->ftag.state = FTAG_INIT;

   // populate recovery info inode/mtime and calculate recovery finfo length
   RECOVERY_FINFO* finfo = &(stream->finfo);
   finfo->inode = stval.st_ino;
   finfo->mode = stval.st_mode;
   finfo->owner = stval.st_uid;
   finfo->group = stval.st_gid;
   finfo->size = stval.st_size; // repack streams keep track of expected total file size here
   finfo->mtime.tv_sec = stval.st_mtim.tv_sec;
   finfo->mtime.tv_nsec = stval.st_mtim.tv_nsec;
   finfo->eof = 0;

   // store the recovery path ( even though this is borderline useless and should be changed later )
   if ( finfo->path ) { free( finfo->path ); }
   finfo->path = strdup(path);
   if (finfo->path == NULL) {
      LOG(LOG_ERR, "Failed to duplicate file path into recovery info\n");
      curmdal->close( rmarker );
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }

   // calculate the length of the recovery info
   size_t recoverybytes = recovery_finfotostr(finfo, NULL, 0);
   if (recoverybytes < 1) {
      LOG(LOG_ERR, "Failed to calculate recovery info size for \"%s\"\n", path);
      free(finfo->path);
      finfo->path = NULL;
      curmdal->close( rmarker );
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   curfile->ftag.recoverybytes = recoverybytes;

   // ensure the recovery info size is compatible with the current object size
   if (curfile->ftag.objsize && (stream->recoveryheaderlen + curfile->ftag.recoverybytes) >= curfile->ftag.objsize) {
      LOG(LOG_ERR, "Recovery info size of new file is incompatible with current object size\n");
      free(finfo->path);
      finfo->path = NULL;
      curmdal->close( rmarker );
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      errno = ENAMETOOLONG; // this is most likely an issue with path length
      return -1;
   }

   // ensure that the current object still has space remaining for this file
   if (curfile->ftag.objsize && (curfile->ftag.objsize - stream->offset) < curfile->ftag.recoverybytes) {
      // we're too far into the current obj to fit any more data
      LOG(LOG_INFO, "Shifting to new object, as current can't hold recovery info\n");
      curfile->ftag.objno++;
      curfile->ftag.offset = stream->recoveryheaderlen;
   }
   else if (curfile->ftag.objfiles && stream->curfile >= curfile->ftag.objfiles) {
      // there are too many files in the current obj to fit this one
      LOG(LOG_INFO, "Shifting to new object, as current can't hold another file\n");
      curfile->ftag.objno++;
      curfile->ftag.offset = stream->recoveryheaderlen;
   }

   // Attach the new FTAG value to the marker file
   MDAL_FHANDLE tgtfh = curfile->metahandle;
   curfile->metahandle = rmarker; // swap marker FH into curfile, to allow putftag() to work on it instead
   if ( putftag( stream, curfile ) ) {
      LOG( LOG_ERR, "Failed to attach tgt FTAG to repack marker file\n" );
      free(finfo->path);
      finfo->path = NULL;
      curmdal->close( rmarker );
      curmdal->close(tgtfh);
      curfile->metahandle = NULL;
      return -1;
   }
   curfile->metahandle = tgtfh; // restore the appropriate FH
   if ( curmdal->close( rmarker ) ) {
      LOG( LOG_ERR, "Failed to properly close repack marker file\n" );
      free(finfo->path);
      finfo->path = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }

   // link the existing file to the new reference location
   char* newpath = datastream_genrpath(&(curfile->ftag), &(stream->ns->prepo->metascheme));
   if (newpath == NULL) {
      LOG(LOG_ERR, "Failed to identify new reference path for repacked file: \"%s\"\n", path);
      if (errno == EBADFD) {
         errno = ENOMSG;
      } // don't allow our reserved EBADFD value
      free(finfo->path);
      finfo->path = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   if ( curmdal->linkref( ctxt, 1, path, newpath ) ) {
      LOG( LOG_ERR, "Failed to link reference file \"%s\" to new stream ref location: \"%s\"\n", path, newpath );
      free( newpath );
      free(finfo->path);
      finfo->path = NULL;
      curmdal->close(curfile->metahandle);
      curfile->metahandle = NULL;
      return -1;
   }
   LOG( LOG_INFO, "Linked repack target (\"%s\") to new location (\"%s\")\n", path, newpath );
   free( newpath );

   if ( stream->recoveryheaderlen == 0 ) {
      // calculate the recovery header length
      RECOVERY_HEADER header =
      {
         .majorversion = RECOVERY_CURRENT_MAJORVERSION,
         .minorversion = RECOVERY_CURRENT_MINORVERSION,
         .ctag = curfile->ftag.ctag,
         .streamid = curfile->ftag.streamid
      };
      size_t recoveryheaderlen = recovery_headertostr(&(header), NULL, 0);
      if (recoveryheaderlen < 1) {
         LOG(LOG_ERR, "Failed to identify length of stream recov header\n");
         curmdal->close(curfile->metahandle);
         curfile->metahandle = NULL;
         return -1;
      }
      stream->recoveryheaderlen = recoveryheaderlen;
   }

   // update stream values
   stream->objno = curfile->ftag.objno;
   stream->offset = curfile->ftag.offset;

   return 0;
}

/**
 * Open the current data object of the given DATASTREAM
 * @param DATASTREAM stream : Current DATASTREAM
 * @return int : Zero on success, or -1 on failure
 */
int open_current_obj(DATASTREAM stream) {
   // shorthand references
   const marfs_ds* ds = &(stream->ns->prepo->datascheme);

   // find the length of the current object name
   FTAG tgttag = stream->files[stream->curfile].ftag;
   tgttag.objno = stream->objno; // we actually want the stream object number
   tgttag.offset = stream->offset;

   // identify current object target
   char* objname = NULL;
   ne_erasure erasure;
   ne_location location;
   if (datastream_objtarget(&(tgttag), ds, &(objname), &(erasure), &(location))) {
      LOG(LOG_ERR, "Failed to identify the current object target\n");
      return -1;
   }

   // open a handle for the new object
   if (stream->type == READ_STREAM) {
      LOG(LOG_INFO, "Opening object for READ: \"%s\"\n", objname);
      stream->datahandle = ne_open(ds->nectxt, objname, location, erasure, NE_RDALL);
   }
   else {
      if (stream->type == CREATE_STREAM  ||  stream->type == REPACK_STREAM) {
         // need to update file bytes and/or datastate
         STREAMFILE* curfile = stream->files + stream->curfile;
         if ((curfile->ftag.state & FTAG_DATASTATE) < FTAG_SIZED) {
            curfile->ftag.state = FTAG_SIZED | (curfile->ftag.state & ~(FTAG_DATASTATE));
         }
         if (putftag(stream, curfile)) {
            LOG(LOG_ERR, "Failed to update FTAG of file %zu\n", curfile->ftag.fileno);
            return -1;
         }
      }
      LOG(LOG_INFO, "Opening object for WRITE: \"%s\"\n", objname);
      stream->datahandle = ne_open(ds->nectxt, objname, location, erasure, NE_WRALL);
   }
   if (stream->datahandle == NULL) {
      LOG(LOG_ERR, "Failed to open object \"%s\"\n", objname);
      free(objname);
      return -1;
   }
   free(objname); // done with object name

   if (stream->type == READ_STREAM) {
      // if we're reading, we may need to seek to a specific offset
      if (stream->offset) {
         LOG(LOG_INFO, "Seeking to offset %zd of object %zu\n",
            stream->offset, stream->objno);
         if (stream->offset != ne_seek(stream->datahandle, stream->offset)) {
            LOG(LOG_ERR, "Failed to seek to offset %zu of object %zu\n", stream->offset, stream->objno);
            ne_abort(stream->datahandle);
            stream->datahandle = NULL;
            return -1;
         }
      }
   }
   else {
      // our offset value should match the recovery header length
      if (stream->offset != stream->recoveryheaderlen) {
         LOG(LOG_ERR, "Stream offset does not match recovery header length of %zu\n",
            stream->recoveryheaderlen);
         ne_abort(stream->datahandle);
         stream->datahandle = NULL;
         return -1;
      }

      // if we're writing out a new object, output a recovery header
      RECOVERY_HEADER header =
      {
         .majorversion = RECOVERY_CURRENT_MAJORVERSION,
         .minorversion = RECOVERY_CURRENT_MINORVERSION,
         .ctag = stream->ctag,
         .streamid = stream->streamid
      };
      char* recovheader = malloc(sizeof(char) * (stream->recoveryheaderlen + 1));
      if (recovheader == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for recovery header string\n");
         ne_abort(stream->datahandle);
         stream->datahandle = NULL;
         return -1;
      }
      if (recovery_headertostr(&(header), recovheader, stream->recoveryheaderlen + 1) != stream->recoveryheaderlen) {
         LOG(LOG_ERR, "Recovery header string has inconsistent length (expected %zu)\n",
            stream->recoveryheaderlen);
         ne_abort(stream->datahandle);
         stream->datahandle = NULL;
         free(recovheader);
         errno = EFAULT;
         return -1;
      }
      if (ne_write(stream->datahandle, recovheader, stream->recoveryheaderlen) != stream->recoveryheaderlen) {
         LOG(LOG_ERR, "Failed to write recovery header to new data object\n");
         ne_abort(stream->datahandle);
         stream->datahandle = NULL;
         free(recovheader);
         return -1;
      }
      free(recovheader); // done with recovery header string
   }

   return 0;
}

/**
 * Close the current DATASTERAM object reference, potentially populating a rebuild string
 * @param DATASTREAM stream : Current DATASTREAM
 * @param FTAG* curftag : Reference to the FTAG value associated with the current object
 *                        ( used to generate the rebuild marker path )
 * @param MDAL_CTXT mdalctxt : Optional reference to an MDAL_CTXT for the current NS
 *                             ( to avoid generating a new one for rebuild marker creation )
 * @return int : Zero on success, or -1 on failure
 */
int close_current_obj(DATASTREAM stream, FTAG* curftag, MDAL_CTXT mdalctxt) {
   ne_state objstate = {
      .versz = 0,
      .blocksz = 0,
      .totsz = 0,
      .meta_status = NULL,
      .data_status = NULL,
      .csum = NULL };
   MDAL mdal = stream->ns->prepo->metascheme.mdal;
   size_t stripewidth = curftag->protection.N + curftag->protection.E;
   objstate.data_status = calloc(sizeof(char), stripewidth);
   objstate.meta_status = calloc(sizeof(char), stripewidth);
   if (objstate.data_status == NULL || objstate.meta_status == NULL) {
      LOG(LOG_ERR, "Failed to allocate data object status arrays\n");
      if (objstate.data_status) {
         free(objstate.data_status);
      }
      if (objstate.meta_status) {
         free(objstate.meta_status);
      }
      return -1;
   }
   int closeres = 0;
   if (stream->datahandle != NULL) {
      closeres = ne_close(stream->datahandle, NULL, &(objstate));
      stream->datahandle = NULL; // never reattempt this process
   }
   if (closeres > 0) {
      // object synced, but with errors
      // generate a rebuild tag to speed up future repair
      char* rtagstr = NULL;
      size_t rtagstrlen = rtag_tostr(&(objstate), stripewidth, NULL, 0);
      if (rtagstrlen == 0) {
         LOG(LOG_ERR, "Failed to identify rebuild tag length\n");
         free(objstate.data_status);
         free(objstate.meta_status);
         return -1;
      }
      if ((rtagstr = (char*)malloc(sizeof(char) * (rtagstrlen + 1))) == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for rebuild tag string\n");
         free(objstate.data_status);
         free(objstate.meta_status);
         return -1;
      }
      if (rtag_tostr(&(objstate), stripewidth, rtagstr, rtagstrlen + 1) != rtagstrlen) {
         LOG(LOG_ERR, "Rebuild tag has inconsistent length\n");
         free(objstate.data_status);
         free(objstate.meta_status);
         free(rtagstr);
         return -1;
      }
      // object state has been encoded into our rtag string
      free(objstate.data_status);
      free(objstate.meta_status);

      // identify the appropraite rebuild marker name
      char* rmarkstr = NULL;
      size_t rmarkstrlen = ftag_rebuildmarker(curftag, NULL, 0);
      if (rmarkstrlen < 1) {
         LOG(LOG_ERR, "Failed to identify rebuild marker path of file %zu\n",
            curftag->fileno);
         free(rtagstr);
         return -1;
      }
      if ((rmarkstr = (char*)malloc(sizeof(char) * (rmarkstrlen + 1))) == NULL) {
         LOG(LOG_ERR, "Failed to allocate rebuild marker string of length %zu\n",
            rmarkstrlen + 1);
         free(rtagstr);
         return -1;
      }
      if (ftag_rebuildmarker(curftag, rmarkstr, rmarkstrlen + 1) != rmarkstrlen) {
         LOG(LOG_ERR, "Rebuild marker string has an inconsistent length\n");
         free(rmarkstr);
         free(rtagstr);
         return -1;
      }

      // identify the complete rebuild marker path
      char* rpath = NULL;
      size_t rpathlen = 0;
      HASH_NODE* noderef = NULL;
      if (hash_lookup(stream->ns->prepo->metascheme.reftable, rmarkstr, &(noderef)) < 0) {
         LOG(LOG_ERR, "Failed to identify reference path for rebuild marker \"%s\"\n",
            rmarkstr);
         free(rmarkstr);
         free(rtagstr);
         return -1;
      }
      rpathlen = strlen(noderef->name) + rmarkstrlen;
      rpath = malloc(sizeof(char) * (rpathlen + 1));
      if (rpath == NULL) {
         LOG(LOG_ERR, "Failed to allocate rebuild marker reference string\n");
         free(rmarkstr);
         free(rtagstr);
         return -1;
      }
      if (snprintf(rpath, rpathlen + 1, "%s%s", noderef->name, rmarkstr) != rpathlen) {
         LOG(LOG_ERR, "Failed to populate rebuild marker reference path\n");
         free(rpath);
         free(rmarkstr);
         free(rtagstr);
         errno = EFAULT;
         return -1;
      }
      free(rmarkstr);

      // create the rebuild marker
      char releasectxt = 0;
      if (mdalctxt == NULL) {
         // need to create a fresh MDAL_CTXT
         releasectxt = 1;
         char* nspath = NULL;
         if (config_nsinfo(stream->ns->idstr, NULL, &(nspath))) {
            LOG(LOG_ERR, "Failed to identify path of NS: \"%s\"\n", stream->ns->idstr);
            free(rpath);
            free(rtagstr);
            return -1;
         }
         mdalctxt = mdal->newctxt(nspath, stream->ns->prepo->metascheme.mdal->ctxt);
         free(nspath);
         if (mdalctxt == NULL) {
            LOG(LOG_ERR, "Failed to create new MDAL_CTXT for NS: \"%s\"\n",
               stream->ns->idstr);
            free(rpath);
            free(rtagstr);
            return -1;
         }
      }
      int olderrno = errno;
      errno = 0;
      MDAL_FHANDLE rhandle = mdal->openref(mdalctxt, rpath, O_CREAT | O_EXCL, 0700);
      if (rhandle == NULL  &&  errno != EEXIST) {
         LOG(LOG_ERR, "Failed to create rebuild marker: \"%s\"\n", rpath);
         free(rpath);
         free(rtagstr);
         if (releasectxt) {
            mdal->destroyctxt(mdalctxt);
         }
         return -1;
      }
      errno = olderrno;
      if ( rhandle == NULL ) {
         // failure with EEXIST
         LOG( LOG_INFO, "Rebuild marker already exists: \"%s\"\n", rpath );
         free( rpath );
         free( rtagstr );
         if (releasectxt && mdal->destroyctxt(mdalctxt)) {
            LOG(LOG_WARNING, "Failed to destroy MDAL_CTXT\n");
         }
         return 0;
      }
      LOG(LOG_INFO, "Created rebuild marker: \"%s\"\n", rpath);
      free(rpath);

      // attach the FTAG
      size_t ftagstrlen = ftag_tostr(curftag, NULL, 0);
      if (ftagstrlen < 1) {
         LOG(LOG_ERR, "Failed to identify string length of FTAG for file %zu\n",
            curftag->fileno);
         mdal->close(rhandle);
         free(rtagstr);
         if (releasectxt) {
            mdal->destroyctxt(mdalctxt);
         }
         return -1;
      }
      char* ftagstr = (char*)malloc(sizeof(char) * (ftagstrlen + 1));
      if (ftagstr == NULL) {
         LOG(LOG_ERR, "Failed to allocate FTAG string for file %zu\n", curftag->fileno);
         mdal->close(rhandle);
         free(rtagstr);
         if (releasectxt) {
            mdal->destroyctxt(mdalctxt);
         }
         return -1;
      }
      if (ftag_tostr(curftag, ftagstr, ftagstrlen + 1) != ftagstrlen) {
         LOG(LOG_ERR, "FTAG string has an inconsistent length\n");
         free(ftagstr);
         mdal->close(rhandle);
         free(rtagstr);
         if (releasectxt) {
            mdal->destroyctxt(mdalctxt);
         }
         return -1;
      }
      if (mdal->fsetxattr(rhandle, 1, FTAG_NAME, ftagstr, ftagstrlen, XATTR_CREATE)) {
         LOG(LOG_ERR, "Failed to attach FTAG: \"%s\"\n", ftagstr);
         free(ftagstr);
         mdal->close(rhandle);
         free(rtagstr);
         if (releasectxt) {
            mdal->destroyctxt(mdalctxt);
         }
         return -1;
      }
      LOG(LOG_INFO, "Attached FTAG: \"%s\"\n", ftagstr);
      free(ftagstr);

      // attach the rebuild tag
      if (mdal->fsetxattr(rhandle, 1, RTAG_NAME, rtagstr, rtagstrlen, XATTR_CREATE)) {
         LOG(LOG_ERR, "Failed to attach rebuild tag: \"%s\"\n", rtagstr);
         mdal->close(rhandle);
         free(rtagstr);
         if (releasectxt) {
            mdal->destroyctxt(mdalctxt);
         }
         return -1;
      }
      LOG(LOG_INFO, "Attached RTAG: \"%s\"\n", rtagstr);
      free(rtagstr);

      // close the rebuild marker
      if (mdal->close(rhandle)) {
         LOG(LOG_WARNING, "Failed to close rebuild marker file\n");
      }
      // potentially destroy our MDAL_CTXT
      if (releasectxt && mdal->destroyctxt(mdalctxt)) {
         LOG(LOG_WARNING, "Failed to destroy MDAL_CTXT\n");
      }
   }
   else {
      free(objstate.data_status);
      free(objstate.meta_status);
      if (closeres < 0) {
         LOG(LOG_ERR, "ne_close() indicates failure for object %zu\n", curftag->objno);
         return -1;
      }
      else {
         LOG(LOG_INFO, "Successfully closed object %zu\n", curftag->objno);
      }
   }

   return 0;
}

/**
 * Generate a new DATASTREAM of the given type and the given initial target file
 * @param STREAM_TYPE type : Type of the DATASTREAM to be created
 * @param const char* path : Path of the initial file target
 * @param char rpathflag : If zero, treat 'path' as a user path
 *                         If non-zero, treat 'path' as a reference path
 *                         NOTE -- ALL datastream_*() functions should use USER PATHS ONLY
 *                                 This flag is intended to provide options to specific
 *                                 administrator programs ( see streamwalker.c, for example )
 * @param mode_t mode : Mode of the target file ( only used if type == CREATE_STREAM )
 * @param const char* ctag : Current MarFS ctag string ( only used if type == CREATE_STREAM )
 * @param MDAL_FHANDLE* phandle : Reference to be populated with a preserved meta handle
 *                                ( if no FTAG value exists; specific to READ streams )
 * @return DATASTREAM : Created DATASTREAM, or NULL on failure
 */
DATASTREAM genstream(STREAM_TYPE type, const char* path, char rpathflag, marfs_position* pos, mode_t mode, const char* ctag, MDAL_FHANDLE* phandle) {
   // create some shorthand references
   marfs_ds* ds = &(pos->ns->prepo->datascheme);

   // allocate the new datastream and check for success
   DATASTREAM stream = malloc(sizeof(struct datastream_struct));
   if (stream == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a datastream\n");
      return NULL;
   }
   // populate default stream values
   stream->type = type;
   stream->ctag = NULL;
   stream->streamid = NULL;
   stream->ns = pos->ns;
   stream->recoveryheaderlen = 0; // redefined below
   stream->fileno = 0;
   stream->objno = 0;
   stream->offset = 0; // redefined below
   stream->excessoffset = 0;
   stream->datahandle = NULL;
   stream->files = NULL; // redefined below
   stream->curfile = 0;
   stream->filealloc = 0; // redefined below
   stream->ftagstr = malloc(sizeof(char) * 512);
   stream->ftagstrsize = 512;
   stream->finfostr = malloc(sizeof(char) * 512);
   stream->finfostrlen = 512;
   // zero out all recovery finfo values; those will be populated later, if needed
   stream->finfo.inode = 0;
   stream->finfo.mode = 0;
   stream->finfo.owner = 0;
   stream->finfo.group = 0;
   stream->finfo.size = 0;
   stream->finfo.mtime.tv_sec = 0;
   stream->finfo.mtime.tv_nsec = 0;
   stream->finfo.eof = 0;
   stream->finfo.path = NULL;
   // verify that all string allocations succeeded
   if (stream->ftagstr == NULL || stream->finfostr == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for stream string elements\n");
      freestream(stream);
      return NULL;
   }

   // allocate our first file reference(s)
   if (type == READ_STREAM || type == EDIT_STREAM) {
      // Read/Edit streams should only ever expect two files to be referenced at a time
      stream->filealloc = allocfiles(&(stream->files), stream->curfile, 2);
   }
   else if (type == CREATE_STREAM  ||  type == REPACK_STREAM) {
      // Create/Repack streams are only restriced by the object packing limits
      stream->filealloc = allocfiles(&(stream->files), stream->curfile, ds->objfiles + 1);
   }
   if (stream->filealloc == 0) {
      LOG(LOG_ERR, "Failed to allocate space for streamfiles OR received ERROR type\n");
      freestream(stream);
      return NULL;
   }

   // populate info for the first stream file
   STREAMFILE* curfile = &(stream->files[0]);
   curfile->metahandle = NULL;
   curfile->ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   curfile->ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   curfile->ftag.ctag = stream->ctag;
   curfile->ftag.streamid = stream->streamid;
   curfile->ftag.objfiles = ds->objfiles;
   curfile->ftag.objsize = ds->objsize;
   curfile->ftag.fileno = 0;
   curfile->ftag.objno = 0;
   curfile->ftag.endofstream = 0;
   curfile->ftag.offset = 0;
   curfile->ftag.protection = ds->protection;
   curfile->ftag.bytes = 0;
   curfile->ftag.availbytes = 0;
   curfile->ftag.recoverybytes = 0;
   curfile->ftag.state = FTAG_INIT; // no data written and no other access
   curfile->times[0].tv_sec = 0;
   curfile->times[0].tv_nsec = 0;
   curfile->times[1].tv_sec = 0;
   curfile->times[1].tv_nsec = 0;
   curfile->dotimes = 0;

   // perform type-dependent initialization
   if (type == CREATE_STREAM  ||  type == REPACK_STREAM) {
      // set the ctag value
      if ( ctag )
         stream->ctag = strdup(ctag);
      else
         stream->ctag = strdup("UNKNOWN-CLIENT");
      if (stream->ctag == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for stream Client Tag\n");
         freestream(stream);
         return NULL;
      }

      // generate a new streamID
      if ( genstreamid( stream->ctag, stream->ns, &(stream->streamid), &(stream->recoveryheaderlen) ) ) {
         LOG(LOG_ERR, "Failed to generate streamID value\n");
         freestream(stream);
         return NULL;
      }
      stream->offset = stream->recoveryheaderlen;

      if ( type == CREATE_STREAM ) {
         // create the output file
         if (create_new_file(stream, path, pos->ctxt, mode)) {
            LOG(LOG_ERR, "Failed to create output file: \"%s\"\n", path);
            freestream(stream);
            return NULL;
         }
      }
      else { // this is a repack stream
         // open the first repack file
         if ( open_repack_file(stream, path, pos->ctxt ) ) {
            LOG(LOG_ERR, "Failed to open repack file: \"%s\"\n", path);
            freestream(stream);
            return NULL;
         }
      }

   }
   else {
      // open an existing file and populate stream info
      int openres = open_existing_file(stream, path, rpathflag, pos->ctxt);
      if (openres > 0 && phandle) {
         LOG(LOG_INFO, "Preserving meta handle for target w/o FTAG: \"%s\"\n", path);
         *phandle = stream->files->metahandle;
         stream->files[0].metahandle = NULL; // so the handle is left open by freestream()
      }
      if (openres) {
         LOG(LOG_ERR, "Failed to initialize stream for file: \"%s\"\n", path);
         freestream(stream);
         return NULL;
      }
   }

   return stream;
}

/**
 * Populate offset and data object target info for the given offset
 * @param DATASTREAM stream : Current DATASTREAM
 * @param off_t offset : Target offset
 * @param int whence : Flag defining offset origin ( see 'seek()' manpage 'whence' arg )
 *                     Supported values: SEEK_SET, SEEK_CUR, SEEK_END
 * @param DATASTREAM_POSITION* dpos : Reference to the DATASTREAM_POSITION struct to be
 *                                    populated
 * @return int : Zero on success, or -1 on failure
 */
int gettargets(DATASTREAM stream, off_t offset, int whence, DATASTREAM_POSITION* dpos) {
   //size_t* tgtobjno, size_t* tgtoffset, size_t* remaining, size_t* excess, size_t* maxobjdata ) {
      // establish some bounds values that we'll need later
   FTAG curtag = stream->files[stream->curfile].ftag;
   size_t dataperobj = (curtag.objsize - (curtag.recoverybytes + stream->recoveryheaderlen));
   size_t minobj = curtag.objno;
   size_t minoffset = curtag.offset - stream->recoveryheaderlen; // data space already occupied in first obj
   size_t filesize = curtag.availbytes;
   if (stream->type == READ_STREAM) {
      // read streams are constrained by the metadata file size
      filesize = stream->finfo.size;
   }
   else if (stream->type == CREATE_STREAM  ||  stream->type == REPACK_STREAM) {
      // create streams are constrained by the actual data size
      filesize = curtag.bytes;
   }

   // convert all values to a seek from the start of the file ( convert to a whence == SEEK_SET offset )
   if (whence == SEEK_END) {
      offset += filesize; // just add the total file size
   }
   else if (whence == SEEK_CUR) {
      if (stream->objno > minobj) {
         offset += (dataperobj - minoffset); // add the data in the first obj
         offset += (stream->objno - (minobj + 1)) * dataperobj; // add the data of all complete objs
         if (stream->offset) {
            offset += (stream->offset - stream->recoveryheaderlen); // add the data in the current obj
         }
      }
      else if (stream->offset) {
         offset += ((stream->offset - stream->recoveryheaderlen) - minoffset);
      }
      offset += stream->excessoffset; // account for any zero-fill offset
   }
   else if (whence != SEEK_SET) {
      // catch an unknown 'whence' value
      LOG(LOG_ERR, "Invalid value of 'whence'\n");
      errno = EINVAL;
      return -1;
   }
   // regardless of 'whence', we are now seeking from the min values ( beginning of file )

   // Check that the offset is still within file bounds
   if (offset < 0) {
      // make sure we aren't seeking before the start of the file
      LOG(LOG_ERR, "Offset value extends prior to beginning of file\n");
      errno = EINVAL;
      return -1;
   }
   if (offset > filesize) {
      // we are seeking beyond the end of the file
      if (stream->type == CREATE_STREAM  ||  stream->type == REPACK_STREAM) {
         LOG(LOG_INFO, "Offset will require extending file from %zu to %zd\n",
            filesize, offset);
         filesize = offset;
      }
      else {
         LOG(LOG_ERR, "Offset value extends beyond end of file\n");
         errno = EINVAL;
         return -1;
      }
   }
   // establish position vals, based on target offset
   size_t tgtobj = minobj;
   size_t tgtoff = minoffset;
   size_t remain = 0;
   size_t excessremain = (filesize > curtag.availbytes) ? filesize - curtag.availbytes : 0;
   size_t excessoff = 0;
   if (offset > curtag.availbytes) {
      // the offset is valid, but exceeds real data bounds ( use zero fill )
      excessoff = offset - curtag.availbytes;
      excessremain -= excessoff;
      offset = curtag.availbytes;
   }
   else {
      // the offset is within real data bounds
      remain = curtag.availbytes - offset;
   }
   if ((offset + minoffset) >= dataperobj) {
      // this offset will cross object boundaries
      tgtobj += ((offset + minoffset) / dataperobj);
      tgtoff = (offset + minoffset) % dataperobj;
   }
   else {
      tgtoff += offset;
   }

   // populate our final values, accounting for recovery info
   dpos->totaloffset = offset + excessoff;
   dpos->dataremaining = remain;
   dpos->excessremaining = excessremain;
   dpos->objno = tgtobj;
   dpos->offset = tgtoff + stream->recoveryheaderlen;
   dpos->excessoffset = excessoff;
   dpos->dataperobj = dataperobj;
   LOG(LOG_INFO, "Data Targets relative to offset %zu\n", dpos->totaloffset);
   LOG(LOG_INFO, "   DataRemaining: %zu\n", dpos->dataremaining);
   LOG(LOG_INFO, "   ExcessRemaining: %zu\n", dpos->excessremaining);
   LOG(LOG_INFO, "   ObjNo: %zu\n", dpos->objno);
   LOG(LOG_INFO, "   Offset: %zu\n", dpos->offset);
   LOG(LOG_INFO, "   ExcessOffset: %zu\n", dpos->excessoffset);
   LOG(LOG_INFO, "   DataPerObj: %zu\n", dpos->dataperobj);
   LOG(LOG_INFO, "   Filesize: %zu\n", filesize);
   return 0;
}

/**
 * Output the RECOVERY_FINFO string representation for the current file to the data object
 * @param DATASTREAM stream : Current DATASTREAM
 * @return int : Zero on success, or -1 on failure
 */
int putfinfo(DATASTREAM stream) {
   // allocate a recovery info string
   size_t recoverybytes = stream->files[stream->curfile].ftag.recoverybytes;
   if (recoverybytes > stream->finfostrlen) {
      LOG(LOG_INFO, "Allocating extended finfo string of %zu bytes\n", recoverybytes + 1);
      char* oldstr = stream->finfostr;
      stream->finfostr = malloc(sizeof(char) * (recoverybytes + 1));
      if (stream->finfostr == NULL) {
         LOG(LOG_ERR, "Failed to allocate new finfo string\n");
         stream->finfostr = oldstr;
         return -1;
      }
      stream->finfostrlen = recoverybytes;
      free(oldstr);
   }
   // update recovery info size values
   size_t origfinfosize = stream->finfo.size;
   if (stream->type == EDIT_STREAM) {
      DATASTREAM_POSITION dpos = {
         .totaloffset = 0,
         .dataremaining = 0,
         .excessremaining = 0,
         .objno = 0,
         .offset = 0,
         .excessoffset = 0,
         .dataperobj = 0
      };
      if (gettargets(stream, 0, SEEK_CUR, &(dpos))) {
         LOG(LOG_ERR, "Failed to calculate current data offset\n");
         return -1;
      }
      stream->finfo.size = dpos.totaloffset;
   }
   else {
      stream->finfo.size = stream->files[stream->curfile].ftag.bytes;
   }
   // populate recovery info string
   size_t genbytes = recovery_finfotostr(&(stream->finfo), stream->finfostr, stream->finfostrlen + 1);
   if (genbytes > recoverybytes) {
      LOG(LOG_ERR, "File recovery info has an inconsistent length ( old=%zu, new=%zu )\n",
         recoverybytes, genbytes);
      if ( stream->type == REPACK_STREAM ) { stream->finfo.size = origfinfosize; } // restore this value for repack
      errno = ENAMETOOLONG; // this is most likely to represent the issue, as this almost
                            //  certainly is the result of the recovery path changing
      return -1;
   }
   else if (genbytes < recoverybytes) {
      // finfo string is shorter than expected, so zero out the unused tail of the string
      bzero(stream->finfostr + genbytes, (recoverybytes + 1) - genbytes);
   }
   if ( stream->type == REPACK_STREAM ) { stream->finfo.size = origfinfosize; } // restore this value for repack
   // Note -- previous writes should have ensured we have at least 'recoverybytes' of
   //         available spaece to write out the recovery string
   if (ne_write(stream->datahandle, stream->finfostr, recoverybytes) != recoverybytes) {
      LOG(LOG_ERR, "Failed to store file recovery info to data object\n");
      return -1;
   }
   stream->offset += recoverybytes; // update our object offset
   LOG(LOG_INFO, "Wrote out RECOVERY_FINFO: \"%s\"\n", stream->finfostr);
   return 0;
}

/**
 * Finalize the current file: Potentially open the data object and/or output the
 *                            RECOVERY_FINFO string, and mark the FTAG_DATASTATE as
 *                            FINALIZED ( but do NOT update the actual FTAG value )
 * NOTE -- This func can be called multiple times on the same file.  Repeated calls
 *         will effectively be a no-op.
 * @param DATASTREAM stream : Current DATASTREAM
 * @return int : Zero on success, or -1 on failure
 */
int finfile(DATASTREAM stream) {
   // get a reference to the active file
   STREAMFILE* curfile = stream->files + stream->curfile;

   if ( stream->type == REPACK_STREAM ) {
      // check if we've hit our expected total file size
      if ( curfile->ftag.bytes != stream->finfo.size ) {
         LOG( LOG_ERR, "Cannot complete repacked file with inappropriate byte count: %zu (expected=%zu)\n",
                       curfile->ftag.bytes, stream->finfo.size );
         return -1;
      }
   }

   // only perform this action if the file has not yet been finalized
   if ((curfile->ftag.state & FTAG_DATASTATE) < FTAG_FIN) {
      if (curfile->ftag.bytes == 0 && stream->datahandle == NULL) {
         // special case, non-extended create stream with no current data content
         // Open the output object to record recov info for zero-length file
         LOG(LOG_INFO, "Opening data object for empty file\n");
         if (open_current_obj(stream)) {
            LOG(LOG_ERR, "Failed to open output object for zero-length prev file\n");
            return -1;
         }
      }
      if (stream->datahandle) {
         // end of prev file
         stream->finfo.eof = 1;
         //  output recovery info for the prev file
         if (putfinfo(stream)) {
            LOG(LOG_ERR, "Failed to output prev file recovery info\n");
            stream->finfo.eof = 0;
            return -1;
         }
      }
      else {
         // this is an extended file and thus can't pack,
         //   so proceed to the next object by default
         stream->objno++;
         stream->offset = stream->recoveryheaderlen;
      }
      // set data state to 'FINALIZED', ensuring we never reattempt with this same handle
      curfile->ftag.state = (curfile->ftag.state & ~(FTAG_DATASTATE)) | FTAG_FIN;
   }

   return 0;
}

/**
 * Completes the given file: truncating to appropriate length, setting the FTAG to a
 *  complete + readable state, setting file times, and closing the meta handle
 * @param DATASTREAM stream : Current DATASTREAM
 * @param STREAMFILE* file : File to be finalized
 * @return int : Zero on success, or -1 on failure
 */
int completefile(DATASTREAM stream, STREAMFILE* file) {
   // check for NULL handle
   if (file->metahandle == NULL) {
      LOG(LOG_ERR, "Tgt file is already closed\n");
      return -1;
   }
   // shorthand references
   const marfs_ms* ms = &(stream->ns->prepo->metascheme);
   // check for an extended file from a create stream
   if ((file->ftag.state & FTAG_WRITEABLE) && 
       ( stream->type == CREATE_STREAM  ||  stream->type == REPACK_STREAM ) ) {
      LOG(LOG_ERR, "Cannot complete extended file from the creating stream\n");
      ms->mdal->close(file->metahandle);
      file->metahandle = NULL; // NULL out this handle, so that we never double close()
      return -1;
   }
   // check for a non-finalized file from an edit stream
   if ((file->ftag.state & FTAG_DATASTATE) != FTAG_FIN && stream->type == EDIT_STREAM) {
      LOG(LOG_ERR, "Cannot complete non-finalized file from edit stream\n");
      ms->mdal->close(file->metahandle);
      file->metahandle = NULL; // NULL out this handle, so that we never double close()
      return -1;
   }
   // set ftag to readable and complete state
   file->ftag.state = (FTAG_COMP | FTAG_READABLE) | (file->ftag.state & ~(FTAG_DATASTATE));
   // update the ftag availbytes to reflect the actual data bytes
   file->ftag.availbytes = file->ftag.bytes;
   // set an updated ftag value
   if (putftag(stream, file)) {
      LOG(LOG_ERR, "Failed to update FTAG on file %zu to complete state\n", file->ftag.fileno);
      ms->mdal->close(file->metahandle);
      file->metahandle = NULL; // NULL out this handle, so that we never double close()
      return -1;
   }
   // repack streams require special consideration
   if ( stream->type == REPACK_STREAM ) {
      // pull the original FTAG string off this file
      STREAMFILE origfile = { .metahandle = file->metahandle };
      if ( getftag( stream, &(origfile) ) ) {
         LOG( LOG_ERR, "Failed to retrieve original FTAG value from repacked file: \"%s\"\n", stream->finfo.path );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      } // NOTE -- stream->ftagstr now contains the original FTAG value
      // produce the original reference path of this file
      char* origrefpath = datastream_genrpath( &(origfile.ftag), ms );
      if ( origrefpath == NULL ) {
         LOG( LOG_ERR, "Failed to identify original refpath of \"%s\"\n", stream->finfo.path );
         free( origfile.ftag.ctag );
         free( origfile.ftag.streamid );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // locate our repack marker file
      char* rmarkstr = repackmarkertgt( origrefpath, &(origfile.ftag), ms );
      if ( rmarkstr == NULL ) {
         LOG( LOG_ERR, "Failed to identify repack marker path of \"%s\"\n", stream->finfo.path );
         free( origrefpath );
         free( origfile.ftag.ctag );
         free( origfile.ftag.streamid );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // done with origfile values
      free( origfile.ftag.ctag );
      free( origfile.ftag.streamid );
      origfile.metahandle = NULL;
      // identify the NS path of the stream and establish a ctxt for it
      char* nspath = NULL;
      if ( config_nsinfo( stream->ns->idstr, NULL, &(nspath) ) ) {
         LOG( LOG_ERR, "Failed to identify the NS path of the repack stream\n" );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      MDAL_CTXT ctxt = ms->mdal->newctxt( nspath, ms->mdal->ctxt );
      if ( ctxt == NULL ) {
         LOG( LOG_ERR, "Failed to create an MDAL_CTXT for NS \"%s\"\n", nspath );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // open a handle for the repack marker
      MDAL_FHANDLE rmarker = ms->mdal->openref( ctxt, rmarkstr, O_WRONLY, 0 );
      if ( rmarker == NULL ) {
         LOG( LOG_ERR, "Failed to open rebuild marker file \"%s\"\n", rmarkstr );
         ms->mdal->destroyctxt( ctxt );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // attach a copy of the original FTAG to the repack marker ( so the GC will pick it up, post rename )
      if ( ms->mdal->fsetxattr( rmarker, 1, FTAG_NAME, stream->ftagstr, strlen(stream->ftagstr), XATTR_CREATE ) ) {
         LOG( LOG_ERR, "Failed to attach orig FTAG value to repack marker \"%s\"\n", rmarkstr );
         ms->mdal->close(rmarker);
         ms->mdal->destroyctxt( ctxt );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // done changing repack marker xattrs
      if ( ms->mdal->close(rmarker) ) {
         LOG( LOG_ERR, "Close failure of repack marker \"%s\"\n", rmarkstr );
         ms->mdal->destroyctxt( ctxt );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // attach the original FTAG to our file ( if not already present, as we want to preserve the TRUE original value )
      if ( ms->mdal->fsetxattr( file->metahandle, 1, OREPACK_TAG_NAME, stream->ftagstr, strlen(stream->ftagstr), XATTR_CREATE )  &&  errno != EEXIST ) {
         LOG( LOG_ERR, "Failed to attach orig FTAG value to repacked file \"%s\"\n", stream->finfo.path );
         ms->mdal->destroyctxt( ctxt );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // use putftag to update active FTAG by swapping out stream type ( this is a bit hacky )
      stream->type = CREATE_STREAM; // switching from REPACK_STREAM causes this to update the real FTAG value
      if( putftag( stream, file ) ) {
         LOG( LOG_ERR, "Failed to update FTAG to final value for repacked file \"%s\"\n", stream->finfo.path );
         stream->type = REPACK_STREAM;
         ms->mdal->destroyctxt( ctxt );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      } // NOTE -- stream->ftagstr is NO LONGER the original FTAG value
      stream->type = REPACK_STREAM;
      // remove the 'target' FTAG value
      if ( ms->mdal->fremovexattr( file->metahandle, 1, TREPACK_TAG_NAME ) ) {
         LOG( LOG_ERR, "Failed to remove the \"%s\" xattr from repacked file \"%s\"\n", stream->finfo.path );
         ms->mdal->destroyctxt( ctxt );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      // finally, rename the repack marker over the original location, causing GC to pick it up
      if ( ms->mdal->renameref( ctxt, rmarkstr, origrefpath ) ) {
         LOG( LOG_ERR, "Failed to rename repack marker \"%s\" over repacked file \"%s\"\n", rmarkstr, origrefpath );
         ms->mdal->destroyctxt( ctxt );
         free( rmarkstr );
         free( origrefpath );
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
      ms->mdal->destroyctxt( ctxt );
      free( rmarkstr );
      free( origrefpath );
   }
   else {
      // truncate the file to an appropriate length
      if (ms->mdal->ftruncate(file->metahandle, file->ftag.availbytes)) {
         LOG(LOG_ERR, "Failed to truncate file %zu to proper size\n", file->ftag.fileno);
         ms->mdal->close(file->metahandle);
         file->metahandle = NULL; // NULL out this handle, so that we never double close()
         return -1;
      }
   }
   // set atime/mtime values
   if (ms->mdal->futimens(file->metahandle, file->times)) {
      LOG(LOG_ERR, "Failed to update time values on file %zu\n", file->ftag.fileno);
      ms->mdal->close(file->metahandle);
      file->metahandle = NULL; // NULL out this handle, so that we never double close()
      return -1;
   }
   // close the meta handle
   if (ms->mdal->close(file->metahandle)) {
      LOG(LOG_ERR, "Failed to close meta handle on file %zu\n", file->ftag.fileno);
      file->metahandle = NULL; // NULL out this handle, so that we never double close()
      return -1;
   }
   file->metahandle = NULL; // NULL out this handle, so that we never double close()
   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------


/**
 * Generate a reference path for the given FTAG
 * @param FTAG* ftag : Reference to the FTAG value to generate an rpath for
 * @param const marfs_ms* ms : Reference to the current MarFS metadata scheme
 * @return char* : Reference to the newly generated reference path, or NULL on failure
 *                 NOTE -- returned path must be freed by caller
 */
char* datastream_genrpath(FTAG* ftag, const marfs_ms* ms) {
   // generate the meta reference name of this file
   size_t rnamelen = ftag_metatgt(ftag, NULL, 0);
   if (rnamelen < 1) {
      LOG(LOG_ERR, "Failed to generate file meta reference name\n");
      return NULL;
   }
   char* refname = malloc(sizeof(char) * (rnamelen + 1));
   if (refname == NULL) {
      LOG(LOG_ERR, "Failed to allocate a temporary meta reference string\n");
      return NULL;
   }
   if (ftag_metatgt(ftag, refname, rnamelen + 1) != rnamelen) {
      LOG(LOG_ERR, "Inconsistent length of file meta reference string\n");
      free(refname);
      return NULL;
   }
   // determine the target reference path of this file
   HASH_NODE* noderef = NULL;
   if (hash_lookup(ms->reftable, refname, &(noderef)) < 0) {
      LOG(LOG_ERR, "Failed to identify reference path for metaname \"%s\"\n", refname);
      free(refname);
      return NULL;
   }
   // populate the complete rpath
   size_t rpathlen = strlen(noderef->name) + strlen(refname);
   char* rpath = malloc(sizeof(char) * (rpathlen + 1));
   if (rpath == NULL) {
      LOG(LOG_ERR, "Failed to allocate rpath string\n");
      free(refname);
      return NULL;
   }
   if (snprintf(rpath, rpathlen + 1, "%s%s", noderef->name, refname) != rpathlen) {
      LOG(LOG_ERR, "Failed to populate rpath string\n");
      free(refname);
      free(rpath);
      errno = EFAULT;
      return NULL;
   }
   free(refname); // done with this tmp string
   return rpath;
}

/**
 * Generate data object target info based on the given FTAG and datascheme references
 * @param FTAG* ftag : Reference to the FTAG value to generate target info for
 * @param const marfs_ds* ds : Reference to the current MarFS data scheme
 * @param char** objectname : Reference to a char* to be populated with the object name
 * @param ne_erasure* erasure : Reference to an ne_erasure struct to be populated with
 *                              object erasure info
 * @param ne_location* location : Reference to an ne_location struct to be populated with
 *                                object location info
 * @return int : Zero on success, or -1 on failure
 */
int datastream_objtarget(FTAG* ftag, const marfs_ds* ds, char** objectname, ne_erasure* erasure, ne_location* location) {
   // check for invalid args
   if (ftag == NULL) {
      LOG(LOG_ERR, "Received a NULL FTAG reference\n");
      errno = EINVAL;
      return -1;
   }
   if (ds == NULL) {
      LOG(LOG_ERR, "Received a NULL marfs_ds reference\n");
      errno = EINVAL;
      return -1;
   }
   if (objectname == NULL) {
      LOG(LOG_ERR, "Received a NULL objname reference\n");
      errno = EINVAL;
      return -1;
   }
   if (erasure == NULL) {
      LOG(LOG_ERR, "Received a NULL ne_erasure reference\n");
      errno = EINVAL;
      return -1;
   }
   if (location == NULL) {
      LOG(LOG_ERR, "Received a NULL ne_location reference\n");
      errno = EINVAL;
      return -1;
   }
   // find the length of the current object name
   ssize_t objnamelen = ftag_datatgt(ftag, NULL, 0);
   if (objnamelen <= 0) {
      LOG(LOG_ERR, "Failed to determine object path from current ftag\n");
      return -1;
   }
   // allocate a new string, and populate it with the object name
   char* objname = malloc(sizeof(char) * (objnamelen + 1));
   if (objname == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for new object name\n");
      return -1;
   }
   if (objnamelen != ftag_datatgt(ftag, objname, objnamelen + 1)) {
      LOG(LOG_ERR, "Ftag producing inconsistent object name string\n");
      free(objname);
      return -1;
   }

   // identify the pod/cap/scatter values for the current object
   ne_location tmplocation = { .pod = -1, .cap = -1, .scatter = -1 };
   int iteration = 0;
   for (; iteration < 3; iteration++) {
      // determine which table we are currently pulling from
      HASH_TABLE curtable = ds->scattertable;
      int* tgtval = &(tmplocation.scatter);
      if (iteration < 1) {
         curtable = ds->podtable;
         tgtval = &(tmplocation.pod);
      }
      else if (iteration < 2) {
         curtable = ds->captable;
         tgtval = &(tmplocation.cap);
      }
      // hash our object name, to identify a target node
      HASH_NODE* node = NULL;
      if (hash_lookup(curtable, objname, &node) < 0) {
         LOG(LOG_ERR, "Failed to lookup %s location for new object \"%s\"\n",
            (iteration < 1) ? "pod" : (iteration < 2) ? "cap" : "scatter",
            objname);
         free(objname);
         return -1;
      }
      // parse our nodename, to produce an integer value
      char* endptr = NULL;
      unsigned long long parseval = strtoull(node->name, &(endptr), 10);
      if (*endptr != '\0' || parseval >= INT_MAX) {
         LOG(LOG_ERR, "Failed to parse %s value of \"%s\" for new object \"%s\"\n",
            (iteration < 1) ? "pod" : (iteration < 2) ? "cap" : "scatter",
            node->name, objname);
         free(objname);
         return -1;
      }
      // assign the parsed value to the appropriate var
      *tgtval = (int)parseval;
   }

   // identify the erasure scheme
   ne_erasure tmperasure = ftag->protection;
   tmperasure.O = (int)(hash_rangevalue(objname, tmperasure.N + tmperasure.E)); // produce tmperasure offset value
   LOG(LOG_INFO, "Object: \"%s\"\n", objname);
   LOG(LOG_INFO, "Position: pod%d, cap%d, scatter%d\n",
      tmplocation.pod, tmplocation.cap, tmplocation.scatter);
   LOG(LOG_INFO, "Erasure: N=%d,E=%d,O=%d,psz=%zu\n",
      tmperasure.N, tmperasure.E, tmperasure.O, tmperasure.partsz);

   // populate all return structs
   *objectname = objname;
   *erasure = tmperasure;
   *location = tmplocation;

   return 0;
}

/**
 * Create a new file associated with a CREATE stream
 * @param DATASTREAM* stream : Reference to an existing CREATE stream; if that ref is NULL
 *                             a fresh stream will be generated to replace that ref
 * @param const char* path : Path of the file to be created
 * @param marfs_position* pos : Reference to the marfs_position value of the target file
 * @param mode_t mode : Mode value of the file to be created
 * @param const char* ctag : Client tag to be associated with this stream
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
int datastream_create(DATASTREAM* stream, const char* path, marfs_position* pos, mode_t mode, const char* ctag) {
   // check for a NULL path arg
   if (path == NULL) {
      LOG(LOG_ERR, "Received a NULL path argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for a NULL position
   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL position argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for NULL stream reference
   if (stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference argument\n");
      errno = EINVAL;
      return -1;
   }
   char closestream = 0;
   DATASTREAM newstream = *stream;
   // check if we've been passed an existing stream
   if (newstream) {
      if (newstream->type != CREATE_STREAM) {
         LOG(LOG_ERR, "Received non-CREATE stream\n");
         errno = EINVAL;
         return -1;
      }
      if (newstream->ns != pos->ns) {
         LOG(LOG_INFO, "Received datastream has different NS target: \"%s\"\n",
            newstream->ns->idstr);
         // can't continue with a stream from a previous NS
         closestream = 1;
         newstream = NULL; // so that stream generation logic kicks in later
      }
      else {
         // we're going to continue using the provided stream structure
         size_t curobj = newstream->objno;
         // finalize the current file
         if (finfile(newstream)) {
            LOG(LOG_ERR, "Failed to finalize previous stream file\n");
            freestream(newstream);
            *stream = NULL; // unsafe to reuse this stream
            errno = EBADFD;
            return -1;
         }
         // progress to the next file
         newstream->curfile++;
         newstream->fileno++;
         // create the new file
         if (create_new_file(newstream, path, pos->ctxt, mode)) {
            LOG(LOG_ERR, "Failed to create new file: \"%s\"\n", path);
            // roll back our stream changes
            newstream->curfile--;
            newstream->fileno--;
            if (errno == EBADFD) {
               errno = ENOMSG;
            } // avoid using our reserved errno value
            return -1;
         }
         // check for an object transition
         STREAMFILE* newfile = newstream->files + newstream->curfile;
         if (newfile->ftag.objno != curobj) {
            size_t newfilepos = newstream->curfile;
            LOG(LOG_INFO, "Stream has transitioned from objno %zu to %zu\n",
               curobj, newfile->ftag.objno);
            // close our data handle
            FTAG oldftag = (newfile - 1)->ftag;
            oldftag.objno = curobj;
            if (close_current_obj(newstream, &(oldftag), pos->ctxt)) {
               LOG(LOG_ERR, "Failure to close data object %zu\n", curobj);
               freestream(newstream);
               *stream = NULL; // unsafe to reuse this stream
               errno = EBADFD;
               return -1;
            }
            // we need to mark all previous files as complete
            char abortflag = 0;
            while (newstream->curfile) {
               newstream->curfile--;
               if (completefile(newstream, newstream->files + newstream->curfile)) {
                  LOG(LOG_ERR, "Failed to complete file %zu\n",
                     (newstream->files + newstream->curfile)->ftag.fileno);
                  abortflag = 1;
               }
            }
            // shift the new file reference to the front of the list
            newstream->files[0] = newstream->files[newfilepos];
            // check for any errors
            if (abortflag) {
               LOG(LOG_INFO, "Terminating datastream due to previous errors\n");
               freestream(newstream);
               *stream = NULL; // unsafe to reuse this stream
               errno = EBADFD;
               return -1;
            }
         }
         else {
            // at least need to push out the 'FINALIZED' state of the previous file
            if (putftag(newstream, newstream->files + (newstream->curfile - 1))) {
               LOG(LOG_ERR, "Failed to push the FINALIZED FTAG for the previous file\n");
               freestream(newstream);
               *stream = NULL; // unsafe to reuse this stream
               errno = EBADFD;
               return -1;
            }
         }
      }
   }
   if (newstream == NULL) { // recheck, so as to catch if the prev stream was abandoned
      // we need to generate a fresh stream structure
      newstream = genstream(CREATE_STREAM, path, 0, pos, mode, ctag, NULL);
   }
   // check if we need to close the previous stream
   if (closestream) {
      if (datastream_close(stream)) {
         LOG(LOG_ERR, "Failed to close previous datastream\n");
         *stream = NULL; // don't attempt to close the original stream again
         if (newstream) {
            freestream(newstream);
         } // get rid of our new stream as well
         errno = EBADFD;
         return -1;
      }
   }
   // finally, check to ensure we at least have a valid stream to return
   if (newstream == NULL) {
      // still NULL means failure of genstream()
      LOG(LOG_ERR, "Failed to generate new stream\n");
      return -1;
   }
   // update the external stream reference
   *stream = newstream;

   return 0;
}

/**
 * Open an existing file associated with a READ or EDIT stream
 * @param DATASTREAM* stream : Reference to an existing DATASTREAM of the requested type;
 *                             if that ref is NULL a fresh stream will be generated to
 *                             replace that ref
 * @param STREAM_TYPE type : Type of the DATASTREAM ( READ_STREAM or EDIT_STREAM )
 * @param const char* path : Path of the file to be opened
 * @param marfs_position* pos : Reference to the marfs_position value of the target file
 * @param MDAL_FHANDLE* phandle : Reference to be populated with a preserved meta handle
 *                                ( if no FTAG value exists; specific to READ streams )
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
int datastream_open(DATASTREAM* stream, STREAM_TYPE type, const char* path, marfs_position* pos, MDAL_FHANDLE* phandle) {
   // check for invalid args
   if (type != EDIT_STREAM &&
      type != READ_STREAM) {
      LOG(LOG_ERR, "Received STREAM_TYPE is unsupported\n");
      errno = EINVAL;
      return -1;
   }
   // check for a NULL path arg
   if (path == NULL) {
      LOG(LOG_ERR, "Received a NULL path argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for a NULL position
   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL position argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for NULL stream reference
   if (stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference argument\n");
      errno = EINVAL;
      return -1;
   }
   char closestream = 0;
   DATASTREAM newstream = *stream;
   // check if we've been passed an existing stream
   if (newstream) {
      if (newstream->type != type) {
         LOG(LOG_ERR, "Received stream does not match requested STREAM_TYPE\n");
         errno = EINVAL;
         return -1;
      }
      STREAMFILE* curfile = newstream->files + newstream->curfile;
      if (newstream->ns != pos->ns) {
         // stream MUST match in NS
         LOG(LOG_INFO, "Received datastream has different NS target: \"%s\"\n",
            newstream->ns->idstr);
         closestream = 1;
         newstream = NULL; // so stream generation logic kicks in
      }
      else if (newstream->type == EDIT_STREAM) {
         // no point hanging onto any info in between edits
         LOG(LOG_INFO, "Received datastream is irrelevant for new edit stream\n");
         closestream = 1;
         newstream = NULL; // so stream generation logic kicks in
      }
      else {
         // we're going to continue using the provided READ stream structure
         MDAL mdal = newstream->ns->prepo->metascheme.mdal;
         newstream->curfile++; // progress to the next file
         // attempt to open the new file target
         size_t origobjno = newstream->objno; // remember original object number
         int openres = open_existing_file(newstream, path, 0, pos->ctxt);
         if (openres > 0 && phandle != NULL) {
            LOG(LOG_INFO, "Preserving meta handle for target w/o FTAG: \"%s\"\n", path);
            *phandle = (newstream->files + newstream->curfile)->metahandle;
            (newstream->files + newstream->curfile)->metahandle = NULL;
         }
         if (openres) {
            LOG(LOG_ERR, "Failed to open target file: \"%s\"\n", path);
            newstream->curfile--; // reset back to our old position
            if (errno == EBADFD) {
               errno = ENOMSG;
            }
            return -1;
         }
         STREAMFILE* newfile = newstream->files + newstream->curfile;
         // check if our old stream targets the same object
         if (strcmp(curfile->ftag.streamid, newfile->ftag.streamid) ||
            strcmp(curfile->ftag.ctag, newfile->ftag.ctag) ||
            origobjno != newfile->ftag.objno) {
            // data objects differ, so close the old reference
            FTAG oldftag = curfile->ftag;
            oldftag.objno = origobjno;
            if (close_current_obj(newstream, &(oldftag), pos->ctxt)) {
               // NOTE -- this doesn't necessarily have to be a fatal error on read.
               //         However, I really don't want us to ignore this sort of thing,
               //         as it could indicate imminent data loss ( corrupt object which
               //         we are now failing to tag ).  So... maybe better to fail
               //         catastrophically.
               LOG(LOG_ERR, "Failed to close old stream data handle\n");
               free(curfile->ftag.ctag);
               free(curfile->ftag.streamid);
               freestream(newstream);
               *stream = NULL;
               errno = EBADFD;
               return -1;
            }
         }
         else {
            LOG(LOG_INFO, "Seeking to %zu of existing object handle\n",
               newfile->ftag.offset);
            if (ne_seek(newstream->datahandle, newfile->ftag.offset) != newfile->ftag.offset) {
               LOG(LOG_ERR, "Failed to seek to %zu of existing object handle\n",
                  newfile->ftag.offset);
               free(curfile->ftag.ctag);
               free(curfile->ftag.streamid);
               freestream(newstream);
               *stream = NULL;
               errno = EBADFD;
               return -1;
            }
         }
         // cleanup our old file reference
         free(curfile->ftag.ctag);
         free(curfile->ftag.streamid);
         if (mdal->close(curfile->metahandle)) {
            // this has no effect on data integrity, so just complain
            LOG(LOG_WARNING, "Failed to close metahandle of old stream file\n");
         }
         curfile->metahandle = NULL;
         // move the new file reference to the first position
         *curfile = *newfile;
         // clean out the old reference location (probably unnecessary)
         newfile->metahandle = NULL;
         newfile->ftag.ctag = NULL;
         newfile->ftag.streamid = NULL;
         newstream->curfile--; // abandon the old reference location
      }
   }
   if (newstream == NULL) { // NOTE -- recheck, so as to catch if the prev stream was closed
      // we need to generate a fresh stream structure
      newstream = genstream(type, path, 0, pos, 0, NULL, (type == READ_STREAM) ? phandle : NULL);
   }
   // check if we need to close the previous stream
   if (closestream) {
      if (datastream_release(stream)) {
         LOG(LOG_ERR, "Failed to release previous datastream\n");
         *stream = NULL; // don't attempt to close the original stream again
         if (newstream) {
            freestream(newstream);
         } // get rid of our new stream as well
         errno = EBADFD;
         return -1;
      }
   }
   // finally, check to ensure we at least have a valid stream to return
   if (newstream == NULL) {
      // still NULL means failure of genstream()
      LOG(LOG_ERR, "Failed to generate new stream\n");
      return -1;
   }
   // update the external stream reference
   *stream = newstream;

   return 0;
}

/**
 * Open an existing file, by reference path, and associate it with a READ stream
 * @param DATASTREAM* stream : Reference to an existing READ DATASTREAM;
 *                             if that ref is NULL a fresh stream will be generated to
 *                             replace that ref
 * @param const char* refpath : Reference path of the file to be opened
 * @param marfs_position* pos : Reference to the marfs_position value of the target file
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
int datastream_scan(DATASTREAM* stream, const char* refpath, marfs_position* pos) {
   // check for invalid args
   if (refpath == NULL) {
      LOG(LOG_ERR, "Received a NULL path argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for a NULL position
   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL position argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for NULL stream reference
   if (stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference argument\n");
      errno = EINVAL;
      return -1;
   }
   char closestream = 0;
   DATASTREAM newstream = *stream;
   // check if we've been passed an existing stream
   if (newstream) {
      if (newstream->type != READ_STREAM) {
         LOG(LOG_ERR, "Received stream does not match requested STREAM_TYPE\n");
         errno = EINVAL;
         return -1;
      }
      STREAMFILE* curfile = newstream->files + newstream->curfile;
      if (newstream->ns != pos->ns) {
         // stream MUST match in NS
         LOG(LOG_INFO, "Received datastream has different NS target: \"%s\"\n",
            newstream->ns->idstr);
         closestream = 1;
         newstream = NULL; // so stream generation logic kicks in
      }
      else {
         // we're going to continue using the provided READ stream structure
         MDAL mdal = newstream->ns->prepo->metascheme.mdal;
         newstream->curfile++; // progress to the next file
         // attempt to open the new file target
         size_t origobjno = newstream->objno; // remember original object number
         int openres = open_existing_file(newstream, refpath, 1, pos->ctxt);
         if (openres) {
            LOG(LOG_ERR, "Failed to open target file: \"%s\"\n", refpath);
            newstream->curfile--; // reset back to our old position
            if (errno == EBADFD) {
               errno = ENOMSG;
            }
            return -1;
         }
         STREAMFILE* newfile = newstream->files + newstream->curfile;
         // check if our old stream targets the same object
         if (strcmp(curfile->ftag.streamid, newfile->ftag.streamid) ||
            strcmp(curfile->ftag.ctag, newfile->ftag.ctag) ||
            origobjno != newfile->ftag.objno) {
            // data objects differ, so close the old reference
            FTAG oldftag = curfile->ftag;
            oldftag.objno = origobjno;
            if (close_current_obj(newstream, &(oldftag), pos->ctxt)) {
               // NOTE -- this doesn't necessarily have to be a fatal error on read.
               //         However, I really don't want us to ignore this sort of thing,
               //         as it could indicate imminent data loss ( corrupt object which
               //         we are now failing to tag ).  So... maybe better to fail
               //         catastrophically.
               LOG(LOG_ERR, "Failed to close old stream data handle\n");
               free(curfile->ftag.ctag);
               free(curfile->ftag.streamid);
               freestream(newstream);
               *stream = NULL;
               errno = EBADFD;
               return -1;
            }
         }
         else {
            LOG(LOG_INFO, "Seeking to %zu of existing object handle\n",
               newfile->ftag.offset);
            if (ne_seek(newstream->datahandle, newfile->ftag.offset) != newfile->ftag.offset) {
               LOG(LOG_ERR, "Failed to seek to %zu of existing object handle\n",
                  newfile->ftag.offset);
               free(curfile->ftag.ctag);
               free(curfile->ftag.streamid);
               freestream(newstream);
               *stream = NULL;
               errno = EBADFD;
               return -1;
            }
         }
         // cleanup our old file reference
         free(curfile->ftag.ctag);
         free(curfile->ftag.streamid);
         if (mdal->close(curfile->metahandle)) {
            // this has no effect on data integrity, so just complain
            LOG(LOG_WARNING, "Failed to close metahandle of old stream file\n");
         }
         curfile->metahandle = NULL;
         // move the new file reference to the first position
         *curfile = *newfile;
         // clean out the old reference location (probably unnecessary)
         newfile->metahandle = NULL;
         newfile->ftag.ctag = NULL;
         newfile->ftag.streamid = NULL;
         newstream->curfile--; // abandon the old reference location
      }
   }
   if (newstream == NULL) { // NOTE -- recheck, so as to catch if the prev stream was closed
      // we need to generate a fresh stream structure
      newstream = genstream(READ_STREAM, refpath, 1, pos, 0, NULL, NULL);
   }
   // check if we need to close the previous stream
   if (closestream) {
      if (datastream_release(stream)) {
         LOG(LOG_ERR, "Failed to release previous datastream\n");
         *stream = NULL; // don't attempt to close the original stream again
         if (newstream) {
            freestream(newstream);
         } // get rid of our new stream as well
         errno = EBADFD;
         return -1;
      }
   }
   // finally, check to ensure we at least have a valid stream to return
   if (newstream == NULL) {
      // still NULL means failure of genstream()
      LOG(LOG_ERR, "Failed to generate new stream\n");
      return -1;
   }
   // update the external stream reference
   *stream = newstream;

   return 0;
}

/**
 * Open a REPACK stream for rewriting the file's contents as a new set of data objects
 * NOTE -- Until this stream is either closed or progressed ( via a repeated call to this func w/ the same stream arg ),
 *         any READ stream opened against the target file will be able to read the original file content.
 * NOTE -- To properly preserve all file times possible ( atime espc. ), this is the expected repacking workflow:
 *         datastream_repack( repackstream, "tgtfile", pos ) -- open a repack stream for the file
 *         datastream_scan( readstream, "tgtfile", pos ) -- open a read stream for the same file
 *         datastream_read( readstream )  AND
 *           datastream_write( repackstream ) -- duplicate all file content into repackstream
 *         datastream_release( readstream )  OR
 *           datastream_scan( readstream, ... ) -- terminate or progress readstream
 *         datastream_close( repackstream )  OR
 *           datastream_repack( repackstream, ... ) -- terminate or progress repackstream
 * @param DATASTREAM* stream : Reference to an existing REPACK DATASTREAM;
 *                             if that stream is NULL a fresh stream will be generated to replace it
 * @param const char* refpath : Reference path of the file to be repacked
 * @param marfs_position* pos : Reference to the marfs_position value of the target file
 * @param const char* ctag : Client tag to be associated with this stream
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
int datastream_repack(DATASTREAM* stream, const char* refpath, marfs_position* pos, const char* ctag) {
   // check for a NULL path arg
   if (refpath == NULL) {
      LOG(LOG_ERR, "Received a NULL refpath argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for a NULL position
   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL position argument\n");
      errno = EINVAL;
      return -1;
   }
   // check for NULL stream reference
   if (stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference argument\n");
      errno = EINVAL;
      return -1;
   }
   char closestream = 0;
   DATASTREAM newstream = *stream;
   // check if we've been passed an existing stream
   if (newstream) {
      if (newstream->type != REPACK_STREAM) {
         LOG(LOG_ERR, "Received non-CREATE stream\n");
         errno = EINVAL;
         return -1;
      }
      if (newstream->ns != pos->ns) {
         LOG(LOG_INFO, "Received datastream has different NS target: \"%s\"\n",
            newstream->ns->idstr);
         // can't continue with a stream from a previous NS
         closestream = 1;
         newstream = NULL; // so that stream generation logic kicks in later
      }
      else {
         // we're going to continue using the provided stream structure
         size_t curobj = newstream->objno;
         // finalize the current file
         if (finfile(newstream)) {
            LOG(LOG_ERR, "Failed to finalize previous stream file\n");
            freestream(newstream);
            *stream = NULL; // unsafe to reuse this stream
            errno = EBADFD;
            return -1;
         }
         // progress to the next file
         newstream->curfile++;
         newstream->fileno++;
         // create the new file
         if (open_repack_file(newstream, refpath, pos->ctxt)) {
            LOG(LOG_ERR, "Failed to repack new file: \"%s\"\n", refpath);
            // roll back our stream changes
            newstream->curfile--;
            newstream->fileno--;
            if (errno == EBADFD) {
               errno = ENOMSG;
            } // avoid using our reserved errno value
            return -1;
         }
         // check for an object transition
         STREAMFILE* newfile = newstream->files + newstream->curfile;
         if (newfile->ftag.objno != curobj) {
            size_t newfilepos = newstream->curfile;
            LOG(LOG_INFO, "Stream has transitioned from objno %zu to %zu\n",
               curobj, newfile->ftag.objno);
            // close our data handle
            FTAG oldftag = (newfile - 1)->ftag;
            oldftag.objno = curobj;
            if (close_current_obj(newstream, &(oldftag), pos->ctxt)) {
               LOG(LOG_ERR, "Failure to close data object %zu\n", curobj);
               freestream(newstream);
               *stream = NULL; // unsafe to reuse this stream
               errno = EBADFD;
               return -1;
            }
            // we need to mark all previous files as complete
            char abortflag = 0;
            while (newstream->curfile) {
               newstream->curfile--;
               if (completefile(newstream, newstream->files + newstream->curfile)) {
                  LOG(LOG_ERR, "Failed to complete file %zu\n",
                     (newstream->files + newstream->curfile)->ftag.fileno);
                  abortflag = 1;
               }
            }
            // shift the new file reference to the front of the list
            newstream->files[0] = newstream->files[newfilepos];
            // check for any errors
            if (abortflag) {
               LOG(LOG_INFO, "Terminating datastream due to previous errors\n");
               freestream(newstream);
               *stream = NULL; // unsafe to reuse this stream
               errno = EBADFD;
               return -1;
            }
         }
         else {
            // at least need to push out the 'FINALIZED' state of the previous file
            if (putftag(newstream, newstream->files + (newstream->curfile - 1))) {
               LOG(LOG_ERR, "Failed to push the FINALIZED FTAG for the previous file\n");
               freestream(newstream);
               *stream = NULL; // unsafe to reuse this stream
               errno = EBADFD;
               return -1;
            }
         }
      }
   }
   if (newstream == NULL) { // recheck, so as to catch if the prev stream was abandoned
      // we need to generate a fresh stream structure
      newstream = genstream(REPACK_STREAM, refpath, 1, pos, 0, ctag, NULL);
   }
   // check if we need to close the previous stream
   if (closestream) {
      if (datastream_close(stream)) {
         LOG(LOG_ERR, "Failed to close previous datastream\n");
         *stream = NULL; // don't attempt to close the original stream again
         if (newstream) {
            freestream(newstream);
         } // get rid of our new stream as well
         errno = EBADFD;
         return -1;
      }
   }
   // finally, check to ensure we at least have a valid stream to return
   if (newstream == NULL) {
      // still NULL means failure of genstream()
      LOG(LOG_ERR, "Failed to generate new stream\n");
      return -1;
   }
   // update the external stream reference
   *stream = newstream;

   return 0;
}

/**
 * Cleans up state from a previous repack operation
 * NOTE -- This should only be necessary for a repack operation left in an incomplete state.
 * @param const char* refpath : Reference path of the repack marker file for the previous operation
 * @param marfs_position* pos : Reference to the marfs_position value of the target file
 * @return int : Zero on successful cleanup, or -1 on failure
 */
int datastream_repack_cleanup(const char* refpath, marfs_position* pos) {
   // check for invalid args
   if ( refpath == NULL ) {
      LOG( LOG_ERR, "Received a NULL refpath argument\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos == NULL  ||  pos->ctxt == NULL  ||  pos->ns == NULL ) {
      LOG( LOG_ERR, "Received an invalid position argument\n" );
      errno = EINVAL;
      return -1;
   }
   // shorthand refs
   marfs_ms* ms = &(pos->ns->prepo->metascheme);
   // stat the repack marker to stash time values
   struct stat stval;
   if ( ms->mdal->statref( pos->ctxt, refpath, &(stval) ) ) {
      LOG( LOG_ERR, "Failed to stat repack marker file: \"%s\"\n", refpath );
      return -1;
   }
   // open the repack marker file
   MDAL_FHANDLE rmarker = ms->mdal->openref( pos->ctxt, refpath, O_RDWR, 0 );
   if ( rmarker == NULL ) {
      LOG( LOG_ERR, "Failed to open repack marker target: \"%s\"\n", refpath );
      return -1;
   }
   // retrieve the 'target' FTAG value
   FTAG tgtftag;
   ssize_t tgtftagstrlen = ms->mdal->fgetxattr( rmarker, 1, TREPACK_TAG_NAME, NULL, 0 );
   if ( tgtftagstrlen < 0 ) {
      if ( errno != ENODATA ) {
         LOG( LOG_ERR, "Failed to retrieve \"%s\" value from repack marker \"%s\"\n", TREPACK_TAG_NAME, refpath );
         ms->mdal->close( rmarker );
         return -1;
      }
      // no 'target' value means the repack barely got anywhere, so just delete the marker
      ms->mdal->close( rmarker );
      if ( ms->mdal->unlinkref( pos->ctxt, refpath ) ) {
         LOG( LOG_ERR, "Failed to unlink repack marker \"%s\"\n", refpath );
         return -1;
      }
      return 0; // all done
   }
   char* tgtftagstr = malloc( sizeof(char) * (tgtftagstrlen + 21) ); // leave extra space ( so we can maybe reuse )
   if ( tgtftagstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate tgtftagstr of size %zd\n", tgtftagstrlen + 1 );
      ms->mdal->close( rmarker );
      return -1;
   }
   if ( ms->mdal->fgetxattr( rmarker, 1, TREPACK_TAG_NAME, tgtftagstr, tgtftagstrlen ) != tgtftagstrlen ) {
      LOG( LOG_ERR, "\"%s\" value of repack marker \"%s\" has inconsistent length\n", TREPACK_TAG_NAME, refpath );
      free( tgtftagstr );
      ms->mdal->close( rmarker );
      return -1;
   }
   *(tgtftagstr + tgtftagstrlen) = '\0'; // ensure a NULL-terminated string
   if ( ftag_initstr( &(tgtftag), tgtftagstr ) ) {
      LOG( LOG_ERR, "Failed to parse \"%s\" value of repack marker \"%s\"\n", TREPACK_TAG_NAME, refpath );
      free( tgtftagstr );
      ms->mdal->close( rmarker );
      return -1;
   }
   // identify and open the repack target file
   char* repacktgtpath = datastream_genrpath( &(tgtftag), ms );
   if ( repacktgtpath == NULL ) {
      LOG( LOG_ERR, "Failed to identify repack tgt path of repack marker \"%s\"\n", refpath );
      free( tgtftagstr );
      free( tgtftag.ctag );
      free( tgtftag.streamid );
      ms->mdal->close( rmarker );
      return -1;
   }
   free( tgtftag.ctag );
   free( tgtftag.streamid );
   MDAL_FHANDLE tgtfile = ms->mdal->openref( pos->ctxt, repacktgtpath, O_RDWR, 0 );
   if ( tgtfile == NULL ) {
      if ( errno == ENOENT ) {
         // absence of the target means it should be safe to simply delete the repack marker
         free( repacktgtpath );
         free( tgtftagstr );
         ms->mdal->close( rmarker );
         if ( ms->mdal->unlinkref( pos->ctxt, refpath ) ) {
            LOG( LOG_ERR, "Failed to unlink repack marker \"%s\"\n", refpath );
            return -1;
         }
         return 0; // all done
      }
      LOG( LOG_ERR, "Failed to open repack target path: \"%s\"\n", repacktgtpath );
      free( repacktgtpath );
      free( tgtftagstr );
      ms->mdal->close( rmarker );
      return -1;
   }
   // retrieve 'tgt' FTAG value from the tgt file
   char activetgtpresent = 0;
   ssize_t activetgtftagstrlen = ms->mdal->fgetxattr( tgtfile, 1, TREPACK_TAG_NAME, NULL, 0 );
   if ( activetgtftagstrlen < 1  &&  errno != ENODATA ) {
      LOG( LOG_ERR, "Failed to retrieve \"%s\" value from \"%s\"\n", TREPACK_TAG_NAME, repacktgtpath );
      ms->mdal->close( tgtfile );
      free( tgtftagstr );
      free( repacktgtpath );
      ms->mdal->close( rmarker );
      return -1;
   }
   else if ( activetgtftagstrlen > 0 ) {
      activetgtpresent = 1;
      if ( activetgtftagstrlen > tgtftagstrlen + 20 ) {
         // expand our tgtftag string, if necessary
         char* newtgtftagstr = realloc( tgtftagstr, activetgtftagstrlen + 1 );
         if ( newtgtftagstr == NULL ) {
            LOG( LOG_ERR, "Failed to allocate %zd bytes for \"%s\" value of file \"%s\"\n",
                 activetgtftagstrlen + 1, TREPACK_TAG_NAME, repacktgtpath );
            ms->mdal->close( tgtfile );
            free( tgtftagstr );
            free( repacktgtpath );
            ms->mdal->close( rmarker );
            return -1;
         }
         tgtftagstr = newtgtftagstr;
      }
      tgtftagstrlen = activetgtftagstrlen;
      if ( ms->mdal->fgetxattr( tgtfile, 1, TREPACK_TAG_NAME, tgtftagstr, tgtftagstrlen ) != tgtftagstrlen ) {
         LOG( LOG_ERR, "\"%s\" value of \"%s\" has an inconsistent length\n", TREPACK_TAG_NAME, repacktgtpath );
         ms->mdal->close( tgtfile );
         free( tgtftagstr );
         free( repacktgtpath );
         ms->mdal->close( rmarker );
         return -1;
      }
   }
   // now check for a real FTAG value
   char* renametgt = NULL;
   ssize_t realftagstrlen = ms->mdal->fgetxattr( rmarker, 1, FTAG_NAME, NULL, 0 );
   if ( realftagstrlen < 0  &&  errno == ENODATA ) {
      // no active FTAG value means we'll be renamed over the target postion
      renametgt = repacktgtpath;
      repacktgtpath = NULL;
      if ( activetgtpresent ) {
         // overwrite our marker's tgt FTAG with that of the active file (if present)
         // NOTE -- this is to save us if the program dies within the next couple of operations, allowing
         //         us to pick up this value again
         if ( ms->mdal->fsetxattr( rmarker, 1, TREPACK_TAG_NAME, tgtftagstr, tgtftagstrlen, XATTR_REPLACE ) ) {
         }
         // remove the tgt FTAG from the active file (if present)
         // NOTE -- this is to save us if this program dies before the actual rename, as it will trigger the 
         //         'existing FTAG' path and we don't want to replace the tgt file's active FTAG
         if ( ms->mdal->fremovexattr( tgtfile, 1, TREPACK_TAG_NAME ) ) {
         }
      }
      // set the marker's real FTAG to the same value, or to the marker's value ( if the tgt file didn't have it )
      if ( ms->mdal->fsetxattr( rmarker, 1, FTAG_NAME, tgtftagstr, tgtftagstrlen, XATTR_CREATE ) ) {
      }
   }
   else if ( realftagstrlen > 0 ) {
      // existing FTAG value means the marker was about to be renamed, so we need to identify that target
      free( repacktgtpath ); // shouldn't need this path any more
      char* realftagstr = malloc( sizeof(char) * (realftagstrlen + 1) );
      if ( realftagstr == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for existing marker FTAG string of length %zd\n", realftagstrlen + 1 );
         ms->mdal->close( tgtfile );
         free( tgtftagstr );
         ms->mdal->close( rmarker );
         return -1;
      }
      if ( ms->mdal->fgetxattr( rmarker, 1, FTAG_NAME, realftagstr, realftagstrlen ) != realftagstrlen ) {
         LOG( LOG_ERR, "FTAG of rebuild marker \"%s\" has inconsistent length\n", refpath );
         free( realftagstr );
         ms->mdal->close( tgtfile );
         free( tgtftagstr );
         ms->mdal->close( rmarker );
         return -1;
      }
      FTAG realftag;
      if ( ftag_initstr( &(realftag), realftagstr ) ) {
         LOG( LOG_ERR, "FTAG of rebuild marker \"%s\" could not be parsed\n", refpath );
         free( realftagstr );
         ms->mdal->close( tgtfile );
         free( tgtftagstr );
         ms->mdal->close( rmarker );
         return -1;
      }
      free( realftagstr );
      ssize_t renamestrlen = ftag_metatgt( &(realftag), NULL, 0 );
      if ( renamestrlen < 1 ) {
         LOG( LOG_ERR, "FTAG of rebuild marker \"%s\" could not be parsed\n", refpath );
         free( realftag.ctag );
         free( realftag.streamid );
         ms->mdal->close( tgtfile );
         free( tgtftagstr );
         ms->mdal->close( rmarker );
         return -1;
      }
      renametgt = malloc( sizeof(char) * (renamestrlen + 1) );
      if ( renametgt == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for rename tgt string of length %zd\n", renamestrlen + 1 );
         free( realftag.ctag );
         free( realftag.streamid );
         ms->mdal->close( tgtfile );
         free( tgtftagstr );
         ms->mdal->close( rmarker );
         return -1;
      }
      if ( ftag_metatgt( &(realftag), renametgt, renamestrlen + 1 ) != renamestrlen ) {
         LOG( LOG_ERR, "Rename tgt of rebuild marker \"%s\" has an inconsistent length\n", refpath );
         free( renametgt );
         free( realftag.ctag );
         free( realftag.streamid );
         ms->mdal->close( tgtfile );
         free( tgtftagstr );
         ms->mdal->close( rmarker );
         return -1;
      }
      free( realftag.ctag );
      free( realftag.streamid );
      if ( activetgtpresent ) {
         // copy the active file's tgt FTAG over the real FTAG (if present)
         if ( ms->mdal->fsetxattr( tgtfile, 1, FTAG_NAME, tgtftagstr, tgtftagstrlen, XATTR_REPLACE ) ) {
            LOG( LOG_ERR, "Failed to update active FTAG of target file\n" );
            free( renametgt );
            ms->mdal->close( tgtfile );
            free( tgtftagstr );
            ms->mdal->close( rmarker );
            return -1;
         }
         // remove the tgt FTAG from the active file (if present)
         if ( ms->mdal->fremovexattr( tgtfile, 1, TREPACK_TAG_NAME ) ) {
            LOG( LOG_ERR, "Failed to remove \"%s\" value of target file\n", TREPACK_TAG_NAME );
            free( renametgt );
            ms->mdal->close( tgtfile );
            free( tgtftagstr );
            ms->mdal->close( rmarker );
            return -1;
         }
      }
   }
   else {
      // some miscellaneous failure
      LOG( LOG_ERR, "Failed to determine if an active FTAG value is attached to repack marker \"%s\"\n", refpath );
      ms->mdal->close( tgtfile );
      free( tgtftagstr );
      free( repacktgtpath );
      ms->mdal->close( rmarker );
      return -1;
   }
   free( tgtftagstr );
   // close outstanding handles
   if ( ms->mdal->close( tgtfile )  ||  ms->mdal->close( rmarker ) ) {
      LOG( LOG_ERR, "Failed to close outstanding file handles\n" );
      free( renametgt );
      return -1;
   }
   // FINALLY, rename the repack marker over our designated tgt
   if ( ms->mdal->renameref( pos->ctxt, refpath, renametgt ) ) {
      LOG( LOG_ERR, "Failed to rename repack marker \"%s\" over \"%s\"\n", refpath, renametgt );
      free( renametgt );
      return -1;
   }
   free( renametgt );
   return 0;
}

/**
 * Release the given DATASTREAM ( close the stream without completing the referenced file )
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be released
 * @return int : Zero on success, or -1 on failure
 */
int datastream_release(DATASTREAM* stream) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != EDIT_STREAM &&
      tgtstream->type != CREATE_STREAM &&
      tgtstream->type != REPACK_STREAM &&
      tgtstream->type != READ_STREAM) {
      LOG(LOG_ERR, "Received STREAM_TYPE is unsupported\n");
      errno = EINVAL;
      return -1;
   }
   // shorthand references
   const marfs_ms* ms = &(tgtstream->ns->prepo->metascheme);
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   // create/edit streams require extra attention
   if (tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) {
      // make sure we're releasing a file that actually got extended
      if (!(curfile->ftag.state & FTAG_WRITEABLE) || tgtstream->curfile) {
         LOG(LOG_ERR, "Cannot release non-extended file reference\n");
         freestream(tgtstream);
         *stream = NULL;
         errno = EINVAL;
         return -1;
      }
      // finalize the current file
      if (finfile(tgtstream)) {
         LOG(LOG_ERR, "Failed to finalize previous stream file\n");
         freestream(tgtstream);
         *stream = NULL; // unsafe to reuse this stream
         return -1;
      }
      curfile->ftag.endofstream = 1; // indicate that the stream ends with this file
      curfile->ftag.availbytes = curfile->ftag.bytes; // allow access to all data content
   }
   else if (tgtstream->type == EDIT_STREAM) { // for edit streams...
      // if we've output data, output file recovery info
      if (tgtstream->datahandle != NULL && putfinfo(tgtstream)) {
         LOG(LOG_ERR, "Failed to output file recovery info to current obj\n");
         freestream(tgtstream);
         *stream = NULL; // unsafe to reuse this stream
         return -1;
      }
   }
   // close our data handle
   char abortflag = 0;
   FTAG curftag = curfile->ftag;
   curftag.objno = tgtstream->objno;
   curftag.offset = tgtstream->offset;
   if (close_current_obj(tgtstream, &(curftag), NULL)) {
      LOG(LOG_ERR, "Close failure for object %zu\n", tgtstream->objno);
      abortflag = 1;
   }
   // for create streams, update the ftag to a finalizd state
   else if ((tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) &&
            putftag(tgtstream, curfile)) {
      LOG(LOG_ERR, "Failed to update FTAG of file %zu\n", curfile->ftag.fileno);
      abortflag = 1;
   }
   // if this is a create stream OR if utimens was called, set atime/mtime values
   else if ((tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM || curfile->dotimes) &&
            ms->mdal->futimens(curfile->metahandle, curfile->times)) {
      LOG(LOG_ERR, "Failed to update time values on file %zu\n", curfile->ftag.fileno);
      abortflag = 1;
   }
   // check for any errors
   if (abortflag) {
      LOG(LOG_INFO, "Terminating datastream due to previous errors\n");
      freestream(tgtstream);
      *stream = NULL; // unsafe to reuse this stream
      return -1;
   }

   // successfully completed all ops, just need to cleanup refs
   *stream = NULL;
   freestream(tgtstream);
   return 0;
}

/**
 * Close the given DATASTREAM ( marking the referenced file as complete, for non-READ )
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be closed
 * @return int : Zero on success, or -1 on failure
 */
int datastream_close(DATASTREAM* stream) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != EDIT_STREAM &&
      tgtstream->type != CREATE_STREAM &&
      tgtstream->type != REPACK_STREAM &&
      tgtstream->type != READ_STREAM) {
      LOG(LOG_ERR, "Received STREAM_TYPE is unsupported\n");
      errno = EINVAL;
      return -1;
   }
   // shorthand references
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   // create/edit streams require extra attention
   if (tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) {
      // make sure we're closing a file that did not get extended
      if (curfile->ftag.state & FTAG_WRITEABLE) {
         LOG(LOG_ERR, "Cannot close extended file reference\n");
         freestream(tgtstream);
         *stream = NULL;
         errno = EINVAL;
         return -1;
      }
      // finalize the current file
      if (finfile(tgtstream)) {
         LOG(LOG_ERR, "Failed to finalize previous stream file\n");
         freestream(tgtstream);
         *stream = NULL; // unsafe to reuse this stream
         return -1;
      }
      curfile->ftag.endofstream = 1; // indicate that the stream ends with this file
   }
   else if (tgtstream->type == EDIT_STREAM) {
      // make sure we're closing a writeable and finalized file
      if (!(curfile->ftag.state & FTAG_WRITEABLE) ||
         (curfile->ftag.state & FTAG_DATASTATE) != FTAG_FIN) {
         LOG(LOG_ERR, "Cannot close non-extended, non-finalized file reference\n");
         freestream(tgtstream);
         *stream = NULL;
         errno = EINVAL;
         return -1;
      }
      // if we've output data, output file recovery info
      if (tgtstream->datahandle != NULL && putfinfo(tgtstream)) {
         LOG(LOG_ERR, "Failed to output file recovery info to current obj\n");
         freestream(tgtstream);
         *stream = NULL; // unsafe to reuse this stream
         return -1;
      }
   }
   // close our data handle
   FTAG curftag = curfile->ftag;
   curftag.objno = tgtstream->objno;
   curftag.offset = tgtstream->offset;
   if (close_current_obj(tgtstream, &(curftag), NULL)) {
      LOG(LOG_ERR, "Failure during close of object %zu\n", tgtstream->objno);
      freestream(tgtstream);
      *stream = NULL; // unsafe to reuse this stream
      return -1;
   }
   // cleanup all open files
   char abortflag = 0;
   MDAL mdal = tgtstream->ns->prepo->metascheme.mdal;
   while (1) {  // exit cond near loop bottom ( we need to run even when curfile == 0, but don't want to decrement further )
      STREAMFILE* compfile = tgtstream->files + tgtstream->curfile;
      // for non-read streams, complete the current file
      if (tgtstream->type != READ_STREAM) {
         // for non-read streams, mark all outstanding files as 'complete'
         if (completefile(tgtstream, compfile)) {
            LOG(LOG_ERR, "Failed to complete file %zu\n", compfile->ftag.fileno);
            abortflag = 1;
         }
      }
      // for read streams, just close the handle
      else if (mdal->close(compfile->metahandle)) {
         LOG(LOG_ERR, "Failed to close metahandle for file %zu\n", compfile->ftag.fileno);
         abortflag = 1;
      }
      compfile->metahandle = NULL; // don't reattempt this close op
      // exit condition
      if (tgtstream->curfile == 0) {
         break;
      }
      tgtstream->curfile--;
   }
   // check for any errors
   if (abortflag) {
      LOG(LOG_INFO, "Terminating datastream due to previous errors\n");
      freestream(tgtstream);
      *stream = NULL; // unsafe to reuse this stream
      return -1;
   }

   // successfully completed all ops, just need to cleanup refs
   *stream = NULL;
   freestream(tgtstream);
   return 0;
}

/**
 * Read from the file currently referenced by the given READ DATASTREAM
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be read from
 * @param void* buf : Reference to the buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 on failure
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
ssize_t datastream_read(DATASTREAM* stream, void* buf, size_t count) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != READ_STREAM) {
      LOG(LOG_ERR, "Provided stream does not support reading\n");
      errno = EINVAL;
      return -1;
   }
   if (count > SSIZE_MAX) {
      LOG(LOG_ERR, "Provided byte count exceeds max return value: %zu\n", count);
      errno = EINVAL;
      return -1;
   }
   // identify current position info
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   DATASTREAM_POSITION streampos = {
      .totaloffset = 0,
      .dataremaining = 0,
      .excessremaining = 0,
      .objno = 0,
      .offset = 0,
      .excessoffset = 0,
      .dataperobj = 0
   };
   if (gettargets(tgtstream, 0, SEEK_CUR, &(streampos))) {
      LOG(LOG_ERR, "Failed to identify position vals of file %zu\n", curfile->ftag.fileno);
      return -1;
   }

   // reduce read request to account for file limits
   size_t zerotailbytes = 0;
   if (count > streampos.dataremaining + streampos.excessremaining) {
      count = streampos.dataremaining + streampos.excessremaining;
      LOG(LOG_INFO, "Read request exceeds file bounds, resizing to %zu bytes\n", count);
   }
   if (count > streampos.dataremaining) {
      zerotailbytes = count - streampos.dataremaining;
      count = streampos.dataremaining;
      LOG(LOG_INFO, "Read request exceeds data content, appending %zu tailing zero bytes\n",
         zerotailbytes);
   }

   // retrieve data until we no longer can
   size_t readbytes = 0;
   while (count) {
      // calculate how much data we can read from the current data object
      size_t toread = streampos.dataperobj - (tgtstream->offset - tgtstream->recoveryheaderlen);
      if (toread == 0) {
         // close the previous data handle
         FTAG curftag = curfile->ftag;
         curftag.objno = tgtstream->objno;
         curftag.offset = tgtstream->offset;
         if (close_current_obj(tgtstream, &(curftag), NULL)) {
            // NOTE -- this doesn't necessarily have to be a fatal error on read.
            //         However, I really don't want us to ignore this sort of thing,
            //         as it could indicate imminent data loss ( corrupt object which
            //         we are now failing to tag ).  So... maybe better to fail
            //         catastrophically.
            LOG(LOG_ERR, "Failed to close previous data object\n");
            freestream(tgtstream);
            *stream = NULL;
            errno = EBADFD;
            return -1;
         }
         // progress to the next data object
         tgtstream->objno++;
         tgtstream->offset = tgtstream->recoveryheaderlen;
         toread = streampos.dataperobj;
         LOG(LOG_INFO, "Progressing read into object %zu ( offset = %zu )\n",
            tgtstream->objno, tgtstream->offset);
      }
      // limit our data read to the actual request size
      if (toread > count) {
         toread = count;
      }
      // open the current data object, if necessary
      if (tgtstream->datahandle == NULL) {
         LOG(LOG_INFO, "Opening object %zu\n", tgtstream->objno);
         if (open_current_obj(tgtstream)) {
            LOG(LOG_ERR, "Failed to open data object %zu\n", tgtstream->objno);
            return (readbytes) ? readbytes : -1;
         }
      }
      // perform the actual read op
      LOG(LOG_INFO, "Reading %zu bytes from object %zu\n", toread, tgtstream->objno);
      ssize_t readres = ne_read(tgtstream->datahandle, buf, toread);
      if (readres <= 0) {
         LOG(LOG_ERR, "Read failure in object %zu at offset %zu ( res = %zd )\n",
            tgtstream->objno, tgtstream->offset, readres);
         return (readbytes) ? readbytes : -1;
      }
      LOG(LOG_INFO, "Read op returned %zd bytes\n", readres);
      // adjust all offsets and byte counts
      buf += readres;
      count -= readres;
      readbytes += readres;
      tgtstream->offset += readres;
   }

   // append zero bytes to account for file truncated beyond data length
   if (zerotailbytes) {
      bzero(buf, zerotailbytes);
      readbytes += zerotailbytes;
      tgtstream->excessoffset += zerotailbytes;
   }

   return readbytes;
}

/**
 * Write to the file currently referenced by the given EDIT or CREATE DATASTREAM
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be written to
 * @param const void* buf : Reference to the buffer containing data to be written
 * @param size_t count : Number of bytes to be written
 * @return ssize_t : Number of bytes written, or -1 on failure
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
ssize_t datastream_write(DATASTREAM* stream, const void* buf, size_t count) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != CREATE_STREAM && tgtstream->type != EDIT_STREAM  &&  tgtstream->type != REPACK_STREAM) {
      LOG(LOG_ERR, "Provided stream does not support writing\n");
      errno = EINVAL;
      return -1;
   }
   if (count > SSIZE_MAX) {
      LOG(LOG_ERR, "Provided byte count exceeds max return value: %zu\n", count);
      errno = EINVAL;
      return -1;
   }
   // check for FTAG states that prohibit writing
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   if (tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) {
      if ((curfile->ftag.state & FTAG_DATASTATE) >= FTAG_FIN) {
         LOG(LOG_ERR, "Provided create stream references a finalized file\n");
         errno = EINVAL;
         return -1;
      }
      if ((curfile->ftag.state & FTAG_WRITEABLE)) {
         LOG(LOG_ERR, "Provided create stream references an extended file\n");
         errno = EINVAL;
         return -1;
      }
   }
   if (tgtstream->type == EDIT_STREAM &&
      ((curfile->ftag.state & FTAG_DATASTATE) == FTAG_INIT ||
         (curfile->ftag.state & FTAG_DATASTATE) == FTAG_COMP)) {
      LOG(LOG_ERR, "Provided edit stream references either a complete or un-sized file\n");
      errno = EINVAL;
      return -1;
   }
   // identify current position info
   DATASTREAM_POSITION streampos = {
      .totaloffset = 0,
      .dataremaining = 0,
      .excessremaining = 0,
      .objno = 0,
      .offset = 0,
      .excessoffset = 0,
      .dataperobj = 0
   };
   if (gettargets(tgtstream, 0, SEEK_CUR, &(streampos))) {
      LOG(LOG_ERR, "Failed to identify position vals of file %zu\n", curfile->ftag.fileno);
      return -1;
   }

   // reduce write request to account for file limits
   if (tgtstream->type == EDIT_STREAM && count > streampos.dataremaining) {
      count = streampos.dataremaining;
      LOG(LOG_INFO, "Write request exceeds file bounds, resizing to %zu bytes\n", count);
   }

   // write all provided data until we no longer can
   size_t writtenbytes = 0;
   while (count) {
      // calculate how much data we can write to the current data object
      size_t towrite = streampos.dataperobj - (tgtstream->offset - tgtstream->recoveryheaderlen);
      if (towrite == 0) {
         // if we have a current data handle, need to output trailing recov info
         if (tgtstream->datahandle && putfinfo(tgtstream)) {
            LOG(LOG_ERR, "Failed to output recovery info to tail of object %zu\n", tgtstream->objno);
            freestream(tgtstream);
            *stream = NULL; // unsafe to continue with previous handle
            return -1;
         }
         // close the previous data handle
         FTAG curftag = curfile->ftag;
         curftag.objno = tgtstream->objno;
         curftag.offset = tgtstream->offset;
         if (close_current_obj(tgtstream, &(curftag), NULL)) {
            LOG(LOG_ERR, "Failed to close previous data object\n");
            freestream(tgtstream);
            *stream = NULL; // unsafe to continue with previous handle
            return -1;
         }
         // we (may) need to mark all previous files as complete
         if (tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) {
            size_t curfilepos = tgtstream->curfile;
            char abortflag = 0;
            while (tgtstream->curfile) {
               tgtstream->curfile--;
               if (completefile(tgtstream, tgtstream->files + tgtstream->curfile)) {
                  LOG(LOG_ERR, "Failed to complete file %zu\n",
                     (tgtstream->files + tgtstream->curfile)->ftag.fileno);
                  abortflag = 1;
               }
            }
            // check for any errors
            if (abortflag) {
               LOG(LOG_INFO, "Terminating datastream due to previous errors\n");
               freestream(tgtstream);
               *stream = NULL; // unsafe to reuse this stream
               errno = EBADFD;
               return -1;
            }
            if (curfilepos != tgtstream->curfile) {
               // shift the new file reference to the front of the list
               tgtstream->files[0] = tgtstream->files[curfilepos];
               curfile = tgtstream->files;
            }
         }

         // progress to the next data object
         tgtstream->objno++;
         tgtstream->offset = tgtstream->recoveryheaderlen;
         towrite = streampos.dataperobj;
         LOG(LOG_INFO, "Progressing write into object %zu\n", tgtstream->objno);
      }
      if (towrite > count) {
         towrite = count;
      }
      // open the current data object, if necessary
      if (tgtstream->datahandle == NULL) {
         if (open_current_obj(tgtstream)) {
            LOG(LOG_ERR, "Failed to open data object %zu\n", tgtstream->objno);
            return (writtenbytes) ? writtenbytes : -1;
         }
      }
      // perform the actual write op
      ssize_t writeres = ne_write(tgtstream->datahandle, buf, towrite);
      if (writeres <= 0) {
         LOG(LOG_ERR, "Write failure in object %zu at offset %zu\n",
            tgtstream->objno, tgtstream->offset);
         return (writtenbytes) ? writtenbytes : -1;
      }
      // adjust all offsets and byte counts
      buf += writeres;
      count -= writeres;
      writtenbytes += writeres;
      tgtstream->offset += writeres;
      if (tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) {
         // for create streams, increase the actual file data size
         curfile->ftag.bytes += writeres;
      }
   }

   // special case check, identify if this is an edit stream just wrote to EOF
   if (tgtstream->type == EDIT_STREAM &&
      (curfile->ftag.state & FTAG_DATASTATE) == FTAG_FIN &&
      writtenbytes == streampos.dataremaining) {
      tgtstream->finfo.eof = 1;
   }

   return writtenbytes;
}

/**
 * Change the recovery info pathname for the file referenced by the given CREATE or
 * EDIT DATASTREAM
 * @param DATASTREAM* stream : Reference to the DATASTREAM to set recovery pathname for
 * @param const char* recovpath : New recovery info pathname for the file
 * @return int : Zero on success, or -1 on failure
 */
int datastream_setrecoverypath(DATASTREAM* stream, const char* recovpath) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   if (recovpath == NULL) {
      LOG(LOG_ERR, "Received a NULL recovpath string\n");
      errno = EINVAL;
      return -1;
   }
   // check if this stream is of an appropriate type
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != CREATE_STREAM && tgtstream->type != EDIT_STREAM  &&  tgtstream->type != REPACK_STREAM) {
      LOG(LOG_ERR, "Received stream type is not supported\n");
      errno = EINVAL;
      return -1;
   }
   // perform stream->type specific check
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   if (tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) {
      // cannot adjust recovery path after we've started laying out data ( write or extend )
      if (curfile->ftag.bytes) {
         LOG(LOG_ERR, "Received CREATE/REPACK stream already has associated data\n");
         errno = EINVAL;
         return -1;
      }
   }
   // adjust the finfo path
   char* oldpath = tgtstream->finfo.path;
   tgtstream->finfo.path = strdup(recovpath);
   if (tgtstream->finfo.path == NULL) {
      LOG(LOG_ERR, "Failed to duplicate new recovery path: \"%s\"\n", recovpath);
      tgtstream->finfo.path = oldpath;
      return -1;
   }
   // identify the new finfo strlen
   size_t newstrlen = recovery_finfotostr(&(tgtstream->finfo), NULL, 0);
   if (newstrlen < 1) {
      LOG(LOG_ERR, "Failed to produce recovery string with new recovery path\n");
      free(tgtstream->finfo.path);
      tgtstream->finfo.path = oldpath;
      return -1;
   }
   // perform stream->type specific actions
   if (tgtstream->type == EDIT_STREAM) {
      // ensure the new path won't exceed the file's existing recovery bytes setting
      if (newstrlen > curfile->ftag.recoverybytes) {
         LOG(LOG_ERR, "New recovery path results in excessive recovery string lenght of %zu bytes\n", newstrlen);
         free(tgtstream->finfo.path);
         tgtstream->finfo.path = oldpath;
         errno = ENAMETOOLONG; // probably the best way to describe this
         return -1;
      }
   }
   else { // implies type == CREATE
      // for create streams, we actually need to update our FTAG
      size_t oldrecovbytes = curfile->ftag.recoverybytes;
      curfile->ftag.recoverybytes = newstrlen;
      if (putftag(tgtstream, curfile)) {
         LOG(LOG_ERR, "Failed to update FTAG value to reflect new recovery length\n");
         curfile->ftag.recoverybytes = oldrecovbytes;
         free(tgtstream->finfo.path);
         tgtstream->finfo.path = oldpath;
         return -1;
      }
   }

   // path change has succeeded; cleanup old string
   free(oldpath);
   return 0;
}

/**
 * Seek to the provided offset of the file referenced by the given DATASTREAM
 * @param DATASTREAM* stream : Reference to the DATASTREAM
 * @param off_t offset : Offset for the seek
 * @param int whence : Flag defining seek start location ( see 'seek()' syscall manpage )
 * @return off_t : Resulting offset within the file, or -1 if a failure occurred
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
off_t datastream_seek(DATASTREAM* stream, off_t offset, int whence) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   DATASTREAM tgtstream = *stream;
   // identify target position info
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   DATASTREAM_POSITION streampos = {
      .totaloffset = 0,
      .dataremaining = 0,
      .excessremaining = 0,
      .objno = 0,
      .offset = 0,
      .excessoffset = 0,
      .dataperobj = 0
   };
   if (gettargets(tgtstream, offset, whence, &(streampos))) {
      LOG(LOG_ERR, "Failed to identify position vals of file %zu\n", curfile->ftag.fileno);
      return -1;
   }
   // CREATE streams are treated differently
   if (tgtstream->type == CREATE_STREAM  ||  tgtstream->type == REPACK_STREAM) {
      // check for reverse seek
      if (streampos.totaloffset < curfile->ftag.bytes) {
         LOG(LOG_ERR, "Cannot reverse seek CREATE stream to target offset: %zu\n",
            streampos.totaloffset);
         errno = EINVAL;
         return -1;
      }
      // check for no-op seek
      if (streampos.totaloffset == curfile->ftag.bytes) {
         LOG(LOG_INFO, "No-op seek to current offset\n");
         return (off_t)(curfile->ftag.bytes);
      }
      // forward seek actually means write out zero-bytes to reach the target
      void* zerobuf = calloc(1024, 1024); // 1MiB zero buffer
      if (zerobuf == NULL) {
         LOG(LOG_ERR, "Failed to allocate 1MiB zero buffer to write out intermediate data\n");
         return -1;
      }
      // write out zero buffers until we reach the target offset
      while (curfile->ftag.bytes < streampos.totaloffset) {
         size_t writesize = 1024 * 1024;
         if (writesize > (streampos.totaloffset - curfile->ftag.bytes)) {
            writesize = (streampos.totaloffset - curfile->ftag.bytes);
         }
         LOG(LOG_INFO, "Writing out %zu zero bytes to skip ahead\n");
         ssize_t writeres = datastream_write(stream, zerobuf, writesize);
         if (writeres != writesize) {
            LOG(LOG_ERR, "Subsized write ( expected = %zu, actual = %zd )\n",
               writesize, writeres);
            free(zerobuf);
            return curfile->ftag.bytes;
         }
      }
      // should now be at target offset
      free(zerobuf);
      LOG(LOG_INFO, "Post-write offset = %zu\n", curfile->ftag.bytes);
      return curfile->ftag.bytes;
   }
   // EDIT streams have specific target restrictions
   if (tgtstream->type == EDIT_STREAM &&
      streampos.offset != tgtstream->recoveryheaderlen) {
      LOG(LOG_ERR, "Edit streams can only seek to exact chunk bounds ( exoff = %zu )\n",
         streampos.offset - tgtstream->recoveryheaderlen);
      errno = EINVAL;
      return -1;
   }
   // check if we will be switching to a new data object and need to close the old handle
   if (tgtstream->objno != streampos.objno && tgtstream->datahandle != NULL) {
      // check if we need to output recovery info to the current obj
      if (tgtstream->type == EDIT_STREAM) {
         // if we have a current data handle, need to output trailing recov info
         if (putfinfo(tgtstream)) {
            LOG(LOG_ERR, "Failed to output recovery info to tail of object %zu\n", tgtstream->objno);
            freestream(tgtstream);
            *stream = NULL; // unsafe to continue with previous handle
            return -1;
         }
         tgtstream->finfo.eof = 0; // unset the EOF flag, as it no longer applies
      }
      // close any existing object handle
      FTAG curftag = curfile->ftag;
      curftag.objno = tgtstream->objno;
      curftag.offset = tgtstream->offset;
      if (close_current_obj(tgtstream, &(curftag), NULL)) {
         LOG(LOG_ERR, "Failed to close old stream data handle for object %zu\n", tgtstream->objno);
         freestream(tgtstream);
         *stream = NULL;
         errno = EBADFD;
         return -1;
      }
   }
   // if we have an open object, seek it to the appropriate offset
   if (tgtstream->datahandle != NULL &&
      ne_seek(tgtstream->datahandle, streampos.offset) != streampos.offset) {
      LOG(LOG_ERR, "Failed to seek to offset %zu of object %zu\n",
         streampos.offset, streampos.objno);
      freestream(tgtstream);
      *stream = NULL;
      errno = EBADFD;
      return -1;
   }
   // update stream position to reflect the new target
   tgtstream->objno = streampos.objno;
   tgtstream->offset = streampos.offset;
   tgtstream->excessoffset = streampos.excessoffset;
   return streampos.totaloffset;
}

/**
 * Identify the data object boundaries of the file referenced by the given DATASTREAM
 * @param DATASTREAM* stream : Reference to the DATASTREAM for which to retrieve info
 * @param int chunknum : Index of the data chunk to retrieve info for ( beginning at zero )
 * @param off_t* offset : Reference to be populated with the data offset of the start of
 *                        the target data chunk
 *                        ( as in, datastream_seek( stream, 'offset', SEEK_SET ) will move
 *                        you to the start of this data chunk )
 * @param size_t* size : Reference to be populated with the size of the target data chunk
 * @return int : Zero on success, or -1 on failure
 */
int datastream_chunkbounds(DATASTREAM* stream, int chunknum, off_t* offset, size_t* size) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   if (offset == NULL || size == NULL) {
      LOG(LOG_ERR, "Received NULL offset and/or size references\n");
      errno = EINVAL;
      return -1;
   }
   // identify the start position info for the current file
   DATASTREAM tgtstream = *stream;
   DATASTREAM_POSITION streampos = {
      .totaloffset = 0,
      .dataremaining = 0,
      .excessremaining = 0,
      .objno = 0,
      .offset = 0,
      .excessoffset = 0,
      .dataperobj = 0
   };
   if (gettargets(tgtstream, 0, SEEK_SET, &(streampos))) {
      LOG(LOG_ERR, "Failed to identify position vals of file %zu\n", (tgtstream->files + tgtstream->curfile)->ftag.fileno);
      return -1;
   }
   streampos.offset -= tgtstream->recoveryheaderlen; // adjust offset to ignore recovery info
   // calculate the total offset of the target chunk
   size_t tgtoff = 0;
   size_t chunksize = streampos.dataperobj - streampos.offset;
   if (chunknum > 0) {
      tgtoff += chunksize + ((chunknum - 1) * streampos.dataperobj);
   }
   // determine if we've exceeded actual data limits
   if (tgtoff > streampos.dataremaining) {
      LOG(LOG_ERR, "Target chunk ( %d ) is not within data bounds\n", chunknum);
      errno = EINVAL;
      return -1;
   }
   // determine if we need to limit chunksize
   if ((tgtoff + chunksize) > streampos.dataremaining) {
      chunksize = streampos.dataremaining - tgtoff;
   }
   // set values and return
   *offset = tgtoff;
   *size = chunksize;
   return 0;
}

/**
 * Extend the file referenced by the given CREATE DATASTREAM to the specified total size
 * This makes the specified data size accessible for parallel write.
 * NOTE -- The final data object of the file will only be accessible after this CREATE
 *         DATASTREAM has been released ( as that finalizes the file's data size ).
 *         This function can only be performed if no data has been written to the target
 *         file via this DATASTREAM.
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be extended
 * @param off_t length : Target total file length to extend to
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- In most failure conditions, any previous DATASTREAM reference will be
 *            preserved ( continue to reference whatever file they previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, the DATASTREAM will be destroyed, the 'stream' reference set
 *            to NULL, and errno set to EBADFD.
 */
int datastream_extend(DATASTREAM* stream, off_t length) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   // check for invalid stream type
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != CREATE_STREAM) {
      LOG(LOG_ERR, "Received a non-create stream\n");
      errno = EINVAL;
      return -1;
   }
   // check that the current file is in an appropriate state
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   if ((curfile->ftag.state & FTAG_DATASTATE) >= FTAG_FIN) {
      LOG(LOG_ERR, "Cannot extend a finalized file\n");
      errno = EINVAL;
      return -1;
   }
   if (curfile->ftag.bytes != 0 && tgtstream->datahandle != NULL) {
      LOG(LOG_ERR, "Cannot extend a file which has already been written to\n");
      errno = EINVAL;
      return -1;
   }
   if (curfile->ftag.bytes >= length) {
      LOG(LOG_ERR, "The current file exceeds or matches the specified length of %zu\n", length);
      errno = EINVAL;
      return -1;
   }
   // check if we have previous file references we need to clean up
   if (tgtstream->curfile) {
      // close our data handle ( if present )
      FTAG oldftag = (curfile - 1)->ftag;
      oldftag.objno = tgtstream->objno;
      oldftag.offset = tgtstream->offset;
      if (close_current_obj(tgtstream, &(oldftag), NULL)) {
         LOG(LOG_ERR, "Failure to close data object %zu\n", tgtstream->objno);
         freestream(tgtstream);
         *stream = NULL; // unsafe to reuse this stream
         errno = EBADFD;
         return -1;
      }
      // we need to mark all previous files as complete
      char abortflag = 0;
      size_t origfilepos = tgtstream->curfile;
      while (tgtstream->curfile) {
         tgtstream->curfile--;
         LOG(LOG_INFO, "Completing file %zu\n",
            (tgtstream->files + tgtstream->curfile)->ftag.fileno);
         if (completefile(tgtstream, tgtstream->files + tgtstream->curfile)) {
            LOG(LOG_ERR, "Failed to complete file %zu\n",
               (tgtstream->files + tgtstream->curfile)->ftag.fileno);
            abortflag = 1;
         }
      }
      // shift the new file reference to the front of the list
      tgtstream->files[0] = tgtstream->files[origfilepos];
      curfile = tgtstream->files;
      // check for any errors
      if (abortflag) {
         LOG(LOG_INFO, "Terminating datastream due to previous errors\n");
         freestream(tgtstream);
         *stream = NULL; // unsafe to reuse this stream
         errno = EBADFD;
         return -1;
      }
      // shift the current file to a fresh data object
      tgtstream->objno++;
      tgtstream->offset = tgtstream->recoveryheaderlen;
      curfile->ftag.objno = tgtstream->objno;
      curfile->ftag.offset = tgtstream->offset;
   }
   // increase the current file to the specified size
   size_t origbytes = curfile->ftag.bytes;
   curfile->ftag.bytes = length;

   // identify current object boundaries
   DATASTREAM_POSITION streampos = {
      .totaloffset = 0,
      .dataremaining = 0,
      .excessremaining = 0, // data added by this op will show up as 'excess' until complete
      .objno = 0,
      .offset = 0,
      .excessoffset = 0,
      .dataperobj = 0
   };
   if (gettargets(tgtstream, 0, SEEK_SET, &(streampos))) {
      LOG(LOG_ERR, "Failed to identify position vals of file %zu\n", curfile->ftag.fileno);
      curfile->ftag.bytes = origbytes;
      return -1;
   }
   if (streampos.offset != tgtstream->recoveryheaderlen) {
      LOG(LOG_ERR, "Unexpected offset value for extended file: %zu\n", streampos.offset);
      curfile->ftag.bytes = origbytes;
      return -1;
   }
   // calculate the number of complete data objects covered by this file
   //  (excluding the current object)
   size_t independentobjs = (streampos.dataremaining + streampos.excessremaining) / streampos.dataperobj;

   // if not done so already, mark the file as sized and writable by other procs
   curfile->ftag.state |= FTAG_WRITEABLE;
   if ((curfile->ftag.state & FTAG_DATASTATE) < FTAG_SIZED ||
      (curfile->ftag.state & FTAG_WRITEABLE) == 0) {
      curfile->ftag.state = (curfile->ftag.state & ~(FTAG_DATASTATE)) |
         FTAG_WRITEABLE |
         FTAG_SIZED;
   }
   // make any independent objects accessible
   curfile->ftag.availbytes = independentobjs * streampos.dataperobj;

   // update the ftag of the extended file
   if (putftag(tgtstream, curfile)) {
      LOG(LOG_ERR, "Failed to update FTAG value of file %zu\n", curfile->ftag.fileno);
      freestream(tgtstream);
      *stream = NULL; // unsafe to reuse this stream
      errno = EBADFD;
      return -1;
   }

   return 0;
}

/**
 * Truncate the file referenced by the given EDIT DATASTREAM to the specified length
 * NOTE -- This operation can only be performed on completed data files
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be truncated
 * @param off_t length : Target total file length to truncate to
 * @return int : Zero on success, or -1 on failure
 */
int datastream_truncate(DATASTREAM* stream, off_t length) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   if (length < 0) {
      LOG(LOG_ERR, "Received a negative length argument\n");
      errno = EINVAL;
      return -1;
   }
   // check if this stream is of an appropriate type
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != EDIT_STREAM) {
      LOG(LOG_ERR, "Received stream type is not supported\n");
      errno = EINVAL;
      return -1;
   }
   // verify that the current file is in an appropriate state
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   if ((curfile->ftag.state & FTAG_DATASTATE) != FTAG_COMP) {
      LOG(LOG_ERR, "Cannon truncate an incomplete file\n");
      errno = EINVAL;
      return -1;
   }
   size_t origbytes = curfile->ftag.availbytes; // stash the original availbytes value
   if (curfile->ftag.availbytes > length) {
      // reduce the files available bytes to the specified size
      curfile->ftag.availbytes = length;
   }
   // truncate the target file to the specified length
   const marfs_ms* ms = &(tgtstream->ns->prepo->metascheme);
   if (ms->mdal->ftruncate(curfile->metahandle, length)) {
      LOG(LOG_ERR, "Failed to truncate file %zu to proper size\n", curfile->ftag.fileno);
      curfile->ftag.availbytes = origbytes;
      return -1;
   }
   // set the updated ftag value
   if (putftag(tgtstream, curfile)) {
      LOG(LOG_ERR, "Failed to update FTAG on file %zu\n", curfile->ftag.fileno);
      return -1;
   }
   return 0;
}

/**
 * Set time values on the file referenced by the given EDIT or CREATE DATASTREAM
 * NOTE -- Time values will only be finalized during datastream_close/release
 * @param DATASTREAM* stream : Reference to the DATASTREAM on which to set times
 * @param const struct timespec times[2] : Time values ( see manpage for 'utimensat' )
 * @return int : Zero on success, or -1 on failure
 */
int datastream_utimens(DATASTREAM* stream, const struct timespec times[2]) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }
   if (times == NULL) {
      LOG(LOG_ERR, "Received a NULL times array reference\n");
      errno = EINVAL;
      return -1;
   }
   // check if this stream is of an appropriate type
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != EDIT_STREAM && tgtstream->type != CREATE_STREAM) {
      LOG(LOG_ERR, "Received stream type is not supported\n");
      errno = EINVAL;
      return -1;
   }
   // verify that the current file is in an appropriate state
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   if ((curfile->ftag.state & FTAG_DATASTATE) != FTAG_COMP &&
      tgtstream->type != CREATE_STREAM &&
      (curfile->ftag.state & FTAG_WRITEABLE) == 0) {
      LOG(LOG_ERR, "Cannot set times on an incomplete/unreleased file\n");
      errno = EINVAL;
      return -1;
   }
   // stash our time values in the stream handle
   curfile->times[0] = times[0];
   curfile->times[1] = times[1];
   curfile->dotimes = 1;
   tgtstream->finfo.mtime = times[1];
   return 0;
}

/**
 * Get the recovery info referenced by the given READ DATASTREAM
 * @param DATASTREAM* stream : Reference to the DATASTREAM on which to get recovery
 * info
 * @param char** recovinfo : Reference to be populated with the recovery info
 * @return int : Zero on success, or -1 on failure
 */
int datastream_recoveryinfo(DATASTREAM* stream, RECOVERY_FINFO* recovinfo) {
   // check for invalid args
   if (stream == NULL || *stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream reference\n");
      errno = EINVAL;
      return -1;
   }

   // check if this stream is of an appropriate type
   DATASTREAM tgtstream = *stream;
   if (tgtstream->type != READ_STREAM) {
      LOG(LOG_ERR, "Received stream type is not supported\n");
      errno = EINVAL;
      return -1;
   }

   // identify current position info
   STREAMFILE* curfile = tgtstream->files + tgtstream->curfile;
   DATASTREAM_POSITION streampos = {
      .totaloffset = 0,
      .dataremaining = 0,
      .excessremaining = 0,
      .objno = 0,
      .offset = 0,
      .excessoffset = 0,
      .dataperobj = 0
   };
   if (gettargets(tgtstream, 0, SEEK_CUR, &(streampos))) {
      LOG(LOG_ERR, "Failed to identify position vals of file %zu\n", curfile->ftag.fileno);
      return -1;
   }

   // open the current data object, if necessary
   if (tgtstream->datahandle == NULL) {
      LOG(LOG_INFO, "Opening object %zu\n", tgtstream->objno);
      if (open_current_obj(tgtstream)) {
         LOG(LOG_ERR, "Failed to open data object %zu\n", tgtstream->objno);
         return -1;
      }
   }

   // seek to the recovery info
   size_t tgtoffset = streampos.offset + streampos.dataremaining;
   if (tgtoffset - tgtstream->recoveryheaderlen > streampos.dataperobj) {
      tgtoffset = tgtstream->recoveryheaderlen + streampos.dataperobj;
   }
   off_t seekres = ne_seek(tgtstream->datahandle, tgtoffset);
   if (seekres != tgtoffset) {
      LOG(LOG_ERR, "Failed to seek to offset %zu in object %zu\n",
         tgtoffset, tgtstream->objno);
      return -1;
   }

   char* infobuf = calloc(1, curfile->ftag.recoverybytes + 1);
   if (infobuf == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for recovery info buffer\n");
      return -1;
   }

   // read recovery info
   LOG(LOG_INFO, "Reading recovery info from object %zu\n", tgtstream->objno);
   ssize_t readres = ne_read(tgtstream->datahandle, infobuf, curfile->ftag.recoverybytes);
   if (readres <= 0) {
      LOG(LOG_ERR, "Read failure in object %zu at offset %zu ( res = %zd )\n",
         tgtstream->objno, tgtstream->offset, readres);
      free(infobuf);
      return -1;
   }

   // seek back to original position
   seekres = ne_seek(tgtstream->datahandle, streampos.offset);
   if (seekres != streampos.offset) {
      LOG(LOG_ERR, "Failed to return seek to offset %zu in object %zu. Closing handle!\n",
         streampos.offset, tgtstream->objno);
      ne_close(tgtstream->datahandle, NULL, NULL);
      tgtstream->datahandle = NULL;
   }

   // parse info string
   if (recovery_finfofromstr(recovinfo, infobuf, curfile->ftag.recoverybytes)) {
      LOG(LOG_ERR, "Failed to parse recovery info string for object %zu (%s)\n",
         tgtstream->objno, strerror(errno));
      free(infobuf);
      return -1;
   }

   free(infobuf);

   return 0;
}
