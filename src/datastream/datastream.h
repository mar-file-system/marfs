#ifndef _DATASTREAM_H
#define _DATASTREAM_H
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

#include "config/config.h"
#include "recovery/recovery.h"
#include "tagging/tagging.h"

typedef enum {
   CREATE_STREAM,
   EDIT_STREAM,
   REPACK_STREAM,
   READ_STREAM
} STREAM_TYPE;

typedef struct streamfile_struct {
   MDAL_FHANDLE    metahandle;
   FTAG            ftag;
   struct timespec times[2];
   char            dotimes;
} STREAMFILE;

typedef struct datastream_struct {
   // Stream Info
   STREAM_TYPE type;
   char* ctag;       // NOTE -- this ref is shared with files->ftag structs
   char* streamid;   // NOTE -- this ref is shared with files->ftag structs
   marfs_ns* ns;   // a stream can only be associated with a single NS
   size_t      recoveryheaderlen;
   // Stream Position Info
   size_t      fileno;
   size_t      objno;
   size_t      offset;
   size_t      excessoffset;
   ne_handle   datahandle;
   // Per-File Info
   STREAMFILE* files;
   size_t      curfile;
   size_t      filealloc;
   RECOVERY_FINFO finfo;
   // Temporary Buffers
   char* ftagstr;
   size_t      ftagstrsize;
   char* finfostr;
   size_t finfostrlen;
}*DATASTREAM;

/**
 * Generate a reference path for the given FTAG
 * @param FTAG* ftag : Reference to the FTAG value to generate an rpath for
 * @param HASH_TABLE reftable : Reference position hash table to be used
 * @return char* : Reference to the newly generated reference path, or NULL on failure
 *                 NOTE -- returned path must be freed by caller
 */
char* datastream_genrpath(FTAG* ftag, HASH_TABLE reftable);

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
int datastream_objtarget(FTAG* ftag, const marfs_ds* ds, char** objname, ne_erasure* erasure, ne_location* location);

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
int datastream_create(DATASTREAM* stream, const char* path, marfs_position* pos, mode_t mode, const char* ctag);

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
int datastream_open(DATASTREAM* stream, STREAM_TYPE type, const char* path, marfs_position* pos, MDAL_FHANDLE* phandle);

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
int datastream_scan(DATASTREAM* stream, const char* refpath, marfs_position* pos);

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
int datastream_repack(DATASTREAM* stream, const char* refpath, marfs_position* pos, const char* ctag);

/**
 * Cleans up state from a previous repack operation
 * NOTE -- This should only be necessary for a repack operation left in an incomplete state.
 * @param const char* refpath : Reference path of the repack marker file for the previous operation
 * @param marfs_position* pos : Reference to the marfs_position value of the target file
 * @return int : Zero on successful cleanup (file repack completed),
 *               A positive value on revert of repack (file returned to original state),
 *               or -1 on complete failure
 */
int datastream_repack_cleanup(const char* refpath, marfs_position* pos);

/**
 * Release the given DATASTREAM ( close the stream without completing the referenced file )
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be released
 * @return int : Zero on success, or -1 on failure
 */
int datastream_release(DATASTREAM* stream);

/**
 * Close the given DATASTREAM ( marking the referenced file as complete, for non-READ )
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be closed
 * @return int : Zero on success, or -1 on failure
 */
int datastream_close(DATASTREAM* stream);

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
ssize_t datastream_read(DATASTREAM* stream, void* buffer, size_t count);

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
ssize_t datastream_write(DATASTREAM* stream, const void* buff, size_t count);

/**
 * Change the recovery info pathname for the file referenced by the given CREATE or
 * EDIT DATASTREAM  ( NOTE -- the file must not have been written to via this stream )
 * @param DATASTREAM* stream : Reference to the DATASTREAM to set recovery pathname for
 * @param const char* recovpath : New recovery info pathname for the file
 * @return int : Zero on success, or -1 on failure
 */
int datastream_setrecoverypath(DATASTREAM* stream, const char* recovpath);

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
off_t datastream_seek(DATASTREAM* stream, off_t offset, int whence);

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
int datastream_chunkbounds(DATASTREAM* stream, int chunknum, off_t* offset, size_t* size);

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
int datastream_extend(DATASTREAM* stream, off_t length);

/**
 * Truncate the file referenced by the given EDIT DATASTREAM to the specified length
 * NOTE -- This operation can only be performed on completed data files
 * @param DATASTREAM* stream : Reference to the DATASTREAM to be truncated
 * @param off_t length : Target total file length to truncate to
 * @return int : Zero on success, or -1 on failure
 */
int datastream_truncate(DATASTREAM* stream, off_t length);

/**
 * Set time values on the file referenced by the given EDIT or CREATE DATASTREAM
 * NOTE -- Time values will only be finalized during datastream_close/release
 * @param DATASTREAM* stream : Reference to the DATASTREAM on which to set times
 * @param const struct timespec times[2] : Time values ( see manpage for 'utimensat' )
 * @return int : Zero on success, or -1 on failure
 */
int datastream_utimens(DATASTREAM* stream, const struct timespec times[2]);

/**
 * Get the recovery info referenced by the given READ DATASTREAM
 * @param DATASTREAM* stream : Reference to the DATASTREAM on which to get recovery
 * info
 * @param char** recovinfo : Reference to be populated with the recovery info
 * @return int : Zero on success, or -1 on failure
 */
int datastream_recoveryinfo(DATASTREAM* stream, RECOVERY_FINFO* recovinfo);

#endif // _DATASTREAM_H

