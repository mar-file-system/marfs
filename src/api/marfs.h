#ifndef _MARFS_H
#define _MARFS_H

#ifdef __cplusplus
extern "C"
{
#endif

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

#include <fcntl.h>
#include <sys/statvfs.h>

/* NOTE: Functions should operate the same as their POSIX counterparts if
possible.
Additionally (and if needed, superceding the previous statement), developers
should ensure functions with numerical return types (int, ssize_t, etc.) always
return -1 (while also setting errno) on failure. This is to ensure software
utilizing this API (e.g. the FUSE interface) have a consistent and predictable
interface across functions.
*/

typedef struct marfs_ctxt_struct *marfs_ctxt;

typedef struct marfs_dhandle_struct *marfs_dhandle;

typedef struct marfs_fhandle_struct *marfs_fhandle;

typedef enum
{
	MARFS_INTERACTIVE,
	MARFS_BATCH
} marfs_interface;

typedef enum
{
	MARFS_READ = O_RDONLY,
	MARFS_WRITE = O_WRONLY
} marfs_flags;

// CONTEXT MGMT OPS

/**
 * Initializes a MarFS Context structure based on the content of the referenced config file
 * Note -- This initialization process may act as a security barrier.  If the caller has 
 *         EUID == root (or some marfs-admin-user), this function can access MDAL/DAL root 
 *         dirs below a protected directory as a one time event.  After initialization, 
 *         the caller can safely setuid() to a user, dropping all elevated perms yet 
 *         maintaining access to the MDAL/DAL root dirs via the returned marfs_ctxt.
 * @param const char* configpath : Path of the config file to initialize based on
 * @param marfs_interface type : Interface type to use for MarFS ops ( interactive / batch )
 * @param char verify : If zero, skip config verification
 *                      If non-zero, verify the config and abort if any problems are found
 * @return marfs_ctxt : Newly initialized marfs_ctxt, or NULL if a failure occurred
 */
marfs_ctxt marfs_init(const char* configpath, marfs_interface type, char verify);

/**
 * Sets a string 'tag' value for the given context struct, causing all output files to 
 * include the string in metadata references and data object IDs
 * @param marfs_ctxt ctxt : marfs_ctxt to be updated
 * @param const char* ctag : New client tag string value
 * @return int : Zero on success, or -1 on failure
 */
int marfs_setctag(marfs_ctxt ctxt, const char* ctag);

/**
 * Populate the given string with the config version of the provided marfs_ctxt
 * @param marfs_ctxt ctxt : marfs_ctxt to retrieve version info from
 * @param char* verstr : String to be populated
 * @param size_t len : Allocated length of the target string
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t marfs_configver(marfs_ctxt ctxt, char* verstr, size_t len);

/**
 * Destroy the provided marfs_ctxt
 * @param marfs_ctxt ctxt : marfs_ctxt to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int marfs_term(marfs_ctxt ctxt);


// MARFS INFO OPS

/**
 * Populates the given string with the path of the MarFS mountpoint
 * ( as defined by the MarFS config file )
 * @param marfs_ctxt ctxt : marfs_ctxt to retrieve mount path from
 * @param char* mountstr : String to be populated with the mount path
 * @param size_t len : Allocated length of the target string
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t marfs_mountpath( marfs_ctxt ctxt, char* mountstr, size_t len );


// METADATA PATH OPS

/**
 * Check access to the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param int mode : F_OK - check for file existence
 *                      or a bitwise OR of the following...
 *                   R_OK - check for read access
 *                   W_OK - check for write access
 *                   X_OK - check for execute access
 * @param int flags : A bitwise OR of the following...
 *                    AT_EACCESS - Perform access checks using effective uid/gid
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_access(marfs_ctxt ctxt, const char* path, int mode, int flags);

/**
 * Stat the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param struct stat* st : Stat structure to be populated
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_stat(marfs_ctxt ctxt, const char* path, struct stat *buf, int flags);

/**
 * Edit the mode of the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param mode_t mode : New mode value for the file (see inode man page)
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_chmod(marfs_ctxt ctxt, const char* path, mode_t mode, int flags);

/**
 * Edit the ownership and group of the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param uid_t owner : New owner
 * @param gid_t group : New group
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_chown(marfs_ctxt ctxt, const char* path, uid_t uid, gid_t gid, int flags);

/**
 * Rename the specified target to a new path
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* from : String path of the target
 * @param const char* to : Destination string path
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_rename(marfs_ctxt ctxt, const char* from, const char* to);

/**
 * Create a symlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* target : String path for the link to target
 * @param const char* linkname : String path of the new link
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_symlink(marfs_ctxt ctxt, const char* target, const char* linkname);

/**
 * Read the target path of the specified symlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target symlink
 * @param char* buf : Buffer to be populated with the link value
 * @param size_t count : Size of the target buffer
 * @return ssize_t : Size of the link target string, or -1 if a failure occurred
 */
ssize_t marfs_readlink(marfs_ctxt ctxt, const char* path, char* buf, size_t count);

/**
 * Unlink the specified file/symlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_unlink(marfs_ctxt ctxt, const char* path);

/**
 * Create a hardlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* oldpath : String path of the target file
 * @param const char* newpath : String path of the new hardlink
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_link(marfs_ctxt ctxt, const char* oldpath, const char* newpath, int flags);

/**
 * Update the timestamps of the target file
 * NOTE -- It is possible that the time values of a file will be updated at any time, while
 *         a referencing marfs_fhandle stream exists ( has not been closed or released ).
 *         marfs_futimens() protects against this, by caching time values and applying them
 *         only after the file has been finalized/completed.
 *         Thus, it is strongly recommend to use that function instead, when feasible, unless
 *         you are certain that the target file has been completed.
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the new directory
 * @param const struct timespec times[2] : Struct references for new times
 *                                         times[0] - atime values
 *                                         times[1] - mtime values
 *                                         (see man utimensat for struct reference)
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero value on success, or -1 if a failure occurred
 */
int marfs_utimens( marfs_ctxt ctxt, const char* path, const struct timespec times[2], int flags );

/**
 * Create the specified directory
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the new directory
 * @param mode_t mode : Mode value of the new directory (see inode man page)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_mkdir(marfs_ctxt ctxt, const char* path, mode_t mode);

/**
 * Delete the specified directory
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target directory
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_rmdir(marfs_ctxt ctxt, const char* path);

/**
 * Return statvfs (filesystem) info for the current namespace
 * @param const marfs_ctxt ctxt : marfs_ctxt to retrieve info for
 * @param struct statvfs* buf : Reference to the statvfs structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_statvfs(marfs_ctxt ctxt, const char* path, struct statvfs *buf);


// METADATA FILE HANDLE OPS

/**
 * Perform a stat operation on the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle to stat
 * @param struct stat* buf : Reference to a stat buffer to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fstat ( marfs_fhandle fh, struct stat* buf );

/**
 * Update the timestamps of the target file
 * NOTE -- It is possible that the time values of the target file may not be updated 
 *         until the referencing marfs_fhandle is closed or released.
 *         Thus, it is essential to check the return values of those functions as well.
 * @param marfs_fhandle fh : File handle on which to set timestamps
 * @param const struct timespec times[2] : Struct references for new times
 *                                         times[0] - atime values
 *                                         times[1] - mtime values
 *                                         (see man utimensat for struct reference)
 * @return int : Zero value on success, or -1 if a failure occurred
 */
int marfs_futimens(marfs_fhandle stream, const struct timespec times[2]);

/**
 * Set the specified xattr on the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to set the xattr
 * @param const char* name : String name of the xattr to set
 * @param const void* value : Buffer containing the value of the xattr
 * @param size_t size : Size of the value buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fsetxattr(marfs_fhandle fh, const char* name, const void* value, size_t size, int flags);

/**
 * Retrieve the specified xattr from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to retrieve the xattr
 * @param const char* name : String name of the xattr to retrieve
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
 */
ssize_t marfs_fgetxattr(marfs_fhandle fh, const char* name, void* value, size_t size);

/**
 * Remove the specified xattr from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to remove the xattr
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fremovexattr(marfs_fhandle fh, const char* name);

/**
 * List all xattr names from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to list xattrs
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t marfs_flistxattr(marfs_fhandle fh, char* buf, size_t size);


// DIRECTORY HANDLE OPS

/**
 * Open a directory, relative to the given marfs_ctxt
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target directory
 * @return marfs_dhandle : Open directory handle, or NULL if a failure occurred
 */
marfs_dhandle marfs_opendir(marfs_ctxt ctxt, const char *path);

/**
 * Iterate to the next entry of an open directory handle
 * @param marfs_dhandle dh : marfs_dhandle to read from
 * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset
 *                          if all entries have been read, or NULL w/ errno set if a
 *                          failure occurred
 */
struct dirent *marfs_readdir(marfs_dhandle handle);

/**
 * Close the given directory handle
 * @param marfs_dhandle dh : marfs_dhandle to close
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_closedir(marfs_dhandle handle);

/**
 * Edit the given marfs_ctxt to reference the given marfs_dhandle for all path operations
 * NOTE -- This is an attempt to implement a current-working-directory style behavior for 
 *         MarFS.  The idea is that any initialized context holds its own internal cwd 
 *         reference.  All 'const char* path' relative path values will be interpreted 
 *         relative to that cwd.
 *         By default, this internal cwd will be the root of the MarFS mountpoint.
 *         However, this function allows you to change that.
 * @param marfs_ctxt ctxt : marfs_ctxt to update
 * @param marfs_dhandle dh : Directory handle to be used by the marfs_ctxt
 *                           NOTE -- this operation will destroy the provided marfs_dhandle
 * @return int : Zero on success, -1 if a failure occurred
 */
int marfs_chdir(marfs_ctxt ctxt, marfs_dhandle newdir);

/**
 * Set the specified xattr on the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to set the xattr
 * @param const char* name : String name of the xattr to set
 * @param const void* value : Buffer containing the value of the xattr
 * @param size_t size : Size of the value buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_dsetxattr(marfs_dhandle dh, const char* name, const void* value, size_t size, int flags);

/**
 * Retrieve the specified xattr from the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to retrieve the xattr
 * @param const char* name : String name of the xattr to retrieve
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
 */
ssize_t marfs_dgetxattr(marfs_dhandle dh, const char* name, void* value, size_t size);

/**
 * Remove the specified xattr from the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to remove the xattr
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_dremovexattr(marfs_dhandle dh, const char* name);

/**
 * List all xattr names from the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to list xattrs
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t marfs_dlistxattr(marfs_dhandle dh, char* buf, size_t size);


// FILE HANDLE MGMT OPS

/**
 * Create a new MarFS file, overwriting any existing file, and opening a marfs_fhandle for it
 * NOTE -- this is the only mechanism for creating files in MarFS
 * @param marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param marfs_fhandle stream : Reference to an existing marfs_fhandle, or NULL
 *                               If non-NULL, the created file will be tied to the provided 
 *                               stream, allowing it to be packed in with previous files.
 *                               The previous stream will be modified to reference the 
 *                               new file and returned by this function.
 *                               If NULL, the created file will be tied to a completely 
 *                               fresh stream.
 *                               NOTE -- Clients should essentially always tie new files to 
 *                               an existing stream, when feasible to do so.
 * @param const char* path : Path of the file to be created
 * @param mode_t mode : Mode value of the file to be created
 * @return marfs_fhandle : marfs_fhandle referencing the created file, 
 *                         or NULL if a failure occurred
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations 
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
marfs_fhandle marfs_creat(marfs_ctxt ctxt, marfs_fhandle stream, const char *path, mode_t mode);

/**
 * Open an existing file
 * @param marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param marfs_fhandle stream : Reference to an existing marfs_fhandle, or NULL
 *                               If non-NULL, the previous marfs_fhandle will be modified  
 *                               to reference the new file, preserving existing meta/data 
 *                               values/buffers to whatever extent is possible.  The 
 *                               modified handle will be returned by this function.
 *                               If NULL, the created file will be tied to a completely 
 *                               fresh marfs_fhandle.
 *                               NOTE -- Clients should essentially always open new files 
 *                               via an existing marfs_fhandle, when feasible to do so.
 * @param const char* path : Path of the file to be opened
 * @param marfs_flags flags : Flags specifying the mode in which to open the file
 * @return marfs_fhandle : marfs_fhandle referencing the opened file,
 *                         or NULL if a failure occurred
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations 
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
marfs_fhandle marfs_open(marfs_ctxt ctxt, marfs_fhandle stream, const char *path, marfs_flags flags);

/**
 * Free the given file handle and 'complete' the underlying file
 * ( make readable and disallow further data modification )
 * NOTE -- For MARFS_READ handles, close and release ops are functionally identical
 * NOTE -- For marfs_fhandles produced by marfs_open(), this function will fail unless the 
 *         original marfs_creat() handle has already been released ( the create handle must 
 *         be released for the data size of the file to be determined )
 * @param marfs_fhandle stream : marfs_fhandle to be closed
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- many ops, such as marfs_ftruncate, extend, utimens, and even write, may not 
 *            be committed to the FS until 'finalization' of the data stream.  This means 
 *            that it is *essential* to check the return code of this function.  A failure 
 *            of this call may indicate incomplete operations throughout the *entire* data 
 *            stream referenced by this handle.
 */
int marfs_close(marfs_fhandle stream);

/**
 * Free the given file handle, but do not 'complete' the underlying file
 * NOTE -- For MARFS_READ handles, close and release ops are functionally identical
 * @param marfs_fhandle stream : marfs_fhandle to be closed
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- many ops, such as marfs_ftruncate, extend, utimens, and even write, may not 
 *            be committed to the FS until 'finalization' of the data stream.  This means 
 *            that it is *essential* to check the return code of this function.  A failure 
 *            of this call may indicate incomplete operations throughout the *entire* data 
 *            stream referenced by this handle.
 */
int marfs_release(marfs_fhandle stream);

/**
 * 'Complete' the file referenced by the given handle, but maintain the handle itself
 * NOTE -- This function exists to better facilitate FUSE integration and is unlikely to 
 *         be useful outside of that context.  The handle structure will be maintained, 
 *         but all subsequent marfs ops will fail against that handle, except for release.
 * @param marfs_fhandle stream : marfs_fhandle referencing the file to be completed
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- many ops, such as marfs_ftruncate, extend, utimens, and even write, may not 
 *            be committed to the FS until 'finalization' of the data stream.  This means 
 *            that it is *essential* to check the return code of this function.  A failure 
 *            of this call may indicate incomplete operations throughout the *entire* data 
 *            stream referenced by this handle.
 */
int marfs_flush(marfs_fhandle stream);

/**
 * Set the file path recovery info to be encoded into data objects for the provided handle
 *    NOTE -- It is essential for all data objects to be encoded with matching recovery 
 *            paths.  This happens automatically when an file is written out entirely from 
 *            a single 'create' handle, but the recovery info can shift for files written 
 *            in parallel ( if MARFS_WRITE handles are opened with varied paths ).
 * @param marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param marfs_fhandle stream : MARFS_WRITE or create file handle for which to set the 
 *                               recovery path
 * @param const char* recovpath : New recovery path to be set
 * @return int : Zero on success, or -1 on failure
 */
int marfs_setrecoverypath(marfs_ctxt ctxt, marfs_fhandle stream, const char* recovpath);


// DATA FILE HANDLE OPS

/**
 * Read from the file currently referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle to be read from
 * @param void* buf : Reference to the buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations 
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
ssize_t marfs_read(marfs_fhandle stream, void* buf, size_t size);

/**
 * Write to the file currently referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle to be written to
 * @param const void* buf : Reference to the buffer containing data to be written
 * @param size_t count : Number of bytes to be written
 * @return ssize_t : Number of bytes written, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations 
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
ssize_t marfs_write(marfs_fhandle stream, const void* buf, size_t size);

/**
 * Seek to the provided offset of the file referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle to seek
 * @param off_t offset : Offset for the seek
 *                       NOTE -- write handles can only seek to exact chunk boundaries
 * @param int whence : Flag defining seek start location ( see 'seek()' syscall manpage )
 * @return off_t : Resulting offset within the file, or -1 if a failure occurred
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations 
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
off_t marfs_seek(marfs_fhandle stream, off_t offset, int whence);

/**
 * Seek to the provided offset of the given marfs_fhandle AND read from that location
 * NOTE -- This function exists for the sole purpose of supporting the FUSE interface, 
 *         which performs reads, using this 'at-offset' format, in parallel
 * @param marfs_fhandle stream : marfs_fhandle to seek and read
 * @param off_t offset : Offset for the seek
 *                       NOTE -- this is assumed to be relative to the start of the file
 *                               ( as in, whence == SEEK_SET )
 * @param void* buf : Reference to the buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
ssize_t marfs_read_at_offset(marfs_fhandle stream, off_t offset, void* buf, size_t count);

/**
 * Identify the data object boundaries of the file referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle for which to retrieve info
 * @param int chunknum : Index of the data chunk to retrieve info for ( beginning at zero )
 * @param off_t* offset : Reference to be populated with the data offset of the start of
 *                        the target data chunk
 *                        ( as in, marfs_seek( stream, 'offset', SEEK_SET ) will move
 *                        you to the start of this data chunk )
 * @param size_t* size : Reference to be populated with the size of the target data chunk
 * @return int : Zero on success, or -1 on failure
 */
int marfs_chunkbounds(marfs_fhandle stream, int chunknum, off_t* offset, size_t* size);

/**
 * Truncate the file referenced by the given marfs_fhandle to the specified length
 * NOTE -- This operation can only be performed on 'completed' files
 * @param marfs_fhandle stream : marfs_fhandle to be truncated
 * @param off_t length : Target total file length to truncate to
 * @return int : Zero on success, or -1 on failure
 */
int marfs_ftruncate(marfs_fhandle stream, off_t length);

/**
 * Extend the file referenced by the given marfs_fhandle to the specified total size
 * This makes the specified data size accessible for parallel write.
 * NOTE -- The final data object of the file will only be accessible after the creating 
 *         marfs_fhandle has been released ( as that finalizes the file's data size ).
 *         This function can only be performed if no data has been written to the target
 *         file via this handle.
 * @param marfs_fhandle stream : marfs_fhandle to be extended
 * @param off_t length : Target total file length to extend to
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations 
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
int marfs_extend(marfs_fhandle stream, off_t length);

/*
Note -- in the case of parallel writing the workflow can be thought of like this:
	Initialization:	marfs_creat(...) -> ohandle
			marfs_extend( ohandle, total_length )
			marfs_release( ohandle )
	Parallel Write:	marfs_open( ... MARFS_WRITE ) -> phandle
			marfs_seek( phandle, chunk_offset, SEEK_SET )
			marfs_write( phandle ... )
			marfs_release( phandle )
	Finalization:	marfs_open( ... MARFS_WRITE ) -> fhandle
			marfs_utimens( fhandle ... )
			marfs_close( fhandle )
			   OR
			marfs_creat( newpath, fhandle ... )
*/


#ifdef __cplusplus
}
#endif

#endif // _MARFS_H

