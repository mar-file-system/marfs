
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

#include "marfs_auto_config.h"
#if defined(DEBUG_ALL)  ||  defined(DEBUG_MDAL)
   #define DEBUG 1
#endif
#define LOG_PREFIX "posix_mdal"
#include "logging/logging.h"

#include "mdal.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>


//   -------------    POSIX DEFINITIONS    -------------

#define PMDAL_PREFX "MDAL_"
#define PMDAL_REF PMDAL_PREFX"ref"
#define PMDAL_PATH "path"
#define PMDAL_SUBSP PMDAL_PREFX"subspaces"
#define PMDAL_DUSE "datasize"
#define PMDAL_IUSE "inodecount"
#define PMDAL_XATTR "user."PMDAL_PREFX


//   -------------    POSIX STRUCTURES    -------------

typedef struct posix_directory_handle_struct {
   DIR*     dirp; // Directory reference
}* POSIX_DHANDLE;

typedef struct posix_scanner_struct {
   DIR*     dirp; // Directory reference
}* POSIX_SCANNER;

typedef struct posix_file_handle_struct {
   int        fd; // File handle
}* POSIX_FHANDLE;

typedef struct posix_mdal_context_struct {
   char*  namespace;  // Current namespace we are working in
   int    refdepth;   // Depth of our reference scatter tree
   DIR*   refd;       // Dir reference for the reference tree of this NS
   DIR*   pathd;      // Dir reference for the path tree of this NS
}* POSIX_MDAL_CTXT;
   


//   -------------    POSIX INTERNAL FUNCTIONS    -------------



//   -------------    POSIX IMPLEMENTATION    -------------


// Management Functions

/**
 *
 * @param MDAL_CTXT ctxt : MDAL_CTXT for which to perform verification
 * @param char fix : 
 * @return int : 
 */
int posix_verify ( MDAL_CTXT ctxt, char fix ) {
   errno = ENOSYS;
   return -1; // TODO -- actually write this
}


/**
 * Cleanup all structes and state associated with the given posix MDAL
 * @param MDAL mdal : MDAL to be freed
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_cleanup( MDAL mdal ) {
}


// Namespace Functions

/**
 * Set the namespace of the given MDAL_CTXT
 * @param MDAL_CTXT ctxt : Context to set the namespace of
 * @param const char* ns : Name of the namespace to set
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_setnamespace( MDAL_CTXT ctxt, const char* ns ) {
}


/**
 * Create the specified namespace
 * @param MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace to be created
 * @param int refdepth : Depth of the reference tree for the new namespace
 * @param int refbreadth : Breadth of the reference tree for the new namespace
 * @param int digits : Minimum number of digits per reference dir ( i.e. 3 digits -> 001 instead of 1 )
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_createnamespace( MDAL_CTXT ctxt, const char* ns, int refdepth, int refbreadth, int digits ) {
}


// Usage Functions

/**
 * Set data usage value for the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @param long long bytes : Number of bytes used by the namespace
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_setdatausage( MDAL_CTXT ctxt, long long bytes ) {
}


/**
 * Retrieve the data usage value of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @return long long : Number of bytes used by the namespace
 */
long long posix_getdatausage( MDAL_CTXT ctxt ) {
}


/**
 * Set the inode usage value of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @param long long bytes : Number of inodes used by the namespace
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_setinodeusage( MDAL_CTXT ctxt, long long files ) {
}


/**
 * Retrieve the inode usage value of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @return long long : Number of inodes used by the current namespace
 */
long long posix_getinodeusage( MDAL_CTXT ctxt ) {
}


// Scanner Functions

/**
 * Open a reference scanner for the given location of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @param const char* refloc : Target reference dir location
 * @return MDAL_SCANNER : Newly opened reference scanner
 */
MDAL_SCANNER posix_openscanner( MDAL_CTXT ctxt, const char* refloc ) {
}


/**
 * Iterate to the next entry of a reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to retrieve an entry from
 * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset if all 
 *                          entries have been read, or NULL w/ errno set if a failure occurred
 */
struct dirent* posix_scan( MDAL_SCANNER scanner ) {
}


/**
 * Open a file, relative to a given reference scanner
 * NOTE -- this is implicitly an open for READ
 * @param MDAL_SCANNER scanner : Reference scanner to open relative to
 * @param const char* path : Relative path of the target file from the scanner
 * @param int flags : Flags specifying behavior (see the 'open()' syscall 'flags' value for full info)
 * @param mode_t mode : Mode for file creation (see the 'open()' syscall 'mode' value for full info)
 * @return MDAL_FHANDLE : An MDAL_READ handle for the target file, or NULL if a failure occurred
 */
MDAL_FHANDLE posix_sopen( MDAL_SCANNER scanner, const char* path, int flags, mode_t mode ) {
}


/**
 * Unlink a file, relative to a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to unlink relative to
 * @param const char* path : Relative path of the target file from the scanner
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_sunlink( MDAL_SCANNER scanner, const char* path ) {
}


/**
 * Retrieve the value of specified file attribute, relative to a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to operate relative to
 * @param const char* path : Relative path of the target file from the scanner
 * @param const char* name : Name of the attribute to retrieve
 * @param void* value : Reference to the buffer to be populated with the value
 * @param size_t size : Byte size of the objectID buffer
 * @return ssize_t : The size of the retrieved objectID, or -1 if a failure occurred
 */
ssize_t posix_sgetattr( MDAL_SCANNER scanner, const char* path, const char* name, void* value, size_t size ) {
}


/**
 * Retrieve the objectInfo value of a file, relative to a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to operate relative to
 * @param const char* path : Relative path of the target file from the scanner
 * @param void* value : Reference to the buffer to be populated with the objectInfo
 * @param size_t size : Byte size of the objectInfo buffer
 * @return ssize_t : The size of the retrieved objectInfo, or -1 if a failure occurred
 */
ssize_t posix_sgetobjinfo( MDAL_SCANNER scanner, const char* path, void* value, size_t size ) {
}


/**
 * Close a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to be closed
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_closescanner( MDAL_SCANNER scanner ) {
}


// DHANDLE Functions

/**
 * Open a directory, relative to the given MDAL_CTXT
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : Relative path of the target directory from the ctxt
 * @return MDAL_DHANDLE : Open directory handle, or NULL if a failure occurred
 */
MDAL_DHANDLE posix_opendir( MDAL_CTXT ctxt, const char* path ) {
}


/**
 * Edit the given MDAL_CTXT to reference the given MDAL_DHANDLE for all path operations
 * @param MDAL_CTXT ctxt : MDAL_CTXT to update
 * @param MDAL_DHANDLE dh : Directory handle to be used by the MDAL_CTXT
 * @return MDAL_DHANDLE : Old directory handle used by the MDAL_CTXT (still open!)
 */
MDAL_DHANDLE posix_chdir( MDAL_CTXT ctxt, MDAL_DHANDLE dh ) {
}


/**
 * Iterate to the next entry of an open directory handle
 * @param MDAL_DHANDLE dh : MDAL_DHANDLE to read from
 * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset if all 
 *                          entries have been read, or NULL w/ errno set if a failure occurred
 */
struct dirent* posix_readdir( MDAL_DHANDLE dh ) {
}


/**
 * Close the given directory handle
 * @param MDAL_DHANDLE dh : MDAL_DHANDLE to close
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_closedir( MDAL_DHANDLE dh ) {
}


// FHANDLE Functions

/**
 * Open a file, relative to the given MDAL_CTXT
 * NOTE -- this function will NOT create a corresponding reference location for the file, nor
 *         will an objectID value automatically be set
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : Relative path to the target file
 * @param MDAL_MODE mode : Mode in which to open the file handle
 * @return MDAL_FHANDLE : The newly opened MDAL_FHANDLE, or NULL if a failure occurred
 */
MDAL_FHANDLE posix_open( MDAL_CTXT ctxt, const char* path, MDAL_MODE mode ) {
}


/**
 * Close the given MDAL_FHANDLE
 * @param fh : File handle to be closed
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_close( MDAL_FHANDLE fh ) {
}


/**
 * Write data to the given MDAL_WRITE, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be written to
 * @param const void* buf : Buffer containing the data to be written
 * @param size_t count : Number of data bytes contained within the buffer
 * @return ssize_t : Number of bytes written, or -1 if a failure occurred
 */
ssize_t posix_write( MDAL_FHANDLE fh, const void* buf, size_t count ) {
}


/**
 * Read data from the given MDAL_READ, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be read from
 * @param void* buf : Buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 if a failure occurred
 */
ssize_t posix_read( MDAL_FHANDLE fh, void* buf, size_t count ) {
}


/**
 * Retrieve the objectID value of the file referenced by the given non-DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to retrieve the objectID from
 * @param void* value : Buffer to be populated with the objectID value
 * @param size_t size : Byte size of the target buffer
 * @return ssize_t : Byte size of the retrieved objectID value, or -1 if a failure occurred
 */
ssize_t posix_fgetobjid( MDAL_FHANDLE fh, void* value, size_t size ) {
}


/**
 * Set the objectInfo value of the file referenced by the given MDAL_WRITE, non-DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to set the objectInfo of
 * @param const void* value : Buffer containing the objectInfo value to be set
 * @param size_t size : Byte size of the given objectInfo value
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_fsetobjinfo( MDAL_FHANDLE fd, const void* value, size_t size ) {
}


/**
 * Retrieve the objectInfo value of the file referenced by the given non-DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to retrieve the objectInfo from
 * @param void* value : Buffer to be populated with the objectInfo value
 * @param size_t size : Byte size of the target buffer
 * @return ssize_t : Byte size of the retrieved objectInfo value, or -1 if a failure occurred
 */
ssize_t posix_fgetobjinfo( MDAL_FHANDLE fd, void* value, size_t size ) {
}


/**
 * Truncate the file referenced by the given MDAL_WRITE, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be truncated
 * @param off_t length : File length to truncate to
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_ftruncate( MDAL_FHANDLE fh, off_t length ) {
}


/**
 * Seek to the specified position in the file referenced by the given MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to seek
 * @param off_t offset : Number of bytes to seek over
 * @param int whence : SEEK_SET - seek from the beginning of the file
 *                     SEEK_CUR - seek from the current location
 *                     SEEK_END - seek from the end of the file
 * @return off_t : Resulting offset within the file, or -1 if a failure occurred
 */
off_t posix_lseek( MDAL_FHANDLE fh, off_t offset, int whence ) {
}


/**
 * Set the specified xattr on the file referenced by the given MDAL_WRITE file handle
 * @param MDAL_FHANDLE fh : File handle for which to set the xattr
 * @param const char* name : String name of the xattr to set
 * @param const void* value : Buffer containing the value of the xattr
 * @param size_t size : Size of the value buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_fsetxattr( MDAL_FHANDLE fh, const char* name, const void* value, size_t size ) {
}


/**
 * Retrieve the specified xattr from the file referenced by the given MDAL_READ file handle
 * @param MDAL_FHANDLE fh : File handle for which to retrieve the xattr
 * @param const char* name : String name of the xattr to retrieve
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
 */
ssize_t posix_fgetxattr( MDAL_FHANDLE fh, const char* name, void* value, size_t size ) {
}


/**
 * Remove the specified xattr from the file referenced by the given MDAL_WRITE file handle
 * @param MDAL_FHANDLE fh : File handle for which to remove the xattr
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_fremovexattr( MDAL_FHANDLE fh, const char* name ) {
}


/**
 * List all xattr names from the file referenced by the given MDAL_READ file handle
 * @param MDAL_FHANDLE fh : File handle for which to list xattrs
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t posix_flistxattr( MDAL_FHANDLE fh, char* buf, size_t size ) {
}


// Path Functions

/**
 * Retrieve the objectID of the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param void* value : Buffer to populate with the objectID value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : The size of the returned objectID, or -1 if a failure occurred
 */
ssize_t posix_getobjid( MDAL_CTXT ctxt, const char* path, void* value, size_t size ) {
}


/**
 * Set the objectInfo value of the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param const void* value : Buffer containing the new objectInfo value
 * @param size_t size : Size of the objectInfo value
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_setobjinfo( MDAL_CTXT ctxt, const char* path, const void* value, size_t size ) {
}


/**
 * Retrieve the objectInfo value of the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param void* value : Buffer to populate with the objectInfo value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned objectInfo, or -1 if a failure occurred
 */
ssize_t posix_getobjinfo( MDAL_CTXT ctxt, const char* path, void* value, size_t size ) {
}


/**
 * Check access to the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param int mode : F_OK - check for file existance
 *                      or a bitwise OR of the following...
 *                   R_OK - check for read access
 *                   W_OK - check for write access
 *                   X_OK - check for execute access
 * @param int flags : A bitwise OR of the following...
 *                    AT_EACCESS - Perform access checks using effective uid/gid
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_access( MDAL_CTXT ctxt, const char* path, int mode, int flags ) {
}


/**
 * Create a filesystem node
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target node
 * @param mode_t mode : Mode value for the created node (see inode man page)
 * @param dev_t dev : S_IFREG  - regular file
 *                    S_IFCHR  - character special file
 *                    S_IFBLK  - block special file
 *                    S_IFIFO  - FIFO
 *                    S_IFSOCK - socket
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_mknod( MDAL_CTXT ctxt, const char* path, mode_t mode, dev_t dev ) {
}


/**
 * Edit the mode of the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param mode_t mode : New mode value for the file (see inode man page)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_chmod( MDAL_CTXT ctxt, const char* path, mode_t mode ) {
}


/**
 * Edit the ownership and group of the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param uid_t owner : New owner
 * @param gid_t group : New group
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_lchown( MDAL_CTXT ctxt, const char* path, uid_t owner, gid_t group ) {
}


/**
 * Truncate the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param off_t size : Size to truncate to
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_truncate( MDAL_CTXT ctxt, const char* path, off_t size ) {
}


/**
 * Stat the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param struct stat* st : Stat structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_lstat( MDAL_CTXT ctxt, const char* path, struct stat* st ) {
}


/**
 * Create a hardlink
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* oldpath : String path of the target file
 * @param const char* newpath : String path of the new hardlink
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_link( MDAL_CTXT ctxt, const char* oldpath, const char* newpath ) {
}


/**
 * Create the specified directory
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the new directory
 * @param mode_t mode : Mode value of the new directory (see inode man page)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_mkdir( MDAL_CTXT ctxt, const char* path, mode_t mode ) {
}


/**
 * Delete the specified directory
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target directory
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_rmdir( MDAL_CTXT ctxt, const char* path ) {
}


/**
 * Read the target path of the specified symlink
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target symlink
 * @param char* buf : Buffer to be populated with the link value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the link target string, or -1 if a failure occurred
 */
ssize_t posix_readlink( MDAL_CTXT ctxt, const char* path, char* buf, size_t size ) {
}


/**
 * Rename the specified target to a new path
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* from : String path of the target
 * @param const char* to : Destination string path
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_rename( MDAL_CTXT ctxt, const char* from, const char* to ) {
}


/**
 * Retrieve the specified xattr value from a given file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param const char* name : Name of the target xattr
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the provided buffer
 * @return ssize_t : Size of the retrieved xattr value, or -1 if a failure occurred
 */
ssize_t posix_lgetxattr( MDAL_CTXT ctxt, const char* path, const char* name, void* value, size_t size ) {
}


/**
 * Set the specified xattr on a given file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param const char* name : Name of the target xattr
 * @param const void* value : Buffer containing the new xattr value
 * @param size_t size : Size of the provided buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_lsetxattr( MDAL_CTXT ctxt, const char* path, const char* name, const void* value, size_t size, int flags ) {
}


/**
 * Remove the specified xattr on a given file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param const char* name : Name of the target xattr
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_lremovexattr( MDAL_CTXT ctxt, const char* path, const char* name ) {
}


/**
 * List all xattrs on a target file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t posix_llistxattr( MDAL_CTXT ctxt, const char* path, char* buf, size_t size ) {
}


/**
 * Return statvfs info for the current namespace
 * @param MDAL_CTXT ctxt : MDAL_CTXT to retrieve info for
 * @param struct statvfs* buf : Reference to the statvfs structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_statvfs( MDAL_CTXT ctxt, struct statvfs* buf ) {
}


/**
 * Create a symlink
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* target : String path for the link to target
 * @param const char* linkname : String path of the new link
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_symlink( MDAL_CTXT ctxt, const char* target, const char* linkname ) {
}


/**
 * Unlink the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_unlink( MDAL_CTXT ctxt, const char* path ) {
}


/**
 * Update the timestamps of the target file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* target : String path of the target file
 * @param const struct timespec times[2] : Struct references for new times
 *                                         times[0] - atime values
 *                                         times[1] - mtime values
 *                                         (see man utimensat for struct reference)
 * @param int flags : Zero - follow symlinks
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a target symlink
 * @return int : Zero value on success, or -1 if a failure occurred
 */
int posix_utimens( MDAL_CTXT ctxt, const char* path, const struct timespec times[2], int flags ) {
}


//   -------------    POSIX INITIALIZATION    -------------

DAL posix_mdal_init( xmlNode* root ) {
   // first, calculate the number of digits required for pod/cap/block/scatter
   int d_pod = num_digits( max_loc.pod );
   if ( d_pod < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum pod: %d\n", max_loc.pod );
      return NULL;
   }
   int d_cap = num_digits( max_loc.cap );
   if ( d_cap < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum cap: %d\n", max_loc.cap );
      return NULL;
   }
   int d_block = num_digits( max_loc.block );
   if ( d_block < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum block: %d\n", max_loc.block );
      return NULL;
   }
   int d_scatter = num_digits( max_loc.scatter );
   if ( d_scatter < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum scatter: %d\n", max_loc.scatter );
      return NULL;
   }

   // make sure we start on a 'dir_template' node
   if ( root->type == XML_ELEMENT_NODE  &&  strncmp( (char*)root->name, "dir_template", 13 ) == 0 ) {

      // make sure that node contains a text element within it
      if ( root->children != NULL  &&  root->children->type == XML_TEXT_NODE ) {

         // allocate space for our context struct
         POSIX_DAL_CTXT dctxt = malloc( sizeof( struct posix_dal_context_struct ) );
         if ( dctxt == NULL ) { return NULL; } // malloc will set errno

         // copy the dir template into the context struct
         dctxt->dirtmp = strdup( (char*)root->children->content );
         if ( dctxt->dirtmp == NULL ) { free(dctxt); return NULL; } // strdup will set errno

         // initialize all other context fields
         dctxt->tmplen = strlen( dctxt->dirtmp );
         dctxt->max_loc = max_loc;
         dctxt->dirpad = 0;

         // calculate a real value for dirpad based on number of p/c/b/s substitutions
         char* parse = dctxt->dirtmp;
         while ( *parse != '\0' ) {
            if ( *parse == '{' ) {
               // possible substituion, but of what type?
               int increase = 0;
               switch ( *(parse+1) ) {
                  case 'p':
                     increase = d_pod;
                     break;

                  case 'c':
                     increase = d_cap;
                     break;

                  case 'b':
                     increase = d_block;
                     break;

                  case 's':
                     increase = d_scatter;
                     break;
               }
               // if this looks like a valid substitution, check for a final '}'
               if ( increase > 0  &&  *(parse+2) == '}' ) { // NOTE -- we know *(parse+1) != '\0'
                  dctxt->dirpad += increase - 3; // add increase, adjusting for chars used in substitution
               }
            }
            parse++; // next char
         }

         // allocate and populate a new DAL structure
         DAL pdal = malloc( sizeof( struct DAL_struct ) );
         if ( pdal == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for a DAL_struct\n" );
            free(dctxt);
            return NULL;
         } // malloc will set errno
         pdal->name = "posix";
         pdal->ctxt = (DAL_CTXT) dctxt;
         pdal->io_size = 1048576;
         pdal->verify = posix_verify;
         pdal->migrate = posix_migrate;
         pdal->open = posix_open;
         pdal->set_meta = posix_set_meta;
         pdal->get_meta = posix_get_meta;
         pdal->put = posix_put;
         pdal->get = posix_get;
         pdal->abort = posix_abort;
         pdal->close = posix_close;
         pdal->del = posix_del;
         pdal->stat = posix_stat;
         pdal->cleanup = posix_cleanup;
         return pdal;
      }
      else { LOG( LOG_ERR, "the \"dir_template\" node is expected to contain a template string\n" ); }
   }
   else { LOG( LOG_ERR, "root node of config is expected to be \"dir_template\"\n" ); }
   errno = EINVAL;
   return NULL; // failure of any condition check fails the function
}



