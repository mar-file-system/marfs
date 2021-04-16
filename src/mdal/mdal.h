
#ifndef __MDAL_H_INCLUDE__
#define __MDAL_H_INCLUDE__


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


#include <libxml/tree.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <attr/xattr.h>
#include <fcntl.h>
#include <unistd.h>

#ifndef LIBXML_TREE_ENABLED
#error "Included Libxml2 does not support tree functionality!"
#endif



// just to provide some type safety (don't want to pass the wrong void*)
typedef void* MDAL_CTXT;
typedef void* MDAL_FHANDLE;
typedef void* MDAL_DHANDLE;
typedef void* MDAL_SCANNER;


// open mode
typedef enum MDAL_MODE_enum {
   MDAL_READ = 0,    // retrieve data or object tags
   MDAL_WRITE = 1,   // store data or object tags
   MDAL_DIRECT = 2   // operate on data directly ( no object tags )
} MDAL_MODE;


typedef struct MDAL_struct {
   // Name -- Used to identify and configure the MDAL
   const char*    name;

   // DAL Internal Context -- passed to each DAL function
   MDAL_CTXT       ctxt;


   // Management Functions

   /**
    *
    * @param MDAL_CTXT ctxt : MDAL_CTXT for which to perform verification
    * @param char fix : 
    * @return int : 
    */
   int (*verify) ( MDAL_CTXT ctxt, char fix );

   /**
    * Cleanup all structes and state associated with the given posix MDAL
    * @param MDAL mdal : MDAL to be freed
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*cleanup) ( MDAL mdal );


   // Namespace Functions

   /**
    * Set the namespace of the given MDAL_CTXT
    * @param MDAL_CTXT ctxt : Context to set the namespace of
    * @param const char* ns : Name of the namespace to set
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*setnamespace) ( MDAL_CTXT ctxt, const char* ns );

   /**
    * Create the specified namespace
    * @param MDAL_CTXT ctxt : Current MDAL context
    * @param const char* ns : Name of the namespace to be created
    * @param int refdepth : Depth of the reference tree for the new namespace
    * @param int refbreadth : Breadth of the reference tree for the new namespace
    * @param int digits : Minimum number of digits per reference dir ( i.e. 3 digits -> 001 instead of 1 )
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*createnamespace) ( MDAL_CTXT ctxt, const char* ns, int refdepth, int refbreadth, int digits );

   /**
    * Destroy the specified namespace
    * NOTE -- This operation will fail with errno=ENOTEMPTY if files persist in the namespace.
    *         This includes files within the reference tree.
    * @param MDAL_CTXT ctxt : Current MDAL context
    * @param const char* ns : Name of the namespace to be deleted
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*destroynamespace) ( MDAL_CTXT ctxt, const char* ns );


   // Usage Functions

   /**
    * Set data usage value for the current namespace
    * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
    * @param long long bytes : Number of bytes used by the namespace
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*setdatausage) ( MDAL_CTXT ctxt, long long bytes );

   /**
    * Retrieve the data usage value of the current namespace
    * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
    * @return long long : Number of bytes used by the namespace
    */
   long long (*getdatausage) ( MDAL_CTXT ctxt );

   /**
    * Set the inode usage value of the current namespace
    * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
    * @param long long bytes : Number of inodes used by the namespace
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*setinodeusage) ( MDAL_CTXT ctxt, long long files );

   /**
    * Retrieve the inode usage value of the current namespace
    * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
    * @return long long : Number of inodes used by the current namespace
    */
   long long (*getinodeusage) ( MDAL_CTXT ctxt );


   // Scanner Functions

   /**
    * Open a reference scanner for the given location of the current namespace
    * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
    * @param const char* rpath : Target reference dir location
    * @return MDAL_SCANNER : Newly opened reference scanner
    */
   MDAL_SCANNER (*openscanner) ( MDAL_CTXT ctxt, const char* rpath );

   /**
    * Close a given reference scanner
    * @param MDAL_SCANNER scanner : Reference scanner to be closed
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*closescanner) ( MDAL_SCANNER scanner );

   /**
    * Iterate to the next entry of a reference scanner
    * @param MDAL_SCANNER scanner : Reference scanner to retrieve an entry from
    * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset if all 
    *                          entries have been read, or NULL w/ errno set if a failure occurred
    */
   struct dirent* (*scan) ( MDAL_SCANNER scanner );

   /**
    * Open a file, relative to a given reference scanner
    * NOTE -- this is implicitly an open for READ
    * @param MDAL_SCANNER scanner : Reference scanner to open relative to
    * @param const char* spath : Relative path of the target file from the scanner
    * @param int flags : Flags specifying behavior (see the 'open()' syscall 'flags' value for full info)
    * @param mode_t mode : Mode for file creation (see the 'open()' syscall 'mode' value for full info)
    * @return MDAL_FHANDLE : An MDAL_READ handle for the target file, or NULL if a failure occurred
    */
   MDAL_FHANDLE (*sopen) ( MDAL_SCANNER scanner, const char* spath, int flags, mode_t mode );

   /**
    * Unlink a file, relative to a given reference scanner
    * @param MDAL_SCANNER scanner : Reference scanner to unlink relative to
    * @param const char* spath : Relative path of the target file from the scanner
    * @return int : Zero on success, -1 if a failure occurred
    */
   int (*sunlink) ( MDAL_SCANNER scanner, const char* spath );


   // DHANDLE Functions

   /**
    * Open a directory, relative to the given MDAL_CTXT
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : Relative path of the target directory from the ctxt
    * @return MDAL_DHANDLE : Open directory handle, or NULL if a failure occurred
    */
   MDAL_DHANDLE (*opendir) ( MDAL_CTXT ctxt, const char* path );

   /**
    * Edit the given MDAL_CTXT to reference the given MDAL_DHANDLE for all path operations
    * @param MDAL_CTXT ctxt : MDAL_CTXT to update
    * @param MDAL_DHANDLE dh : Directory handle to be used by the MDAL_CTXT
    * @return MDAL_DHANDLE : Old directory handle used by the MDAL_CTXT (still open!)
    */
   MDAL_DHANDLE (*chdir) ( MDAL_CTXT ctxt, MDAL_DHANDLE dh );

   /**
    * Iterate to the next entry of an open directory handle
    * @param MDAL_DHANDLE dh : MDAL_DHANDLE to read from
    * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset if all 
    *                          entries have been read, or NULL w/ errno set if a failure occurred
    */
   struct dirent* (*readdir) ( MDAL_DHANDLE dh );

   /**
    * Close the given directory handle
    * @param MDAL_DHANDLE dh : MDAL_DHANDLE to close
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*closedir) ( MDAL_DHANDLE dh );


   // FHANDLE Functions

   /**
    * Open a file, relative to the given MDAL_CTXT
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : Relative path to the target file
    * @param int flags : Flags specifying behavior (see the 'open()' syscall 'flags' value for full info)
    * @param mode_t mode : Mode for file creation (see the 'open()' syscall 'mode' value for full info)
    * @return MDAL_FHANDLE : The newly opened MDAL_FHANDLE, or NULL if a failure occurred
    */
   MDAL_FHANDLE (*open) ( MDAL_CTXT ctxt, const char* path, int flags, mode_t mode );

   /**
    * Open a file, relative to the reference tree of the given MDAL_CTXT
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* rpath : Relative reference path to the target file
    * @param int flags : Flags specifying behavior (see the 'open()' syscall 'flags' value for full info)
    * @param mode_t mode : Mode for file creation (see the 'open()' syscall 'mode' value for full info)
    * @return MDAL_FHANDLE : The newly opened MDAL_FHANDLE, or NULL if a failure occurred
    */
   MDAL_FHANDLE (*openref) ( MDAL_CTXT ctxt, const char* rpath, int flags, mode_t mode );

   /**
    * Open a file, creating both a reference location and a user visible location (hardlinks)
    * NOTE -- equivalent to openref() with flags O_WRONLY|O_CREAT|O_EXCL   (1st)
    *         AND
    *         open() with flags O_WRONLY|O_CREAT|O_TRUNC                   (2nd)
    *         with both locations referenced by the produced MDAL_FHANDLE
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : User visible path of the target file
    * @param const char* rpath : Reference path of the target file
    * @param mode_t mode : Mode for file creation (see the 'open()' syscall 'mode' value for full info)
    * @return MDAL_FHANDLE : The newly opened MDAL_FHANDLE, or NULL if a failure occurred
    */
   MDAL_FHANDLE (*creat) ( MDAL_CTXT ctxt, const char* path, const char* rpath, mode_t mode );

   /**
    * Close the given MDAL_FHANDLE
    * @param fh : File handle to be closed
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*close) ( MDAL_FHANDLE fh );

   /**
    * Set the value of the specified file attribute on the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : MDAL_FHANDLE to operate on
    * @param const char* name : Name of the file attribute
    * @param const void* value : Buffer containing the new attribute value
    * @param size_t size : Number of data bytes contained within the buffer
    * @param int flags : Flags for attribute setting (see the 'setxattr()' syscall 'flags' value for full info)
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*fsetattr) ( MDAL_FHANDLE fh, const char* name, const void* value, size_t size, int flags );

   /**
    * Get the value of the specified file attribute from the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : MDAL_FHANDLE to operate on
    * @param const char* name : Name of the file attribute
    * @param void* value : Buffer to be populated with the attribute value
    * @param size_t size : Byte size of the target buffer
    * @return ssize_t : Byte size of the retrieved attribute value, or -1 if a failure occurred
    */
   ssize_t (*fgetattr) ( MDAL_FHANDLE fh, const char* name, void* value, size_t size );

   /**
    * Remove the specified attribute from the file referenced by the given MDAL_FHANDLE
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* name : Name of the file attribute
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*fremoveattr) ( MDAL_FHANDLE fh, const char* name );

   /**
    * List all attibute names from the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle for which to list attributes
    * @param char* buf : Buffer to be populated with attribute names
    * @param size_t size : Size of the target buffer
    * @return ssize_t : Size of the returned attribute name list, or -1 if a failure occurred
    */
   ssize_t (*flistattr) ( MDAL_FHANDLE fh, char* buf, size_t size );

   /**
    * Write data to the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle to be written to
    * @param const void* buf : Buffer containing the data to be written
    * @param size_t count : Number of data bytes contained within the buffer
    * @return ssize_t : Number of bytes written, or -1 if a failure occurred
    */
   ssize_t (*write) ( MDAL_FHANDLE fh, const void* buf, size_t count );

   /**
    * Read data from the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle to be read from
    * @param void* buf : Buffer to be populated with read data
    * @param size_t count : Number of bytes to be read
    * @return ssize_t : Number of bytes read, or -1 if a failure occurred
    */
   ssize_t (*read) ( MDAL_FHANDLE fh, void* buf, size_t count );

   /**
    * Truncate the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle to be truncated
    * @param off_t length : File length to truncate to
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*ftruncate) ( MDAL_FHANDLE fh, off_t length );

   /**
    * Seek to the specified position in the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle to seek
    * @param off_t offset : Number of bytes to seek over
    * @param int whence : SEEK_SET - seek from the beginning of the file
    *                     SEEK_CUR - seek from the current location
    *                     SEEK_END - seek from the end of the file
    * @return off_t : Resulting offset within the file, or -1 if a failure occurred
    */
   off_t (*lseek) ( MDAL_FHANDLE fh, off_t offset, int whence );

   /**
    * Set the specified xattr on the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle for which to set the xattr
    * @param const char* name : String name of the xattr to set
    * @param const void* value : Buffer containing the value of the xattr
    * @param size_t size : Size of the value buffer
    * @param int flags : Zero value    - create or replace the xattr
    *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
    *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*fsetxattr) ( MDAL_FHANDLE fh, const char* name, const void* value, size_t size );

   /**
    * Retrieve the specified xattr from the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle for which to retrieve the xattr
    * @param const char* name : String name of the xattr to retrieve
    * @param void* value : Buffer to be populated with the xattr value
    * @param size_t size : Size of the target buffer
    * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
    */
   ssize_t (*fgetxattr) ( MDAL_FHANDLE fh, const char* name, void* value, size_t size );

   /**
    * Remove the specified xattr from the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle for which to remove the xattr
    * @param const char* name : String name of the xattr to remove
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*fremovexattr) ( MDAL_FHANDLE fh, const char* name );

   /**
    * List all xattr names from the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle for which to list xattrs
    * @param char* buf : Buffer to be populated with xattr names
    * @param size_t size : Size of the target buffer
    * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
    */
   ssize_t (*flistxattr) ( MDAL_FHANDLE fh, char* buf, size_t size );

   /**
    * Perform a stat operation on the file referenced by the given MDAL_FHANDLE
    * @param MDAL_FHANDLE fh : File handle to stat
    * @param struct stat* buf : Reference to a stat buffer to be populated
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*fstat) ( MDAL_FHANDLE fh, struct stat* buf );


   // Path Functions

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
   int (*access) ( MDAL_CTXT ctxt, const char* path, int mode, int flags );

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
   int (*mknod) ( MDAL_CTXT ctxt, const char* path, mode_t mode, dev_t dev );

   /**
    * Edit the mode of the specified file
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target file
    * @param mode_t mode : New mode value for the file (see inode man page)
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*chmod) ( MDAL_CTXT ctxt, const char* path, mode_t mode );

   /**
    * Edit the ownership and group of the specified file
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target file
    * @param uid_t owner : New owner
    * @param gid_t group : New group
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*chown) ( MDAL_CTXT ctxt, const char* path, uid_t owner, gid_t group );

   /**
    * Stat the specified file
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target file
    * @param struct stat* st : Stat structure to be populated
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*lstat) ( MDAL_CTXT ctxt, const char* path, struct stat* st );

   /**
    * Create a hardlink
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* oldpath : String path of the target file
    * @param const char* newpath : String path of the new hardlink
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*link) ( MDAL_CTXT ctxt, const char* oldpath, const char* newpath );

   /**
    * Create the specified directory
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the new directory
    * @param mode_t mode : Mode value of the new directory (see inode man page)
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*mkdir) ( MDAL_CTXT ctxt, const char* path, mode_t mode );

   /**
    * Delete the specified directory
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target directory
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*rmdir) ( MDAL_CTXT ctxt, const char* path );

   /**
    * Read the target path of the specified symlink
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target symlink
    * @param char* buf : Buffer to be populated with the link value
    * @param size_t size : Size of the target buffer
    * @return ssize_t : Size of the link target string, or -1 if a failure occurred
    */
   ssize_t (*readlink) ( MDAL_CTXT ctxt, const char* path, char* buf, size_t size );

   /**
    * Rename the specified target to a new path
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* from : String path of the target
    * @param const char* to : Destination string path
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*rename) ( MDAL_CTXT ctxt, const char* from, const char* to );

   /**
    * Retrieve the specified xattr value from a given file
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target file
    * @param const char* name : Name of the target xattr
    * @param void* value : Buffer to be populated with the xattr value
    * @param size_t size : Size of the provided buffer
    * @return ssize_t : Size of the retrieved xattr value, or -1 if a failure occurred
    */
   ssize_t (*lgetxattr) ( MDAL_CTXT ctxt, const char* path, const char* name, void* value, size_t size );

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
   int (*lsetxattr) ( MDAL_CTXT ctxt, const char* path, const char* name, const void* value, size_t size, int flags );

   /**
    * Remove the specified xattr on a given file
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target file
    * @param const char* name : Name of the target xattr
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*lremovexattr) ( MDAL_CTXT ctxt, const char* path, const char* name );

   /**
    * List all xattrs on a target file
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target file
    * @param char* buf : Buffer to be populated with xattr names
    * @param size_t size : Size of the target buffer
    * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
    */
   ssize_t (*llistxattr) ( MDAL_CTXT ctxt, const char* path, char* buf, size_t size );

   /**
    * Return statvfs info for the current namespace
    * @param MDAL_CTXT ctxt : MDAL_CTXT to retrieve info for
    * @param struct statvfs* buf : Reference to the statvfs structure to be populated
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*statvfs) ( MDAL_CTXT ctxt, struct statvfs* buf );

   /**
    * Create a symlink
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* target : String path for the link to target
    * @param const char* linkname : String path of the new link
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*symlink) ( MDAL_CTXT ctxt, const char* target, const char* linkname );

   /**
    * Unlink the specified file
    * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
    * @param const char* path : String path of the target file
    * @return int : Zero on success, or -1 if a failure occurred
    */
   int (*unlink) ( MDAL_CTXT ctxt, const char* path );

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
   int (*utimens) ( MDAL_CTXT ctxt, const char* path, const struct timespec times[2], int flags );


} *MDAL;


// Forward decls of specific MDAL initializations
MDAL posix_mdal_init( xmlNode* posix_mdal_conf_root );


// Function to provide specific MDAL initialization calls based on name
MDAL init_mdal( xmlNode* mdal_conf_root );



#endif


