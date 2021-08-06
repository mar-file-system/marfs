#ifndef _MARFS_H
#define _MARFS_H
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

#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#define _GNU_SOURCE

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

// CONTEXT MGMT

/* This initializes a MarFS Context structure ( opaque type, from your perspective ) based on
the content of the referenced config file.  That context will be a required arg for all
further ops that don't instead reference a MarFS Handle structure.
Note -- the plan is to use this initialization step as a sort of security barrier.  The
intention is for the caller to have euid == root at this point ( or some other
marfs-specific user, we can discuss this ).  This function will access secure files/dirs.
This is a one time event.  After completion of initialization, the caller can safely setuid()
to a user, dropping ALL elevated permissions.*/
marfs_ctxt marfs_init(const char *configpath, marfs_interface type);

/* Sets a string 'tag' value for the given context struct.  This will cause all underlying
data objects and admin metadata references created via this context to include the specified
'tag' prefix.*/
int marfs_settag(marfs_ctxt ctxt, const char *tag);

/* If buffer 'buf' is not NULL, fills 'buf' with a string indicating the MarFS
configuration version. Returns the total length of the configuration string.
*/
int marfs_configver(marfs_ctxt ctxt, char *buf, ssize_t size, off_t offset);

/* This function destroys the previous context struct.*/
int marfs_term(marfs_ctxt ctxt);

/* METADATA OPS  -- rule of thumb:  metadata ops take a pathname argument
                 -- exception = setting mtime / atime, which is done via file handle
                              = chdir, which takes a directory handle arg*/

/* Check file access permissions*/
int marfs_access(marfs_ctxt ctxt, const char *path, int mode);

/* Get file attributes.  Does not follow symlinks.*/
int marfs_lstat(marfs_ctxt ctxt, const char *path, struct stat *buf);

/* Change the permission bits of a file*/
int marfs_chmod(marfs_ctxt ctxt, const char *path, mode_t mode);

/* Change the owner and group of a file*/
int marfs_chown(marfs_ctxt ctxt, const char *path, uid_t uid, gid_t gid);

/* Rename a file*/
int marfs_rename(marfs_ctxt ctxt, const char *oldpath, const char *newpath);

/* Create a symbolic link*/
int marfs_symlink(marfs_ctxt ctxt, const char *target, const char *linkname);

/* Read the target of a symbolic link*/
ssize_t marfs_readlink(marfs_ctxt ctxt, const char *path, char *buf, size_t size);

/* Remove a file*/
int marfs_unlink(marfs_ctxt ctxt, const char *path);

/* Create a hardlink to an existing file*/
int marfs_link(marfs_ctxt ctxt, const char *oldpath, const char *newpath);

/* Create a directory*/
int marfs_mkdir(marfs_ctxt ctxt, const char *path, mode_t mode);

/* Opens a directory handle.  To me, it seems simpler to keep this as a separate function,
which produces a handle that you can call marfs_readdir() against, rather than handling the
marfs_open(  ...  O_DIRECTORY ... ) case.  I plan to disallow directory access via
marfs_open().*/
marfs_dhandle marfs_opendir(marfs_ctxt ctxt, const char *path);

/* Read directory from open directory handle*/
struct dirent *marfs_readdir(marfs_dhandle handle);

/* Close the given directory handle*/
int marfs_closedir(marfs_dhandle handle);

/* Remove a directory*/
int marfs_rmdir(marfs_ctxt ctxt, const char *path);

/* This is my attempt to implement a current-working-directory style behavior for MarFS.  The
idea is that any initialized context will use its own internal marfs_dhandle to reference a
cwd path.  All 'const char* path' relative path values will be interpreted relative to that
cwd.
By default, this internal cwd will be the root of the MarFS mountpoint.  However, this
function allows you to change that.
When executed, marfs will replace the internal cwd marfs_dhandle of the context structure
with the 'newdir' dhandle.  The original, internal marfs_dhandle will then be returned to
the caller.
CRUCIAL NOTES -- it is the caller's responsibility to close the returned marfs_dhandle if it
is no longer needed.  Additionally, the caller should *not* alter the state of the 'newdir'
handle after passing it into the context structure.*/
marfs_dhandle marfs_chdir(marfs_ctxt ctxt, marfs_dhandle newdir);

/* This is the only mechanism I plan to implement for setting metadata times.  See the
manpage of utimensat() for a description of the 'times[2]' arg.  Note that I do not plan to
offer functionality for setting times on symlinks themselves, which seems unnecessary to me.*/
int marfs_futimens(marfs_fhandle stream, const struct timespec times[2]);

/* Get file system statistics*/
int marfs_statvfs(marfs_ctxt ctxt, const char *path, struct statvfs *buf);

/* Set extended attributes*/
int marfs_lsetxattr(marfs_ctxt ctxt, const char *path, const char *name, const char *value, size_t size, int flags);

/* Get extended attributes*/
ssize_t marfs_lgetxattr(marfs_ctxt ctxt, const char *path, const char *name, void *value, size_t size);

/* List extended attributes*/
ssize_t marfs_llistxattr(marfs_ctxt ctxt, const char *path, char *list, size_t size);

/* Remove extended attributes*/
int marfs_lremovexattr(marfs_ctxt ctxt, const char *path, const char *name);

/*DATA OPS  -- rule of thumb: data ops take a marfs_fhandle argument ( no pathname )*/

/* Open a 'write' file handle for a newly created file.  Any existing file at the given path
will be unlinked.  An existing directory at that path will result in an error.
Note that the 'stream' arg is meant to provide support for file packing.  Passing in a
fhandle from a previous set of files will allow those files to be packed together.
Otherwise, if the 'stream' arg is NULL, this will begin a new data stream.
Note -- I've chosen to mirror the posix creat() implementation to distinguish between
creation of a new file, and update of an existing file.  Any file creation must be a
complete overwrite of any existing file, with the entire data set written from beginning to
end.  The exception would be for chunked files, which can be written at specific offsets by
parallel processes ( see below ).*/
marfs_fhandle marfs_creat(marfs_ctxt ctxt, marfs_fhandle stream, const char *path, mode_t mode);

/* This opens either a read or write handle for an existing file.  This op *will not* create
a new file.  The 'flags' arg can be one of either MARFS_READ or MARFS_WRITE.
A MARFS_READ handle is intended for reading data.
A MARFS_WRITE handle is intended for use by parallel writers, writing various 'chunks' of a
large file, or for use in data modifications such as marfs_ftruncate().
Note -- for security reasons, it will be impossible for file handles created by this
function to finalize this file ( via close() or creat() ) unless the 'original' handle has
been released ( see marfs_release() below ).*/
marfs_fhandle marfs_open(marfs_ctxt ctxt, const char *path, marfs_flags flags);

/* Read data from an open file*/
ssize_t marfs_read(marfs_fhandle stream, void *buf, size_t size);

/* Write data to an open file*/
ssize_t marfs_write(marfs_fhandle stream, const void *buf, size_t size);

/* Seek a fhandle to the given offset, relative to 'whence' (see the posix lseek() manpage
for a description, excluding the SEEK_DATA and SEEK_HOLE values).  For MARFS_READ handles,
this can be any offset value.  For MARFS_WRITE handles, the provided offset *must* align
with underlying chunk boundaries (this is intended to support parallel write).*/
off_t marfs_seek(marfs_fhandle stream, off_t offset, int whence);

/* Calculate the offset and size of the given 'chunknum' of the provided fhandle.  This is
useful for parallel writers, allowing them to identify locations at which is is safe for
them to write data.*/
int marfs_chunkbounds(marfs_fhandle stream, int chunknum, off_t *offset, size_t *size);

/* Truncate the file, referenced via MARFS_WRITE handle, to the given size.
Note -- this can only be done on 'finalized' files (see below).*/
int marfs_ftruncate(marfs_fhandle stream, off_t length);

/* Similar to ftruncate, this will adjust the referenced file to the given size.  However,
there is a crucial difference between the two.  marfs_ftruncate() will only allow the caller
to operate on finalized files, for which the total data size has been locked in (via
marfs_close(), marfs_creat(), or marfs_release()), and will only allow the true data size of
the file to shrink (truncating smaller reduces actual data size, truncating larger only
adds 'fake' zero-fill data).
This function is the opposite case, as it can only be used against files whose data has not
yet been finalized.  In such a case, the referenced file will have its real data bounds
extended to include the specified amount of data.  To put it another way, this function will
instruct marfs to assume that 'length' bytes of actual data objects will be written to this
file.  This enables parallel writers to use marfs_open() handles to write these non-existent
data chunks in arbitrary order.
Note -- this function is only usable via an 'original' file handle, meaning a handle created
specifically via a marfs_creat() operation.  This is intended to limit race-conditions
associated with multiple processes extending file bounds as other processes attempt to close
the file and continue with the same data stream.*/
int marfs_extend(marfs_fhandle stream, off_t length);

/* Possibly sync data objects and flush cached data.  If called more than once on the same
file handle, is a no-op.*/
int marfs_flush(marfs_fhandle stream);

/* Close the given file handle and finalize the data stream.
Note -- many ops, such as marfs_ftruncate, extend, utimens, and even write, may not be
committed to the FS until 'finalization' of the data stream.  This means that it is
*essential* to check the return code of this function.  A failure of this call may indicate
incomplete operations throughout the *entire* data stream referenced by this handle.*/
int marfs_close(marfs_fhandle stream);

/* Close the given file handle, but allow the data stream to be updated in the future (such
as by parallel writers).*/
int marfs_release(marfs_fhandle stream);

/*Note -- in the case of parallel writing the workflow can be thought of like this:
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
			marfs_creat( newpath, fhandle ... )*/

#endif // _MARFS_H

