#ifndef _MARFS_FUSE_H
#define _MARFS_FUSE_H

/*
This file is part of MarFS, which is released under the BSD license.


Copyright (c) 2015, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANS, LLC added functionality to the original work. The original work plus
LANS, LLC contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at <http://www.gnu.org/licenses/>.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2015, Los Alamos National Security, LLC All rights reserved.
Copyright 2015. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



#include "common.h"



#  ifdef __cplusplus
extern "C" {
#  endif

// "The return value will passed in the private_data field of
//  fuse_context to all file operations and as a parameter to the
//  destroy() method."
void* marfs_init(struct fuse_conn_info* conn);

void  marfs_destroy(void* private_data);



int  marfs_access(const char* path, int mask);

int  marfs_chmod(const char* path, mode_t mode);

int  marfs_chown(const char* path, uid_t uid, gid_t gid);

int  marfs_create(const char* path, mode_t mode, struct fuse_file_info* ffi);

int  marfs_flush(const char* path, struct fuse_file_info* fi);

int  marfs_fsync(const char* path, int isdatasync, struct fuse_file_info* fi);

int  marfs_fsyncdir(const char* path, int isdatasync, struct fuse_file_info* fi);

int  marfs_ftruncate(const char* path, off_t size, struct fuse_file_info* fi);
	
int  marfs_getattr(const char* path, struct stat* stbuf);

int  marfs_getxattr(const char* path, const char* name, char* value, size_t size);

int  marfs_ioctl(const char* path, int cmt, void* arg,
                 struct fuse_file_info* fi, unsigned int flags, void* data);

int  marfs_listxattr(const char* path, char* list, size_t size);

int  marfs_mkdir(const char* path, mode_t mode);

int  marfs_mknod(const char* path, mode_t mode, dev_t rdev);

int  marfs_open(const char* path, struct fuse_file_info* fi);

int  marfs_opendir(const char* path, struct fuse_file_info* fi);

int  marfs_read(const char* path, char* buf, size_t size, off_t offset,
                      struct fuse_file_info* fi);

int  marfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                         off_t offseet, struct fuse_file_info* fi);

int  marfs_readlink(const char* path, char* buf, size_t size);

int  marfs_release(const char* path, struct fuse_file_info* fi);

int  marfs_releasedir(const char* path, struct fuse_file_info* fi);

int  marfs_removexattr(const char* path, const char* name);

int  marfs_rename(const char* from, const char* to);

int  marfs_rmdir(const char* path);

int  marfs_setxattr(const char* path, const char* name,
                          const char* value, size_t size, int flags);

int  marfs_statfs(const char* path, struct statvfs* stbuf);

int  marfs_symlink(const char* to, const char* from);

int  marfs_truncate(const char* path, off_t size);

int  marfs_unlink(const char* path);

// deprecated in 2.6
int  marfs_utime(const char* path, struct utimbuf* tb);

int  marfs_utimens(const char* path, const struct timespec ts[2]);

int  marfs_write(const char* path, const char* buf, size_t size, off_t offset,
                 struct fuse_file_info* fi);



// currently unimplemented
#if 0

int  marfs_bmap(const char* path, size_t blocksize, uint64_t* blockno);

// not in 2.6
int  marfs_fallocate(const char* path, int mode, off_t offset, off_t length, struct fuse_file_info* fi);

int  marfs_fgetattr(const char* path, struct stat* stbuf);

int  marfs_flock(const char* path, struct fuse_file_info* fi, int op);

// deprecated in 2.6
int  marfs_getdir(const char *path, fuse_dirh_t , fuse_dirfil_t);

int  marfs_link(const char* from, const char* to);

int  marfs_lock(const char* path, struct fuse_file_info* fi, int cmd,
                      struct flock* locks);

int  marfs_poll(const char* path, struct fuse_file_info* fi, struct fuse_pollhandle* ph, unsigned* reventsp);

#endif



#  ifdef __cplusplus
}
#  endif



#endif // _MARFS_FUSE_H
