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

#define FUSE_USE_VERSION 26

#define LOG_PREFIX "fuse"

#include <fuse.h>
#include <unistd.h>

#include "marfs.h"
#include "change_user.h"
#include "logging.h"

#define CTXT (marfs_ctxt)(fuse_get_context()->private_data)

int fuse_access(const char *path, int mode)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_access(CTXT, path, mode) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_chmod(const char *path, mode_t mode)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_chmod(CTXT, path, mode) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_chown(const char *path, uid_t uid, gid_t gid)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_chown(CTXT, path, uid, gid) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_create(const char *path, mode_t mode, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (ffi->fh)
  {
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  ffi->fh = (uint64_t)marfs_creat(CTXT, NULL, path, mode);
  int err = errno;

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    return -err;
  }

  return 0;
}

int fuse_flush(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "missing file descriptor\n");
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  int ret = marfs_flush((marfs_fhandle)ffi->fh) * errno;

  if (ret)
  {
    LOG(LOG_ERR, "%s\n", errno);
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_fsync(const char *path, int datasync, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  return 0;
}

int fuse_fsyncdir(const char *path, int datasync, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  return 0;
}

int fuse_ftruncate(const char *path, off_t length, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_ftruncate((marfs_fhandle)ffi->fh, length) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_getattr(const char *path, struct stat *statbuf)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_lstat(CTXT, path, statbuf) * errno;

  if (ret)
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_getxattr(const char *path, const char *name, char *value, size_t size)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_lgetxattr(CTXT, path, name, value, size) * errno;

  if (ret)
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *ffi, unsigned int flags, void *data)
{
  LOG(LOG_INFO, "%s\n", path);

  return 0;
}

int fuse_link(const char *oldpath, const char *newpath)
{
  LOG(LOG_INFO, "%s %s\n", oldpath, newpath);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_link(CTXT, oldpath, newpath) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_listxattr(const char *path, char *list, size_t size)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  int ret = marfs_llistxattr(CTXT, path, list, size) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_mkdir(const char *path, mode_t mode)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_mkdir(CTXT, path, mode) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_open(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (ffi->fh)
  {
    LOG(LOG_ERR, "previously open file descriptor\n");
    return -EBADF;
  }

  marfs_flags flags = MARFS_READ;
  if (ffi->flags & O_RDWR)
  {
    LOG(LOG_ERR, "invalid flags %x %x\n", ffi->flags, ffi->flags & O_RDWR);
    return -EINVAL;
  }
  else if (ffi->flags & O_WRONLY)
  {
    flags = MARFS_WRITE;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  ffi->fh = (uint64_t)marfs_open(CTXT, path, flags);
  int err = errno;

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "%s\n", strerror(err));
    return -err;
  }

  return 0;
}

int fuse_opendir(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (ffi->fh)
  {
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  ffi->fh = (uint64_t)marfs_opendir(CTXT, path);
  int err = errno;

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "%s\n", strerror(err));
    return -err;
  }

  return 0;
}

int fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "missing file descriptor\n");
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  if (marfs_seek((marfs_fhandle)ffi->fh, offset, SEEK_SET))
  {
    LOG(LOG_ERR, "failed to seek %s\n", strerror(errno));
    int err = errno;
    exit_user(&u_ctxt);
    return -err;
  }

  int ret = marfs_read((marfs_fhandle)ffi->fh, (void *)buf, size);

  if (ret < 0)
  {
    ret = -errno;
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    return -EBADF;
  }

  struct dirent *de;

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  while ((de = marfs_readdir((marfs_dhandle)ffi->fh)) != NULL)
  {
    if (filler(buf, de->d_name, NULL, 0))
    {
      LOG(LOG_ERR, "%s\n", ENOMEM);
      exit_user(&u_ctxt);
      return -ENOMEM;
    }
  }

  exit_user(&u_ctxt);

  return 0;
}

int fuse_readlink(const char *path, char *buf, size_t size)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_readlink(CTXT, path, buf, size) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_release(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "missing file descriptor\n");
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  int ret = marfs_close((marfs_fhandle)ffi->fh) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_releasedir(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  int ret = marfs_closedir((marfs_dhandle)ffi->fh) * errno;

  if (ret)
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
  }

  exit_user(&u_ctxt);

  if (!ret)
  {
    ffi->fh = (uint64_t)NULL;
  }

  return ret;
}

int fuse_removexattr(const char *path, const char *name)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_lremovexattr(CTXT, path, name) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_rename(const char *oldpath, const char *newpath)
{
  LOG(LOG_INFO, "%s %s\n", oldpath, newpath);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_rename(CTXT, oldpath, newpath) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_rmdir(const char *path)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_rmdir(CTXT, path) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_lsetxattr(CTXT, path, name, value, size, flags) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_statvfs(const char *path, struct statvfs *statbuf)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_statvfs(CTXT, path, statbuf) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_symlink(const char *target, const char *linkname)
{
  LOG(LOG_INFO, "%s %s\n", target, linkname);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_symlink(CTXT, target, linkname) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_truncate(const char *path, off_t length)
{
  LOG(LOG_INFO, "%s\n", path);

  marfs_fhandle fh;
  int err;

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  if ((fh = marfs_open(CTXT, path, MARFS_WRITE)) == NULL)
  {
    err = errno;
    exit_user(&u_ctxt);
    return -err;
  }

  int ret = marfs_ftruncate(fh, length) * errno;

  if (marfs_close(fh) && !ret)
  {
    err = errno;
    exit_user(&u_ctxt);
    return -err;
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_unlink(const char *path)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_unlink(CTXT, path) * errno;

  exit_user(&u_ctxt);

  return ret;
}

int fuse_utimens(const char *path, const struct timespec tv[2])
{
  LOG(LOG_INFO, "%s\n", path);

  marfs_fhandle fh;
  int err;

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  if ((fh = marfs_open(CTXT, path, MARFS_WRITE)) == NULL)
  {
    err = errno;
    exit_user(&u_ctxt);
    return -err;
  }

  int ret = marfs_futimens(fh, tv) * errno;

  if (marfs_close(fh) && !ret)
  {
    err = errno;
    exit_user(&u_ctxt);
    return -err;
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  if (marfs_seek((marfs_fhandle)ffi->fh, offset, SEEK_SET))
  {
    int err = errno;
    exit_user(&u_ctxt);
    return -err;
  }

  int ret = marfs_write((marfs_fhandle)ffi->fh, buf, size);

  if (ret < 0)
  {
    ret = -errno;
  }

  exit_user(&u_ctxt);

  return ret;
}

void *marfs_fuse_init(struct fuse_conn_info *conn)
{
  LOG(LOG_INFO, "init\n");
  return marfs_init(getenv("MARFSCONFIGRC"), MARFS_FUSE);
}

void marfs_fuse_destroy(void *userdata)
{
  LOG(LOG_INFO, "destroy\n");
  marfs_term(CTXT);
}

int main(int argc, char *argv[])
{

  struct fuse_operations marfs_oper = {
      .init = marfs_fuse_init,
      .destroy = marfs_fuse_destroy,

      .access = fuse_access,
      .chmod = fuse_chmod,
      .chown = fuse_chown,
      .flush = fuse_flush,
      .ftruncate = fuse_ftruncate,
      .fsync = fuse_fsync,
      .fsyncdir = fuse_fsyncdir,
      .getattr = fuse_getattr,
      .getxattr = fuse_getxattr,
      .listxattr = fuse_listxattr,
      .mkdir = fuse_mkdir,
      .open = fuse_open,
      .opendir = fuse_opendir,
      .read = fuse_read,
      .readdir = fuse_readdir,
      .readlink = fuse_readlink,
      .release = fuse_release,
      .releasedir = fuse_releasedir,
      .removexattr = fuse_removexattr,
      .rename = fuse_rename,
      .rmdir = fuse_rmdir,
      .setxattr = fuse_setxattr,
      .statfs = fuse_statvfs,
      .symlink = fuse_symlink,
      .truncate = fuse_truncate,
      .unlink = fuse_unlink,
      .utimens = fuse_utimens,
      .write = fuse_write,
      .create = fuse_create,
      .link = fuse_link

  };

  if ((getuid() != 0) || (geteuid() != 0))
  {
    return -1;
  }

  return fuse_main(argc, argv, &marfs_oper, NULL);
}
