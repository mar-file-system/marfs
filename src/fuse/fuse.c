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

#include "marfs_auto_config.h"
#ifdef DEBUG_FUSE
#define DEBUG DEBUG_FUSE
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "fuse"
#include <logging.h>

#include <fuse.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

#include "change_user.h"
#include "api/marfs.h"

// ENOATTR is not always defined, so define a convenience val
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

#define CONFIGVER_FNAME "/.configver"

#define CTXT (marfs_ctxt)(fuse_get_context()->private_data)

char* translate_path( marfs_ctxt ctxt, const char* path ) {
  if ( path == NULL ) {
    LOG( LOG_INFO, "NULL path value\n" );
    return NULL;
  }
  // identify the length of the resulting path
  size_t mountlen = marfs_mountpath( ctxt, NULL, 0 );
  if ( mountlen == 0 ) {
    LOG( LOG_ERR, "Failed to identify length of marfs mountpoint path\n" );
    return NULL;
  }
  size_t pathlen = strlen( path );
  char* newpath = NULL;
  if ( *path == '/' ) {
    // absolute paths must be adjusted to include mountpoint prefix
    // NOTE -- pretty sure every FUSE path is absolute; but check just in case...
    newpath = malloc( pathlen + mountlen + 1 );
    if ( newpath == NULL ) {
      LOG( LOG_ERR, "Failed to allocate newpath of length %zu\n", pathlen + mountlen + 1 );
      return NULL;
    }
    // add in the mountpath first
    if ( marfs_mountpath( ctxt, newpath, mountlen + 1 ) != mountlen ) {
      LOG( LOG_ERR, "Inconsistent length of marfs mountpoint path\n" );
      free( newpath );
      return NULL;
    }
    // append the normal path
    if ( snprintf( newpath + mountlen, pathlen + 1, "%s", path ) != pathlen ) {
      LOG( LOG_ERR, "Path has inconsistent length\n" );
      free( newpath );
      return NULL;
    }
    return newpath;
  }
  LOG( LOG_ERR, "Unexpected relative path value: \"%s\"\n", path );
  return NULL;
}

int fuse_access(const char *path, int mode)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_access(CTXT, newpath, mode, AT_SYMLINK_NOFOLLOW | AT_EACCESS);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_chmod(const char *path, mode_t mode)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_chmod(CTXT, newpath, mode, 0);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_chown(const char *path, uid_t uid, gid_t gid)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_chown(CTXT, newpath, uid, gid, 0);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_create(const char *path, mode_t mode, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (ffi->fh)
  {
    LOG( LOG_ERR, "File handle is still open\n" );
    return -EBADF;
  }

  if (!strcmp(path, CONFIGVER_FNAME)) {
    LOG( LOG_ERR, "Cannot create reserved config version file \"%s\"\n", CONFIGVER_FNAME );
    return -EPERM;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  ffi->fh = (uint64_t)marfs_creat(CTXT, NULL, newpath, mode);
  int err = errno;
  free( newpath );

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    return (err) ? -err : -ENOMSG;
  }

  LOG( LOG_INFO, "New MarFS Create Handle: %p\n", (void*)ffi->fh );
  return 0;
}

int fuse_flush(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh  &&  strcmp(path, CONFIGVER_FNAME))
  {
    if (!strcmp(path, CONFIGVER_FNAME)) {
      return 0;
    }
    LOG(LOG_ERR, "missing file descriptor\n");
    return -EBADF;
  }

//  struct user_ctxt_struct u_ctxt;
//  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
//  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);
//
//  LOG( LOG_INFO, "Flushing marfs_fhandle %p\n", (void*)ffi->fh );
//  int ret = marfs_flush((marfs_fhandle)ffi->fh);
//
//  if (ret)
//  {
//    LOG(LOG_ERR, "%s\n", errno);
//  }
  LOG( LOG_INFO, "NO-OP for fuse_flush()\n" );

//  exit_user(&u_ctxt);

  return 0;
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
    LOG( LOG_ERR, "Cannot truncate a NULL file handle\n" );
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_ftruncate((marfs_fhandle)ffi->fh, length);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_getattr(const char *path, struct stat *statbuf)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!strcmp(path, CONFIGVER_FNAME)) {
    statbuf->st_uid = getuid();
    statbuf->st_gid = getgid();
    statbuf->st_atime = time( NULL );
    statbuf->st_mtime = time( NULL );
		statbuf->st_mode = S_IFREG | 0444;
		statbuf->st_nlink = 1;
		statbuf->st_size = marfs_configver(CTXT, NULL, 0) + 1;
    return 0;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_stat(CTXT, newpath, statbuf, AT_SYMLINK_NOFOLLOW);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_getxattr(const char *path, const char *name, char *value, size_t size)
{
  LOG(LOG_INFO, "%s -- %s\n", path, name);

  if (!strcmp(path, CONFIGVER_FNAME)) {
    LOG( LOG_INFO, "Faking absent \"%s\" xattr for reserved config ver file\n", name );
    return -ENOATTR;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);
  int cachederrno = errno; // store our orig errno value

  // we need to use a file handle for this op
  char* newpath = translate_path( CTXT, path );
  LOG( LOG_INFO, "Attempting to open an fhandle for target path: \"%s\"\n", path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( CTXT, NULL, newpath, MARFS_READ | MARFS_META );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( CTXT, newpath );
      err = errno;
    }
    if ( dh == NULL ) {
      // no file handle, and no dir handle
      LOG( LOG_ERR, "Failed to open marfs_fhandle for target path: \"%s\" (%s)\n",
           path, strerror(errno) );
      free( newpath );
      exit_user(&u_ctxt);
      return (err) ? -err : -ENOMSG;
    }
  }
  free( newpath );

  // perform the op
  ssize_t xres = 0;
  int ret = 0;
  if ( fh ) { xres = marfs_fgetxattr(fh, name, value, size); }
  else { xres = marfs_dgetxattr(dh, name, value, size); }
  if (xres < 0)
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  else { ret = (int)xres; }

  // cleanup our handle
  if ( fh ) {
    if ( marfs_release(fh) )
      LOG( LOG_WARNING, "Failed to close marfs_fhandle following getxattr() op\n" );
  }
  else if ( marfs_closedir(dh) ) {
    LOG( LOG_WARNING, "Failed to close marfs_dhandle following getxattr() op\n" );
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *ffi, unsigned int flags, void *data)
{
  LOG(LOG_INFO, "%s\n", path);

  return -EOPNOTSUPP;
}

int fuse_link(const char *oldpath, const char *newpath)
{
  LOG(LOG_INFO, "%s %s\n", oldpath, newpath);

  if (!strcmp(newpath, CONFIGVER_FNAME)) {
    LOG(LOG_ERR, "cannot link over reserved config version file\n");
    return -EPERM;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newoldpath = translate_path( CTXT, oldpath );
  char* newnewpath = translate_path( CTXT, newpath );
  int ret = marfs_link(CTXT, newoldpath, newnewpath, 0);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newoldpath );
  free( newnewpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_listxattr(const char *path, char *list, size_t size)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!strcmp(path, CONFIGVER_FNAME)) {
    LOG( LOG_INFO, "Faking lack of all xattrs for reserved config ver file\n" );
    return 0;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);
  int cachederrno = errno; // cache orig errno

  // we need to use a file handle for this op
  char* newpath = translate_path( CTXT, path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( CTXT, NULL, newpath, MARFS_READ | MARFS_META );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( CTXT, newpath );
      err = errno;
    }
    if ( dh == NULL ) {
      // no file handle, and no dir handle
      LOG( LOG_ERR, "Failed to open marfs_fhandle for target path: \"%s\" (%s)\n",
           path, strerror(errno) );
      free( newpath );
      exit_user(&u_ctxt);
      return (err) ? -err : -ENOMSG;
    }
  }
  free( newpath );

  // perform the op
  ssize_t xres = 0;
  int ret = 0;
  if ( fh ) { xres = marfs_flistxattr(fh, list, size); }
  else { xres = marfs_dlistxattr(dh, list, size); }
  if (xres < 0)
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  else { ret = (int)xres; }

  // cleanup our handle
  if ( fh ) {
    if ( marfs_release(fh) )
      LOG( LOG_WARNING, "Failed to close marfs_fhandle following getxattr() op\n" );
  }
  else if ( marfs_closedir(dh) ) {
    LOG( LOG_WARNING, "Failed to close marfs_dhandle following getxattr() op\n" );
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_mkdir(const char *path, mode_t mode)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_mkdir(CTXT, newpath, mode);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

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

  if (!strcmp(path, CONFIGVER_FNAME)) {
    if (flags == MARFS_WRITE) {
      LOG( LOG_ERR, "Cannot open config version file \"%s\" for write\n", CONFIGVER_FNAME );
      return -EPERM;
    }
    ffi->fh = (uint64_t)0;
    return 0;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  ffi->fh = (uint64_t)marfs_open(CTXT, NULL, newpath, flags);
  int err = errno;
  free( newpath );

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "%s\n", strerror(err));
    return (err) ? -err : -ENOMSG;
  }

  LOG( LOG_INFO, "New MarFS %s Handle: %p\n", (flags == MARFS_READ) ? "Read" : "Write",
       (void*)ffi->fh );
  return 0;
}

int fuse_opendir(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (ffi->fh)
  {
    LOG(LOG_ERR, "previously open file descriptor\n");
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  ffi->fh = (uint64_t)marfs_opendir(CTXT, newpath);
  int err = errno;
  free( newpath );

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "%s\n", strerror(err));
    return (err) ? -err : -ENOMSG;
  }

  LOG( LOG_INFO, "New MarFS Directory Handle: %p\n", (void*)ffi->fh );
  return 0;
}

int fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffi)
{
  int ret = 0;

  if (!ffi->fh)
  {
    if (!strcmp(path, CONFIGVER_FNAME)) {
      // Read the MarFS config version
      LOG(LOG_INFO, "CONFIG-VER-READ of %zubytes from %s at offset %zd\n", size, path, offset);
      if (offset == 0) {
        ret = marfs_configver(CTXT, buf, size);
      }
      else {
        ret = marfs_configver(CTXT, NULL, 0);
        if (ret > 0) {
          char tmpBuf[ret];

          ret = marfs_configver(CTXT, tmpBuf, ret);

          if (offset < ret) {
            ret = (ret - offset) < size ? (ret - offset) : size;
            memcpy(buf, tmpBuf + offset, ret);
          }
        }
      }
      if (ret == 0) {
        ret = (errno) ? -errno : -ENOMSG;
      }
      else if (ret < size) {
        buf[ret] = '\n';
        ret++;
      }
      return ret;
    }
    else {
      LOG(LOG_ERR, "missing file descriptor\n");
      return -EBADF;
    }
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  LOG( LOG_INFO, "Performing read of %zubytes at offset %zd\n", size, offset );
  ssize_t rres = marfs_read_at_offset((marfs_fhandle)ffi->fh, offset, (void *)buf, size);

  if (rres < 0)
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  else { ret = (int)rres; }

  exit_user(&u_ctxt);

  if ( ret >= 0 ) { LOG( LOG_INFO, "Successfully read %d bytes\n", ret ); }
  else { LOG( LOG_ERR, "Read of %zd bytes failed (%s)\n", size, strerror(errno) ); }
  return ret;
}

int fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "missing file descriptor\n");
    return -EBADF;
  }

  struct dirent *de;

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);
  int cachederrno = errno; // cache and potentially reset errno

  errno = 0;
  while ((de = marfs_readdir((marfs_dhandle)ffi->fh)) != NULL)
  {
    if (filler(buf, de->d_name, NULL, 0))
    {
      LOG(LOG_ERR, "%s\n", ENOMEM);
      exit_user(&u_ctxt);
      return -ENOMEM;
    }
  }
  int ret = 0;
  if ( errno != 0 ) {
    LOG( LOG_ERR, "Detected errno value post-readdir (%s)\n", strerror(errno) );
    ret = -errno;
  }
  else {
    // reset errno value to original
    errno = cachederrno;
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_readlink(const char *path, char *buf, size_t size)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  ssize_t ret = marfs_readlink(CTXT, newpath, buf, size);
  free( newpath );
  if ( ret < 0 ) {
    LOG( LOG_ERR, "%s\n", strerror(errno) );
    return (errno) ? -errno : -ENOMSG;
  }

  exit_user(&u_ctxt);

  return 0;
}

int fuse_release(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    if (!strcmp(path, CONFIGVER_FNAME)) {
      LOG(LOG_INFO, "No-Op for config version file \"%s\"\n", CONFIGVER_FNAME);
      return 0;
    }
    LOG(LOG_ERR, "missing file descriptor\n");
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  int ret = marfs_close((marfs_fhandle)ffi->fh);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_releasedir(const char *path, struct fuse_file_info *ffi)
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

  int ret = marfs_closedir((marfs_dhandle)ffi->fh);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  else
  {
    ffi->fh = (uint64_t)NULL;
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_removexattr(const char *path, const char *name)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);
  int cachederrno = errno; // cache orig errno

  // we need to use a file handle for this op
  char* newpath = translate_path( CTXT, path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( CTXT, NULL, newpath, MARFS_READ | MARFS_META );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( CTXT, newpath );
      err = errno;
    }
    if ( dh == NULL ) {
      // no file handle, and no dir handle
      LOG( LOG_ERR, "Failed to open marfs_fhandle for target path: \"%s\" (%s)\n",
           path, strerror(errno) );
      free( newpath );
      exit_user(&u_ctxt);
      return (err) ? -err : -ENOMSG;
    }
  }
  free( newpath );

  // perform the op
  int ret = 0;
  if ( fh ) { ret = marfs_fremovexattr(fh, name); }
  else { ret = marfs_dremovexattr(dh, name); }
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }

  // cleanup our handle
  if ( fh ) {
    if ( marfs_release(fh) )
      LOG( LOG_WARNING, "Failed to close marfs_fhandle following removexattr() op\n" );
  }
  else if ( marfs_closedir(dh) ) {
    LOG( LOG_WARNING, "Failed to close marfs_dhandle following removexattr() op\n" );
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_rename(const char *oldpath, const char *newpath)
{
  LOG(LOG_INFO, "%s %s\n", oldpath, newpath);

  if (!strcmp(newpath, CONFIGVER_FNAME)  ||  !strcmp(oldpath, CONFIGVER_FNAME)) {
    LOG( LOG_ERR, "Cannot target reserved config version path with a rename op\n" );
    return -EPERM;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newoldpath = translate_path( CTXT, oldpath );
  char* newnewpath = translate_path( CTXT, newpath );
  int ret = marfs_rename(CTXT, newoldpath, newnewpath);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newoldpath );
  free( newnewpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_rmdir(const char *path)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_rmdir(CTXT, newpath);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);
  int cachederrno = errno; // cache orig errno

  // we need to use a file handle for this op
  char* newpath = translate_path( CTXT, path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( CTXT, NULL, newpath, MARFS_READ | MARFS_META );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( CTXT, newpath );
      err = errno;
    }
    if ( dh == NULL ) {
      // no file handle, and no dir handle
      LOG( LOG_ERR, "Failed to open marfs_fhandle for target path: \"%s\" (%s)\n",
           path, strerror(errno) );
      free( newpath );
      exit_user(&u_ctxt);
      return (err) ? -err : -ENOMSG;
    }
  }
  free( newpath );

  // perform the op
  int ret = 0;
  if ( fh ) { ret = marfs_fsetxattr(fh, name, value, size, flags); }
  else { ret = marfs_dsetxattr(dh, name, value, size, flags); }
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }

  // cleanup our handle
  if ( fh ) {
    if ( marfs_release(fh) )
      LOG( LOG_WARNING, "Failed to close marfs_fhandle following removexattr() op\n" );
  }
  else if ( marfs_closedir(dh) ) {
    LOG( LOG_WARNING, "Failed to close marfs_dhandle following removexattr() op\n" );
  }

  exit_user(&u_ctxt);

  return ret;
}

int fuse_statvfs(const char *path, struct statvfs *statbuf)
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_statvfs(CTXT, newpath, statbuf);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_symlink(const char *target, const char *linkname)
{
  LOG(LOG_INFO, "%s %s\n", target, linkname);

  if (!strcmp(linkname, CONFIGVER_FNAME)) {
    return -EPERM;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  // leave target path unmodified
  char* newname = translate_path( CTXT, linkname );
  int ret = marfs_symlink(CTXT, target, newname);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newname );

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

  char* newpath = translate_path( CTXT, path );
  if ((fh = marfs_open(CTXT, NULL, newpath, MARFS_WRITE)) == NULL)
  {
    err = errno;
    free( newpath );
    exit_user(&u_ctxt);
    return (err) ? -err : -ENOMSG;
  }
  free( newpath );

  int ret = marfs_ftruncate(fh, length);
  if ( ret  ||  marfs_close(fh) )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
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

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_unlink(CTXT, newpath);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_utimens(const char *path, const struct timespec tv[2])
{
  LOG(LOG_INFO, "%s\n", path);

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( CTXT, path );
  int ret = marfs_utimens(CTXT, newpath, tv, 0);
  if ( ret )
  {
    LOG(LOG_ERR, "%s\n", strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  free( newpath );

  exit_user(&u_ctxt);

  return ret;
}

int fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    LOG( LOG_ERR, "Cannot write no a NULL file handle\n" );
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  off_t sret = marfs_seek((marfs_fhandle)ffi->fh, offset, SEEK_SET);
  if ( sret != offset)
  {
    LOG( LOG_ERR, "Unexpected seek res: %zd (%s)\n", sret, strerror(errno) );
    int err = errno;
    exit_user(&u_ctxt);
    return (err) ? -err : -ENOMSG;
  }

  ssize_t ret = marfs_write((marfs_fhandle)ffi->fh, buf, size);

  if (ret < 0)
  {
    LOG( LOG_ERR, "Unexpected write res: %zd (%s)\n", ret, strerror(errno) );
    ret = (errno) ? -errno : -ENOMSG;
  }
  else if ( ret != size )
  {
    LOG( LOG_ERR, "Unexpected write res: %zd (%s)\n", ret, strerror(errno) );
    ret = (errno) ? -errno : -ENOMSG;
  }

  exit_user(&u_ctxt);

  return (int)ret;
}

void *marfs_fuse_init(struct fuse_conn_info *conn)
{
  LOG(LOG_INFO, "init\n");
  // initialize the MarFS config
  // NOTE -- FUSE doesn't attempt to verify the config, as it will almost always be run as root.
  //         We want to allow the 'secure root dir' to be owned by a non-root user, if desired.
  marfs_ctxt ctxt = marfs_init(getenv("MARFS_CONFIG_PATH"), MARFS_INTERACTIVE, 0);
  if ( ctxt == NULL ) {
    LOG( LOG_ERR, "Failed to initialize MarFS context!\n" );
    exit(-1);
  }
  if ( marfs_setctag( ctxt, "FUSE" ) ) {
    LOG( LOG_WARNING, "Failed to set Client Tag String\n" );
  }
  return (void*)ctxt;
}

void marfs_fuse_destroy(void *userdata)
{
  LOG(LOG_INFO, "destroy\n");
  if ( marfs_term(CTXT) ) {
    LOG( LOG_WARNING, "Failed to properly terminate marfs_ctxt\n" );
  }
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

//  if ((getuid() != 0) || (geteuid() != 0))
//  {
//    LOG( LOG_ERR, "Cannot be run by non-root user\n" );
//    errno = EPERM;
//    return -1;
//  }

  return fuse_main(argc, argv, &marfs_oper, NULL);
}
