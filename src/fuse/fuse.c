/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#define FUSE_USE_VERSION 26

#include "marfs_auto_config.h"
#ifdef DEBUG_FUSE
#define DEBUG DEBUG_FUSE
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "fuse"
#include "logging/logging.h"

#include <fuse.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>

#include "change_user.h"
#include "api/marfs.h"

// ENOATTR is not always defined, so define a convenience val
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

#define CONFIGVER_FNAME "/.configver"

typedef struct marfs_fuse_ctxt_struct {
   marfs_ctxt ctxt;
   pthread_mutex_t erasurelock;
}* marfs_fuse_ctxt;

marfs_fuse_ctxt fctxt;

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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_access(fctxt->ctxt, newpath, mode, AT_SYMLINK_NOFOLLOW | AT_EACCESS);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_chmod(fctxt->ctxt, newpath, mode, 0);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_chown(fctxt->ctxt, newpath, uid, gid, AT_SYMLINK_NOFOLLOW);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
    LOG( LOG_ERR, "%s: File handle is still open\n", path );
    return -EBADF;
  }

  if (!strcmp(path, CONFIGVER_FNAME)) {
    LOG( LOG_ERR, "Cannot create reserved config version file \"%s\"\n", CONFIGVER_FNAME );
    return -EPERM;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( fctxt->ctxt, path );
  ffi->fh = (uint64_t)marfs_creat(fctxt->ctxt, NULL, newpath, mode);
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
    LOG( LOG_ERR, "%s: Cannot truncate a NULL file handle\n", path );
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  int ret = marfs_ftruncate((marfs_fhandle)ffi->fh, length);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
		statbuf->st_size = marfs_configver(fctxt->ctxt, NULL, 0) + 1;
    return 0;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_stat(fctxt->ctxt, newpath, statbuf, AT_SYMLINK_NOFOLLOW);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
  char* newpath = translate_path( fctxt->ctxt, path );
  LOG( LOG_INFO, "Attempting to open an fhandle for target path: \"%s\"\n", path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( fctxt->ctxt, NULL, newpath, O_RDONLY | O_NOFOLLOW | O_ASYNC );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( fctxt->ctxt, newpath );
      err = errno;
    }
    else if ( errno == ELOOP ) { err = ENODATA; } // assume symlink target ( MarFS doesn't support symlink xattrs )
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
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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

  char* newoldpath = translate_path( fctxt->ctxt, oldpath );
  char* newnewpath = translate_path( fctxt->ctxt, newpath );
  int ret = marfs_link(fctxt->ctxt, newoldpath, newnewpath, AT_SYMLINK_NOFOLLOW);
  if ( ret )
  {
    LOG(LOG_ERR, "%s %s: %s\n", oldpath, newpath, strerror(errno))
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
  char* newpath = translate_path( fctxt->ctxt, path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( fctxt->ctxt, NULL, newpath, O_RDONLY | O_NOFOLLOW | O_ASYNC );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( fctxt->ctxt, newpath );
      err = errno;
    }
    if ( dh == NULL ) {
      // no file handle, and no dir handle
      LOG( LOG_ERR, "Failed to open marfs_fhandle for target path: \"%s\" (%s)\n",
           path, strerror(errno) );
      free( newpath );
      exit_user(&u_ctxt);
      return (err) ? ( (err == ELOOP) ? 0 : -err ) : -ENOMSG; // assume ELOOP -> symlink ( MarFS doesn't support symlink xattrs )
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
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  else { ret = (int)xres; }

  // cleanup our handle
  if ( fh ) {
    if ( marfs_release(fh) )
      LOG( LOG_WARNING, "Failed to close marfs_fhandle following listxattr() op\n" );
  }
  else if ( marfs_closedir(dh) ) {
    LOG( LOG_WARNING, "Failed to close marfs_dhandle following listxattr() op\n" );
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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_mkdir(fctxt->ctxt, newpath, mode);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
    LOG(LOG_ERR, "%s: previously open file descriptor\n", path);
    return -EBADF;
  }

  int flags = O_RDONLY;
  if (ffi->flags & O_RDWR)
  {
    LOG(LOG_ERR, "%s: invalid flags %x %x\n", path, ffi->flags, ffi->flags & O_RDWR);
    return -EINVAL;
  }
  else if (ffi->flags & O_WRONLY)
  {
    flags = O_WRONLY;
  }

  if (!strcmp(path, CONFIGVER_FNAME)) {
    if (flags == O_WRONLY) {
      LOG( LOG_ERR, "Cannot open config version file \"%s\" for write\n", CONFIGVER_FNAME );
      return -EPERM;
    }
    ffi->fh = (uint64_t)0;
    return 0;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( fctxt->ctxt, path );
  ffi->fh = (uint64_t)marfs_open(fctxt->ctxt, NULL, newpath, flags);
  int err = errno;
  free( newpath );

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(err));
    return (err) ? -err : -ENOMSG;
  }

  LOG( LOG_INFO, "New MarFS %s Handle: %p\n", (flags == O_RDONLY) ? "Read" : "Write",
       (void*)ffi->fh );
  return 0;
}

int fuse_opendir(const char *path, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (ffi->fh)
  {
    LOG(LOG_ERR, "%s: previously open file descriptor\n", path);
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);

  char* newpath = translate_path( fctxt->ctxt, path );
  ffi->fh = (uint64_t)marfs_opendir(fctxt->ctxt, newpath);
  int err = errno;
  free( newpath );

  exit_user(&u_ctxt);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(err));
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
        ret = marfs_configver(fctxt->ctxt, buf, size);
      }
      else {
        ret = marfs_configver(fctxt->ctxt, NULL, 0);
        if (ret > 0) {
          char tmpBuf[ret];

          ret = marfs_configver(fctxt->ctxt, tmpBuf, ret);

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
      LOG(LOG_ERR, "%s: missing file descriptor\n", path);
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
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }
  else { ret = (int)rres; }

  exit_user(&u_ctxt);

  if ( ret >= 0 ) { LOG( LOG_INFO, "Successfully read %d bytes\n", ret ); }
  else { LOG( LOG_ERR, "%s: Read of %zd bytes failed (%s)\n", path, size, strerror(errno) ); }
  return ret;
}

int fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *ffi)
{
  LOG(LOG_INFO, "%s\n", path);

  if (!ffi->fh)
  {
    LOG(LOG_ERR, "%s: missing file descriptor\n", path);
    return -EBADF;
  }

  struct dirent *de;

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 1);
  int cachederrno = errno; // cache and potentially reset errno

  // potentially seek to the specified offset
  int ret = 0;
  if ( offset != marfs_telldir((marfs_dhandle)ffi->fh) ) {
    int seekres = 0;
    if ( offset ) {
      seekres = marfs_seekdir((marfs_dhandle)ffi->fh, offset);
    }
    else {
      seekres = marfs_rewinddir((marfs_dhandle)ffi->fh);
    }
    if ( seekres ) {
      LOG(LOG_ERR, "%s\n", strerror(errno) );
      ret = (errno) ? -errno : -ENOMSG;
      exit_user(&u_ctxt);
      return ret;
    }
  }

  errno = 0;
  while ((de = marfs_readdir((marfs_dhandle)ffi->fh)) != NULL)
  {
    long posval = marfs_telldir((marfs_dhandle)ffi->fh);
    if ( posval == -1 ) {
      LOG(LOG_ERR, "%s\n", strerror(errno) );
      ret = (errno) ? -errno : -ENOMSG;
      exit_user(&u_ctxt);
      return ret;
    }
    int fillret = filler(buf, de->d_name, NULL, (off_t)posval);
    if ( fillret < 0 )
    {
      LOG(LOG_ERR, "%s: %s\n", path, strerror(ENOMEM));
      exit_user(&u_ctxt);
      return -ENOMEM;
    }
    else if ( fillret ) { break; }
  }
  if ( errno != 0 ) {
    LOG( LOG_ERR, "%s: Detected errno value post-readdir (%s)\n", path, strerror(errno) );
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

  char* newpath = translate_path( fctxt->ctxt, path );
  ssize_t ret = marfs_readlink(fctxt->ctxt, newpath, buf, size);
  free( newpath );
  if ( ret < 0 ) {
    LOG( LOG_ERR, "%s: %s\n", path, strerror(errno) );
    return (errno) ? -errno : -ENOMSG;
  }
  // sanity check return values and null-terminate on the behalf of fuse because it just can't be bothered to define its readlink interface sensibly
  if ( ret < size ) { *(buf + ret) = '\0'; ret = 0; } // NULL terminate the string, and indicate success
  else {
    if ( size > 0 ) { // silly check to try to properly null-terminate, even if we're called to fill a zero-length buffer
      *(buf + (size - 1)) = '\0';
    }
    if ( ret > INT_MAX ) { ret = INT_MAX; }
  }

  exit_user(&u_ctxt);

  return (int)ret;
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
    LOG(LOG_ERR, "%s: missing file descriptor\n", path);
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  int ret = marfs_close((marfs_fhandle)ffi->fh);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
    LOG(LOG_ERR, "%s: missing file descriptor\n", path);
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  int ret = marfs_closedir((marfs_dhandle)ffi->fh);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
  char* newpath = translate_path( fctxt->ctxt, path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( fctxt->ctxt, NULL, newpath, O_RDONLY | O_NOFOLLOW | O_ASYNC );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( fctxt->ctxt, newpath );
      err = errno;
    }
    else if ( errno == ELOOP ) { err = ENODATA; } // assume symlink target ( MarFS doesn't support symlink xattrs )
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
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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

  char* newoldpath = translate_path( fctxt->ctxt, oldpath );
  char* newnewpath = translate_path( fctxt->ctxt, newpath );
  int ret = marfs_rename(fctxt->ctxt, newoldpath, newnewpath);
  if ( ret )
  {
    LOG(LOG_ERR, "%s %s: %s\n", oldpath, newpath, strerror(errno));
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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_rmdir(fctxt->ctxt, newpath);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
  char* newpath = translate_path( fctxt->ctxt, path );
  marfs_dhandle dh = NULL;
  marfs_fhandle fh = marfs_open( fctxt->ctxt, NULL, newpath, O_RDONLY | O_NOFOLLOW | O_ASYNC );
  if (!fh) {
    int err = errno;
    if ( errno == EISDIR ) {
      // this is a dir, and requires a directory handle
      LOG( LOG_INFO, "Attempting to open a dhandle for target path: \"%s\"\n", path );
      errno = cachederrno; // restore orig errno ( if op succeeds, want to leave unchanged )
      dh = marfs_opendir( fctxt->ctxt, newpath );
      err = errno;
    }
    else if ( errno == ELOOP ) { err = ENOSYS; } // assume symlink target ( MarFS doesn't support symlink xattrs )
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
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
    ret = (errno) ? -errno : -ENOMSG;
  }

  // cleanup our handle
  if ( fh ) {
    if ( marfs_release(fh) )
      LOG( LOG_WARNING, "Failed to close marfs_fhandle following setxattr() op\n" );
  }
  else if ( marfs_closedir(dh) ) {
    LOG( LOG_WARNING, "Failed to close marfs_dhandle following setxattr() op\n" );
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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_statvfs(fctxt->ctxt, newpath, statbuf);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
  char* newname = translate_path( fctxt->ctxt, linkname );
  int ret = marfs_symlink(fctxt->ctxt, target, newname);
  if ( ret )
  {
    LOG(LOG_ERR, "%s %s: %s\n", target, linkname, strerror(errno));
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

  char* newpath = translate_path( fctxt->ctxt, path );
  if ((fh = marfs_open(fctxt->ctxt, NULL, newpath, O_WRONLY)) == NULL)
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
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_unlink(fctxt->ctxt, newpath);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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

  char* newpath = translate_path( fctxt->ctxt, path );
  int ret = marfs_utimens(fctxt->ctxt, newpath, tv, 0);
  if ( ret )
  {
    LOG(LOG_ERR, "%s: %s\n", path, strerror(errno));
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
    LOG( LOG_ERR, "%s: Cannot write to a NULL file handle\n", path );
    return -EBADF;
  }

  struct user_ctxt_struct u_ctxt;
  memset(&u_ctxt, 0, sizeof(struct user_ctxt_struct));
  enter_user(&u_ctxt, fuse_get_context()->uid, fuse_get_context()->gid, 0);

  off_t sret = marfs_seek((marfs_fhandle)ffi->fh, offset, SEEK_SET);
  if ( sret != offset)
  {
    LOG( LOG_ERR, "%s: unexpected seek res: %zd (%s)\n", path, sret, strerror(errno) );
    int err = errno;
    exit_user(&u_ctxt);
    return (err) ? -err : -ENOMSG;
  }

  ssize_t ret = marfs_write((marfs_fhandle)ffi->fh, buf, size);

  if (ret < 0)
  {
    LOG( LOG_ERR, "%s: unexpected write res: %zd (%s)\n", path, ret, strerror(errno) );
    ret = (errno) ? -errno : -ENOMSG;
  }
  else if ( ret != size )
  {
    LOG( LOG_ERR, "%s: unexpected write res: %zd (%s)\n", path, ret, strerror(errno) );
    ret = (errno) ? -errno : -ENOMSG;
  }

  exit_user(&u_ctxt);

  return (int)ret;
}

void marfs_fuse_init(void)
{
  LOG(LOG_INFO, "init\n");
  fctxt = calloc( 1, sizeof( struct marfs_fuse_ctxt_struct ) );
  if ( fctxt == NULL ) {
    fprintf( stderr, "Failed to allocate a marfs_fuse_ctxt struct\n" );
    exit(-1);
  }
  if ( pthread_mutex_init( &(fctxt->erasurelock), NULL ) ) {
    fprintf( stderr, "Failed to initialize local erasurelock\n" );
    free( fctxt );
    exit(-1);
  }
  // initialize the MarFS config
  fctxt->ctxt = marfs_init( getenv("MARFS_CONFIG_PATH"), MARFS_INTERACTIVE, &(fctxt->erasurelock) );
  if ( fctxt->ctxt == NULL ) {
    fprintf( stderr, "Failed to initialize MarFS context!\n" );
    pthread_mutex_destroy( &(fctxt->erasurelock) );
    free( fctxt );
    exit(-1);
  }
  if ( marfs_setctag( fctxt->ctxt, "FUSE" ) ) {
    fprintf( stderr, "Warning: Failed to set Client Tag String\n" );
  }
}

void marfs_fuse_destroy(void *userdata)
{
  LOG(LOG_INFO, "destroy\n");
  if ( marfs_term(fctxt->ctxt) ) {
    LOG( LOG_WARNING, "Failed to properly terminate marfs_ctxt\n" );
  }
  if ( pthread_mutex_destroy( &(fctxt->erasurelock) ) ) {
    LOG( LOG_WARNING, "Failed to properly destroy local erasurelock\n" );
  }
  free( fctxt );
}

int main(int argc, char *argv[])
{

  struct fuse_operations marfs_oper;
  bzero( &(marfs_oper), sizeof( struct fuse_operations ) );
  // initialize startup / teardown funcs
  marfs_oper.init = NULL;
  marfs_oper.destroy = marfs_fuse_destroy;
  // initialize basic metadata ops
  marfs_oper.access = fuse_access;
  marfs_oper.chmod = fuse_chmod;
  marfs_oper.chown = fuse_chown;
  marfs_oper.getattr = fuse_getattr;
  marfs_oper.getxattr = fuse_getxattr;
  marfs_oper.setxattr = fuse_setxattr;
  marfs_oper.listxattr = fuse_listxattr;
  marfs_oper.readlink = fuse_readlink;
  marfs_oper.removexattr = fuse_removexattr;
  marfs_oper.rename = fuse_rename;
  marfs_oper.symlink = fuse_symlink;
  marfs_oper.link = fuse_link;
  marfs_oper.unlink = fuse_unlink;
  marfs_oper.utimens = fuse_utimens;
  marfs_oper.statfs = fuse_statvfs;
  // initialize directory ops
  marfs_oper.mkdir = fuse_mkdir;
  marfs_oper.rmdir = fuse_rmdir;
  marfs_oper.opendir = fuse_opendir;
  marfs_oper.readdir = fuse_readdir;
  marfs_oper.fsyncdir = fuse_fsyncdir;
  marfs_oper.releasedir = fuse_releasedir;
  // initialize file ops
  marfs_oper.create = fuse_create;
  marfs_oper.open = fuse_open;
  marfs_oper.read = fuse_read;
  marfs_oper.write = fuse_write;
  marfs_oper.ftruncate = fuse_ftruncate;
  marfs_oper.truncate = fuse_truncate;
  marfs_oper.flush = fuse_flush;
  marfs_oper.fsync = fuse_fsync;
  marfs_oper.release = fuse_release;

  if ( getenv("MARFS_CONFIG_PATH") == NULL )
  {
    fprintf( stderr, "MARFS_CONFIG_PATH is not specified, will not start fuse.\n" );
    return EXIT_FAILURE;
  }

  marfs_fuse_init();

//  if ((getuid() != 0) || (geteuid() != 0))
//  {
//    LOG( LOG_ERR, "Cannot be run by non-root user\n" );
//    errno = EPERM;
//    return -1;
//  }

  return fuse_main(argc, argv, &marfs_oper, NULL);
}
