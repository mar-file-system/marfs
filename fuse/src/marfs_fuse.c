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
#include "marfs_fuse.h"

/*
@@@-HTTPS:
This was added on 24-Jul-2015 because we need this for the RepoFlags type.
However, it was also needed for other uses of types in marfs_base.h before
this date. It happened that it is included in common.h. Rather than rely
on that fact, because that could change, it is best practice to explicitly
include the files on which a code unit depends.
*/

#include "marfs_base.h"

#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <attr/xattr.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <utime.h>              /* for deprecated marfs_utime() */
#include <stdio.h>



// ---------------------------------------------------------------------------
// utilities
//
// These are some chunks of code used in more than one fuse function.
// Should probably be moved to another file, eventually.
// ---------------------------------------------------------------------------

// update the URL in the ObjectStream, in our FileHandle
int update_url(ObjectStream* os, PathInfo* info) {
   //   size_t rc;                   // for TRY
   //   __TRY0(update_pre, &info->pre);
   strncpy(os->url, info->pre.objid, MARFS_MAX_URL_SIZE);

   // log the full URL, if possible:
   IOBuf*        b  = &os->iob;
   __attribute__ ((unused)) AWSContext*   ctx = ((b) ? b->context : aws_context_clone());

   LOG(LOG_INFO, "generated URL %s %s/%s/%s\n",
       ((b) ? "" : "(defaults)"),
       ctx->S3Host, ctx->Bucket, os->url);

   return 0;
}




// ---------------------------------------------------------------------------
// inits
// ---------------------------------------------------------------------------

// "The return value of this function is available to all file operations
// in the private_data field of fuse_context."  (e.g. via get_fuse_context())
// [http://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html]
//
void* marfs_init(struct fuse_conn_info* conn) {
   conn->max_write = MARFS_WRITEBUF_MAX;
   conn->want      = FUSE_CAP_BIG_WRITES;

   // To disable: Set zero here, and clear FUSE_CAP_ASYNC_READ from <want>
   conn->async_read = 0;

   return conn;
}

// called when fuse file system exits
void marfs_destroy (void* private_data) {
   // nothing for us to do here, we wont have dirty data when the fuse
   // daemon exits. I suppose they wait for all threads to finish before
   // leaving, so this should be ok.
   LOG(LOG_INFO, "shutting down\n");
}





// ---------------------------------------------------------------------------
// Fuse routines in alpha order (so you can actually find them)
// Unimplmented functions are gathered at the bottom
// ---------------------------------------------------------------------------


int marfs_access (const char* path,
                  int         mask) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  access call filling in fuse structure 
   //
   // jti: which fuse structure?
   TRY0(access, info.post.md_path, mask);
 
   POP_USER();
   return 0;
}


// NOTE: we don't allow setuid or setgid.  We're using those bits for two
//     purposes:
//
//     (a) on a file, the setuid-bit indicates SEMI-DIRECT mode.  In this
//     case, the size of the MD file is not correct, because it isn't
//     truncated to corect size, because the underlying FS is parallel, and
//     in the case of N:1 writes, it is awkward for us to manage locking
//     across multiple writers, so we can correctly trunc the MD file.
//     Instead, we let the parallel FS do that (because it can).  So, to
//     get the size, we need to go stat the PFS file, instead.  If we wrote
//     this flag in an xattr, we'd have to read xattrs for every stat, just
//     so we can learn whether the file-size returned by 'stat' is correct
//     or not.
//
//     (b) on a directory, the setuid-bit indicates MD-sharding.  [FUTURE]
//     In this case, the MD-directory will be a stand-in for a remote MD
//     directory.  We hash on a namespace + directory-inode and lookup that
//     spot in a remote-directory, to get the MD contents.

int marfs_chmod(const char* path,
                mode_t      mode) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   if (mode && (S_ISUID | S_ISGID)) {
      LOG(LOG_INFO, "attempt to setuid or setgid on path '%s'\n", path);
      return -EPERM;
   }

   // No need for access check, just try the op
   // Appropriate  chmod call filling in fuse structure
   // WARNING: No lchmod() on rrz.
   //          chmod() always follows links.
   TRY0(chmod, info.post.md_path, mode);

   POP_USER();
   return 0;
}

int marfs_chown (const char* path,
                 uid_t       uid,
                 gid_t       gid) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  chown call filling in fuse structure
   TRY0(lchown, info.post.md_path, uid, gid);

   POP_USER();
   return 0;
}

// Looking for "marfs_close()"?  It's called "marfs_release()".


int marfs_fsync (const char*            path,
                 int                    isdatasync,
                 struct fuse_file_info* ffi) {
   // I don’t know if we do anything here, I don’t think so, we will be in
   // sync at the end of each thread end

   // [jti:] in the case of SEMI_DIRECT, we could fsync the storage

   LOG(LOG_INFO, "NOP for %s", path);
   return 0; // Just return
}


int marfs_fsyncdir (const char*            path,
                    int                    isdatasync,
                    struct fuse_file_info* ffi) {
   // don’t think there is anything to do here, we wont have dirty data
   // unless its trash

   // [jti:] in the case of SEMI_DIRECT, we could fsync the storage

   LOG(LOG_INFO, "NOP for %s", path);
   return 0; // just return
}


// I read that FUSE will never call open() with O_TRUNC, but will instead
// call truncate first, then open.  However, a user might still call
// truncate() or ftruncate() explicitly.  For these cases, I guess we
// assume the file is already open, and the filehandle is good.
//
// UPDATE: for 'truncate -s 0 /marfs/file' or 'echo test >
//     /marfs/existing_file', fuse calls open() / ftruncate() / close().
//
// If a user calls ftruncate() they expect the file-handle to remain open.
// Therefore, we need to leave things as though open had been called with
// the current <path>.  That means, the new object-handle we create
// (because the old one goes with trashed file), must be open and ready for
// business.
//
// In the case where the filehandle was opened for writing, open() will
// have an open filehandle to the MD file.  We copy away the current
// contents of the metadata file, so we can leave the existing MD fd open,
// however, we should ftruncate that.
//

int marfs_ftruncate(const char*            path,
                    off_t                  length,
                    struct fuse_file_info* ffi) {

   // *** this may not be needed until we implement write in the fuse daemon ***
   // *** may not be needed for the kind of support we want to provide ***

   PUSH_USER();

   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   PathInfo*         info = &fh->info;                  /* shorthand */
   // IOBuf*            b    = &fh->os.iob;                /* shorthand */

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info->ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // Call access() syscall to check/act if allowed to truncate for this user
   ACCESS(info->post.md_path, (W_OK));        /* for truncate? */

   // stat_xattrs, or look up info stuffed into memory pointed at in fuse
   // open table if this is not just a normal [object-storage case?], use
   // the md for file data
   STAT_XATTRS(info);
   if (! has_any_xattrs(info, MARFS_ALL_XATTRS)) {
      LOG(LOG_INFO, "no xattrs\n");
      TRY0(ftruncate, fh->md_fd, length);
      return 0;
   }

   // POSIX ftruncate returns EBADF or EINVAL, if fd not opened for writing.
   if (! (fh->flags & FH_WRITING)) {
      LOG(LOG_ERR, "was not opened for writing\n");
      return -EINVAL;
   }

   // Check/act on truncate-to-zero only.
   if (length)
      return -EPERM;


   //***** this may or may not work, may need a trash_truncate() that uses
   //***** ftruncate since the file is already open (may need to modify the
   //***** trash_truncate to use trunc or ftrunc depending on if file is
   //***** open or not

   // copy metadata to trash, resets original file zero len and no xattr
   // updates info->pre.objid
   TRASH_TRUNCATE(info, path);

   // metadata filehandle is still open to the current MD file.
   // That's okay, but we need to ftruncate it, as well.
   if (fh->md_fd) {
      if (fh->flags & FH_WRITING)
         ftruncate(fh->md_fd, length); // (length == 0)
   }

   // object-stream is still open to the old object.  Close that in such a
   // way that the server will not persist the PUT.
   TRY0(stream_abort, &fh->os);
   TRY0(stream_close, &fh->os);

   // open a stream to the new object.  We assume that the libaws4c context
   // initializations done in marfs_open are still valid.  trash_truncate()
   // will already have updated our URL.
   TRY0(update_url, &fh->os, info);
   TRY0(stream_open, &fh->os, OS_PUT);

   // (see marfs_mknod() -- empty non-DIRECT file needs *some* marfs xattr,
   // so marfs_open() won't assume it is a DIRECT file.)
   if (info->ns->iwrite_repo->access_method != ACCESSMETHOD_DIRECT) {
      LOG(LOG_INFO, "marking with RESTART, so open() won't think DIRECT\n");
      info->flags  |= PI_RESTART;
      info->xattrs |= XVT_RESTART;
      SAVE_XATTRS(info, XVT_RESTART);
   }
   else
      LOG(LOG_INFO, "iwrite_repo.access_method = DIRECT\n");

   POP_USER();
   return 0;
}


// This is stat()
int marfs_getattr (const char*  path,
                   struct stat* stp) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);
   LOG(LOG_INFO, "expanded    %s -> %s\n", path, info.post.md_path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, R_META);

   // The "root" namespace is artificial
   // It appears to be owned by root, with X-only access to users
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");

      // everything defaults to zero
      memset(stp, 0, sizeof(struct stat));

      // match the size of a directory in GPFS
      stp->st_size    = 512;
      stp->st_blksize = 512;
      stp->st_blocks  = 1;

      time_t     now = time(NULL);
      if (now == (time_t)-1) {
         LOG(LOG_ERR, "time() failed\n");
         return -1;
      }
      stp->st_atime  = now;
      stp->st_mtime  = now;     // TBD: use mtime of config-file, or mount-time
      stp->st_ctime  = now;     // TBD: use ctime of config-file, or mount-time

      stp->st_uid     = 0;
      stp->st_gid     = 0;
      stp->st_mode = (S_IFDIR
                      | (S_IRUSR | S_IXUSR)
                      | (S_IRGRP | S_IXGRP)
                      | S_IXOTH );            // "dr-xr-x--x."

      return 0;
   }

   // No need for access check, just try the op
   // appropriate statlike call filling in fuse structure (dont mess with xattrs here etc.)
   //
   // NOTE: kernel should already have called readlink, to get past any
   //     symlinks.  lstat here is just to be safe.
   LOG(LOG_INFO, "lstat %s\n", info.post.md_path);
   TRY_GE0(lstat, info.post.md_path, stp);

   // mask out setuid/setgid bits.  Those are belong to us.  (see marfs_chmod())
   stp->st_mode &= ~(S_ISUID | S_ISGID);

   POP_USER();
   return 0;
}


// *** this may not be needed until we implement user xattrs in the fuse daemon ***
//
// Kernel calls this with key 'security.capability'
//
int marfs_getxattr (const char* path,
                    const char* name,
                    char*       value,
                    size_t      size) {
   //   LOG(LOG_INFO, "not implemented  (path %s, key %s)\n", path, name);
   //   return -ENOSYS;
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -ENOATTR;          // fingers in ears, la-la-la
   }

   // *** make sure they aren’t getting a reserved xattr***
   if ( !strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize) ) {
      LOG(LOG_ERR, "denying reserved getxattr(%s, %s, ...)\n", path, name);
      return -EPERM;
   }

   // No need for access check, just try the op
   // Appropriate  getxattr call filling in fuse structure
   //
   // NOTE: GPFS returns -1, errno==ENODATA, for
   //     lgetxattr("system.posix_acl_access",path,0,0).  The kernel calls
   //     us with this, for 'ls -l /marfs/jti/blah'.

   TRY_GE0(lgetxattr, info.post.md_path, name, (void*)value, size);
   ssize_t result = rc_ssize;

   POP_USER();
   return result;
}


int marfs_ioctl(const char*            path,
                int                    cmd,
                void*                  arg,
                struct fuse_file_info* ffi,
                unsigned int           flags,
                void*                  data) {
   // if we need an ioctl for something or other
   // ***Maybe a way to get fuse deamon to read up new config file
   // *** we need a way for daemon to read up new config file without stopping

   LOG(LOG_INFO, "NOP for %s", path);
   return 0;
}




// *** this may not be needed until we implement user xattrs in the fuse daemon ***
//
// NOTE: Even though we remove reserved xattrs, user can call with empty
//       buffer and receive back length of xattr names.  Then, when we
//       remove reserved xattrs (in a subsequent call), user will see a
//       different list length than the previous call lead him to expect.
int marfs_listxattr (const char* path,
                     char*       list,
                     size_t      size) {
   //   LOG(LOG_INFO, "listxattr(%s, ...) not implemented\n", path);
   //   return -ENOSYS;

   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      size = 0;                 // amount needed for 
      return 0;
   }

   // No need for access check, just try the op
   // Appropriate  listxattr call
   // filling in fuse structure.
   // NOTE: If caller passes <list>=0, we'll be fine.
   TRY_GE0(llistxattr, info.post.md_path, list, size);

   // In the case where list==0, we return the size of the buffer that
   // caller would need, in order to receive all our xattr data.
   if (! list) {
      POP_USER();
      return rc_ssize;
   }


   // *** remove any reserved xattrs from list ***
   //
   // We could malloc our own storage here, listxattr into our storage,
   // remove any reserved xattrs, then copy to user's storage.  Or, we
   // could just use the caller's space to receive results, and then remove
   // any reserved xattrs from that list.  The latter would be faster, but
   // potentially allows a user to discover the *names* of reserved xattrs
   // (seeing them before we've deleted them). Given that the user can't
   // actually use fuse to get *values* for the reserved xattrs, and the
   // names of reserved xattrs are to be documented for public consumption,
   // the former approach seems secure enough.
   //
   // However, shuffling MarFS xattr names to cover-up system names takes
   // as much trouble as just copying the legit names into callers list, so
   // we should just do the copy, instead.  On third thought, it may take
   // the same amount of trouble to do the shuffling, but that also
   // requires a malloc, so we stick with using caller's buffer.
   //
   char* end  = list + rc_ssize;
   char* name = list;
   int   result_size = 0;
   *end = 0;
   while (name < end) {
      const size_t len = strlen(name) +1;

      // if it's a system xattr, shift subsequent data to cover it.
      if (! strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize)) {

         /* llistxattr() should have returned neg, in this case */
         if (name + len > end) {
            LOG(LOG_ERR, "name + len(%ld) exceeds end\n", len);
            return -EINVAL;
         }

         LOG(LOG_INFO, "skipping '%s'\n", name);

         // shuffle subsequent keys forward to cover this one
         memmove(name, name + len, end - name + len);

         // wipe the tail clean
         memset(end - len, 0, len);

         end -= len;
      }
      else {
         LOG(LOG_INFO, "allowing '%s'\n", name);
         name += len;
         result_size += len;
      }
   }


   POP_USER();
   return result_size;
}


int marfs_mkdir (const char* path,
                 mode_t      mode) {
   PUSH_USER();

   PathInfo  info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // Check/act on quota num files

   // No need for access check, just try the op
   // Appropriate mkdirlike call filling in fuse structure 
   TRY0(mkdir, info.post.md_path, mode);

   POP_USER();
   return 0;
}


// [See discussion at marfs_create().]
//
// This only gets called when fuse determines file doesn't exist and needs
// to be created.  If it needs to be truncated, fuse calls
// truncate/ftruncate, before calling us.
// 
// It might make sense to do object-creation, and initialization of
// PathInfo.pre, here.  However, open() is where we do tests that should be
// done before creating the object.  Maybe we should move all those here,
// as well?  Meanwhile, we currently construct obj-id inside stat_xattrs(),
// in the case where MD exists but xattrs don't.
// 
int marfs_mknod (const char* path,
                 mode_t      mode,
                 dev_t       rdev) {

   //   // mknod() is now superceded by create().
   //   // Making sure this never gets called ...
   //   assert(0);

   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op
   // requires RMWM NOTE: We assure that open(), if called after us, can't
   // discover that user lacks sufficient access.  However, because we
   // don't know what the open call might be, we may be imposing
   // more-restrictive constraints than necessary.
   //
   //   CHECK_PERMS(info.ns->iperms, (R_META | W_META));
   //   CHECK_PERMS(info.ns->iperms, (R_META | W_META | W_DATA | T_DATA));
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA | T_DATA));

   // Check/act on quotas of total-space and total-num-names
   CHECK_QUOTAS(&info);

   // No need for access check, just try the op
   // Appropriate mknod-like/open-create-like call filling in fuse structure
   TRY0(mknod, info.post.md_path, mode, rdev);
   LOG(LOG_INFO, "mode: (octal) 0%o\n", mode); // debugging

   // PROBLEM: marfs_open() assumes that a file that exists, which doesn't
   //     have xattrs, is something that was created when
   //     repo.access_method was DIRECT.  For such files, marfs_open()
   //     expects to read directly from the file.  We have just created a
   //     file.  If repo.access_method is not direct, we'd better find a
   //     way to let marfs_open() know about it.  However, it would be nice
   //     to leave most of the xattr creation to marfs_release().
   //
   // SOLUTION: set the RESTART xattr flag.  It will be clear that this
   //     object hasn't been successfully closed, yet.  It will also be
   //     clear that this is not one of those files with no xattrs, so open
   //     will treat it properly. Thus, if someone reads from this file
   //     while it's being written, fuse will see it as a file-with-xattrs
   //     (which is incomplete), and could throw an error, instead of
   //     seeing it as a file-without-xattrs, and allowing readers to see
   //     our internal data (e.g. in a MULTI file).
   if (info.ns->iwrite_repo->access_method != ACCESSMETHOD_DIRECT) {
      LOG(LOG_INFO, "marking with RESTART, so open() won't think DIRECT\n");
      info.flags  |= PI_RESTART;
      info.xattrs |= XVT_RESTART;
      SAVE_XATTRS(&info, XVT_RESTART);
   }
   else
      LOG(LOG_INFO, "iwrite_repo.access_method = DIRECT\n");

   POP_USER();
   return 0;
}




// OPEN
//
// We maintain a MarFS_FileHandle, which has info needed by
// read/write/close, etc.
//
// Fuse will never call open() with O_CREAT or O_TRUNC.  In the O_CREAT
// case, it will just call maknod() first.  In the O_TRUNC case, it calls
// truncate.  Either way, these flags are stripped off.  We had a
// conversation about the fact that mknod() doesn't have access to the
// open-flags, whereas create() does, but decided mknod() should check
// RM/WM/RD/WD/TD
//
// No need to check quotas here, that's also done in mknod.
//
// Fuse guarantees that it will always call close on any open files, when a
// user-process exits.  However, if we throw an error (return non-zero)
// *during* the open, I'm assuming the user is not considered to have that
// file open.  Therefore, we re-#define RETURN() to add some custom
// clean-up to the common macros.  (See discussion below.)



// A FileHandle is dynamically allocated in marfs_open().  We want to make
// sure any error-returns will deallocate the file-handle.  In C++, it's
// easy to get an object to do cleanup when it goes out of scope.  In C, we
// need to add some code before every return, to do any cleanup that might
// be needed before returning.
//
// RETURN() is used inside TRY(), which is the basis of all the test-macros
// defined in common.h.  So, we redefine that to add our cleanup checks.
//
#undef RETURN
#define RETURN(VALUE)                             \
   do {                                           \
      LOG(LOG_INFO, "returning %d\n", (VALUE));   \
      free((MarFS_FileHandle*)ffi->fh);           \
      ffi->fh = 0;                                \
      return (VALUE);                             \
   } while(0)



// NOTE: stream_open() assumes the OS is in a pristine state.  marfs_open()
//       currently always allocates a fresh OS (inside the new FileHandle),
//       so that assumption is safe.  stream_close() doesn't wipe
//       everything clean, because we want some of that info (e.g. how much
//       data was written).  If you decide to start reusing FileHandles,
//       you should probably (a) assure they have been flushed/closed, and
//       (b) wipe them clean.  [See marfs_read(), which now performs a
//       distinct S3 request for every call, and reuses the ObjectStream
//       inside the FileHandle.]
int marfs_open (const char*            path,
                struct fuse_file_info* ffi) {

   assert(ffi->fh == 0);
   PUSH_USER();

   // Poke the xattr stuff into some memory for the file (poke the address
   //    of that memory into the fuse open structure so you have access to
   //    it when the file is open)
   //
   //    also poke how to access the objrepo for where/how to write and how to read
   //    also put space for read to attach a structure for object read mgmt
   //
   if (! (ffi->fh = (uint64_t) calloc(1, sizeof(MarFS_FileHandle))))
      return -ENOMEM;

   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   IOBuf*            b    = &fh->os.iob;                /* shorthand */
   PathInfo*         info = &fh->info;                  /* shorthand */
   EXPAND_PATH_INFO(info, path);

   // Check/act on iperms from expanded_path_info_structure
   //   If readonly RM/RD 
   //   If wronly/rdwr/trunk  RM/WM/RD/WD/TD
   //   If append we don’t support that
   //
   // NOTE: FUSE doesn't actually call open with O_TRUNC, but instead calls
   //       truncate(), in that case.  Similarly, we won't get called with
   //       O_CREAT, cause mknod() handles that.
   //
   assert(! (ffi->flags & O_CREAT));
   assert(! (ffi->flags & O_TRUNC));

   if (ffi->flags & (O_RDONLY)) {
      fh->flags |= FH_READING;
      ACCESS(info->post.md_path, R_OK);
      CHECK_PERMS(info->ns->iperms, (R_META | R_DATA));
   }
   else if (ffi->flags & (O_WRONLY)) {
      fh->flags |= FH_WRITING;
      ACCESS(info->post.md_path, W_OK);
      CHECK_PERMS(info->ns->iperms, (R_META | W_META | R_DATA | W_DATA));
   }

   //   if (info->flags & (O_TRUNC)) {
   //      CHECK_PERMS(info->ns->iperms, (T_DATA));
   //   }

   // unsupported operations
   if (ffi->flags & (O_RDWR)) {
      fh->flags |= (FH_READING | FH_WRITING);
      LOG(LOG_INFO, "open(O_RDWR) not implemented\n");
      RETURN(-ENOSYS);          /* for now */
   }
   if (ffi->flags & (O_APPEND)) {
      LOG(LOG_INFO, "open(O_APPEND) not implemented\n");
      RETURN(-ENOSYS);
   }


   STAT_XATTRS(info);

   // If no xattrs, we let user read/write directly into the file.
   // This corresponds to a file that was created in DIRECT repo-mode.
   if (! has_any_xattrs(info, MARFS_ALL_XATTRS)) {
      fh->md_fd = open(info->post.md_path, ffi->flags);
      if (fh->md_fd < 0) {
         fh->md_fd = 0;
         RETURN(-errno);
      }
   }
   // some kinds of reads need to get info from inside the MD-file
   else if ((fh->flags & FH_READING)
            && ((info->post.obj_type == OBJ_MULTI)
                || (info->post.obj_type == OBJ_PACKED))) {
      fh->md_fd = open(info->post.md_path, (O_RDONLY)); // no O_BINARY in Linux.  Not needed.
      if (fh->md_fd < 0) {
         fh->md_fd = 0;
         RETURN(-errno);
      }
   }
   else if (fh->flags & FH_WRITING) {

      // COMMENTED OUT. This obj_type is now set in init_post().  I want to
      // make sure that that works correctly, before deleting this.
      //
      //      // start out assuming write object-type is Uni.  We'll change to
      //      // Multi if writes require a second object.
      //      info->post.obj_type = OBJ_UNI;


      // Don't open MD file, here.  It isn't needed until the object-size
      // exceeds the threshold for Uni
      // (i.e. Namespace.iwrite_repo->chunk_size).  If we open it here,
      // then we unnecessarily slow down writing small files.
      //
      //      fh->md_fd = open(info->post.md_path,(O_WRONLY));  // no O_BINARY in Linux.
      //      if (fh->md_fd < 0) {
      //         fh->md_fd = 0;
      //         RETURN(-errno);
      //      }
   }

   // Configure a private AWSContext, for this request
   AWSContext* ctx = aws_context_clone();
   if (ACCESSMETHOD_IS_S3(info->pre.repo->access_method)) { // (includes S3_EMC)
      s3_set_host_r(info->pre.repo->host, ctx);
      LOG(LOG_INFO, "host   '%s'\n", info->pre.repo->host);

      s3_set_bucket_r(info->pre.bucket, ctx);
      LOG(LOG_INFO, "bucket '%s'\n", info->pre.bucket);
   }

   if (info->pre.repo->access_method == ACCESSMETHOD_S3_EMC) {
      s3_enable_EMC_extensions_r(1, ctx);

      // For now if we're using HTTPS, I'm just assuming that it is without
      // validating the SSL certificate (curl's -k or --insecure flags). If
      // we ever get a validated certificate, we will want to put a flag
      // into the MarFS_Repo struct that says it's validated or not.
      if ( info->pre.repo->ssl ) {
        s3_https_r( 1, ctx );
        s3_https_insecure_r( 1, ctx );
      }
   }

   if (info->pre.repo->access_method == ACCESSMETHOD_SPROXYD) {
      s3_enable_Scality_extensions_r(1, ctx);
      s3_sproxyd_r(1, ctx);

      // For now if we're using HTTPS, I'm just assuming that it is without
      // validating the SSL certificate (curl's -k or --insecure flags). If
      // we ever get a validated certificate, we will want to put a flag
      // into the MarFS_Repo struct that says it's validated or not.
      if ( info->pre.repo->ssl ) {
        s3_https_r( 1, ctx );
        s3_https_insecure_r( 1, ctx );
      }
   }

   // install custom context
   aws_iobuf_context(b, ctx);

   // initialize the URL in the ObjectStream, in our FileHandle
   TRY0(update_url, &fh->os, info);

#if 0
   TRY0(stream_open, &fh->os, ((fh->flags & FH_WRITING) ? OS_PUT : OS_GET));
#else
   // To support seek() [for reads], and allow reading at arbitrary
   // offsets, we are now implementing each call to marfs_read() as an
   // independent GET, with byte-ranges.  Therefore, for reads, we don't
   // open the stream, here.
   if (fh->flags & FH_WRITING)
      TRY0(stream_open, &fh->os, OS_PUT);
#endif


   POP_USER();
   return 0;
}
#undef RETURN
#define RETURN(VALUE) return(VALUE)




int marfs_opendir (const char*            path,
                   struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   if (! (ffi->fh = (uint64_t) calloc(1, sizeof(MarFS_DirHandle))))
      return -ENOMEM;

   MarFS_DirHandle* dh   = (MarFS_DirHandle*)ffi->fh; /* shorthand */

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");

      if (geteuid()) {
         free(dh);
         ffi->fh = 0;
         return -EACCES;
      }

      // root 
      dh->use_it = 1;
      dh->internal.it = namespace_iterator();
      return 0;
   }

   // No need for access check, just try the op
   // Appropriate  opendir call filling in fuse structure
   ///   mode_t mode = ~(fuse_get_context()->umask); /* ??? */
   ///   TRY_GE0(opendir, info.md_path, ffi->flags, mode);
   ///   TRY_GE0(opendir, info.post.md_path);
   TRY_GT0(opendir, info.post.md_path);

   ///   ffi->fh = rc_ssize;          /* open() successfully returned a dirp */
   dh->internal.dirp = (DIR*)rc_ssize;

   POP_USER();
   return 0;
}

// return actual number of bytes read.  0 indicates EOF.
// negative understood to be negative errno.
//
// NOTE: 
// TBD: Don't do object-interaction if file is DIRECT.  See marfs_open().
//
int marfs_read (const char*            path,
                char*                  buf,
                size_t                 size,
                off_t                  offset,
                struct fuse_file_info* ffi) {
   PUSH_USER();

   ///   PathInfo info;
   ///   memset((char*)&info, 0, sizeof(PathInfo));
   ///   EXPAND_PATH_INFO(&info, path);
   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;
   IOBuf*            b    = &os->iob;


   // Check/act on iperms from expanded_path_info_structure, this op requires RM  RD
   CHECK_PERMS(info->ns->iperms, (R_META | R_DATA));

   // No need to call access as we called it in open for read
   // Case
   //   File has no xattr objtype
   //     Just read the bytes from the file and fill in fuse read buffer
   //
   if (! has_any_xattrs(info, MARFS_ALL_XATTRS)
       && (info->ns->iwrite_repo->access_method == ACCESSMETHOD_DIRECT)) {
      LOG(LOG_INFO, "reading DIRECT\n");
      TRY_GE0(read, fh->md_fd, buf, size);
      return rc_ssize;
   }

   //   File is objtype packed or uni
   //      Make sure start and end are within the object
   //           (according to file size and objoffset)
   //      Make sure security is set up for accessing objrepo using table
   //      Read bytes from object server and fill in fuse read buffer
   //   File is objtype multipart
   //     Make sure start and end are within the object
   //           (according to file size and objoffset)
   //     Make sure security is set up for accessing objrepo using table
   //     If this is the first read, 
   //           Malloc space for read obj mgmt. and put address in fuse open table area
   //           read data from metadata file (which is already open and is the handle 
   //           passed in and put in buffer pointed to in fuse open table)
   //   File is striped
   //       We will implement this later perhaps
   //      look up in read obj mgmt. area for which object(s)
   //      for loop objects needed to honor read, read obj data and fill in fuse read buffer

#if 0
   // Old approach, where open() opens stream, and we read contiguously from it
   TRY_GE0(stream_get, os, buf, size);
   LOG(LOG_INFO, "result: '%*s'\n", rc_ssize, buf);

   POP_USER();
   return rc_ssize;

#elif 0
   // new approach, where we do an open/close for every call to read().  We
   // correctly read byte ranges (but don't handle Multi files).
   LOG(LOG_INFO, "offset: %ld, size: %ld\n", offset, size);
   s3_set_byte_range_r(offset, size, b->context);
   TRY0(stream_open, os, OS_GET);

   // because we are reading byte-ranges, we may see '206 Partial Content'
   //
   // TRY_GE0(stream_get, os, buf, size);
   rc_ssize = stream_get(os, buf, size);
   if ((rc_ssize < 0)
       && (os->iob.code != 200)
       && (os->iob.code != 206)) {
      LOG(LOG_ERR, "stream_get returned %ld (%d '%s')\n",
          rc_ssize, os->iob.code, os->iob.result);
      return -1;
   }

   // // handy for debugging small things, but don't want it there always
   // LOG(LOG_INFO, "result: '%*s'\n", rc_ssize, buf);

   TRY0(stream_sync, os);
   TRY0(stream_close, os);

   POP_USER();
   return rc_ssize;

#else
   // Newest approach, where we still do open/close per read(), and we also
   // handle Multi files.  This means that the data that is requested may
   // span multiple objects (we call these internal objects "chunks").  In
   // that case, we will actually do multiple open/close actions per
   // read(), because we must open objects individually.
   //
   // Marfs_write (and pftool) promise that we can compute the object-IDs
   // of the chunk and offset, for the Multi-object that matches a given
   // logical offset, using the following assumptions:
   //
   // (a) every object except possibly the last one will contain
   //     repo.chunk_size of data (which includes the recovery-info at the
   //     end, which we must skip over).
   //
   // (b) Object-IDs are all the same as the object-ID in the "objid"
   //     xattr, but changing the final ".0" to the appropriate
   //     chunk-number.
   //
   // These assumptions mean we can easily compute the IDs of chunk(s) we
   // need, given only the desired data-offset (i.e. the "logical" offset)
   // and the original object-ID.
   //
   // NOTE: even-newer approach: for performance, we're now trying to
   //     restrict the number of stream close/open operations to only those
   //     that are strictly necessary (for example, when ending one Multi
   //     object and starting the next one).  Specifically, we want to
   //     allow multiple calls to read() that are getting contiguous data
   //     to skip doing an open on every call.


   // In the case of "Packed" objects, many user-level files are packed
   // (with recovery-info at the tail of each) into a single physical
   // object.  The "logical offset" of the user's data must then be added
   // to the physical offset of the beginning of the packed object, in
   // order to skip over the objects (and their recovery-info) that
   // preceded the "logical object" within the physical object.
   // Post.obj_offset is only non-zero for Packed files, where it holds the
   // absolute physical byte_offset of the beginning of user's logical
   // data, within the physical object.
   const size_t phy_offset = info->post.obj_offset + offset;

   // The presence of recovery-info at the tail-end of objects means we
   // have to detect the case when fuse attempts to read beyond the end of
   // the last part of user-data in the last object.  (It always tries
   // this, to make us identify EOF.)  The object does have data there,
   // because of the recovery-info at the tail of the final object.  We use
   // the stat-size of the MDFS file to indicate the logical extent of user
   // data, so we can recognize where the legitimate data ends.
   STAT(info);
   const size_t max_extent = info->st.st_size;       // max logical index
   const size_t max_read   = ((offset >= max_extent) // max logical span from offset
                              ? 0
                              : max_extent - offset);

   // portions of each chunk that are used for system-data vs. user-data.
   // NOTE: Post.chunks can be non-0 for Multi or Packed.
   const size_t recovery   = sizeof(RecoveryInfo) +8; // sys bytes, per chunk
   const size_t data1      = (info->pre.chunk_size - recovery); // log bytes, per chunk
   const size_t data       = ((info->post.obj_type == OBJ_PACKED) // tot log bytes in obj(s)
                              ? (info->pre.chunk_size
                                 - (info->post.chunks * recovery))
                              : data1);


   size_t chunk        = phy_offset / data; // only non-zero for Multi
   if (phy_offset == data)
      chunk -= 1; // bizarre case of user reading 0 bytes at tail of object

   size_t chunk_offset = phy_offset - (chunk * data1); // offset in <chunk>
   size_t chunk_remain = data1 - chunk_offset;         // max for this chunk
   size_t total_remain = (max_read < size) ? max_read : size; // for this read()

   char*  buf_ptr      = buf;
   size_t read_size    = ((total_remain < chunk_remain) // read size for this chunk
                          ? total_remain
                          : chunk_remain);

   size_t read_count   = 0;     // amount read during this call


   // discontiguous read could happen if user calls seek()
   if ((  offset != fh->read_status.log_offset)
       && (os->flags & OSF_OPEN)) {

      LOG(LOG_WARNING, "discontiguous read detected: gap %ld-%ld\n",
          fh->read_status.log_offset, offset);

      TRY0(stream_sync, os);
      TRY0(stream_close, os);

      fh->read_status.log_offset = offset;
   }
   else if (! (os->flags & OSF_OPEN))
      fh->read_status.log_offset = offset;



   // Starting at the appropriate chunk and offset to match caller's
   // logical offset in the multi-object data, move through successive
   // chunks, reading contiguous user-data (skipping recovery-info), until
   // we've filled caller's buffer.  <read_size> is the amount to read from
   // this chunk.
   while (read_size) {

      // read as much user-data as we have room for from the current chunk
      LOG(LOG_INFO, "iter: chnk=%ld, off=%ld, loff=%ld, choff=%ld, "
          "rdsz=%ld, chrem=%ld, totrem=%ld\n",
          chunk, offset, fh->read_status.log_offset, chunk_offset,
          read_size, chunk_remain, total_remain);


      if (! (os->flags & OSF_OPEN)) {
         // open-ended byte-range, starting at offset in this chunk
         s3_set_byte_range_r(chunk_offset, -1, b->context);

         // NOTE: stream_open() wipes ObjectStream.written.  We want
         //     this field to track the total amount read across all
         //     chunks, so we save it before calling open, and restore
         //     it after.
         //
         // NOTE: In the case of discontiguous reads (see above), we
         //     probably should allow os->written to be reset.
         //
         size_t written = os->written;
         TRY0(stream_open, os, OS_GET);
         os->written = written;
      }

      // Because we are reading byte-ranges, we may see '206 Partial Content'.
      //
      // It's probably also legit that the server could give us less than we
      // asked for. (i.e. 'Partial Content' might not only mean that we
      // asked for a byte range; it might also mean we didn't even get
      // all of our byte range.)  In that case, instead of moving on to
      // the next object, we should try again on this one.
      //
      // Keep trying until we read all of read_size, or get an error (or get 0).

      size_t sub_read = read_size; // bytes remaining within <read_size>
      do {
         rc_ssize = stream_get(os, buf_ptr, sub_read);
         if ((rc_ssize < 0)
             && (os->iob.code != 200)
             && (os->iob.code != 206)) {
            LOG(LOG_ERR, "stream_get returned < 0: %ld '%s' (%d '%s')\n",
                rc_ssize, strerror(errno), os->iob.code, os->iob.result);
            return -EIO;
         }
         if (rc_ssize < 0) {
            LOG(LOG_ERR, "stream_get returned < 0: %ld '%s' (%d '%s')\n",
                rc_ssize, strerror(errno), os->iob.code, os->iob.result);
            return -EIO;
         }
         // // handy for debugging small reads (but too voluminous for normal use)
         // LOG(LOG_INFO, "result: '%*s'\n", rc_ssize, buf);

         // Q: This might legitimately happen if stream_get() hits EOF?
         // A: No, read_size was adjusted to account for logical EOF
         if (rc_ssize == 0) {
            LOG(LOG_ERR, "request for range %ld-%ld (%ld bytes) returned 0 bytes\n",
                (chunk_offset + read_size - sub_read),
                (chunk_offset + read_size),
                sub_read);
            return -EIO;
         }


         if (rc_ssize != sub_read) {
            LOG(LOG_INFO, "request for range %ld-%ld (%ld bytes) returned only %ld bytes\n",
                (chunk_offset + read_size - sub_read),
                (chunk_offset + read_size),
                sub_read, rc_ssize);
            // return -EIO;
         }

         buf_ptr       += rc_ssize;
         sub_read      -= rc_ssize;

      } while (sub_read);
      LOG(LOG_INFO, "completed read_size = %lu\n", read_size);

      total_remain  -= read_size;
      read_count    += read_size;
      fh->read_status.log_offset += read_size;


      // We got all of read_size.  We're either at the end of this chunk or
      // at the end of the amount requested for this read().
      chunk         += 1;
      chunk_offset   = 0;
      chunk_remain   = data;

      read_size      = ((total_remain < chunk_remain)
                        ? total_remain
                        : chunk_remain);

      // reading another chunk?
      if (total_remain) {

         TRY0(stream_sync, os);
         TRY0(stream_close, os);

         // update the URL in the ObjectStream, in our FileHandle
         info->pre.chunk_no = chunk;
         update_pre(&info->pre);
         update_url(&fh->os, info);
      }
   }

   POP_USER();
   return read_count;


#endif
}


int marfs_readdir (const char*            path,
                   void*                  buf,
                   fuse_fill_dir_t        filler,
                   off_t                  offset,
                   struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  readdir call filling in fuse structure  (fuse does this in chunks)
   MarFS_DirHandle* dh   = (MarFS_DirHandle*)ffi->fh; /* shorthand */

   if (dh->use_it) {
      NSIterator* it = &dh->internal.it;
      while (1) {
         MarFS_Namespace* ns = namespace_next(it);
         if (! ns)
            break;              // EOF
         if (IS_ROOT_NS(ns))
            continue;
         char* dir_name = (char*)ns->mnt_path; // we're not going to modify it
         while (dir_name && *dir_name && *dir_name=='/')
            ++dir_name;
         LOG(LOG_INFO, " ns = %s -> '%s'\n", ns->name, dir_name);
         if (filler(buf, dir_name, NULL, 0))
            break;              // no more room in <buf>
      }
   }
   else {
      /// DIR*           dirp = (DIR*)ffi->fh;
      DIR*           dirp = dh->internal.dirp;
      struct dirent* dent;
   
      while (1) {
         // #if _POSIX_C_SOURCE >= 1 || _XOPEN_SOURCE || _BSD_SOURCE || _SVID_SOURCE || _POSIX_SOURCE
         //      struct dirent* dent_r;       /* for readdir_r() */
         //      TRY0(readdir_r, dirp, dent, &dent_r);
         //      if (! dent_r)
         //         break;                 /* EOF */
         //      if (filler(buf, dent_r->d_name, NULL, 0))
         //         break;                 /* no more room in <buf>*/

         // #else
         errno = 0;
         TRY_GE0(readdir, dirp);
         if (! rc_ssize) {
            if (errno)
               return -errno;      /* error */
            break;                 /* EOF */
         }
         dent = (struct dirent*)rc_ssize;
         if (filler(buf, dent->d_name, NULL, 0))
            break;                 /* no more room in <buf>*/
         // #endif
      
      }
   }

   POP_USER();
   return 0;
}


// It appears that, unlike readlink(2), we shouldn't return the number of
// chars in the path.  Also, unlike readlink(2), we *should* write the
// final '\0' into the caller's buf.
//
// NOTE: We do an extra copy to avoid putting anything into the caller's
//     buf until we know that our result is "safe".  This means:
//
//     (a) link-contents are fully "canonicalized"
//     (b) link-contents refer to something in a MarFS namespace, or ...
//     (c) link-contents refer to something in this user's MDFS.
//
//     It's hard to check (b), because canonicalization for (a) seems to
//     require expand_path_info(), and that puts us into MDFS space.  To
//     convert back to "mount space", we'd need the equivalent of
//     find_namespace_by_md_path(), which is thinkable, but more effort
//     than we need to allow.  Instead, we'll just require (c); that the
//     canonicalized name must be in *this* namespace (which is an easier
//     check).
//
// ON SECOND THOUGHT: It's not our job to see where a link points.
//     readlink of "/marfs/ns/mylink" which refers to "../../test" should
//     just return "../../test" The kernel will then expand
//     "/marfs/ns/../../test" and then use that to do whatever the
//     operation was.  We are not allowing any subversion of the MDFS by
//     responding with the contents of the symlink.

int marfs_readlink (const char* path,
                    char*       buf,
                    size_t      size) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate readlink-like call filling in fuse structure 
   TRY_GE0(readlink, info.post.md_path, buf, size);
   int count = rc_ssize;
   if (count >= size) {
      LOG(LOG_ERR, "no room for '\\0'\n");
      return -ENAMETOOLONG;
   }
   buf[count] = '\0';
   LOG(LOG_INFO, "readlink '%s' -> '%s' = (%d)\n", info.post.md_path, buf, count);

   POP_USER();
   return 0; // return result;
}


// [http://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html]
//
//   "This is the only FUSE function that doesn't have a directly
//    corresponding system call, although close(2) is related. Release is
//    called when FUSE is completely done with a file; at that point, you
//    can free up any temporarily allocated data structures. The IBM
//    document claims that there is exactly one release per open, but I
//    don't know if that is true."
//

int marfs_release (const char*            path,
                   struct fuse_file_info* ffi) {

   PUSH_USER(); // ENTRY();

   // if writing there will be an objid stuffed into a address  in fuse open table
   //       seal that object if needed
   //       free the area holding that objid
   // if  reading, there might be a malloced space for read obj mgmt. in fuse open table
   //       close any objects if needed
   //       free the area holding that stuff
   // close the metadata file handle

   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;

   // close object stream (before closing MDFS file).  For writes, this
   // means telling our readfunc in libaws4c that there won't be any more
   // data, so it should return 0 to curl.  For reads, the writefunc may be
   // waiting for another buffer to fill, so it can be told to terminate.
#if 0
   TRY0(stream_sync, os);
   TRY0(stream_close, os);
#elif 0
   // New approach.  read() handles its own open/read/close
   if (fh->flags & FH_WRITING) {
      TRY0(stream_sync, os);
      TRY0(stream_close, os);
   }
#else
   // Newer approach.  read() handles its own open/read/close write(). In
   // the case of Multi, after the first object, write() doesn't open the
   // next object until there's data to be written to it.
   //
   // NOTE: Even-newer approach: we now allow that maybe read left a stream
   //     open, in an attempt to avoid extra calls to stream_close/reopen.
   if (fh->os.flags & OSF_OPEN) {

      if (fh->flags & FH_WRITING) {
         // add final recovery-info, at the tail of the object
         TRY_GE0(write_recoveryinfo, os, info);
         fh->write_status.sys_writes += rc_ssize; // accumulate non-user-data written
      }

      TRY0(stream_sync, os);
      TRY0(stream_close, os);
   }
#endif

   // close MD file, if it's open
   if (fh->md_fd) {

      // If obj-type is Multi, write the final MultiChunkInfo into the MD file.
      if ((fh->flags & FH_WRITING)
          && (info->post.obj_type == OBJ_MULTI)) {

         TRY0(write_chunkinfo, fh->md_fd, info, os->written - fh->write_status.sys_writes);

         // keep count of amount of real chunk-info written into MD file
         info->post.chunk_info_bytes += sizeof(MultiChunkInfo);

         // update count of objects, in POST
         info->post.chunks = info->pre.chunk_no +1;

         // reset current chunk-number, so xattrs will represent obj 0
         info->pre.chunk_no = 0;
      }

      ///      // QUESTION: does adding an fsync here cause the xattrs to appear
      ///      //     immediately on GPFS files, instead of being delayed?
      ///      //     [NOTE: We also moved SAVE_XATTRS() earlier, for this test.]
      ///      // ANSWER:  No.
      ///      TRY0(fsync, fh->md_fd);

      TRY0(close, fh->md_fd);
      fh->md_fd = 0;
   }

   // truncate length to reflect length of data
   if ((fh->flags & FH_WRITING)
       && has_any_xattrs(info, MARFS_ALL_XATTRS)) {
      TRY0(truncate, info->post.md_path, os->written - fh->write_status.sys_writes);
   }

   // no longer incomplete
   info->flags  &= ~(PI_RESTART);
   info->xattrs &= ~(XVT_RESTART);

   // install xattrs
   if ((info->ns->iwrite_repo->access_method != ACCESSMETHOD_DIRECT)
       && (fh->flags & FH_WRITING)) {
   
      SAVE_XATTRS(info, MARFS_ALL_XATTRS);
   }
   //   // QUESTION: Does sync cause GPFS xattrs to be immediately visible to
   //   //     direct readers?
   //   // ANSWER: No.
   //   sync();


   // reclaim FileHandle allocated in marfs_open()
   free(fh);
   ffi->fh = 0;
   

   POP_USER(); // EXIT();
   return 0;
}


//  [Like release(), this doesn't have a directly corresponding system
//  call.]  This is also the only function I've seen (so far) that gets
//  called with fuse_context->uid of 0, even when running as non-root.
//  This seteuid() will fail.
//
// NOTE: Testing as myself, I notice releasedir() gets called with
//     fuse_context.uid ==0.  Other functions are all called with
//     fuse_context.uid == my_uid.  I’m ignoring push/pop UID in this case,
//     in order to be able to continue debugging.
//
// NOTE: It is possible that we get called with a directory that doesn't
//     exist!  That happens e.g. after 'rm -rf /marfs/jti/mydir' In that
//     case, it seems the kernel calls us with <path> = "-".

int marfs_releasedir (const char*            path,
                      struct fuse_file_info* ffi) {
   LOG(LOG_INFO, "releasedir %s\n", path);
#ifndef LINK_LIBFUSE
   LOG(LOG_INFO, "entry -- skipping push_user(%d)\n", fuse_get_context()->uid);
#endif
   //   PUSH_USER();
   size_t rc = 0;

   // If path == "-", assume we are closing a deleted dir.  (see NOTE)
   if ((path[0] != '-') || (path[1] != 0)) {
      PathInfo info;
      memset((char*)&info, 0, sizeof(PathInfo));
      EXPAND_PATH_INFO(&info, path);

      // Check/act on iperms from expanded_path_info_structure, this op requires RM
      CHECK_PERMS(info.ns->iperms, (R_META));

      // No need for access check, just try the op
      // Appropriate  closedir call filling in fuse structure
      MarFS_DirHandle* dh   = (MarFS_DirHandle*)ffi->fh; /* shorthand */
      if (! dh->use_it) {
         LOG(LOG_INFO, "not root-dir\n");
         DIR* dirp = dh->internal.dirp;
         TRY0(closedir, dirp);
      }
      free(dh);
      ffi->fh = 0;
   }

   LOG(LOG_INFO, "exit -- skipping pop_user()\n");
   //   POP_USER();
   return 0;
}


// *** this may not be needed until we implement user xattrs in the fuse daemon ***
//
// Kernel calls this with key 'security.capability'
//
int marfs_removexattr (const char* path,
                       const char* name) {
   //   LOG(LOG_INFO, "removexattr(%s, %s) not implemented\n", path, name);
   //   return -ENOSYS;

   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -EACCES;
   }

   // *** make sure they aren’t removing a reserved xattr***
   if (! strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize))
      return -EPERM;

   // No need for access check, just try the op
   // Appropriate  removexattr call filling in fuse structure 

#if 0 // for speed, we just ignore this
   TRY0(lremovexattr, info.post.md_path, name);
#endif

   POP_USER();
   return 0;
}


int marfs_rename (const char* path,
                  const char* to) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   PathInfo info2;
   memset((char*)&info2, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info2, to);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "src is_root\n");
      return -EPERM;
   }
   if (IS_ROOT_NS(info2.ns)) {
      LOG(LOG_INFO, "dst is_root\n");
      return -EPERM;
   }

   // No need for access check, just try the op
   // Appropriate  rename call filling in fuse structure 
   TRY0(rename, info.post.md_path, info2.post.md_path);

   POP_USER();
   return 0;
}

// using looked up mdpath, do statxattr and get object name
int marfs_rmdir (const char* path) {
   // (we wont trash directories, we will preserve full paths of all files trashed instead)
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -EPERM;
   }

   // No need for access check, just try the op
   // Appropriate rmdirlike call filling in fuse structure 
   TRY0(rmdir, info.post.md_path);

   POP_USER();
   return 0;
}


int marfs_setxattr (const char* path,
                    const char* name,
                    const char* value,
                    size_t      size,
                    int         flags) {

   //   // *** this may not be needed until we implement user xattrs in the fuse daemon ***
   //   LOG(LOG_INFO, "not implemented\n");
   //   return -ENOSYS;

   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -EPERM;
   }

   // *** make sure they aren’t setting a reserved xattr***
   if ( !strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize) ) {
      LOG(LOG_ERR, "denying reserved setxattr(%s, %s, ...)\n", path, name);
      return -EPERM;
   }

   // No need for access check, just try the op
   // Appropriate  setxattr call filling in fuse structure 
   TRY0(lsetxattr, info.post.md_path, name, value, size, flags);

   POP_USER();
   return 0;
}

// The OS seems to call this from time to time, with <path>=/ (and
// euid==0).  We could walk through all the namespaces, and accumulate
// total usage.  (Maybe we should have a top-level fsinfo path?)  But I
// guess we don't want to allow average users to do this.
int marfs_statfs (const char*      path,
                  struct statvfs*  statbuf) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // NOTE: Until fsinfo is available, we're just ignoring.
   return -ENOSYS;
   //   if (geteuid()) {
   //      LOG(LOG_ERR, "non-root can't stavfs()\n");
   //      return -EACCES;
   //   }
   //   // Open and read from lazy-fsinfo data file updated by batch process fsinfopath 
   //   // Size of file sytem is quota etc.
   //   memset(statbuf, 0, sizeof(struct statvfs));
   //   statbuf->f_bsize = 512;      // matches GPFS
   //   ...


   POP_USER();
   return 0;
}


// NOTE: <target> is given as a full path.  It might or might not be under
//     our fuse mount-point, but even if it is, we should just stuff
//     whatever <target> we get into the symlink.  If it is something under
//     a marfs mount, then marfs_readlink() will be called when the link is
//     followed.

int marfs_symlink (const char* target,
                   const char* linkname) {
   PUSH_USER();

   // <linkname> is given to us as a path under the fuse-mount,
   // in the usual way for fuse-functions.
   LOG(LOG_INFO, "linkname: %s\n", linkname);
   PathInfo lnk_info;
   memset((char*)&lnk_info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&lnk_info, linkname);   // (okay if this file doesn't exist)

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(lnk_info.ns->iperms, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(lnk_info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -EPERM;
   }

   // No need for access check, just try the op
   // Appropriate  symlink call filling in fuse structure 
   TRY0(symlink, target, lnk_info.post.md_path);

   POP_USER();
   return 0;
}

// *** this may not be needed until we implement write in the fuse daemon ***
int marfs_truncate (const char* path,
                    off_t       size) {

   // Check/act on truncate-to-zero only.
   if (size)
      return -EPERM;

   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // Call access syscall to check/act if allowed to truncate for this user 
   ACCESS(info.post.md_path, (W_OK));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -EPERM;
   }

   // If this is not just a normal md, it's the file data
   STAT_XATTRS(&info); // to get xattrs
   if (! has_any_xattrs(&info, MARFS_ALL_XATTRS)) {
      LOG(LOG_INFO, "no xattrs\n");
      TRY0(truncate, info.post.md_path, size);
      return 0;
   }

   // copy metadata to trash, resets original file zero len and no xattr
   TRASH_TRUNCATE(&info, path);

   // (see marfs_mknod() -- empty non-DIRECT file needs *some* marfs xattr,
   // so marfs_open() won't assume it is a DIRECT file.)
   if (info.ns->iwrite_repo->access_method != ACCESSMETHOD_DIRECT) {
      LOG(LOG_INFO, "marking with RESTART, so open() won't think DIRECT\n");
      info.flags  |= PI_RESTART;
      info.xattrs |= XVT_RESTART;
      SAVE_XATTRS(&info, XVT_RESTART);
   }
   else
      LOG(LOG_INFO, "iwrite_repo.access_method = DIRECT\n");

   POP_USER();
   return 0;
}


int marfs_unlink (const char* path) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -EPERM;
   }

   // Call access() syscall to check/act if allowed to unlink for this user 
   //
   // NOTE: if path is a symlink, pointing to another marfs file, access()
   //       will hang forever, because it will require interaction with us,
   //       but we're unavailable until we return from this.  Therefore, in
   //       the case of a symlink, which points to a marfs-file, we skip
   //       the call to access().  How do we know if it's a marfs file?
   //       Well, if it's an absolute path with our same mount-point, or
   //       it's a relative path, then we'll assume it's a marfs file.
   //       This doesn't mean user gets to rename links she shouldn't be
   //       touching: if there is insufficient permission, the op will
   //       fail.
   STAT(&info);
   int call_access = 1;
   if (S_ISLNK(info.st.st_mode)) {
      char target[MARFS_MAX_MD_PATH];

      TRY_GE0(readlink, info.post.md_path, target, MARFS_MAX_MD_PATH);
      if ((rc_ssize >= marfs_config->mnt_top_len)
          && (! strncmp(marfs_config->mnt_top, target, marfs_config->mnt_top_len)))
         call_access = 0;
      else if ((rc_ssize > 0)
               && (target[0] != '/'))
         call_access = 0;
   }
   if (call_access)
      ACCESS(info.post.md_path, (W_OK));

   // rename file with all xattrs into trashdir, preserving objects and paths 
   TRASH_UNLINK(&info, path);

   POP_USER();
   return 0;
}


// deprecated in 2.6
// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int marfs_utime(const char*     path,
                struct utimbuf* buf) {   
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure
   // NOTE: we're assuming expanded path is absolute, so dirfd is ignored
   TRY_GE0(utime, info.post.md_path, buf);

   POP_USER();
   return 0;
}

// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int marfs_utimens(const char*           path,
                  const struct timespec tv[2]) {   
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root\n");
      return -EPERM;
   }

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure
   // NOTE: we're assuming expanded path is absolute, so dirfd is ignored
   TRY_GE0(utimensat, 0, info.post.md_path, tv, AT_SYMLINK_NOFOLLOW);

   POP_USER();
   return 0;
}


int marfs_write(const char*            path,
                const char*            buf,
                size_t                 size,
                off_t                  offset,
                struct fuse_file_info* ffi) {
   PUSH_USER();
   LOG(LOG_INFO, "%s\n", path);
   LOG(LOG_INFO, "offset: %ld, size: %ld\n", offset, size);

   //   PathInfo info;
   //   memset((char*)&info, 0, sizeof(PathInfo));
   //   EXPAND_PATH_INFO(&info, path);
   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;

   // NOTE: It seems that expanding the path-info here is unnecessary.
   //    marfs_open() will already have done this, and if our path isn't
   //    the same as what marfs_open() saw, that's got to be a bug in fuse.
   //
   //   EXPAND_PATH_INFO(info, path);
   

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info->ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // No need to call access as we called it in open for write
   // Make sure security is set up for accessing repo using iwrite_datarepo

   // If file has no xattrs its just a normal use the md file for data,
   //   just do the write and return, don’t bother with all this stuff below
   if (! has_any_xattrs(info, MARFS_ALL_XATTRS)
       && (info->ns->iwrite_repo->access_method == ACCESSMETHOD_DIRECT)) {
      LOG(LOG_INFO, "no xattrs, and DIRECT: writing to file\n");
      TRY_GE0(write, fh->md_fd, buf, size);
      return rc_ssize;
   }

   // If first write, check/act on quota bytes
   // TBD ...

   ///   // If first write, it has to start at offset 0, if not fail
   ///   if ((! os->written) && offset) {
   ///      LOG(LOG_ERR, "first write started at non-zero offset %ld\n", offset);
   ///      errno = EINVAL;
   ///      return -1;
   ///   }

   // If first write, it has to start at offset 0, if not fail
   // If write is not contiguous with previous write, fail
   //
   // NOTE: Marfs recovery-info written into the object is included in
   //     os->written, which keeps track of *all* data that is written to
   //     the object-stream.  The "logical offset" is just the amount of
   //     user-data.  To compute this, we subtract the amount of non-user
   //     data, written by MarFS.  That amount is tracked in
   //     fh->write_status.sys_writes.
   size_t log_offset = (os->written - fh->write_status.sys_writes);
   if ( offset != log_offset) {
      LOG(LOG_ERR, "non-contig write: offset %ld, after %ld (+ %ld)\n",
          offset, log_offset, fh->write_status.sys_writes);
      errno = EINVAL;
      return -1;
   }

   // If first write allocate space for current obj being written put addr
   //     in fuse open table
   // If first write or if new file length will make object bigger than
   //     chunksize seal old ojb get new obj
   // If first write, add objrepo, objid, objtype(unitype), confversion,
   //     datasecurity, chnksz xattrs
   // If “new” obj
   //   If first “new” obj
   //       Write old and new objid to md file set end marker
   //       Change objid xattr to file
   //       Change objtype to multipart
   //   Else
   //       Write new objid to mdfile
   //   Put objid into current obj being written into fuse open table place
   //
   // Write bytes to object
   // Trunc file to current last byte, and set end marker


   // It's possible that we finished and closed the first object, without
   // knowing that it was going to be Multi, until now.  In that case, the
   // OS is closed.  (We also might have closed the previous Multi object
   // without knowing there would be more written.  This works for that,
   // too.)
   if (os->flags & OSF_CLOSED) {
      info->post.obj_type = OBJ_MULTI;
      info->pre.chunk_no += 1;

      // update the URL in the ObjectStream, in our FileHandle
      TRY0(update_pre, &info->pre);
      TRY0(update_url, os, info);

      TRY0(stream_open, os, OS_PUT);
   }

   // Span across objects for "Multi" format.  Repo.chunk_size is the
   // maximum size of an individual object written by MarFS.  Check whether
   // this write will put us over (leaving room at the end for
   // recovery-info).  If so, then write as much data into this object as
   // can fit (minus size of recovery-info), write the recovery-info into
   // the tail, close the object, open a new one, and resume writing there.
   //
   // For each chunk of a Multi object, we also write a corresponding copy
   // of the object-ID into the MD file.  The MD file is not opened for us
   // in marfs_open(), because it wasn't known until now that we'd need it.
   size_t       write_size = size;
   const size_t recovery   = sizeof(RecoveryInfo) +8; // written in tail of object
   size_t       chunk_end  = ((info->pre.chunk_no +1) * (info->pre.chunk_size));
   char*        buf_ptr    = (char*)buf;

   while ((os->written + write_size + recovery) >= chunk_end) {

      // write <fill> more bytes, to fill this object
      ssize_t fill   = chunk_end - (os->written + recovery);
      size_t  remain = write_size - fill; // remaining after <fill>

      LOG(LOG_INFO, "iterating: written=%ld, fill=%ld, rec=%ld, remain=%ld, chnksz=%ld\n",
          os->written, fill, recovery, remain, info->pre.chunk_size);

      // possible silly config: (recovery > chunk_size)
      // This config is now ruled out by validate_config() ?
      if (fill <= 0) {
         LOG(LOG_ERR, "fill %ld < 0  (written %ld, rec: %ld, wr: %ld)\n",
             fill, os->written, recovery, write_size);
         return -EIO;
      }


      TRY_GE0(stream_put, os, buf_ptr, fill);
      buf_ptr += fill;

      TRY_GE0(write_recoveryinfo, os, info);
      fh->write_status.sys_writes += rc_ssize; // track non-user-data written

      // close the object
      LOG(LOG_INFO, "closing chunk: %ld\n", info->pre.chunk_no);
      TRY0(stream_sync, os);
      TRY0(stream_close, os);

      // if we haven't already opened the MD file, do it now.
      if (! fh->md_fd) {
         fh->md_fd = open(info->post.md_path, (O_WRONLY));// no O_BINARY in Linux.  Not needed.
         if (fh->md_fd < 0) {
            LOG(LOG_ERR, "open %s failed (%s)\n", info->post.md_path, strerror(errno));
            fh->md_fd = 0;
            RETURN(-errno);
         }
      }

      // MD file gets per-chunk information
      TRY0(write_chunkinfo, fh->md_fd, info, os->written - fh->write_status.sys_writes);

      // keep count of amount of real chunk-info written into MD file
      info->post.chunk_info_bytes += sizeof(MultiChunkInfo);


      // if we still have more data to write, prepare for next iteration
      if (remain) {

         // MarFS file-type is definitely "Multi", now
         info->post.obj_type = OBJ_MULTI;

         // update chunk-number, and generate the new obj-id
         info->pre.chunk_no += 1;
      
         // update the URL in the ObjectStream, in our FileHandle
         TRY0(update_pre, &info->pre);
         TRY0(update_url, os, info);

         // open next chunk
         //
         // NOTE: stream_open() wipes ObjectStream.written.  We want this
         //     field to track the total amount written across all chunks,
         //     so we save it before calling open, and restore it after.
         size_t written = os->written;
         TRY0(stream_open, os, OS_PUT);
         os->written = written;


         chunk_end = ((info->pre.chunk_no +1) * (info->pre.chunk_size));
      }

      // compute limits of new chunk
      write_size = remain;
   }
   LOG(LOG_INFO, "done iterating\n");

   // write more data into object. This amount doesn't finish out any
   // object, so don't write chunk-info to MD file.
   if (write_size)
      TRY_GE0(stream_put, os, buf_ptr, write_size);


   POP_USER();
   return size;
}




// ---------------------------------------------------------------------------
// unimplemented routines, for now
// ---------------------------------------------------------------------------

#if 0

int marfs_bmap(const char* path,
               size_t      blocksize,
               uint64_t*   idx) {
   // don’t support  its is for block mapping
   return 0;
}


// UNDER CONSTRUCTION
//
// We don’t need this as if its not present, fuse will call mknod()+open().
// Don’t implement.
//
// NOTE: If create() is not defined, fuse calls mknod() then open().
//       However, mknod() doesn't know whether the file is being opened
//       with TRUNC or not.  Therefore, it can't formulate the correct call
//       to CHECK_PERMS().  Therefore, it has to check all perms that
//       *might* be checked in open() [so as to avoid the case where mknod
//       succeeds but open() fails].
//
//       However, if create() is defined, fuse calls that to do the create
//       and open in one step.  This allows us to check the appropriate set
//       of meta-perms.

int marfs_create(const char*            path,
                 mode_t                 mode,
                 struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure
   //   If readonly RM  RD 
   //   If wronly/rdwr/trunk  RMWMRDWD
   //   If append we don’t support that
   if (info.flags & (O_RDONLY)) {
      ACCESS(info.post.md_path, W_OK);
      CHECK_PERMS(info.ns->iperms, (R_META | R_DATA));
   }
   else if (info.flags & (O_WRONLY)) {
      ACCESS(info.post.md_path, W_OK);
      CHECK_PERMS(info.ns->iperms, (R_META | W_META | | R_DATA | W_DATA));
   }

   if (info.flags & (O_APPEND | O_RDWR)) {
      return -EPERM;
   }
   if (info.flags & (O_APPEND | O_TRUNC)) { /* can this happen, with create()? */
      CHECK_PERMS(info.ns->iperms, (T_DATA));
   }


   // Check/act on iperms from expanded_path_info_structure, this op
   // requires RMWM
   //
   // NOTE: We assure that open(), if called after us, can't discover that
   //       user lacks sufficient access.  However, because we don't know
   //       what the open call might be, we may be imposing
   //       more-restrictive constraints than necessary.
   //
   //   CHECK_PERMS(info.ns->iperms, (R_META | W_META));
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA | T_DATA));

   // Check/act on quota num names
   // No need for access check, just try the op
   // Appropriate mknod-like/open-create-like call filling in fuse structure
   TRY0(mknod, info.post.md_path, mode, rdev);

   POP_USER();
   return 0;
}


// obsolete, in fuse 2.6
int marfs_fallocate(const char*            path,
                    int                    mode,
                    off_t                  offset,
                    off_t                  length,
                    struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // Check space quota
   //    If  we get here just return ok  this is just a check to see if you can write to the fs
   POP_USER();
   return 0;
}


// this is really fstat() ??
//
// Instead of using the <fd> (which is not yet implemented in our
// FileHandle), I'm calling fstat on the file itself.  Good enough?
int marfs_fgetattr(const char*            path,
                   struct stat*           st,
                   struct fuse_file_info* ffi) {
   PUSH_USER();

   // don’t need path info    (this is for a file that is open, so everything is resolved)
   // don’t need to check on IPERMS
   // No need for access check, just try the op
   // appropriate fgetattr/fstat call filling in fuse structure (dont mess with xattrs)
   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   PathInfo*         info = &fh->info;                  /* shorthand */

   TRY0(lstat, info->post.md_path, st);

   POP_USER();
   return 0;
}


// Fuse "flush" is not the same as "fflush()".  Fuse flush is called as
// part of fuse close, and shouldn't return until all I/O is complete on
// the file-handle, such that no further I/O errors are possible.  In other
// words, this is our last chance to return errors to the user.
//
// Maybe flush() is also the best place to assure that final recovery-blobs
// are written into objects, and pending object-reads are cut short, etc,
// ... instead of doing that in close.
//
// TBD: Don't do object-interaction if file is DIRECT.  See marfs_open().
//
// NOTE: It also appears that fuse calls stat immediately after flush.
//       (Hard to be sure, because fuse calls stat all the time.)  I'd
//       guess we shouldn't return until we are sure that stat will see the
//       final state of the file.  Probably also applies to xattrs (?)
//       But hanging here seems to cause fuse to hang.
//
//       BECAUSE OF THIS, we took a simpler route: move all the
//       synchronization into marfs_release(), and don't implement fuse
//       flush.  This seems to have the desired effect of getting fuse to
//       do all the sync at close-time.

int marfs_flush (const char*            path,
                 struct fuse_file_info* ffi) {

   //   // I don’t think we will have dirty data that we can control
   //   // I guess we could call flush on the filehandle  that is being written
   //   // But the only data we will write is multi-part objects, 
   //   // All other data would be to some object interface
   //
   //   LOG(LOG_INFO, "NOP for %s", path);

   PUSH_USER();

   ///   PathInfo info;
   ///   memset((char*)&info, 0, sizeof(PathInfo));
   ///   EXPAND_PATH_INFO(&info, path);
   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   // PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;

   //   // shouldn't do this for DIRECT files!  See marfs_open().
   //   LOG(LOG_INFO, "synchronizing object stream %s\n", path);
   //   TRY0(stream_sync, os);

   POP_USER();
   return 0;
}


// new in 2.6, yet deprecated?
// combines opendir(), readdir(), closedir() into one call.
int  marfs_getdir(const char *path, fuse_dirh_t , fuse_dirfil_t) {
   LOG(LOG_ERR, "getdir(%s, ...) not implemented\n", path);
   return -ENOSYS;
}


int marfs_flock(const char*            path,
                struct fuse_file_info* ffi,
                int                    op) {
   // don’t implement or throw error
   return 0;
}

int marfs_link (const char* path,
                const char* to) {
   // for now, I think we should not allow link, its pretty complicated to do
   LOG(LOG_INFO, "link(%s, ...) not implemented\n", path);
   return -ENOSYS;
}


int marfs_lock(const char*            path,
               struct fuse_file_info* ffi,
               int                    cmd,
               struct flock*          locks) {
   // don’t support it, either don’t implement or throw error
   LOG(LOG_INFO, "lock(%s, ...) not implemented\n", path);
   return -ENOSYS;
}

int marfs_poll(const char*             path,
               struct fuse_file_info*  ffi,
               struct fuse_pollhandle* ph,
               unsigned*               reventsp) {
   // either don’t implement or just return 0;
   LOG(LOG_INFO, "poll(%s, ...) stubbed out.  Just returns 0\n", path);
   return 0;
}


#endif


