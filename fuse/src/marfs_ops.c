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
#include "marfs_ops.h"

/*
@@@-HTTPS:
This was added on 24-Jul-2015 because we need this for the RepoFlags type.
However, it was also needed for other uses of types in marfs_base.h before
this date. It happened that it is included in common.h. Rather than rely
on that fact, because that could change, it is best practice to explicitly
include the files on which a code unit depends.
*/

#include "marfs_base.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <limits.h>             // ULONG_MAX, etc
#include <attr/xattr.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <utime.h>              /* for deprecated marfs_utime() */
#include <stdio.h>



// ---------------------------------------------------------------------------
// Fuse/pftool support-routines in alpha order (so you can actually find them)
// Unimplmented functions are gathered at the bottom
// ---------------------------------------------------------------------------


int marfs_access (const char* path,
                  int         mask) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, (R_META));

   // No need for access check, just try the op
   TRY0( MD_PATH_OP(access, info.ns, info.post.md_path, mask) );
 
   EXIT();
   return 0;
}


// Called from pftool.
//
// This is an alternative to access() that doesn't follow links.
// faccessat() requires a directory-fd, if the path is relative, but, for
// now, these will always be absolute paths.  Someday, maybe it will make
// sense to consider providing or generating an fd, for relative paths.
//
int marfs_faccessat (const char* path,
                     int         mask,
                     int         flags) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, (R_META));

   // No need for access check, just try the op
   TRY0( MD_PATH_OP(faccessat, info.ns, -1, info.post.md_path, mask, flags) );
 
   EXIT();
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
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   if (mode & S_ISUID) {
      LOG(LOG_ERR, "attempt to change setuid bits, on path '%s' (mode: %x)\n",
          path, mode);
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // WARNING: No lchmod() on rrz.
   //          chmod() always follows links.
   TRY0( MD_PATH_OP(chmod, info.ns, info.post.md_path, mode) );

   EXIT();
   return 0;
}

int marfs_chown (const char* path,
                 uid_t       uid,
                 gid_t       gid) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // No need for access check, just try the op
   TRY0( MD_PATH_OP(lchown, info.ns, info.post.md_path, uid, gid) );

   EXIT();
   return 0;
}

// --- Looking for "marfs_close()"?  It's a combination of "marfs_flush()".
//     and "marfs_release()"

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
//
// UPDATE: FUSE calls release() asynchronously, and ignores the return values
//         from the call. This leaves us vulnerable to silent data loss if the
//         call to release() fails. We need to take all the steps which may
//         fail here, in marfs_flush(), so we can return an error to the user.
//         These steps are:
//         - writing recovery info
//         - the final close of the object stream
//         - writing the final chunkinfo (for MULTI files)
//         - writing the POST xattr
//         - truncating the metadata file
//         - removing the restart xattr
//         However, this isn't as simple as moving the code from release() to
//         flush(). FUSE may call flush() multiple times for a given file,
//         once each time a reference to that file is closed (this happens
//         in the case of dup'd fds, ie. when redirecting output from the shell.
//
int marfs_flush (const char*        path,
                 MarFS_FileHandle*  fh) {
   ENTRY();

   //   // I don’t think we will have dirty data that we can control
   //   // I guess we could call flush on the filehandle  that is being written
   //   // But the only data we will write is multi-part objects,
   //   // All other data would be to some object interface
   //
   //   LOG(LOG_INFO, "NOP for %s", path);

   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;

   // It is now possible that we had never opened the stream, this
   // happens in the case of attempting to overwrite a file for which
   // the user does not have write permission. In this case we simply
   // skip all the operations below and return.
   if( fh->flags & FH_WRITING && !(os->flags & OSF_OPEN) ) {
      LOG(LOG_INFO, "releasing unopened stream.\n");
      EXIT();
      return 0;
   }

   // close object stream (before closing MDFS file).  For writes, this
   // means telling our readfunc in libaws4c that there won't be any more
   // data, so it should return 0 to curl.  For reads, the writefunc may be
   // waiting for another buffer to fill, so it can be told to terminate.

   // Newer approach.  read() handles its own open/read/close write(). In
   // the case of Multi, after the first object, write() doesn't open the
   // next object until there's data to be written to it.
   //
   // NOTE: Even-newer approach: we now allow that maybe read left a stream
   //     open, in an attempt to avoid extra calls to stream_close/reopen.
   if (fh->os.flags & OSF_OPEN) {

      if (fh->flags & FH_WRITING) {
         if (! (fh->os.flags & (OSF_ERRORS | OSF_ABORT))) {

            // add final recovery-info, at the tail of the object
            TRY_GE0( write_recoveryinfo(os, info, fh) );
         }
      }
      else {

         // release any pending read threads
         terminate_all_readers(fh);
      }

      // we will not close the stream for packed files
      if( !(fh->flags & FH_PACKED) ) {
         close_data(fh, 0, 1);
      }
   }

   // free aws4c resources if the file is not packed
   if( !(fh->flags & FH_PACKED) ) {
      aws_iobuf_reset_hard(&os->iob);
   }

   if (! (fh->os.flags & (OSF_ERRORS | OSF_ABORT))) {

      // If obj-type is Multi, write the final MultiChunkInfo into the
      // MD file.  (unless pftool is writting N:1, in which case it will
      // do that in post_process)
      if ((fh->flags & FH_WRITING)
          && (info->post.obj_type == OBJ_MULTI)) {

         if (info->pre.obj_type != OBJ_Nto1) {
            TRY0( write_chunkinfo(fh,
                                  // fh->open_offset,
                                  (os->written - fh->write_status.sys_writes),
                                  0) );
         }

         // keep count of amount of real chunk-info written into MD file
         info->post.chunk_info_bytes += sizeof(MultiChunkInfo);

         // update count of objects, in POST
         info->post.chunks = info->pre.chunk_no +1;

         // reset current chunk-number, so xattrs will represent obj 0
         info->pre.chunk_no = 0;
      }
   }

   // new lock controls access to read() for multi-threaded nfsd
   if (os->flags & OSF_RLOCK_INIT)
      SEM_DESTROY(&os->read_lock);

   // close MD file, if it's open
   if (is_open_md(fh)) {
      LOG(LOG_INFO, "closing MD\n");
      close_md(fh);
   }

   if (fh->os.flags & OSF_ERRORS) {
      EXIT();
      // return 0;       /* the "close" was successful */
      // return -1;      /* "close" was successful, but need to report errs */
      return 0; // errs should be reported at EOF by marfs_write(), etc ?
   }
   else if (fh->os.flags & OSF_ABORT) {
      EXIT();
      return 0;       /* the "close" was successful */
   }

   // truncate length to reflect length of data
   if ((fh->flags & FH_WRITING)
       && !(fh->flags & FH_Nto1_WRITES)
       && has_any_xattrs(info, MARFS_ALL_XATTRS)) {

      off_t size = os->written - fh->write_status.sys_writes;
      TRY0( MD_PATH_OP(truncate, info->ns, info->post.md_path, size) );
   }


   // Note whether RESTART is preserving a more-restrictive mode
   mode_t  final_mode; // intended final mode?
   int     install_final_mode = 0;
   if (has_any_xattrs(info, XVT_RESTART)
       && (info->restart.flags & RESTART_MODE_VALID)) {

      final_mode = info->restart.mode;
      install_final_mode = 1;
   }

   // no longer incomplete unless this is a packed file
   if( !(fh->flags & FH_PACKED) ) {
      info->xattrs &= ~(XVT_RESTART);
   }

   // update xattrs (unless writing N:1), while we still can
   if ((info->pre.repo->access_method != ACCESSMETHOD_DIRECT)
       && (fh->flags & FH_WRITING)
       && !(fh->flags & FH_Nto1_WRITES)) {

      SAVE_XATTRS(info, MARFS_ALL_XATTRS);

      // install final access-mode, if needed. (We might have added our own
      // more-permissive access-mode-bits in open(), in order to allow
      // manipulating xattrs while the file was open.  If so, we preserved
      // the desired final mode in RESTART.)
      if (install_final_mode) {
         TRY0( MD_PATH_OP(chmod, info->ns,
                          info->post.md_path, info->restart.mode) );
      }
   }

   EXIT();
   return 0;
}


int marfs_fsync (const char*            path,
                 int                    isdatasync,
                 MarFS_FileHandle*      fh) {
   ENTRY();
   // I don’t know if we do anything here, I don’t think so, we will be in
   // sync at the end of each thread end

   // [jti:] in the case of SEMI_DIRECT, we could fsync the storage

   LOG(LOG_INFO, "NOP for %s", path);
   EXIT();
   return 0; // Just return
}


int marfs_fsyncdir (const char*            path,
                    int                    isdatasync,
                    MarFS_DirHandle*       dh) {
   ENTRY();
   // don’t think there is anything to do here, we wont have dirty data
   // unless its trash

   // [jti:] in the case of SEMI_DIRECT, we could fsync the storage

   LOG(LOG_INFO, "NOP for %s", path);
   EXIT();
   return 0; // just return
}


// I read that FUSE will never call open() with O_TRUNC, but will instead
// call truncate first, then open.  However, a user might still call
// truncate() or ftruncate() explicitly.  For these cases, I guess we
// assume the file is already open, and the filehandle is good.
//
// UPDATE: Fuse uses open/ftruncate in the following cases:
//    'truncate -s 0 /marfs/file'          --> open() / ftruncate() / close()
//    'echo test > /marfs/existing_file',  --> open() / ftruncate()
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
                    MarFS_FileHandle*      fh) {
   ENTRY();

   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;
   // IOBuf*            b    = &fh->os.iob;

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info->ns, (R_META | W_META | R_DATA | T_DATA));

   // POSIX ftruncate returns EBADF or EINVAL, if fd not opened for writing.
   if (! (fh->flags & FH_WRITING)) {
      LOG(LOG_ERR, "was not opened for writing\n");
      errno = EINVAL;
      return -1;
   }

   // On non-DIRECT repos, we only allow truncate-to-zero.
   if (length) {
      errno = EPERM;
      return -1;
   }

   // object-stream is still open to the old object (if we were opened for
   // writing).  Close that in such a way that the server will not persist
   // the PUT.  Do this before any MD access checks, so that we will abort
   // the stream even if something goes wrong doing the MD access.
   if(os->flags & OSF_OPEN) {
      TRY0( close_data(fh, 1, 0) );
   }

   // Call access() syscall to check/act if allowed to truncate for this user
   ACCESS(info->ns, info->post.md_path, (W_OK));

   TRY0( open_md(fh, 1) );

   // stat_xattrs, or look up info stuffed into memory pointed at in fuse
   // open table if this is not just a normal [object-storage case?], use
   // the md for file data
   STAT_XATTRS(info);
   if (! has_any_xattrs(info, MARFS_ALL_XATTRS)) {
      LOG(LOG_INFO, "no xattrs.  Treating as DIRECT.\n");
      TRY0( MD_FILE_OP(ftruncate, fh, length) );
      return 0;
   }


   //***** this may or may not work, may need a trash_truncate() that uses
   //***** ftruncate since the file is already open (may need to modify the
   //***** trash_truncate to use trunc or ftrunc depending on if file is
   //***** open or not

   // copy metadata to trash, resets original file zero len and no xattr
   // updates info->pre.objid
   TRASH_TRUNCATE(info, path);

   // metadata filehandle is still open to the current MD file.
   // That's okay, but we need to ftruncate it, as well.
   TRY0( open_md(fh, 1) );
   MD_FILE_OP(ftruncate, fh, length); // (length == 0)

   // open a stream to the new object.  We assume that the libaws4c context
   // initializations done in marfs_open are still valid.  trash_truncate()
   // will already have updated our URL.  Assume data_remain is still valid
   // (i.e. there was no prior request that completed).  If we exceed the
   // logical chunk boundary, our request should also include the size of
   // the recovery-info, to be written at the tail.
   //
   // UPDATE: With lazy-opens, it is no longer necessary to open a new stream
   //         here. If one needs to be opened, it will be opened on the next
   //         call to marfs_write().

   // (see marfs_mknod() -- empty non-DIRECT file needs *some* marfs xattr,
   // so marfs_open() won't assume it is a DIRECT file.)
   if (info->pre.repo->access_method != ACCESSMETHOD_DIRECT) {
      LOG(LOG_INFO, "marking with RESTART, so open() won't think DIRECT\n");
      info->xattrs |= XVT_RESTART;
      SAVE_XATTRS(info, XVT_RESTART); // already done in open() ?
   }
   else
      LOG(LOG_INFO, "iwrite_repo.access_method = DIRECT\n");

   EXIT();
   return 0;
}


// This is "stat()"
int marfs_getattr (const char*  path,
                   struct stat* stp) {
   ENTRY();
   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);
   LOG(LOG_INFO, "expanded    %s -> %s\n", path, info.post.md_path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, R_META);

   // The "root" namespace is artificial.  If it is configured to refer to
   // a real MDFS path, then stat'ing the root-NS will look at that path.
   // Otherwise, we gin up fake data such that the root-dir appears to be
   // owned by root, with X-only access to users.  For blocksizes, etc, we
   // match what statvfs("/gpfs/gpfs_test/") returned, on the ODSU
   // test-bed.
   //
   // In either case, it may be confusing to non-root users if the
   // indicated accessibility of the directory differs from what is allowed
   // in marfs_opendir().
   if (IS_ROOT_NS(info.ns)) {

      LOG(LOG_INFO, "is_root_ns\n");
      if (! stat_regular(&info)) {
         *stp = info.st;        // root md_path exists.  Use that.
      }
      else {

         // everything defaults to zero
         memset(stp, 0, sizeof(struct stat));

         // match the results for GPFS-mount on the test-bed

         // stp->st_size    = 16384; // ccstar test-bed
         // stp->st_blksize = 16384;
         stp->st_size    = 262144;  // ODSU test-bed
         stp->st_blksize = 262144;

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
      }


   }
   else  {
      // (Not root-NS.)  No need for access check, just try the op
      // appropriate statlike call filling in fuse structure (dont mess with xattrs here etc.)
      //
      // NOTE: kernel should already have called readlink, to get past any
      //     symlinks.  lstat here is just to be safe.
      LOG(LOG_INFO, "lstat %s\n", info.post.md_path);
      TRY0( MD_PATH_OP(lstat, info.ns, info.post.md_path, stp) );
   }

   // mask out setuid bits.  Those are belong to us.  (see marfs_chmod())
   stp->st_mode &= ~(S_ISUID);

   EXIT();
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
   ENTRY();
   LOG(LOG_INFO, "key         %s\n", name);
   //   LOG(LOG_INFO, "not implemented  (path %s, key %s)\n", path, name);
   //   errno = ENOSYS;
   //   return -1;

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, (R_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = ENOATTR;          // fingers in ears, la-la-la
      return -1;
   }

   // *** make sure they aren’t getting a reserved xattr***
   if ( !strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize) ) {
      LOG(LOG_ERR, "denying reserved getxattr(%s, %s, ...)\n", path, name);
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // Appropriate  getxattr call filling in fuse structure
   //
   // NOTE: GPFS returns -1, errno==ENODATA, for
   //     lgetxattr("system.posix_acl_access",path,0,0).  The kernel calls
   //     us with this, for 'ls -l /marfs/jti/blah'.

   TRY_GE0( MD_PATH_OP(lgetxattr, info.ns,
                       info.post.md_path, name, (void*)value, size) );
   ssize_t result = rc_ssize;

   EXIT();
   return result;
}


int marfs_ioctl(const char*            path,
                int                    cmd,
                void*                  arg,
                MarFS_FileHandle*      fh,
                unsigned int           flags,
                void*                  data) {
   ENTRY();
   // if we need an ioctl for something or other
   // *** we need a way for daemon to read up new config file without stopping

   LOG(LOG_INFO, "NOP for %s", path);
   EXIT();
   return 0;
}





// NOTE: Even though we remove reserved xattrs, user can call with empty
//       buffer and receive back length of xattr names.  Then, when we
//       remove reserved xattrs (in a subsequent call), user will see a
//       different list length than the previous call lead him to expect.

int marfs_listxattr (const char* path,
                     char*       list,
                     size_t      size) {
   ENTRY();
   //   LOG(LOG_INFO, "listxattr(%s, ...) not implemented\n", path);
   //   errno = ENOSYS;
   //   return -1;

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      size = 0;                 // amount needed for 
      return 0;
   }

   // No need for access check, just try the op
   // Appropriate  listxattr call
   // NOTE: If caller passes <list>=0, we'll be fine.
   TRY_GE0( MD_PATH_OP(llistxattr, info.ns, info.post.md_path, list, size) );

   // In the case where list==0, we return the size of the buffer that
   // caller would need, in order to receive all our xattr data.
   if (! list) {
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
   // we should just do the copy, instead.  On third thought, the copy
   // approach might save some shufling work, but it also requires a
   // malloc, so we stick with using caller's buffer.
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
            errno = EINVAL;
            return -1;
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

   EXIT();
   return result_size;
}


int marfs_mkdir (const char* path,
                 mode_t      mode) {
   ENTRY();

   PathInfo  info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // Check/act on quota num files

   // No need for access check, just try the op
   TRY0( MD_D_PATH_OP(mkdir, info.ns, info.post.md_path, mode) );

   EXIT();
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
// NOTE: This is not actually opening the file, so we allow mknod() to
//    possibly install a mode that is not readable or writeable, which
//    would affect the ability to install xattrs.  We shouldn't do the
//    trick of stashing the intended mode on the RESTART xattr, to preserve
//    writability of xattrs on the file.  that will be done, if needed, at
//    open-time.
int marfs_mknod (const char* path,
                 mode_t      mode,
                 dev_t       rdev) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op
   // requires RMWM NOTE: We assure that open(), if called after us, can't
   // discover that user lacks sufficient access.  However, because we
   // don't know what the open call might be, we may be imposing
   // more-restrictive constraints than necessary.
   //
   //   CHECK_PERMS(info.ns, (R_META | W_META));
   //   CHECK_PERMS(info.ns, (R_META | W_META | W_DATA | T_DATA));
   CHECK_PERMS(info.ns, (R_META | W_META | R_DATA | W_DATA | T_DATA));

   // Check/act on quotas of total-space and total-num-names
   // 0=OK, 1=exceeded, -1=error
   TRY_GE0( check_quotas(&info) );
   if (rc_ssize) {
      errno = EDQUOT;
      return -1;
   }

   // if mode would prevent installing RESTART xattr,
   // then create with a more-lenient mode, at first.
   // Save the bits to be XORed in the final state.
   const mode_t user_rw   = (S_IRUSR | S_IWUSR);
   mode_t       orig_mode = mode; // save intended final mode
   mode_t       missing   = (mode & user_rw) ^ user_rw;
   if (missing) {
      LOG(LOG_INFO, "mode: (octal) 0%o, missing: 0%o\n", mode, missing);
      mode |= missing;          // more-permissive mode
   }

   // No need for access check, just try the op
   // Appropriate mknod-like/open-create-like call filling in fuse structure
   TRY0( MD_PATH_OP(mknod, info.ns, info.post.md_path, mode, rdev) );
   LOG(LOG_INFO, "mode: (octal) 0%o\n", mode);

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
   //
   // NOTE: If needed, marfs_open() also installs readable-writable
   //     access-mode bits, in order to allow xattrs to be manipulated.  In
   //     that case, it will preserve the original mode-bits into the
   //     RESTART xattr.  (NOTE: As long as there is a RESTART xattr, all
   //     access to the file is prohibited by libmarfs.)
   //
   STAT_XATTRS(&info);
   if (info.pre.repo->access_method != ACCESSMETHOD_DIRECT) {
      LOG(LOG_INFO, "marking with RESTART, so open() won't think DIRECT\n");

      info.xattrs |= XVT_RESTART;
      if (missing) {
         info.restart.mode   = orig_mode; // desired final mode
         info.restart.flags |= RESTART_MODE_VALID;
      }
      SAVE_XATTRS(&info, XVT_RESTART);
   }
   else
      LOG(LOG_INFO, "iwrite_repo.access_method = DIRECT\n");


   EXIT();
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
// NOTE: stream_open() assumes the OS is in a pristine state.  marfs_open()
//       currently always allocates a fresh OS (inside the new FileHandle),
//       so that assumption is safe.  stream_close() doesn't wipe
//       everything clean, because we want some of that info (e.g. how much
//       data was written).  If you decide to start reusing FileHandles,
//       you should probably (a) assure they have been flushed/closed, and
//       (b) wipe them clean.  [See marfs_read(), which sometimes reuses
//       the ObjectStream inside the FileHandle.]

// NOTE: We take an additional set of "stream" flags, which are
//       ultimately passed to stream_open().  These allow the underlying
//       stream to be given different properties, for the needs of fuse
//       versus pftool.  Currently, this is only used for setting CTE on
//       write streams used by fuse.
//
//       For example, Fuse write doesn't know how big the file will be, so
//       it can't tell the server (in the PUT header) how many bytes are
//       going to be written, so it must write with
//       chunked-transfer-encoding, which is somewhat less effecient,
//       especially at the server.  Thus, fuse would use stream_flags ==
//       OSOF_CTE.
//
//       On the other hand, pftool *does* know how big the file it's
//       writing is going to be.  Therefore, pftool could supply
//       stream_flags=0.  The "content-length" header will be computed in
//       stream_write as either (a) the size of the iobuf contents, or (b)
//       the size of the file being copied.  Either way, pftool can't do
//       another write to the same stream (without closing and re-opening),
//       because the content-length header will already have said how much
//       data is coming.
//
// UPDATE: Instead of passing a flag to stream_open() to [not] set CTE, and
//       then another argument to install the content-length, stream_open
//       now just takes a content-length argument.  If zero, it implies
//       writing chunked transfer-encoding for an unknown length.  If
//       non-zero, it implies installing that specific length, and writing
//       non-CTE.


int marfs_open(const char*         path,
               MarFS_FileHandle*   fh,
               int                 flags,
               curl_off_t          content_length) { // use 0 for unknown
   ENTRY();
   LOG(LOG_INFO, "flags=(oct)%02o, content-length: %ld\n", flags, content_length);

   // Poke the xattr stuff into some memory for the file (poke the address
   //    of that memory into the fuse open structure so you have access to
   //    it when the file is open)
   //
   //    also poke how to access the objrepo for where/how to write and how to read
   //    also put space for read to attach a structure for object read mgmt
   //
   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;
   //   IOBuf*            b    = &fh->os.iob;

   EXPAND_PATH_INFO(info, path);

   // Check/act on iperms from expanded_path_info_structure
   //   If readonly RM/RD 
   //   If wronly/rdwr/trunk  RM/WM/RD/WD/TD
   //   If append we don’t support that

   // in the case of fuse, this should've been done already as a separate
   // call.  But other callers may expect open() to support this.
   if (flags & O_CREAT) {

      // Need to call readlink() on path, before this, but if path is a
      // marfs-path, readlink will invoke marfs_readlink() and we'll be
      // stuck?
      if (under_mdfs_top(path)) {
         LOG(LOG_ERR, "attempt to open path under mdfs_top '%s'\n", path);
         errno = EPERM;
         return -1;
      }
      if (stat_regular(info))
         TRY0( marfs_mknod(path, flags, 0) );
   }

   // unsupported operations
   if (flags & (O_APPEND)) {
      LOG(LOG_INFO, "open(O_APPEND) not implemented\n");
      errno = ENOSYS;
      return -1;
   }
   ///   else if (flags & O_CREAT) {
   ///      LOG(LOG_ERR, "open(O_CREAT) should've been handled by mknod()\n");
   ///      errno = ENOSYS;          /* for now */
   ///      return -1;
   ///   }
   else if (flags & O_TRUNC) {
      LOG(LOG_ERR, "open(O_TRUNC) should've been handled by frtuncate()\n");
      errno = ENOSYS;          /* for now */
      return -1;
   }
   else if (flags & (O_RDWR)) {
      fh->flags |= (FH_READING | FH_WRITING);
      LOG(LOG_INFO, "open(O_RDWR) not implemented\n");
      errno = ENOSYS;          /* for now */
      return -1;
   }
   else if (flags & (O_WRONLY)) {
      fh->flags |= FH_WRITING;
      ACCESS(info->ns, info->post.md_path, W_OK);
      CHECK_PERMS(info->ns, (R_META | W_META | R_DATA | W_DATA));

      // Need to call readlink() on path, before this, but if path is a
      // marfs-path, readlink will invoke marfs_readlink() and we'll be
      // stuck?
      if (under_mdfs_top(path)) {
         LOG(LOG_ERR, "attempt to open path under mdfs_top '%s'\n", path);
         errno = EPERM;
         return -1;
      }
   }
   else {
      // NOTE: O_RDONLY is not actually a flag!
      //       It's just the absence of O_WRONLY!
      fh->flags |= FH_READING;
      ACCESS(info->ns, info->post.md_path, R_OK);
      CHECK_PERMS(info->ns, (R_META | R_DATA));
   }

   //   if (info->flags & (O_TRUNC)) {
   //      CHECK_PERMS(info->ns, (T_DATA));
   //   }


   STAT_XATTRS(info);

   // we need to check if it is a packed file, and should not be one.
   // Maybe pftool is opening what will be the first chunk in an Nto1 file.
   // We should not let it write that object-ID with a "Packed" type,
   // because object-IDs for all the subsequent chunks will have "N" type.
   // Q: How do we tell if that is the situation?  A: if pftool is opening
   // a full chunk-sized object, we decree that it can't be packed.
   if ((fh->flags & FH_PACKED) &&
       ((content_length > info->pre.repo->max_pack_file_size) ||
        (content_length >= (info->pre.repo->chunk_size - MARFS_REC_UNI_SIZE)))) {

      return ENOTPACKABLE;
   }



   // If no xattrs, we let user read/write directly on the file.
   // This implies a file that created in DIRECT repo-mode,
   if (! has_any_xattrs(info, MARFS_ALL_XATTRS)) {
      LOG(LOG_INFO, "no xattrs.  Opening as DIRECT.\n");

      //      // Wrong.  The repo might no longer have DIRECT-mode.
      //      if (info->pre.repo->access_method != ACCESSMETHOD_DIRECT) {
      //         LOG(LOG_INFO, "Has RESTART, but (%s) repo->access_method != DIRECT\n",
      //             info->pre.repo->name);
      //         errno = EINVAL;
      //         return -1;
      //      }

      TRY0( open_md(fh, (fh->flags & FH_WRITING)) );
   }

   else if (fh->flags & FH_WRITING) {
      LOG(LOG_INFO, "writing\n");

      // Check/act on quotas of total-space and total-num-names
      // 0=OK, 1=exceeded, -1=error
      TRY_GE0( check_quotas(info) );
      if (rc_ssize) {
         errno = EDQUOT;
         return -1;
      }

      // Support for pftool N:1, where (potentially) multiple writers are
      // writing different parts of the file.  It's up to the caller to
      // assure that objects are only written to proper offsets.  Caller
      // took on that risk when they called marfs_open_at_offset().
      if (fh->flags & FH_Nto1_WRITES) {

         LOG(LOG_INFO, "writing N:1\n");

         const size_t recovery   = MARFS_REC_UNI_SIZE; // sys bytes, per chunk
         size_t       chunk_size = info->pre.repo->chunk_size - recovery;
         size_t       chunk_no   = (fh->open_offset / chunk_size);

         // assure that the offset is on a chunk-boundary
         if (fh->open_offset % chunk_size) {
            LOG(LOG_ERR, "opening for write not at chunk-boundary (%s, %ld, %ld)\n",
                path, fh->open_offset, content_length);
            LOG(LOG_ERR, "repo '%s' has chunksize=%ld (logical=%ld)\n",
                info->pre.repo->name, info->pre.repo->chunk_size, chunk_size);
            errno = EFAULT;        // most feasible-looking POSIX-compliant error?
            return -1;
         }

         // set marfs_objid.obj_type such that pftool can recognize that it
         // must do additional cleanup after closing.  (Actually done in
         // marfs_utime().)
         info->pre.obj_type = OBJ_Nto1;

         // update URL for this chunk
         info->pre.chunk_no = chunk_no;
         update_pre(&info->pre);
      }

      if( fh->flags & FH_PACKED ) {
         LOG(LOG_INFO, "writing PACKED\n");

         if ((  (fh->objectSize + content_length + MARFS_REC_UNI_SIZE)
                 > info->pre.repo->chunk_size)
             || ((-1 != info->pre.repo->max_pack_file_count)
                 && (fh->fileCount +1 > info->pre.repo->max_pack_file_count))) {

            LOG(LOG_INFO, "releasing fh: objectSize: %ld, content_length: %ld, "
                "chunk_size: %lu, fileCount: %d, "
                "max_pack_file_count: %ld\n",
                fh->objectSize, content_length,
                info->pre.repo->chunk_size, fh->fileCount,
                info->pre.repo->max_pack_file_count);
            // we need to close the current object stream and open a new
            // one if it is a packed object
            // we will now just tell pftool (or something else to do the work)
            return EFHFULL;
         }

         // set the object type
         info->pre.obj_type = OBJ_PACKED;
         info->post.obj_type = OBJ_PACKED;
         info->post.obj_offset = fh->objectSize;
         update_pre(&info->pre);

         // update the object info
         fh->objectSize += content_length+MARFS_REC_UNI_SIZE;
         fh->fileCount += 1;
      }

   }


   // Install namespace-path (e.g. for writing into recovery-info)
   size_t path_len = strlen(path);
   if (path_len >= MARFS_MAX_NS_PATH) {
      LOG(LOG_ERR, "path '%s' longer than max %d\n", path, MARFS_MAX_NS_PATH);
      errno = EIO;
      return -1;
   }
   strncpy(fh->ns_path, path, path_len +1);

   // we need to check if we need a new stream
   if ( !(fh->flags & FH_PACKED)
        || (0 == fh->os_init)) {
      LOG(LOG_INFO, "opening new object stream\n");

#if 0
      // DELETE THIS after we're sure that it is working correctly
      // in its new home as stream_init().

      // Configure a private AWSContext, for this request
      AWSContext* ctx = aws_context_clone();
      if (ACCESSMETHOD_IS_S3(info->pre.repo->access_method)) { // (includes S3_EMC)

         // install the host and bucket
         s3_set_host_r(info->pre.host, ctx);
         LOG(LOG_INFO, "host   '%s'\n", info->pre.host);
         // fprintf(stderr, "host   '%s'\n", info->pre.host); // for debugging pftool

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
      else if (info->pre.repo->access_method == ACCESSMETHOD_SPROXYD) {
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

      if (info->pre.repo->security_method == SECURITYMETHOD_HTTP_DIGEST) {
         s3_http_digest_r(1, ctx);
      }

      // install custom context
      aws_iobuf_context(b, ctx);

#endif

      // To support seek() [for reads], and allow reading at arbitrary
      // offsets, we let marfs_read() determine the offset where it should
      // open, so it can do its own GET, with byte-ranges.  Therefore, for
      // reads, we don't open the stream, here.
      //
      // We don't open streams for writing here either, since they may
      // not be used (ie. when overwriting a file we open the old
      // object first, then close it without performing any
      // operations). Not opening streams here saves us from doing
      // unneeded work.
      if (! (fh->flags & FH_WRITING)) {
         SEM_INIT(&os->read_lock, 0, 1);
         os->flags |= OSF_RLOCK_INIT;

#ifdef NFS_THREADS
         // This is a special build-time flag that lets us just assume that it
         // will be NFS calling marfs_read().  Otherwise,
         // marfs_read_internal() will attempt to detect whether it is NFS
         // calling.  The effect of defining NFS_THREADS is that *all*
         // discontiguous reads will be enqueued until they become contiguous.
         // You should not do this if what you are building is fuse (or some
         // other app) which might need to support a seek before a read.  But
         // normal sequential reads through fuse (i.e. outside of NFS) should
         // also work fine with this flag defined.  See marfs_read_internal().
         //
         // NOT A GOOD IDEA.  It turns out that NFS sometimes invokes the
         // scattered reads across multiple file-handles.  Thus, some
         // out-of-order threads have no other threads following along behind.

         fh->flags |= FH_MULTI_THR;
#endif
      }

      fh->os_init = 1;
   }

#if 0
   // COMMENTED OUT.  Turns out NFS calls truncate() on the same path,
   // after opening a file that exists.  In other words, it assumes we can
   // correlate action on some path with the status of some other open
   // file-descriptor to that path, such as the one we have here.  That's a
   // dumb assumption.  Bottom line, for this and other reasons, NFS is not
   // really usable on top of MarFS fuse, at the moment.

   // Accomodate NFS trying to open an existing file.
   // Unlike fuse, it won't automatically call ftruncate() after open().
   // (See NOTE above fuse_open(), in main.c)
   //
   if ((fh->flags & FH_WRITING)
       && (info->pre.obj_type != OBJ_Nto1)
       && (info->st.st_size)) {

      LOG(LOG_INFO, "assuming NFS needs ftruncate(%s, 0, ...)\n",
          info->post.md_path);
      TRY0( marfs_ftruncate(path, 0, fh) );
   }
#endif


   EXIT();
   return 0;
}

// This open command allows you to provide an alreay populated fh for
// marfs to work with. This allows marfs to create a packed file
// by resuing a curl stream if there is still room in the current object.
//
// If there is not room the stream is closed and a new one is created.
//
// It only makes sense to use this on a create call.
//
// ENOTPACKABLE will be returned if the file is not packable
// EFHFULL will be returned if the filehandle is full
int  marfs_open_packed   (const char* path, MarFS_FileHandle* fh, int flags,
                          curl_off_t content_length) {
   // if you are trying to read the file just use regular open
   if(!(flags & O_WRONLY)) {
      return ENOTPACKABLE;
   }

   if( 0 >= content_length) {
      return ENOTPACKABLE;
   }

   // if the flag is not already set go ahead set and clear the data structure
   if( !(fh->flags & FH_PACKED)) {
      memset(fh, 0, sizeof(MarFS_FileHandle));
      fh->flags |= FH_PACKED;
   }

   // run open
   return marfs_open(path, fh, flags, content_length);
}


// For writing, this is a potentially-risky variant of marfs_open(), which
// assumes you know what you are doing.
//
// (a) If you are reading, then opening with offset is more efficient than
//     opening without an offset and then closing and re-opening in your
//     first call to marfs_read().  open_at_offset() for read is not risky.
//
// (b) However, if you are writing, you can potentially create unreadable
//     Multi files, by creating objects that are not at the proper logical
//     offset in the total data, or by failing to make a final pass to
//     clean-up file-size and xattrs.  *That* is risky.  If you are pftool,
//     you are smart enough to only call this with offsets at the
//     boundaries of Multi objects, and to write full-sized objects when
//     you do (except possibly the last object).  If you are FUSE, you
//     probably do not know enough to be using this function, [unless you
//     are some super FUSE-client from the future, where parallel streams
//     are written to known offsets in a Multi file, in which case, we
//     salute you!]
//
// NOTE: Even if you are opening for writing at offset 0, if you're calling
//     this interface, we have to assume there are more than 1 writers.
//     This is because we need everyone to agree that the POST xattr should
//     have type MULTI.  Otherwise, there is a race condition at close-time
//     to update the xattr.
//
int marfs_open_at_offset(const char*         path,
                         MarFS_FileHandle*   fh,
                         int                 flags,
                         curl_off_t          offset,
                         curl_off_t          content_length) {
   TRY_DECLS();
   LOG(LOG_INFO, "opening at offset=%ld, length=%ld\n", offset, content_length);

   // keep track of the offset for close/re-open
   fh->open_offset = offset;

   if (flags & (O_WRONLY | O_RDWR)) {
      // FH_Nto1_WRITES implies N:1 writing from pftool
      LOG(LOG_INFO, "allowing N:1 writes\n");
      fh->flags |= FH_Nto1_WRITES;

      // for get_stream_wr_open_size()
      fh->write_status.sys_req     = MARFS_REC_UNI_SIZE;
      fh->write_status.data_remain = content_length;
   }
   else {
      // for get_stream_rd_open_size()
      fh->read_status.log_offset  = offset;
      fh->read_status.data_remain = content_length;
   }

   // TBD: update_pre(), to install correct chunk-number, for writes?

   TRY0( marfs_open(path, fh, flags, content_length) );
   return 0;
}




int marfs_opendir (const char*       path,
                   MarFS_DirHandle*  dh) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, (R_META));

   if (under_mdfs_top(path)) {
      LOG(LOG_ERR, "attempt to open path under mdfs_top '%s'\n", path);
      errno = EPERM;
      return -1;
   }

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");

#if 0
      // COMMENTED OUT.

      // prevent non-root users from listing namespaces.
      if (geteuid()) {
         errno = EACCES;
         return -1;
      }
#endif

      // EUID=0
      dh->use_it = 1;           // use iterator-member
      dh->internal.it = namespace_iterator();
      return 0;
   }

   // set dh->use_it to false so that we know to use the dirp/MDAL-impl
   dh->use_it = 0;

   // No need for access check, just try the op
   // Appropriate  opendir call filling in fuse structure
   ///   mode_t mode = ~(fuse_get_context()->umask); /* ??? */

   TRY_GT0( opendir_md(dh, &info) );

   EXIT();
   return 0;
}


// [For more details, see "stupid NFS tricks" in common.h]
//
// With an NFS-mount on top of fuse, where nfsd has more than one thread,
// we sometimes receive concurrent reads at different offsets on the same
// file-handle.  These reads would both be interacting with the same
// object-stream, which would cause corruption in the file-handle and/or
// errors in the stream-interaction. So, we must allow only one thread at a
// time to read on a given file-handle.  We do that by adding a lock.
// Because our existing lock support is all associated with ObjectStream,
// we added the new read_lock there.  Because the old marfs_read() might
// return from multip[le places without warning, we just wrap it, to insure
// we always do the unlock.
//
// To address the problem that a reader might legitimately want to read
// from multiple positions, without reading the intervening gap, we don't
// lock and enqueue pending requests until we have determined that there
// are deifinitely multiple threads reading on the same file-handle.  In
// that case, we assume it is NFS reading the whole thing sequentially

// fwd-decl
static ssize_t marfs_read_internal (const char*        path,
                                    char*              buf,
                                    size_t             size,
                                    off_t              offset,
                                    MarFS_FileHandle*  fh);



ssize_t marfs_read (const char*        path,
                    char*              buf,
                    size_t             size,
                    off_t              offset,
                    MarFS_FileHandle*  fh) {
   ENTRY();
   LOG(LOG_INFO, "%s, fh=0x%lx, off=%lu, size=%lu\n", path, (size_t)fh, offset, size);

   ObjectStream*     os   = &fh->os; // shorthand

   static const uint16_t default_timeout = 20; /* totally made up out of thin air */
   uint16_t timeout_sec = (os->timeout ? os->timeout : default_timeout);

   LOG(LOG_INFO, "(%08lx) waiting %ds for read-lock\n", (size_t)os, timeout_sec); 
   SAFE_WAIT(&os->read_lock, timeout_sec, os);


   //   // Don't let NFS try to read something we've already read 
   //   //#ifdef NFS_THREADS 
   //   if ((os->flags & OSF_OPEN)                       // already did some reading 
   //       && (offset < fh->read_status.log_offset)) {  // our read is a re-read 
   //
   //      POST(&os->read_lock); 
   //      return 0; 
   //   } 
   //   //#endif 

   // If this is a discontiguous read, and it's NFS calling, then there may
   // be another thread that could consume from the stream such that our
   // discontiguous read would become contiguous.  Give it a chance.
   int discontig_retries = 5;
   while ((os->flags & OSF_OPEN)                    // already did some reading
          && !(fh->flags & FH_MULTI_THR)            // not sure if NFS threads are reading
          && (offset > fh->read_status.log_offset)  // our read is discontig
          && discontig_retries--) {

      LOG(LOG_INFO, "(%08lx) deferring discontig read at %lu != %lu (remaining retries: %d)\n",
          (size_t)os, offset, fh->read_status.log_offset, discontig_retries);

      // note the stream-offset as it is now
      off_t str_offset_before = fh->read_status.log_offset;

      // let any blocked read threads proceed
      POST(&os->read_lock);

      /// sched_yield();
      usleep(20000);            // 20 ms
      SAFE_WAIT(&os->read_lock, timeout_sec, os);

      // if someone else moved the ball, then there is someone else
      if (fh->read_status.log_offset != str_offset_before) {
         LOG(LOG_INFO, "(%08lx) detected threaded NFS at %lu != %lu (remaining retries: %d)\n",
             (size_t)os, offset, str_offset_before, discontig_retries);
         fh->flags |= FH_MULTI_THR;
      }
   }


   // The retry-loop above allows some discontig-reads to fall into their
   // proper place without an expensive close/reopen, in the case where
   // multiple NFS threads are reading at different offsets on the same
   // file-handle.  But some readers would use up their retries and go on
   // to do the close/reopen, which really destroys BW.
   //
   // So, we add this secondary enhancement: If the loop above *ever*
   // notices that the current stream-offset changed during one of the
   // waits above, then we conclude that there are multiple threads reading
   // on the same file-handle.  (We'll also assume that only nfsd would be
   // arrogant enough to try that, and we'll assume that the client is
   // reading sequentially through the entire file.  We're assuming someone
   // isn't seeking on their NFS-mounted MarFS file).
   //
   // After we've detected this situation, we'll thenceforth (for the life
   // of the file-handle) fall over to this better scheme: enqueue
   // out-of-order reads along with their offset onto a queue in the
   // file-handle, keeping the queue in ascending order of offsets.  Each
   // queue-element includes a lock that its owning thread is waiting on.
   // When any read() finishes, and the queue is non-empty, check whether
   // the current stream-position matches the offset at the front of the
   // queue.  If so, post the lock tha that reader is waiting on.  (The
   // released reader-thread is the one who then pops the quueue.)
   //
   // NOTE: We always hold the read_lock while manipulating
   //     FH.read_status.read_queue
   if ((fh->flags & FH_MULTI_THR)
       && (offset > fh->read_status.log_offset)) {

      volatile ReadQueueElt* elt = enqueue_reader(offset, fh);
      SEM_T* wait_sem   = (SEM_T*)&elt->lock; // volatile RQE* worries compiler
      POST(&os->read_lock);     // let other readers go

      LOG(LOG_INFO, "(%08lx) waiting for read at %lu\n", (size_t)os, offset);
#if 0
      WAIT(wait_sem);           // wait our turn
#else
      static const size_t timeout_sec = 4;
      while (TIMED_WAIT(wait_sem, timeout_sec)) { // wait our turn

         // timed-out, waiting for new reads at offsets ahead of us to read
         // us into order.  If we're still at the front of the queue, (and
         // some other waiting reader ahead of us didn't already do the
         // close/reopen), then let us go ahead and do the close/reopen
         // ourselves.  We set the "rewinding" flag in the other current
         // queue members, so they won't also decide to do this, at least
         // until after we've finished this read (and possibly read them
         // into order).
         WAIT(&os->read_lock);
         LOG(LOG_INFO, "(%08lx) queued read at %lu timed out\n", (size_t)os, offset);
         if ((! elt->rewinding)
             && (elt == fh->read_status.read_queue)) {

            LOG(LOG_INFO, "(%08lx) queued read at %lu going ahead\n", (size_t)os, offset);
            volatile ReadQueueElt* q;
            for (q=fh->read_status.read_queue; q; q=q->next)
               q->rewinding = 1;

            POST(&os->read_lock);
            break;
         }
         POST(&os->read_lock);
      }
#endif
      LOG(LOG_INFO, "(%08lx) done waiting for read at %lu\n", (size_t)os, offset);

      // other threads advanced the offset to our position, or else we
      // timed out waiting for that.  [Or, marfs_release() is shutting us
      // down.]  Either way, it's time to get on with things.  In the
      // former case, marfs_read_internal() will not have to close/reopen,
      // and in the latter case it will.  If downstream readers don't time
      // out immediately after us, they might be able to take advantage of
      // the re-opened strream.
      WAIT(&os->read_lock);     // our turn now
      dequeue_reader(offset, fh);

      // maybe marfs_release() is just shutting us down?
      if (fh->flags & FH_RELEASING) {
         POST(&os->read_lock);
         LOG(LOG_INFO, "(%08lx) abandoning read at %lu\n", (size_t)os, offset);
         return 0;
      }
   }

   rc_ssize = marfs_read_internal(path, buf, size, offset, fh);

   check_read_queue(fh);        // maybe another waiter can go
   POST(&os->read_lock);

   EXIT();
   return rc_ssize;
}

// return actual number of bytes read.  0 indicates EOF.
// negative means error.
//
// NOTE: 
// TBD: Don't do object-interaction if file is DIRECT.  See marfs_open().
//
static ssize_t marfs_read_internal (const char*        path,
                                    char*              buf,
                                    size_t             size,
                                    off_t              offset,
                                    MarFS_FileHandle*  fh) {
   ENTRY();
   LOG(LOG_INFO, "%s, fh=0x%lx, off=%lu, size=%lu\n", path, (size_t)fh, offset, size);

   ///   PathInfo info;
   ///   memset((char*)&info, 0, sizeof(PathInfo));
   ///   EXPAND_PATH_INFO(&info, path);
   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;
   //   IOBuf*            b    = &os->iob;

   // Check/act on iperms from expanded_path_info_structure, this op requires RM  RD
   CHECK_PERMS(info->ns, (R_META | R_DATA));

   // No need to call access as we called it in open for read
   // Case
   //   File has no xattr objtype
   //     Just read the bytes from the file and fill in fuse read buffer
   //
   if ((! has_any_xattrs(info, MARFS_ALL_XATTRS))
       // && (info->pre.repo->access_method == ACCESSMETHOD_DIRECT)
       ) {
      LOG(LOG_INFO, "reading DIRECT\n");
      TRY_GE0( MD_FILE_OP(read, fh, buf, size) );
      return rc_ssize;
   }

   else if (has_any_xattrs(info, MARFS_MD_XATTRS)
            && (! has_all_xattrs(info, MARFS_MD_XATTRS))) {
      LOG(LOG_ERR, "has some, but not all MARFS_MD_XATTRS\n");
      errno = EINVAL;
      return -1;
   }

   // This could be e.g. a file written Nto1 from pftool, in which there
   // were some write-errors, or the job was stopped before all parts were
   // written.  Such files are legit, because pftool can do another pass
   // and write the parts that are missing.  But we don't let anyone try to
   // read the file until that's been done.  In this case, it will still
   // have the RESTART xattr.  [TBD? allow reading regions that were
   // successfully written?]
   else if (has_all_xattrs(info, XVT_RESTART)) {
      LOG(LOG_ERR, "has RESTART\n");
      errno = EINVAL;
      return -1;
   }

   // discontiguous read could happen if fuse calls seek().
   // (Or on first call from fuse.)
   if (offset != fh->read_status.log_offset) {

      if (os->flags & OSF_OPEN) {

         LOG(LOG_INFO, "discontiguous read detected: gap %lu-%lu\n",
             fh->read_status.log_offset, offset);
         TRY0( close_data(fh, 0, 0) );
      }

      fh->read_status.log_offset = offset;

      // Someone called marfs_open_at_offset(), then did a seek?  If so,
      // they effed up our ability to be smart about their byte-ranges.
      // From now on, they'll get max-sized byte-ranges, in which they'll
      // have to crop out recovery-info, follow-on packed data, etc, just
      // like fuse.
      if (fh->read_status.data_remain)
         fh->read_status.data_remain = 0;
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


   // We handle Uni, Multi, and Packed files.  In the case of MULTI files,
   // the data that is requested may span multiple objects (we call these
   // internal objects "chunks").  In that case, we will actually do
   // multiple open/close actions per read(), because we must open objects
   // individually.
   //
   // For performance, we restrict the number of stream close/open
   // operations to only those that are strictly necessary (for example,
   // when ending one Multi object and starting the next one).
   // Specifically, if we receive multiple calls to read() that are getting
   // contiguous data, we avoid opening a new stream on every call (by
   // leaving the stream open).
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
   //     xattr, changing only the final ".0" to the appropriate
   //     chunk-number.
   //
   // These assumptions mean we can easily compute the IDs of chunk(s) we
   // need, given only the read-offset (i.e. the "logical" offset) and the
   // original object-ID.



   // In the case of "Packed" objects, many user-level files are packed
   // (with recovery-info at the tail of each) into a single physical
   // object.  The "logical offset" of the user's data must then be added
   // to the physical offset of the beginning of the user's object within
   // the packed object, in order to skip over the objects (and their
   // recovery-info) that preceded the "logical object" within the physical
   // object.  Post.obj_offset is only non-zero for Packed files, where it
   // holds the absolute physical byte_offset of the beginning of user's
   // logical data, within the physical object.
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
   size_t       max_read   = ((offset >= max_extent) // max logical span from offset
                              ? 0
                              : max_extent - offset);

   // truncate <size>, if larger than max legitimate read from offset
   if (size > max_read)
      size = max_read;
   if (! size) {
      LOG(LOG_INFO, "end of extent for %s\n", path);
      return 0;
   }

   // portions of each chunk that are used for system-data vs. user-data.
   const size_t recovery   = MARFS_REC_UNI_SIZE; // sys bytes, per chunk
   const size_t data1      = (info->pre.chunk_size - recovery); // log bytes, per chunk

   size_t chunk_no     = phy_offset / data1; // only non-zero for Multi
   size_t chunk_offset = phy_offset - (chunk_no * data1); // offset in <chunk_no>
   size_t chunk_remain = data1 - chunk_offset;         // max for this chunk

   // request whichever is smaller, here to EOD, or here to end-of-chunk
   size_t open_size    = (max_read < chunk_remain) ? max_read : chunk_remain; // for this open()

   // pftool may know that it only wants to read up to the middle of some chunk
   if (fh->read_status.data_remain
       && (open_size > fh->read_status.data_remain))
      open_size = fh->read_status.data_remain;

   // Config may specify reads requests of less-than-chunksize.  [We saw
   // "incast" problems on one 10GbE installation, which seemed to be
   // mitigated by breaking large requests up into smaller ones.  This is
   // now a per-repo configuration parameter.  Given an erasure-coding
   // schema (n = k + m), the config parm should probably be set to a
   // multiple of the number of data blocks (k) * data block size.]
   if (info->pre.repo->max_get_size
       && (open_size > info->pre.repo->max_get_size))
      open_size = info->pre.repo->max_get_size;

   size_t read_size    = ((size < open_size) ? size : open_size); // for this chunk

   char*  buf_ptr      = buf;
   size_t read_count   = 0;     // total amount read in this marfs_read()

   if ((info->post.obj_type == OBJ_PACKED)
       && chunk_no) {
      LOG(LOG_ERR, "off(%lu) + packed-obj off(%lu) > chunksize(%lu) - recovery(%lu)\n",
          offset, info->post.obj_offset, info->pre.chunk_size, recovery);
      errno = EPERM;
      return -1;
   }

   uint16_t rd_timeout = info->pre.repo->read_timeout;

   // Starting at the appropriate chunk and offset to match caller's
   // logical offset in the multi-object data, move through successive
   // chunks, reading contiguous user-data (skipping recovery-info), until
   // we've filled caller's buffer.  <read_size> is the amount to read from
   // this chunk, which might be smaller than <size>.  It is incremented at
   // the tail of the loop, to advance through additional chunks, if
   // necessary.  read_size always gets us to the end of logical data in
   // this chunk, or to the end of caller's request, or to
   // repo.max_get_size, or to EOF.
   while (read_size) {

      // read as much user-data as we have room for from the current chunk
      LOG(LOG_INFO, "iter: chnk=%lu, off=%lu, loff=%lu, choff=%lu, "
          "rdsz=%lu, chrem=%lu, open=%lu\n",
          chunk_no, offset, fh->read_status.log_offset, chunk_offset,
          read_size, chunk_remain, open_size);


      // e.g. we read to the end of max_get_size on a previous call to
      //      marfs_read(), but there's still more in this chunk.
      if ((os->flags & OSF_OPEN)
          && (os->flags & OSF_EOF)) {
         LOG(LOG_INFO, "closing stream at EOF\n");
         TRY0( close_data(fh, 0, 0) );
      }


      if (! (os->flags & OSF_OPEN)) {

         // update the URL in the ObjectStream, in our FileHandle
         info->pre.chunk_no = chunk_no;
         update_pre(&info->pre);

         // request data from the offset in this chunk, to the end of
         // logical data in this chunk, or to offset+size, whichever comes
         // first.
         LOG(LOG_INFO, "byte_range: %lu, %lu\n",
             chunk_offset, chunk_offset + open_size -1);
#if 0
         // DELETE THIS after the corresponding new functionality in
         // open_data() and corresponding DALs is working.

         s3_set_byte_range_r(chunk_offset, open_size, b->context); // offset, length
#endif

         // NOTE: stream_open() potentially wipes ObjectStream.written.
         //     Fuse writes want this field to track the total amount
         //     written across all chunks, but for reads, fuse/pftool want
         //     it to be available for stream_sync() to compare with the
         //     content_length provided in stream_open().  Therefore, we
         //     let written be wiped.
         //
         TRY0( open_data(fh, OS_GET, chunk_offset, open_size, 0, rd_timeout) );
      }

      // Because we are reading byte-ranges, we may see '206 Partial Content'.
      //
      // It's probably also legit that the server could give us less than we
      // asked for. (i.e. 'Partial Content' might not only mean that we
      // asked for a byte range; it might also mean we didn't even get
      // all of our byte range?)  In that case, instead of moving on to
      // the next object, we should try again on this one.
      //
      // Keep trying until we read all of read_size, or get an error (or get 0).
      //
      // NOTE: This is overkill.  stream_get() will just time-out, if the
      //    server doesn't give us everything we asked for.  However, we
      //    may get 206, even for successful reads.

      size_t sub_read = read_size; // bytes remaining within <read_size>
      do {
         rc_ssize = DAL_OP(get, fh, buf_ptr, sub_read);
         if ((rc_ssize < 0)
             && (os->iob.code != 200)
             && (os->iob.code != 206)) {
            LOG(LOG_ERR, "stream_get returned < 0: %ld '%s' (%d '%s')\n",
                rc_ssize, strerror(errno), os->iob.code, os->iob.result);
            errno = EIO;
            return -1;
         }
         if (rc_ssize < 0) {
            LOG(LOG_ERR, "stream_get returned < 0: %ld '%s' (%d '%s')\n",
                rc_ssize, strerror(errno), os->iob.code, os->iob.result);
            errno = EIO;
            return -1;
         }
         // // handy for debugging small reads (but too voluminous for normal use)
         // LOG(LOG_INFO, "result: '%*s'\n", rc_ssize, buf);

         // Q: This might legitimately happen if stream_get() hits EOF?
         // A: No, read_size was adjusted to account for logical EOF
         //
         // Q: Yeah, but maybe we just used up max_get_size, and now we
         //    need another request?
         if (rc_ssize == 0) {
            LOG(LOG_ERR, "request for range %ld-%ld (%ld bytes) returned 0 bytes\n",
                (chunk_offset + read_size - sub_read),
                (chunk_offset + read_size),
                sub_read);
            errno = EIO;
            return -1;
         }


         if (rc_ssize != sub_read) {
            LOG(LOG_INFO, "request for range %ld-%ld (%ld bytes) returned only %ld bytes\n",
                (chunk_offset + read_size - sub_read),
                (chunk_offset + read_size),
                sub_read, rc_ssize);
            // errno = EIO;
            // return -1;
         }

         buf_ptr       += rc_ssize;
         sub_read      -= rc_ssize;

      } while (sub_read);
      LOG(LOG_INFO, "completed read_size = %lu\n", read_size);

      // We got all of read_size.  We're either at the end of this chunk or
      // at the end of the amount requested for this read().
      read_count    += read_size;
      max_read      -= read_size;
      chunk_remain  -= read_size;

      fh->read_status.log_offset  += read_size;
      if (fh->read_status.data_remain)
         fh->read_status.data_remain -= read_size;

      // reading more?
      if (read_count < size) {

         TRY0( close_data(fh, 0, 0) );

         // ready to move on to the next chunk?
         if (! chunk_remain) {
            chunk_no      += 1;
            chunk_offset   = 0;
            chunk_remain   = data1;
         }

         // prepare for next iteration
         open_size    = (max_read < chunk_remain) ? max_read : chunk_remain;

         if (fh->read_status.data_remain
             && (open_size > fh->read_status.data_remain))
            open_size = fh->read_status.data_remain;
         
         if (info->pre.repo->max_get_size
             && (open_size > info->pre.repo->max_get_size))
            open_size = info->pre.repo->max_get_size;

         size_t size_remain = size - read_count;
         read_size          = ((size_remain < open_size) ? size_remain : open_size);
      }
      else
         read_size      = 0;

   }

   EXIT();
   return read_count;
}


int marfs_readdir (const char*        path,
                   void*              buf,
                   marfs_fill_dir_t   filler,
                   off_t              offset,
                   MarFS_DirHandle*   dh) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, (R_META));

   // No need for access check, just try the op
   // Appropriate  readdir call filling in fuse structure  (fuse does this in chunks)
   int retval = 0;

   // <use_it> is used in the case where opendir() dtected that we are
   // operating on the NS root-dir.  Otherwise, we use a standard DIR*.
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

#if USE_MDAL
      retval = D_OP(readdir, dh, path, buf, filler, offset);

#else
      DIR*           dirp = dh->internal.dirp;
      struct dirent* dent;

      while (1) {
         errno = 0;

         // #if _POSIX_C_SOURCE >= 1 || _XOPEN_SOURCE || _BSD_SOURCE || _SVID_SOURCE || _POSIX_SOURCE
         //      struct dirent* dent_r;       /* for readdir_r() */
         //      TRY0( readdir_r(dirp, dent, &dent_r) );
         //      if (! dent_r)
         //         break;                 /* EOF */
         //      if (filler(buf, dent_r->d_name, NULL, 0))
         //         break;                 /* no more room in <buf>*/

         // #else
         rc_ssize = (ssize_t)readdir(dirp);
         if (! rc_ssize) {
            if (errno)
               retval = -1;     /* error */
            break;              /* EOF */
         }
         dent = (struct dirent*)rc_ssize;
         if (filler(buf, dent->d_name, NULL, 0))
            break;                 /* no more room in <buf>*/
         // #endif
      }
#endif

   }

   EXIT();
   return retval;
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
//     just return "../../test" The kernel will then canonicalize
//     "/marfs/ns/../../test" and then use that to do whatever the
//     operation was.  We are not allowing any subversion of the MDFS by
//     responding with the contents of the symlink.


// for fuse
int marfs_readlink_ok (const char* path,
                       char*       buf,
                       size_t      size) {
   ENTRY();

   TRY_GE0( marfs_readlink(path, buf, size) );

   EXIT();
   return 0;
}

ssize_t marfs_readlink (const char* path,
                        char*       buf,
                        size_t      size) {
   ENTRY();
   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, (R_META));

#if 0
   // Now that we plan to mount fuse in a context where the mdfs is
   // unshared, this should no longer be necessary:

   // We shouldn't call readlink() if the target of the symlink is a marfs
   // target.  That will deadlock fuse.  But, if it points outside marfs to
   // another symlink, fuse will not come back to us with the new symlink,
   // to let us evaluate whether the new path points back into marfs
   // (e.g. to an mdfs file).  Therefore, in the case where the destination
   // is a symlink that points outside marfs, we not only can, but must,
   // follow the links ourselves, until we either find a marfs path, or
   // come to the end of the symlink chain.
   TRY0( follow_some_links(&info, buf, size) );

#else
   // No need for access check, just try the op
   // Appropriate readlink-like call filling in fuse structure 
   TRY_GE0( MD_PATH_OP(readlink, info.ns, info.post.md_path, buf, size) );
   if (rc_ssize >= size) {
      LOG(LOG_ERR, "no room for '\\0'\n");
      errno = ENAMETOOLONG;
      return -1;
   }
   buf[rc_ssize] = '\0';
   LOG(LOG_INFO, "readlink '%s' -> '%s' = (%ld)\n", info.post.md_path, buf, rc_ssize);
#endif

   EXIT();
   return rc_ssize;
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
// NOTE: The FH_Nto1_WRITES flag, in FH->flags, means that multiple writers
//    may be writing the file (e.g. from pftool).  They take responsibility
//    for only writing complete chunks, and only writing at chunk
//    boundaries.  But they can't be sure how big the file is at close
//    time, or how much chunk-info will utimately be written, so we don't
//    do that at close() time.  Instead, pftool will do a final
//    (single-threaded) update when it is setting the modification-time.
//
// NOTE: we return "success" (0) if we are able to do our job, even in the
//    case where we see timeout-related errors in the stream.  Those errors
//    will already have returned failure from some open/read/write call.
//    Our job is just to do the close.

int marfs_release (const char*        path,
                   MarFS_FileHandle*  fh) {
   ENTRY();

   // For backwards compatability, call flush if it has not already
   // been called on this object.
   if( !(fh->flags & FH_FLUSHED) ) {
      LOG(LOG_INFO, "flushing unflushed stream\n");
      marfs_flush(path, fh);
      EXIT();
      return 0;
   }

   // if writing there will be an objid stuffed into a address  in fuse open table
   //       seal that object if needed
   //       free the area holding that objid
   // if  reading, there might be a malloced space for read obj mgmt. in fuse open table
   //       close any objects if needed
   //       free the area holding that stuff
   // close the metadata file handle

   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;

   EXIT();
   return 0;
}

/**
 * Pftool needs a way to finialize files after it has confirmed that those
 * files have been commited. This function sets the object field to be correct
 * for packed files.
 *
 * @param path The path to cleanup
 * @param packFileCount The number of packed files in the object
 * @return 0 on success
 */
int marfs_packed_set_post(const char* path, size_t packedFileCount) {
   TRY_DECLS();
   LOG(LOG_INFO, "%s\n", path);

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   STAT_XATTRS(&info);
   if (has_all_xattrs(&info, MARFS_MD_XATTRS)) {
      // set the packed file count for the packed file
      info.post.chunks = packedFileCount;

      // save the new xattr
      SAVE_XATTRS(&info, (XVT_POST));

      }
   else
      LOG(LOG_INFO, "no xattrs\n");

   return 0;

}

/**
 * Pftool needs a way to finialize files after it has confirmed that those
 * files have been commited. This function clears the restart flag after the
 * file is written.
 *
 * @param path The path to cleanup
 * @return 0 on success
 */
int marfs_packed_clear_restart(const char* path) {
   TRY_DECLS();
   LOG(LOG_INFO, "%s\n", path);

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   STAT_XATTRS(&info);
   if (has_all_xattrs(&info, MARFS_MD_XATTRS)) {
      // As in release(), we take note of whether RESTART was saved with
      // a restrictive mode, which we couldn't originally install because
      // would've prevented manipulating xattrs.  If so, then (after removing
      // the RESTART xattr) install the more-restrictive mode.
      int    install_new_mode = 0;
      mode_t new_mode = 0;      // else gcc worries about "used uninitialized"

      if (has_all_xattrs(&info, XVT_RESTART)
          && (info.restart.flags & RESTART_MODE_VALID)) {

         install_new_mode = 1;
         new_mode = info.restart.mode;
      }

      // remove "restart" xattr. cannot do this at close time for packed files
      // because the object stream has not been closed and we cannot know that
      // the data has been commited to disc
      info.xattrs &= ~(XVT_RESTART);

      // save the new xattr
      SAVE_XATTRS(&info, (XVT_RESTART));

      // install more-restrictive mode, if needed.
      if (install_new_mode) {
         TRY0( MD_PATH_OP(chmod, info.ns, info.post.md_path, new_mode) );
      }
   }
   else
      LOG(LOG_INFO, "no xattrs\n");

   return 0;

}


// If we are sharing a file handle to write to a packed object we need to
// close the stream once we are completly done. Before a program exists it
// needs to call this on all file handles that it used as packed
int marfs_release_fh(MarFS_FileHandle* fh) {
   ENTRY();
   LOG(LOG_INFO, "release_fh\n");

   ObjectStream*     os   = &fh->os;

   if(fh->os.flags & OSF_OPEN) {
     // Opens are defered.
     // If open_data wasn't called fh->dal_handle.dal will be NULL
     // prevent problems by not closing unopened streams
     close_data(fh, 0, 1);
   }

   // free aws4c resources
   aws_iobuf_reset_hard(&os->iob);

   memset(fh, 0, sizeof(MarFS_FileHandle));

   fh->flags |= FH_PACKED;

   EXIT();
   return 0;
}


//  [Like release(), this doesn't have a directly corresponding system
//  call.]  This is also the only function I've seen (so far) that gets
//  called with fuse_context->uid of 0, even when running as non-root.
//  Thus, seteuid() would fail.
//
// NOTE: Testing as myself, I notice releasedir() gets called with
//     fuse_context.uid ==0.  Other functions are all called with
//     fuse_context.uid == my_uid.  I’m ignoring push/pop UID in this case,
//     in order to be able to continue debugging.
//
// NOTE: It is possible that we get called with a directory that doesn't
//     exist!  That happens e.g. after 'rm -rf /marfs/jti/mydir'. In that
//     case, it seems the kernel calls us with <path> = "-".

int marfs_releasedir (const char*       path,
                      MarFS_DirHandle*  dh) {
   ENTRY();
   LOG(LOG_INFO, "releasedir %s\n", path);

   // If path == "-", assume we are closing a deleted dir.  (see NOTE)
   if ((path[0] != '-') || (path[1] != 0)) {
      PathInfo info;
      memset((char*)&info, 0, sizeof(PathInfo));
      EXPAND_PATH_INFO(&info, path);

      // Check/act on iperms from expanded_path_info_structure, this op requires RM
      CHECK_PERMS(info.ns, (R_META));
   }

   // Even if the directory has been deleted we still need to close
   // the DIR*; however, we can't expand the path info since we don't
   // have a path anymore. We just skip that step in this case and
   // close the directory.
   //
   // The only time the CHECK_PERMS call above is skipped is if
   // path="-" In this case the directory has been deleted and we can
   // safely skip the permission checks since they were done by opendir
   // for RM and rmdir for RM|WM. The following closedir will not read
   // or write the metadata.
   
   // No need for access check, just try the op
   // Appropriate  closedir call filling in fuse structure
   if (! dh->use_it) {
      LOG(LOG_INFO, "not root-dir\n");
      TRY0( closedir_md(dh) );
   }

   EXIT();
   return 0;
}


// *** this may not be needed until we implement user xattrs in the fuse daemon ***
//
// Kernel calls this with key 'security.capability'
//
int marfs_removexattr (const char* path,
                       const char* name) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = EACCES;
      return -1;
   }

   // *** make sure they aren’t removing a reserved xattr***
   if (! strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize)) {
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // Appropriate  removexattr call filling in fuse structure 
   TRY0( MD_PATH_OP(lremovexattr, info.ns, info.post.md_path, name) );

   EXIT();
   return 0;
}


int marfs_rename (const char* path,
                  const char* to) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   PathInfo info2;
   memset((char*)&info2, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info2, to);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns,  (R_META | W_META));
   CHECK_PERMS(info2.ns, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "src is_root_ns\n");
      errno = EPERM;
      return -1;
   }
   if (IS_ROOT_NS(info2.ns)) {
      LOG(LOG_INFO, "dst is_root_ns\n");
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // Appropriate  rename call filling in fuse structure 
   TRY0( MD_PATH_OP(rename, info.ns, info.post.md_path, info2.post.md_path) );

   EXIT();
   return 0;
}

// using looked up mdpath, do statxattr and get object name
//
// NOTE: directories don't go to the trash.  All deleted files contain
//     metadata about their original path, so undeleting files (TBD) would
//     also recreate any required directories.
//
int marfs_rmdir (const char* path) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // Appropriate rmdirlike call filling in fuse structure 
   TRY0( MD_D_PATH_OP(rmdir, info.ns, info.post.md_path) );

   EXIT();
   return 0;
}


int marfs_setxattr (const char* path,
                    const char* name,
                    const char* value,
                    size_t      size,
                    int         flags) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = EPERM;
      return -1;
   }

   // *** make sure they aren’t setting a reserved xattr***
   if ( !strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize) ) {
      LOG(LOG_ERR, "denying reserved setxattr(%s, %s, ...)\n", path, name);
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // Appropriate  setxattr call filling in fuse structure 
   TRY0( MD_PATH_OP(lsetxattr, info.ns,
                    info.post.md_path, name, value, size, flags) );

   EXIT();
   return 0;
}

// The OS seems to call this from time to time, with <path>=/ (and
// euid==0).  We could walk through all the namespaces, and accumulate
// total usage.  (Maybe we should have a top-level fsinfo path?)  But I
// guess we don't want to allow average users to do this.
int marfs_statvfs (const char*      path,
                   struct statvfs*  statbuf) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns, (R_META));

   // wipe caller's statbuf
   memset(statbuf, 0, sizeof(struct statvfs));


   // Gather total storage accounted for in fsinfo files of all namespaces.
   //
   // TBD: The tool that crawls the MDFS and updates fsinfo for all the
   //      namespaces (truncating to the size of storage used by all
   //      non-trash files in that namespace), coould also update one for
   //      the "root" namespace (i.e. truncating to a size that is the sum
   //      of all the fsinfos in all the namespaces).  Then we could do
   //      this with a single stat.
   // 
   NSIterator       it = namespace_iterator();
   MarFS_Namespace* root_ns = NULL;
   MarFS_Namespace* ns;

#if 0
   // Accumulate sum of storage noted in all the fsinfo files.
   // Also, find the root namespace.
   size_t           used = 0;
   struct stat      st;
   for (ns = namespace_next(&it);
        ns;
        ns = namespace_next(&it)) {

      LOG(LOG_INFO, "checking fsinfo for NS '%s'\n", ns->name);
      if (IS_ROOT_NS(info.ns)) {
         root_ns = ns;          // save for later
         continue;
      }
      else if (! ns->fsinfo_path)
         continue;
      else if (stat(ns->fsinfo_path, &st)) {
         LOG(LOG_ERR, "couldn't stat '%s' for NS %s (%s)\n",
             ns->fsinfo_path, ns->name, strerror(errno));
      }
      else {
         used += st.st_size;
         LOG(LOG_INFO, "fsinfo for '%s' = %lu\n", ns->name, st.st_size);
      }
   }
   LOG(LOG_INFO, "used = %lu\n", used);

#else
   // Just find the root namespace.  We do not currently make use of the
   // accumulated sum of used storage recorded in the fsinfo files.
   if (IS_ROOT_NS(info.ns))
      root_ns = info.ns;
   else {
      for (ns = namespace_next(&it);
           ns;
           ns = namespace_next(&it)) {

         if (IS_ROOT_NS(ns)) {
            root_ns = ns;          // save for later
            break;
         }
      }
   }
#endif

   // Get free/used info regarding inodes from the MDFS.
   //
   // The "root" filesystem now can have its md_path set to the top of the
   // MDFS (e.g. to the root fileset in a GPFS installation, where the
   // other namespaces are explicit filesets under that, or elsewhere.
   //
   // statvfs() supports NFS exports, where we might want to export
   // individual namespaces, or the root namespace.  We'll try to return
   // statvfs of the md_path, if possible.  Otherwise, fall back to the
   // root NS.  If that fails, return defaults that suggest we've got tons
   // of room.  (Real quotas will be enforced when they try to create new
   // files.)
   //
   // TBD: We use the MDFS as the basis for blocksize.  Gary suggests we
   //      might be able to improve performance by reporting something else
   //      here.

   if (IS_ROOT_NS(info.ns)
       && stat_regular(&info)) {

      // backward compatibility, for old configs that still have non-existent
      // md_path installed into root NS.
      LOG(LOG_INFO, "couldn't stat md_path '%s' for root ns '%s' %s\n",
          info.ns->md_path, info.ns->name, strerror(errno));

      // defaults, if we couldn't statvfs root_ns->md_path
      // lifted from stavfs("/gpfs/marfs-gpfs")
      static const unsigned long BSIZE = 16384;        // from GPFS
      statbuf->f_bsize   = BSIZE; /* file system block size */
      statbuf->f_frsize  = BSIZE; /* fragment size */
      statbuf->f_blocks  = ULONG_MAX/BSIZE;     /* size of fs in f_frsize units */
      statbuf->f_bfree   = ULONG_MAX/BSIZE;     /* # free blocks */
      statbuf->f_bavail  = ULONG_MAX/BSIZE;     /* # free blocks for non-root */
      statbuf->f_files   = 1;     /* # inodes */
      statbuf->f_ffree   = ULONG_MAX -1;     /* # free inodes */
      statbuf->f_favail  = ULONG_MAX -1;     /* # free inodes for non-root */
      statbuf->f_fsid    = 0;     /* file system ID */
      statbuf->f_flag    = 0;     /* mount flags */
      statbuf->f_namemax = 255;     /* maximum filename length */
   }
   else {
      TRY0( MD_PATH_OP(statvfs, info.ns, info.ns->md_path, statbuf) );
   }

   EXIT();
   return 0;
}


// NOTE: <target> is given as a full path.  It might or might not be under
//     our fuse mount-point, but even if it is, we should just stuff
//     whatever <target> we get into the symlink.  If it is something under
//     a marfs mount, then marfs_readlink() will be called when the link is
//     followed.

int marfs_symlink (const char* target,
                   const char* linkname) {
   ENTRY();

   // <linkname> is given to us as a path under the fuse-mount,
   // in the usual way for fuse-functions.
   LOG(LOG_INFO, "linkname: %s\n", linkname);
   PathInfo lnk_info;
   memset((char*)&lnk_info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&lnk_info, linkname);   // (okay if this file doesn't exist)

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(lnk_info.ns, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(lnk_info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = EPERM;
      return -1;
   }

   // don't allow creating a link that explicitly points below <mdfs_top>
   // Maybe it should be allowed?  The fuse unshare will prevent access to
   // it anyhow.  But what are they up to?
   if (under_mdfs_top(target)) {
      LOG(LOG_ERR, "symlink target is under mdfs_top '%s'\n",
          marfs_config->mdfs_top);
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // Appropriate  symlink call filling in fuse structure
   TRY0( MD_PATH_OP(symlink, lnk_info.ns, target, lnk_info.post.md_path) );

   EXIT();
   return 0;
}

// *** this may not be needed until we implement write in the fuse daemon ***
int marfs_truncate (const char* path,
                    off_t       size) {
   ENTRY();

   // Check/act on truncate-to-zero only.
   if (size) {
      errno = EPERM;
      return -1;
   }

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns, (R_META | W_META | R_DATA | T_DATA));

   // Call access syscall to check/act if allowed to truncate for this user 
   ACCESS(info.ns, info.post.md_path, (W_OK));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = EPERM;
      return -1;
   }

   // If this is not just a normal md, it's the file data
   STAT_XATTRS(&info); // to get xattrs
   if (! has_any_xattrs(&info, MARFS_ALL_XATTRS)) {
      LOG(LOG_INFO, "no xattrs\n");
      TRY0( MD_PATH_OP(truncate, info.ns, info.post.md_path, size) );
      return 0;
   }

   // copy metadata to trash, resets original file zero len and no xattr
   TRASH_TRUNCATE(&info, path);

   // (see marfs_mknod() -- empty non-DIRECT file needs *some* marfs xattr,
   // so marfs_open() won't assume it is a DIRECT file.)
   if (info.ns->iwrite_repo->access_method != ACCESSMETHOD_DIRECT) {
      LOG(LOG_INFO, "marking with RESTART, so open() won't think DIRECT\n");
      info.xattrs |= XVT_RESTART;
      SAVE_XATTRS(&info, XVT_RESTART);
   }
   else
      LOG(LOG_INFO, "iwrite_repo.access_method = DIRECT\n");

   EXIT();
   return 0;
}


int marfs_unlink (const char* path) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns, (R_META | W_META | R_DATA | U_DATA));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = EPERM;
      return -1;
   }

   // Call access() syscall to check/act if allowed to unlink for this user 
   //
   // PROBLEM: access() follows symlinks.  We are unlinking the
   //       symlink, not the thing it points to.  trash_unlink()
   //       just unlinks them outright (because they don't have
   //       xattrs).  There's also untested code there, to try to
   //       move symlinks to the trash.  If you want to do that,
   //       look at trash_unlink().
   //
   //       Meanwhile, we can skip access().

   STAT(&info);

   // rename file with all xattrs into trashdir, preserving objects and paths 
   TRASH_UNLINK(&info, path);

   EXIT();
   return 0;
}


// deprecated in 2.6
// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
//
// NOTE If opened N:1 (identifiable because the marfs_objid xattr will have
//     obj-type OBJ_Nto1), this is our chance to reconcile things that
//     parallel writers couldn't do without locking, such as xattrs, and
//     file-size.  It's even possible that only one chunk was written, so
//     we couldn't know for sure that the POST.obj_type should be Multi,
//     until now.  (We can find out by counting MultiChunkInfos in the
//     metadata file.)
//
int marfs_utime(const char*     path,
                struct utimbuf* buf) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure
   // NOTE: we're assuming expanded path is absolute, so dirfd is ignored
   TRY_GE0( MD_PATH_OP(utime, info.ns, info.post.md_path, buf) );

   EXIT();
   return 0;
}

// If pftool uses marfs_utime(), we'll only set the destination atime/mtime
// to the second, truncating the microseconds component.  Comparisons will
// still show them the same (if we use st.st_atime, instead of st.st_atim),
// but everyone will wonder why the microseconds are always different.
int marfs_utimensat(const char*           path,
                    const struct timespec times[2],
                    int                   flags) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure
   // NOTE: we're assuming expanded path is absolute, so dirfd is ignored
   TRY_GE0( MD_PATH_OP(utimensat, info.ns,
                       AT_FDCWD, info.post.md_path, times, flags) );

   EXIT();
   return 0;
}


// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
//
// NOTE If opened N:1 (identifiable because the marfs_objid xattr will have
//     obj-type OBJ_Nto1), this is our chance to reconcile things that
//     parallel writers couldn't do without locking, such as xattrs, and
//     file-size.  It's even possible that only one chunk was written, so
//     we couldn't know for sure that the POST.obj_type should be Multi,
//     until now.  (We can find out by counting MultiChunkInfos in the
//     metadata file.)
//
int marfs_utimens(const char*           path,
                  const struct timespec tv[2]) {   
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns, (R_META | W_META));

   // The "root" namespace is artificial
   if (IS_ROOT_NS(info.ns)) {
      LOG(LOG_INFO, "is_root_ns\n");
      errno = EPERM;
      return -1;
   }

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure
   // NOTE: we're assuming expanded path is absolute, so dirfd is ignored
   TRY_GE0( MD_PATH_OP(utimensat, info.ns,
                       0, info.post.md_path, tv, AT_SYMLINK_NOFOLLOW) );

   EXIT();
   return 0;
}


ssize_t marfs_write(const char*        path,
                    const char*        buf,
                    size_t             size,
                    off_t              offset,
                    MarFS_FileHandle*  fh) {
   ENTRY();

   LOG(LOG_INFO, "%s\n", path);
   LOG(LOG_INFO, "offset: (%ld)+%ld, size: %ld\n", fh->open_offset, offset, size);

   PathInfo*         info = &fh->info;                  /* shorthand */
   ObjectStream*     os   = &fh->os;

   // NOTE: It seems that expanding the path-info here is unnecessary.
   //    marfs_open() will already have done this, and if our path isn't
   //    the same as what marfs_open() saw, that's got to be a bug in fuse.
   //
   //   EXPAND_PATH_INFO(info, path);
   

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info->ns, (R_META | W_META | R_DATA | W_DATA));

   // No need to call access as we called it in open for write
   // Make sure security is set up for accessing the selected repo

   // If file has no xattrs its just a normal use the md file for data,
   //   just do the write and return, don’t bother with all this stuff below
   if (! has_any_xattrs(info, MARFS_ALL_XATTRS)
       && (info->pre.repo->access_method == ACCESSMETHOD_DIRECT)) {
      LOG(LOG_INFO, "no xattrs, and DIRECT: writing to file\n");
      TRY_GE0( MD_FILE_OP(write, fh, buf, size) );

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
   //
   size_t log_offset = (fh->open_offset + os->written - fh->write_status.sys_writes);
   if ( offset != log_offset) {

      LOG(LOG_ERR, "non-contig write: offset %ld, after %ld (+ %ld)\n",
          offset, log_offset, fh->write_status.sys_writes);
#if 0
      // NFS EXPERIMENT.  Multiple NFS threads on the server-side make
      // calls to write() at different offsets, writing their own buffers.
      // The blind assumption that the underlying file-system supports
      // sparse files (aka N-to-1) is mistaken, in our case.
      //
      // Q: what if we just return 0?  Will that get NFS to back off?  (It
      //   is, of course, perfectly legitimate for us to return 0 as the
      //   number of bytes that were written.)
      //
      // A: No.  NFS returns an error to the client, if we do this.
      return 0;
#else
      errno = EINVAL;
      return -1;
#endif
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
   // without knowing there would be more written.  This works for both
   // cases.)
   if (os->flags & OSF_CLOSED) {

      // NEW MULTI: When writing through fuse, and object first transitions
      // to Multi, save objid, so, if fuse crashes, the resulting object(s)
      // can still be GC'ed by crawling the MD chunkinfo.  We aren't going
      // to maintain POST every time we write a new object for this multi
      // (e.g. chunks, and chunk_info_bytes), so it will always be wrong,
      // until release().  However, trash_truncate() can generate a POST,
      // for incomplete files (i.e. if fuse crashes before we close thyis
      // object), so GC can have correct info.
      if ((info->post.obj_type != OBJ_MULTI) // implies fuse
          && (info->pre.chunk_no == 0)) {

         SAVE_XATTRS(info, XVT_PRE);
      }
      // MarFS file-type is definitely "Multi", now
      info->post.obj_type = OBJ_MULTI;
      info->pre.chunk_no += 1;

      // update the URL in the ObjectStream, in our FileHandle
      TRY0( update_pre(&info->pre) );

      // NOTE: stream_open() potentially wipes ObjectStream.written.  We
      //     want this field to track the total amount written, across all
      //     chunks, within a given open(), so we preserve it.
      size_t   open_size  = get_stream_wr_open_size(fh, 1);
      uint16_t wr_timeout = info->pre.repo->write_timeout;

      TRY0( open_data(fh, OS_PUT, 0, open_size, 1, wr_timeout) );
   }
   else if(! (os->flags & OSF_OPEN)) {

      // Then this is the first write on a new file (or the first
      // write that is "overwriting" an existing file). We don't
      size_t   open_size  = get_stream_wr_open_size(fh, 0);
      uint16_t wr_timeout = info->pre.repo->write_timeout;

      TRY0( open_data(fh, OS_PUT, 0, open_size, 0, wr_timeout) );
   }

   // Span across objects, for "Multi" format.  Repo.chunk_size is the
   // maximum size of an individual object written by MarFS (including
   // user-inaccessible recovery-info, written at the end).  We refer to
   // the "logical end" of a block as the place where recovery info begins.
   // Similarly, the logical offset is the amount of user data (not
   // counting recovery info) in all chunks of a multi, up to some point.
   // Here, <log_end> refers to the offset in the user's data corresponding
   // with the logical-end of a chunk.
   //
   // If this write goes past the logical end of this chunk, then write as
   // much data into this object as can fit (minus size of recovery-info),
   // write the recovery-info into the tail, close the object, open a new
   // one, and resume writing there.
   //
   // For each chunk of a Multi object, we also write a corresponding copy
   // of the object-ID into the MD file.  The MD file is not opened for us
   // in marfs_open(), because it wasn't known until now that we'd need it.
   size_t       write_size = size;
   const size_t recovery   = MARFS_REC_UNI_SIZE; // written in tail of object
   size_t       log_end    = (info->pre.chunk_no +1) * (info->pre.chunk_size - recovery);
   char*        buf_ptr    = (char*)buf;

   while (write_size && ((log_offset + write_size) >= log_end)) {

      // write <fill> more bytes, to fill this object
      ssize_t fill   = log_end - log_offset;
      size_t  remain = write_size - fill; // remaining after <fill>

      LOG(LOG_INFO, "iterating: "
          "loff=%ld, wr=%ld, fill=%ld, remain=%ld, chnksz=%ld, rec=%ld\n",
          log_offset, write_size, fill, remain, info->pre.chunk_size, recovery);

      // possible silly config: (recovery > chunk_size)
      // This config is now ruled out by validate_config() ?
      if (fill <= 0) {
         LOG(LOG_ERR, "fill=%ld <= 0  (wr=%ld, writ=%ld-%ld)\n",
             fill, write_size, os->written, fh->write_status.sys_writes);
         errno = EIO;
         return -1;
      }


      TRY_GE0( DAL_OP(put, fh, buf_ptr, fill) );
      buf_ptr    += fill;
      log_offset += fill;

      TRY_GE0( write_recoveryinfo(os, info, fh) );


      // close the object
      LOG(LOG_INFO, "closing chunk: %ld\n", info->pre.chunk_no);
      TRY0( close_data(fh, 0, 0) );

      // MD file gets per-chunk information
      // pftool (OBJ_Nto1) will install chunkinfo directly.
      if (info->pre.obj_type != OBJ_Nto1) {
         TRY0( write_chunkinfo(fh,
                               // fh->open_offset,
                               (os->written - fh->write_status.sys_writes),
                               0) );
      }

      // keep count of amount of real chunk-info written into MD file
      info->post.chunk_info_bytes += sizeof(MultiChunkInfo);


      // if we still have more data to write, prepare for next iteration
      if (remain) {

         // NEW MULTI: (see earlier comment re "NEW MULTI")
         if ((info->post.obj_type != OBJ_MULTI) // implies fuse
             && (info->pre.chunk_no == 0)) {

            SAVE_XATTRS(info, XVT_PRE);
         }
         // MarFS file-type is definitely "Multi", now
         info->post.obj_type = OBJ_MULTI;

         // update chunk-number, and generate the new obj-id
         info->pre.chunk_no += 1;

         // update the URL in the ObjectStream, in our FileHandle
         TRY0( update_pre(&info->pre) );

         // pos in user-data corresponding to the logical end of a chunk
         log_end += (info->pre.repo->chunk_size - recovery);

         // open next chunk
         //
         // NOTE: stream_open() potentially wipes ObjectStream.written.  We
         //     want this field to track the total amount written, across all
         //     chunks, within a given open(), so we preserve it.
         size_t   open_size  = get_stream_wr_open_size(fh, 1);
         uint16_t wr_timeout = info->pre.repo->write_timeout;

         TRY0( open_data(fh, OS_PUT, 0, open_size, 1, wr_timeout) );
      }

      // compute limits of new chunk
      write_size = remain;
   }
   LOG(LOG_INFO, "done iterating (with final %ld)\n", write_size);


   // write more data into object. This amount doesn't finish out any
   // object, so don't write chunk-info to MD file.
   if (write_size)
      TRY_GE0( DAL_OP(put, fh, buf_ptr, write_size) );

#if 0
   // EXPERIMENT for NFS
   //
   // Q: do we have to maintain the size of the file, so that getattr()
   //    will see it?
   //
   // A: Not sure.  Seemed to work okay without this costly addition.
   //    However, NFS is not playing nicely in other departments, so this
   //    point has become moot.

   if ((fh->flags & FH_WRITING)
       && has_any_xattrs(info, MARFS_ALL_XATTRS)
       && !(fh->flags & FH_Nto1_WRITES)) {

      off_t size = os->written - fh->write_status.sys_writes;
      TRY0( MD_PATH_OP(truncate, info->ns, info->post.md_path, size) );
   }
#endif

   EXIT();
   return size;
}




// ---------------------------------------------------------------------------
// unimplemented routines, for now
// ---------------------------------------------------------------------------

#if 0

int marfs_bmap(const char* path,
               size_t      blocksize,
               uint64_t*   idx) {
   ENTRY();
   // don’t support  its is for block mapping
   EXIT();
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

int marfs_create(const char*        path,
                 mode_t             mode,
                 MarFS_FileHandle*  fh) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure
   //   If readonly RM  RD 
   //   If wronly/rdwr/trunk  RMWMRDWD
   //   If append we don’t support that
   if (info.flags & (O_RDONLY)) {
      ACCESS(info.ns, info.post.md_path, W_OK);
      CHECK_PERMS(info.ns, (R_META | R_DATA));
   }
   else if (info.flags & (O_WRONLY)) {
      ACCESS(info.ns, info.post.md_path, W_OK);
      CHECK_PERMS(info.ns, (R_META | W_META | | R_DATA | W_DATA));
   }

   if (info.flags & (O_APPEND | O_RDWR)) {
      errno = EPERM;
      return -1;
   }
   if (info.flags & (O_APPEND | O_TRUNC)) { /* can this happen, with create()? */
      CHECK_PERMS(info.ns, (T_DATA));
   }


   // Check/act on iperms from expanded_path_info_structure, this op
   // requires RMWM
   //
   // NOTE: We assure that open(), if called after us, can't discover that
   //       user lacks sufficient access.  However, because we don't know
   //       what the open call might be, we may be imposing
   //       more-restrictive constraints than necessary.
   //
   //   CHECK_PERMS(info.ns, (R_META | W_META));
   CHECK_PERMS(info.ns, (R_META | W_META | R_DATA | W_DATA | T_DATA));

   // Check/act on quota num names
   // No need for access check, just try the op
   // Appropriate mknod-like/open-create-like call filling in fuse structure
   TRY0( MD_PATH_OP(mknod, info.ns, info.post.md_path, mode, rdev) );

   EXIT();
   return 0;
}


// obsolete, in fuse 2.6
int marfs_fallocate(const char*        path,
                    int                mode,
                    off_t              offset,
                    off_t              length,
                    MarFS_FileHandle*  fh) {
   ENTRY();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns, (R_META | W_META | R_DATA | W_DATA));

   // Check space quota
   //    If  we get here just return ok
   //    this is just a check to see if you can write to the fs
   EXIT();
   return 0;
}


// this is really fstat() ??
//
// Instead of using the <fd> (which is not yet implemented in our
// FileHandle), I'm calling fstat on the file itself.  Good enough?
int marfs_fgetattr(const char*        path,
                   struct stat*       st,
                   MarFS_FileHandle*  fh) {
   ENTRY();

   // don’t need path info    (this is for a file that is open, so everything is resolved)
   // don’t need to check on IPERMS
   // No need for access check, just try the op
   // appropriate fgetattr/fstat call filling in fuse structure (dont mess with xattrs)
   PathInfo*         info = &fh->info;                  /* shorthand */

   // While this is being called on an open file, the op is still
   // context free since we are using lstat to get the file attrs and
   // that operates on raw pathnames.
   TRY0( MD_PATH_OP(lstat, info->ns, info->post.md_path, st) );

   EXIT();
   return 0;
}

int marfs_flock(const char*        path,
                MarFS_FileHandle*  fh,
                int                op) {
   ENTRY();

   // don’t implement or throw error
   EXIT();
   return 0;
}


int marfs_link (const char* path,
                const char* to) {
   ENTRY();

   // for now, I think we should not allow link, its pretty complicated to do
   LOG(LOG_INFO, "link(%s, ...) not implemented\n", path);
   EXIT();
   errno = ENOSYS;
   return -1;
}


int marfs_lock(const char*        path,
               MarFS_FileHandle*  fh,
               int                cmd,
               struct flock*      locks) {
   ENTRY();

   // don’t support it, either don’t implement or throw error
   LOG(LOG_INFO, "lock(%s, ...) not implemented\n", path);
   EXIT();
   errno = ENOSYS;
   return -1;
}


#endif


