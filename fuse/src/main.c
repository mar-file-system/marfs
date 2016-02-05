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
#include "marfs_base.h"
#include "marfs_ops.h"

#include <sys/stat.h>
#include <stdlib.h>
#include <attr/xattr.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <utime.h>              /* for deprecated marfs_utime() */
#include <stdio.h>
#include <time.h>

// syscall(2) manpage says do this
#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

// #include "marfs_fuse.h"
#define FUSE_USE_VERSION 26
#include <fuse.h>


// ---------------------------------------------------------------------------
// utilities
//
// These are some chunks of code used in more than one fuse function.
// Should probably be moved to another file, eventually.
// ---------------------------------------------------------------------------



// ...........................................................................
// rev 4
// ...........................................................................

#include <grp.h>
#include <pwd.h>
#include <limits.h>             // NGROUPS_MAX

// using "saved" uid/gid to hold the state, so no state needed in Context.
// using syscalls to get thread-safety, based on this:
// http://stackoverflow.com/questions/1223600/change-uid-gid-only-of-one-thread-in-linux

typedef struct PerThreadContext {
   int   pushed;                 // detect double-push
   int   pushed_groups;          // need to pop groups, too?

   gid_t groups[NGROUPS_MAX];    // orig group list of *process*
   int   group_ct;               // size of <groups>
} PerThreadContext;


#define PUSH_USER(GROUPS_TOO)                                           \
   ENTRY();                                                             \
   PerThreadContext ctx;                                                \
   memset(&ctx, 0, sizeof(PerThreadContext));                           \
   __TRY0(push_user4, &ctx, (GROUPS_TOO))

#define POP_USER()                                                      \
   __TRY0(pop_user4, &ctx);                                             \
   EXIT()


// If <push_groups> is non-zero, we also save the current per-process
// group-list, lookup the group-list for the new euid, and install that as
// the process group-list.  Apparently, Linux processes have a group-list.
// Setting euid/egid doesn't change the set of groups associated with this
// process (well, setegid() adds one element).  Presumably, this is an
// efficiency hack, to avoid having to lookup groups all the time.
// However, it means that after setting euid/eguid, files that ought to be
// inaccessible to the new user, based on group-access, might be
// accessible.
//
// For example, if we start fuse as root, the initial process group-list is
// just (0).  After seteuid(500), setegid(500), the process group-list is
// (500, 0).  This user will be able to read the following directory:
//
//    drwxrwx---. 2 root root 4096 Jan 21 14:49 secret_root_stuff
//
// However, chgrp 500 on this file will fail:
//
//    -rwxrwx---. 1 500 502 0 Jan 15 08:31 share_with_502
//
// even if user 500 is a member of group 502.  That's because the
// group-list of the process is (500, 0), which doesn't include 502.
//
// The solution to both these problems is to have push_user() save the
// original process group-list, go get the group-list for user 500, and
// install it as the process group-list.  We have to do this before
// seteuid(), because it requires reading /etc/passwd.  Then, we undo these
// actions in pop_user().
//
// That sounds expensive.  We probably don't want to do it when we don't
// have to, like in write().  So, we'll make it an option.

int push_groups4(PerThreadContext* ctx) {

   TRY_DECLS();
   uid_t uid = fuse_get_context()->uid;
   gid_t gid = fuse_get_context()->gid;

   // check for two pushes without a pop
   if (ctx->pushed_groups) {
      LOG(LOG_ERR, "double-push (groups) -> %u\n", uid);
      errno = EPERM;
      return -1;
   }

   // save the group-list for the current process
   ctx->group_ct = getgroups(sizeof(ctx->groups), ctx->groups);
   if (ctx->group_ct < 0) {
      // ctx->group_ct = 0; // ?
      LOG(LOG_ERR, "getgroups() failed\n");
      return -1;
   }

   // find user-name from uid
   struct passwd  pwd;
   struct passwd* result;
   const size_t   STR_BUF_LEN = 1024; // probably enough
   char           str_buf[STR_BUF_LEN];
   if (getpwuid_r(uid, &pwd, str_buf, STR_BUF_LEN, &result)) {
      LOG(LOG_ERR, "getpwuid_r() failed: %s\n", strerror(errno));
      return -EINVAL;
   }
   else if (! result) {
      LOG(LOG_ERR, "No passwd entries found, for uid %u\n", uid);
      return -EINVAL;
   }
   LOG(LOG_INFO, "uid %u = user '%s'\n",
       uid, result->pw_name);
      

   // find group-membership of user, using user-name
   gid_t groups[NGROUPS_MAX +1];
   int   ngroups = NGROUPS_MAX +1;
   int   group_ct = getgrouplist(result->pw_name, gid, groups, &ngroups);
   if (group_ct < 0) {
      LOG(LOG_ERR, "No passwd entries found, for user '%s'\n",
          result->pw_name);
      return -1;
   }

   // DEBUGGING
   int i;
   for (i=0; i<group_ct; ++i) {
      LOG(LOG_INFO, "group = %u\n", groups[i]);
   }

   // change group membership of process
   TRY0(setgroups, ngroups, groups);

   ctx->pushed_groups = 1;   // so we can pop
   return 0;
}


int push_user4(PerThreadContext* ctx, int push_groups) {
#  if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   int rc;

   // check for two pushes without a pop
   if (ctx->pushed) {
      LOG(LOG_ERR, "double-push -> %u\n", fuse_get_context()->uid);
      errno = EPERM;
      return -1;
   }

   // --- maybe install the user's group-list (see comments, above)
   if (push_groups) {
      if (push_groups4(ctx)) {
         return -1;
      }
   }

   // get current real/effective/saved gid
   gid_t old_rgid;
   gid_t old_egid;
   gid_t old_sgid;

   rc = syscall(SYS_getresgid, &old_rgid, &old_egid, &old_sgid);
   if (rc) {
      LOG(LOG_ERR, "getresgid() failed\n");
      exit(EXIT_FAILURE);       // fuse should fail
   }

   // gid to push
   gid_t new_egid = fuse_get_context()->gid;

   // install the new egid, and save the current one
   LOG(LOG_INFO, "gid %u(%u) -> (%u)\n", old_rgid, old_egid, new_egid);
   rc = syscall(SYS_setresgid, -1, new_egid, old_egid);
   if (rc == -1) {
      if ((errno == EACCES) && ((new_egid == old_egid) || (new_egid == old_rgid))) {
         LOG(LOG_INFO, "failed (but okay)\n"); /* okay [see NOTE] */
      }
      else {
         LOG(LOG_ERR, "failed!\n");
         return -1;             // no changes were made
      }
   }

   // get current real/effect/saved uid
   uid_t old_ruid;
   uid_t old_euid;
   uid_t old_suid;

   rc = syscall(SYS_getresuid, &old_ruid, &old_euid, &old_suid);
   if (rc) {
      LOG(LOG_ERR, "getresuid() failed\n");
      exit(EXIT_FAILURE);       // fuse should fail
   }

   // uid to push
   gid_t new_euid = fuse_get_context()->uid;

   // install the new euid, and save the current one
   LOG(LOG_INFO, "uid %u(%u) -> (%u)\n", old_ruid, old_euid, new_euid);
   rc = syscall(SYS_setresuid, -1, new_euid, old_euid);
   if (rc == -1) {
      if ((errno == EACCES) && ((new_euid == old_ruid) || (new_euid == old_euid))) {
         LOG(LOG_INFO, "failed (but okay)\n");
         return 0;              /* okay [see NOTE] */
      }

      // try to restore gid from before push
      rc = syscall(SYS_setresgid, -1, old_egid, -1);
      if (! rc) {
         LOG(LOG_ERR, "failed!\n");
         return -1;             // restored to initial conditions
      }
      else {
         LOG(LOG_ERR, "failed -- couldn't restore egid %d!\n", old_egid);
         exit(EXIT_FAILURE);  // don't leave thread stuck with wrong egid
         // return -1;
      }
   }

   return 0;
#  else
#  error "No support for seteuid()/setegid()"
#  endif
}


// restore the group-membership of the current process to what it was
// before push_groups4()
int pop_groups4(PerThreadContext* ctx) {
   TRY_DECLS();

   // DEBUGGING
   int i;
   for (i=0; i<ctx->group_ct; ++i) {
      LOG(LOG_INFO, "group = %u\n", ctx->groups[i]);
   }

   TRY0(setgroups, ctx->group_ct, ctx->groups);

   ctx->pushed_groups = 0;
   return 0;
}

//  pop_user() changes the effective UID.  Here, we revert to the
//  euid from before the push.
int pop_user4(PerThreadContext* ctx) {
#  if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   int rc;

   uid_t old_ruid;
   uid_t old_euid;
   uid_t old_suid;

   rc = syscall(SYS_getresuid, &old_ruid, &old_euid, &old_suid);
   if (rc) {
      LOG(LOG_ERR, "getresuid() failed\n");
      exit(EXIT_FAILURE);       // fuse should fail
   }

   LOG(LOG_INFO, "uid (%u) <- %u(%u)\n", old_suid, old_ruid, old_euid);
   rc = syscall(SYS_setresuid, -1, old_suid, -1);
   if (rc == -1) {
      if ((errno == EACCES) && ((old_suid == old_ruid) || (old_suid == old_euid))) {
         LOG(LOG_INFO, "failed (but okay)\n"); /* okay [see NOTE] */
      }
      else {
         LOG(LOG_ERR, "failed\n");
         exit(EXIT_FAILURE);    // don't leave thread stuck with wrong euid
         // return -1;
      }
   }



   gid_t old_rgid;
   gid_t old_egid;
   gid_t old_sgid;

   rc = syscall(SYS_getresgid, &old_rgid, &old_egid, &old_sgid);
   if (rc) {
      LOG(LOG_ERR, "getresgid() failed\n");
      exit(EXIT_FAILURE);       // fuse should fail
   }

   LOG(LOG_INFO, "gid (%u) <- %u(%u)\n", old_sgid, old_rgid, old_euid);
   rc = syscall(SYS_setresgid, -1, old_sgid, -1);
   if (rc == -1) {
      if ((errno == EACCES) && ((old_sgid == old_rgid) || (old_sgid == old_egid))) {
         LOG(LOG_INFO, "failed (but okay)\n");
         return 0;              /* okay [see NOTE] */
      }
      else {
         LOG(LOG_ERR, "failed!\n");
         exit(EXIT_FAILURE);    // don't leave thread stuck with wrong egid
         // return -1;
      }
   }



   if (ctx->pushed_groups) {
      if (pop_groups4(ctx))
         return -1;
   }


   return 0;
#  else
#  error "No support for seteuid()/setegid()"
#  endif
}








// --- wrappers just call the corresponding library-function, to support a
//     given fuse-function.  The library-functions are meant to be used by
//     both fuse and pftool, so they don't do seteuid(), and don't expect
//     any fuse-related structures.

#define WRAP_internal(GROUPS_TOO, FNCALL)              \
   PUSH_USER(GROUPS_TOO);                              \
   int fncall_rc = FNCALL;                             \
   POP_USER();                                         \
   if (fncall_rc < 0) {                                \
      LOG(LOG_ERR, "ERR %s, errno=%d '%s'\n",          \
          #FNCALL, errno, strerror(errno));            \
      return -errno;                                   \
   }                                                   \
   return fncall_rc /* caller provides semi */

// This version doesn't call push/pop_groups()
#define WRAP(FNCALL)                                   \
   WRAP_internal(0, (FNCALL));

// This version DOES call push/pop_groups()
#define WRAP_PLUS(FNCALL)                              \
   WRAP_internal(1, (FNCALL));




// ---------------------------------------------------------------------------
// inits
// ---------------------------------------------------------------------------

// "The return value of this function is available to all file operations
// in the private_data field of fuse_context."  (e.g. via get_fuse_context())
// [http://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html]
//
void* marfs_fuse_init(struct fuse_conn_info* conn) {
   conn->max_write = MARFS_WRITEBUF_MAX;
   conn->want      = FUSE_CAP_BIG_WRITES;

   // To disable: Set zero here, and clear FUSE_CAP_ASYNC_READ from <want>
   conn->async_read = 0;

   return conn;
}

// called when fuse file system exits
void marfs_fuse_exit (void* private_data) {
   // nothing for us to do here, we wont have dirty data when the fuse
   // daemon exits. I suppose they wait for all threads to finish before
   // leaving, so this should be ok.
   LOG(LOG_INFO, "shutting down\n");
}





// ---------------------------------------------------------------------------
// Fuse routines in alpha order (so you can actually find them)
// Unimplmented functions are gathered at the bottom
// ---------------------------------------------------------------------------



int fuse_access (const char* path,
                 int         mask) {

   WRAP_PLUS( marfs_access(path, mask) );
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

int fuse_chmod(const char* path,
               mode_t      mode) {

   WRAP_PLUS( marfs_chmod(path, mode) );
}


// int fuse_chown_helper(const char* path,
//                       uid_t       uid,
//                       gid_t       gid) {
//    ENTRY();
//    TRY0(setegid, gid);
//    rc = marfs_chown(path, uid, gid);
//    EXIT();
//    return rc;
// }
int fuse_chown (const char* path,
                uid_t       uid,
                gid_t       gid) {

   //   WRAP( marfs_chown(path, uid, gid) );
   //   //   WRAP( fuse_chown_helper(path, uid, gid) );
   //   // TBD: WRAP2( gid, fuse_chown_helper(path, uid, gid) );
   WRAP_PLUS( marfs_chown(path, uid, gid) );
}

// int fuse_close()   --->  it's called "fuse_release()".


int fuse_fsync (const char*            path,
                int                    isdatasync,
                struct fuse_file_info* ffi) {

   WRAP( marfs_fsync(path, isdatasync, (MarFS_FileHandle*)ffi->fh) );
}


int fuse_fsyncdir (const char*            path,
                   int                    isdatasync,
                   struct fuse_file_info* ffi) {

   WRAP( marfs_fsyncdir(path, isdatasync, (MarFS_DirHandle*)ffi->fh) );
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

int fuse_ftruncate(const char*            path,
                   off_t                  length,
                   struct fuse_file_info* ffi) {

   WRAP_PLUS( marfs_ftruncate(path, length, (MarFS_FileHandle*)ffi->fh) );
}


// This is "stat()"
int fuse_getattr (const char*  path,
                  struct stat* stp) {

   WRAP_PLUS( marfs_getattr(path, stp) );
}


// *** this may not be needed until we implement user xattrs in the fuse daemon ***
//
// Kernel calls this with key 'security.capability'
//
int fuse_getxattr (const char* path,
                   const char* name,
                   char*       value,
                   size_t      size) {

   WRAP_PLUS( marfs_getxattr(path, name, value, size) );
}


int fuse_ioctl(const char*            path,
               int                    cmd,
               void*                  arg,
               struct fuse_file_info* ffi,
               unsigned int           flags,
               void*                  data) {
   // if we need an ioctl for something or other
   // ***Maybe a way to get fuse deamon to read up new config file
   // *** we need a way for daemon to read up new config file without stopping

   WRAP( marfs_ioctl(path, cmd, arg, (MarFS_FileHandle*)ffi->fh, flags, data) );
}




// NOTE: Even though we remove reserved xattrs, user can call with empty
//       buffer and receive back length of xattr names.  Then, when we
//       remove reserved xattrs (in a subsequent call), user will see a
//       different list length than the previous call lead him to expect.

int fuse_listxattr (const char* path,
                    char*       list,
                    size_t      size) {

   WRAP( marfs_listxattr(path, list, size) );
}


int fuse_mkdir (const char* path,
                mode_t      mode) {

   WRAP_PLUS( marfs_mkdir(path, mode) );
}


// [See discussion at fuse_create().]
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
int fuse_mknod (const char* path,
                mode_t      mode,
                dev_t       rdev) {

   WRAP_PLUS( marfs_mknod(path, mode, rdev) );
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
// TBD: NFS doesn't call mknod!
//
// A FileHandle is dynamically allocated in fuse_open().  We want to make
// sure any error-returns will deallocate the file-handle.  In C++, it's
// easy to get an object to do cleanup when it goes out of scope.  In C, we
// need to add some code before every return, to do any cleanup that might
// be needed before returning.
//



// NOTE: stream_open() assumes the OS is in a pristine state.  fuse_open()
//       currently always allocates a fresh OS (inside the new FileHandle),
//       so that assumption is safe.  stream_close() doesn't wipe
//       everything clean, because we want some of that info (e.g. how much
//       data was written).  If you decide to start reusing FileHandles,
//       you should probably (a) assure they have been flushed/closed, and
//       (b) wipe them clean.  [See fuse_read(), which now performs a
//       distinct S3 request for every call, and reuses the ObjectStream
//       inside the FileHandle.]
//
// NOTE: In the context of fuse, the kernel will call mknod() before
//       open(), if the file doesn't exist.  If the file does exist, the
//       kernel will call ftruncate() after opening.  We've relaied on
//       that.
//
//       However, when exporting the fuse-mount over NFS, we don't get the
//       mknod / ftruncate calls automatically.  Therefore, we have to
//       detect this situation.  The following seem to be appropriate
//       conditions to trigger our own call to ftruncate() in the NFS
//       context only: (a) the file has non-zero size [from getattr()], and
//       (b) the file doesn't have Pre.obj_type==OBJ_Nto1 (as would be the
//       case with pftool down concurrent opens of the file at different
//       offsets).  This is done inside marfs_open(), if needed.

int fuse_open (const char*            path,
               struct fuse_file_info* ffi) {

   PUSH_USER(1);

   if (ffi->fh != 0) {
      // failed to free the file-handle in fuse_release()?
      LOG(LOG_ERR, "unexpected non-NULL file-handle\n");
      return -EINVAL;
   }
   if (! (ffi->fh = (uint64_t) calloc(1, sizeof(MarFS_FileHandle)))) {
      LOG(LOG_ERR, "couldn't allocate a MarFS_FileHandle\n");
      return -ENOMEM;
   }
   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */

   rc_ssize = marfs_open(path, fh, ffi->flags, 0); /* content-length unknown */
   if (rc_ssize < 0) {
      free(fh);
      ffi->fh = 0;
   }

   POP_USER();
   if (rc_ssize)
      return -errno;

   return 0;
}



int fuse_opendir (const char*            path,
                  struct fuse_file_info* ffi) {
   PUSH_USER(1);

   if (ffi->fh != 0) {
      // failed to free the dirhandle in fuse_releasedir()?
      LOG(LOG_ERR, "unexpected non-NULL dir-handle\n");
      return -EINVAL;
   }
   if (! (ffi->fh = (uint64_t) calloc(1, sizeof(MarFS_DirHandle)))) {
      LOG(LOG_ERR, "couldn't allocate a MarFS_FileHandle\n");
      return -ENOMEM;
   }
   MarFS_DirHandle* dh   = (MarFS_DirHandle*)ffi->fh; /* shorthand */

   rc_ssize = marfs_opendir(path, dh);
   if (rc_ssize < 0) {
      free(dh);
      ffi->fh = 0;
   }

   POP_USER();
   if (rc_ssize)
      return -errno;
   return 0;
}



// return actual number of bytes read.  0 indicates EOF.
// negative understood to be negative errno.
//
// NOTE: 
// TBD: Don't do object-interaction if file is DIRECT.  See fuse_open().
//
int fuse_read (const char*            path,
               char*                  buf,
               size_t                 size,
               off_t                  offset,
               struct fuse_file_info* ffi) {

   WRAP( marfs_read(path, buf, size, offset, (MarFS_FileHandle*)ffi->fh) );
}


int fuse_readdir (const char*            path,
                  void*                  buf,
                  fuse_fill_dir_t        filler,
                  off_t                  offset,
                  struct fuse_file_info* ffi) {

   WRAP( marfs_readdir(path, buf, (marfs_fill_dir_t)filler, offset, (MarFS_DirHandle*)ffi->fh) );
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

int fuse_readlink (const char* path,
                   char*       buf,
                   size_t      size) {

   WRAP_PLUS( marfs_readlink(path, buf, size) );
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

int fuse_release (const char*            path,
                  struct fuse_file_info* ffi) {

   PUSH_USER(0);

   if (! ffi->fh) {
      LOG(LOG_ERR, "unexpected NULL file-handle\n");
      return -EINVAL;
   }
   MarFS_FileHandle* fh = (MarFS_FileHandle*)ffi->fh; /* shorthand */

   rc_ssize = marfs_release(path, fh);
   free(fh);
   ffi->fh = 0;

   POP_USER();
   if (rc_ssize)
      return -errno;
   return 0;
}


//  [Like release(), this doesn't have a directly corresponding system
//  call.]
//
// NOTE: This is also the only function I've seen (so far) that gets called
//       with fuse_context->uid of 0, even when running as non-root.  In
//       that case, seteuid() will fail.  However, we assume fuse is always
//       run as root.
//
// NOTE: Testing as myself, I notice releasedir() gets called with
//     fuse_context.uid ==0.  Other functions are all called with
//     fuse_context.uid == my_uid.  I’m ignoring push/pop UID in this case,
//     in order to be able to continue debugging.
//
// NOTE: It is possible that we get called with a directory that doesn't
//     exist!  That happens e.g. after 'rm -rf /marfs/jti/mydir' In that
//     case, it seems the kernel calls us with <path> = "-".

int fuse_releasedir (const char*            path,
                     struct fuse_file_info* ffi) {

   PUSH_USER(0);

   if (! ffi->fh) {
      LOG(LOG_ERR, "unexpected NULL dir-handle\n");
      return -EINVAL;
   }
   MarFS_DirHandle* dh = (MarFS_DirHandle*)ffi->fh; /* shorthand */

   rc_ssize = marfs_releasedir(path, dh);
   free(dh);
   ffi->fh = 0;

   POP_USER();
   if (rc_ssize)
      return -errno;
   return 0;
}


// Kernel calls this with key 'security.capability'
//
int fuse_removexattr (const char* path,
                      const char* name) {

   WRAP_PLUS( marfs_removexattr(path, name) );
}


int fuse_rename (const char* path,
                 const char* to) {

   WRAP_PLUS( marfs_rename(path, to) );
}


// using looked up mdpath, do statxattr and get object name
int fuse_rmdir (const char* path) {

   WRAP_PLUS( marfs_rmdir(path) );
}


int fuse_setxattr (const char* path,
                   const char* name,
                   const char* value,
                   size_t      size,
                   int         flags) {

   WRAP_PLUS( marfs_setxattr(path, name, value, size, flags) );
}

// The OS seems to call this from time to time, with <path>=/ (and
// euid==0).  We could walk through all the namespaces, and accumulate
// total usage.  (Maybe we should have a top-level fsinfo path?)  But I
// guess we don't want to allow average users to do this.
//
// This is also called by the client when initiating an NFS/sshfs/etc a
// mount over NFS/sshfs.

int fuse_statvfs (const char*      path,
                 struct statvfs*  statbuf) {

   WRAP_PLUS( marfs_statvfs(path, statbuf) );
}


// NOTE: <target> is given as a full path.  It might or might not be under
//     our fuse mount-point, but even if it is, we should just stuff
//     whatever <target> we get into the symlink.  If it is something under
//     a marfs mount, then fuse_readlink() will be called when the link is
//     followed.

int fuse_symlink (const char* target,
                  const char* linkname) {

   WRAP_PLUS( marfs_symlink(target, linkname) );
}


// *** this may not be needed until we implement write in the fuse daemon ***
int fuse_truncate (const char* path,
                   off_t       size) {

   WRAP_PLUS( marfs_truncate(path, size) );
}


int fuse_unlink (const char* path) {

   WRAP_PLUS( marfs_unlink(path) );
}

// deprecated in 2.6
// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int fuse_utime(const char*     path,
               struct utimbuf* buf) {   

   WRAP_PLUS( marfs_utime(path, buf) );
}

// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int fuse_utimens(const char*           path,
                 const struct timespec tv[2]) {   

   WRAP_PLUS( marfs_utimens(path, tv) );
}


int fuse_write(const char*            path,
               const char*            buf,
               size_t                 size,
               off_t                  offset,
               struct fuse_file_info* ffi) {

#if 1
   WRAP( marfs_write(path, buf, size, offset, (MarFS_FileHandle*)ffi->fh) );
#else
   // EXPERIMENT: For large writes, the kernel will call us with <size> ==
   //     128k, and marfs_write will hand this off to the curl readfunc
   //     thread.  Normally, the curl readfunc gets called for increments
   //     of 16k.  However, when using chunked-transfer-encoding, there is
   //     an overhead of 12 bytes for each transfer (for the CTE header),
   //     so the callback function actually gets called with for 8 times
   //     16k-12, with an extra final call for 96 bytes.
   //
   //     If fuse will allow us to only move 16k-96, then maybe we can be
   //     more-efficient in our interactions with curl.
   //
   // RESULT: This buys us about ~%20 BW improvement.
   //
   // REVISITED: Measuring with a single task running against fuse, it's no
   //     longer clear what this buys (because BW for a move of 32G varies
   //     widely).  Furthermore, NFS (being the knucklehead that it is)
   //     apparently does close/open/seek, whenever a call to write returns
   //     less than was attempted.  That won't work with MarFS.  Therefore,
   //     in the interest of supporting NFS, we'll leave this approach
   //     commented-out.
   //
   size_t wk_size = size;
   if (wk_size == (128 * 1024))
      wk_size -= 96;

   WRAP( marfs_write(path, buf, wk_size, offset, (MarFS_FileHandle*)ffi->fh) );
#endif
}




// ---------------------------------------------------------------------------
// unimplemented routines, for now
// ---------------------------------------------------------------------------

#if 0

int fuse_bmap(const char* path,
              size_t      blocksize,
              uint64_t*   idx) {

   // don’t support -- this is for block mapping
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

int fuse_create(const char*            path,
                mode_t                 mode,
                struct fuse_file_info* ffi) {

   WRAP_PLUS( marfs_create(path, mode, (MarFS_FileHandle*)ffi->fh) );
}


// obsolete, in fuse 2.6
int fuse_fallocate(const char*            path,
                   int                    mode,
                   off_t                  offset,
                   off_t                  length,
                   struct fuse_file_info* ffi) {

   WRAP_PLUS( marfs_fallocate(path, mode, offset, length, (MarFS_FileHandle*)ffi->fh) );
}


// this is really fstat() ??
//
// Instead of using the <fd> (which is not yet implemented in our
// FileHandle), I'm calling fstat on the file itself.  Good enough?
int fuse_fgetattr(const char*            path,
                  struct stat*           st,
                  struct fuse_file_info* ffi) {

   WRAP_PLUS( marfs_fgetattr(path, st, (MarFS_FileHandle*)ffi->fh) );
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
// TBD: Don't do object-interaction if file is DIRECT.  See fuse_open().
//
// NOTE: It also appears that fuse calls stat immediately after flush.
//       (Hard to be sure, because fuse calls stat all the time.)  I'd
//       guess we shouldn't return until we are sure that stat will see the
//       final state of the file.  Probably also applies to xattrs (?)
//       But hanging here seems to cause fuse to hang.
//
//       BECAUSE OF THIS, we took a simpler route: move all the
//       synchronization into fuse_release(), and don't implement fuse
//       flush.  This seems to have the desired effect of getting fuse to
//       do all the sync at close-time.

int fuse_flush (const char*            path,
                struct fuse_file_info* ffi) {

   WRAP( marfs_flush(path, (MarFS_FileHandle*)ffi->fh) );
}


// new in 2.6, yet deprecated?
// combines opendir(), readdir(), closedir() into one call.
int fuse_getdir(const char*    path,
                fuse_dirh_t    fdh,
                fuse_dirfil_t  fill) {

   WRAP( marfs_getdir(path, fdh, fill) );
}


int fuse_flock(const char*            path,
               struct fuse_file_info* ffi,
               int                    op) {

   WRAP( marfs_flock(path, (MarFS_FileHandle*)ffi->fh, op) );
}


int fuse_link (const char* path,
               const char* to) {

   WRAP( marfs_link(path, to) );
}


int fuse_lock(const char*            path,
              struct fuse_file_info* ffi,
              int                    cmd,
              struct flock*          locks) {

   WRAP( marfs_lock(path, (MarFS_FileHandle*)ffi->fh, cmd, locks) );
}

int fuse_poll(const char*             path,
              struct fuse_file_info*  ffi,
              struct fuse_pollhandle* ph,
              unsigned*               reventsp) {

   WRAP( marfs_poll(path, (MarFS_FileHandle*)ffi->fh, ph, reventsp) );
}


#endif




// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------


int main(int argc, char* argv[])
{
   TRY_DECLS();

   INIT_LOG();
   LOG(LOG_INFO, "\n");
   LOG(LOG_INFO, "=== FUSE starting\n");

   // Not sure why, but I've seen machines where I'm logged in as root, and
   // I run fuse in the background, and the process has an euid of some other user.
   // This fixes that.
   //
   // NOTE: This also now *requires* that marfs fuse is always only run as root.
   __TRY0(seteuid, 0);

   if (read_configuration()) {
      LOG(LOG_ERR, "read_configuration() failed.  Quitting\n");
      return -1;
   }

   if (validate_config()) {
      LOG(LOG_ERR, "validate_config() failed.  Quitting\n");
      return -1;
   }

   init_xattr_specs();


   // initialize libaws4c/libcurl
   //
   // NOTE: We're making initializations in the default-context.  These
   //       will be copied into the per-file-handle context via
   //       aws_context_clone(), in stream_open().  Instead of having to
   //       make these initializations in every context, we make them once
   //       here.
   aws_init();
   aws_reuse_connections(1);

#if (DEBUG > 1)
   aws_set_debug(1);
#endif

#ifdef USE_SPROXYD
   // NOTE: sproxyd doesn't require authentication, and so it could work on
   //     an installation without a ~/.awsAuth file.  But suppose we're
   //     supporting some repos that use S3 and some that use sproxyd?  In
   //     that case, the s3 requests will need this.  Loading it once
   //     up-front, like this, at start-time, means we don't have to reload
   //     it inside fuse_open(), for every S3 open, but it also means we
   //     don't know whether we really need it.
   //
   // ALSO: At start-up time, $USER is "root".  If we want per-user S3 IDs,
   //     then we would have to either (a) load them all now, and
   //     dynamically pick the one we want inside fuse_open(), or (b) call
   //     aws_read_config() inside fuse_open(), using the euid of the user
   //     to find ~/.awsAuth.
   int config_fail_ok = 1;
#else
   int config_fail_ok = 0;
#endif

   char* const user_name = (getenv("USER"));
   if (aws_read_config(user_name)) {
      // probably missing a line in ~/.awsAuth
      LOG(LOG_ERR, "aws-read-config for user '%s' failed\n", user_name);
      if (! config_fail_ok)
         exit(1);
   }

   // make sure all support directories exist.  (See the config file.)
   // This includes mdfs, fsinfo, the trash "scatter-tree", for all namespaces,
   // plus a storage "scatter-tree" for any semi-direct repos.
   __TRY0(init_mdfs);

   // function-pointers used by fuse, to dispatch calls to our handlers.
   struct fuse_operations marfs_oper = {
      .init        = marfs_fuse_init,
      .destroy     = marfs_fuse_exit,

      .access      = fuse_access,
      .chmod       = fuse_chmod,
      .chown       = fuse_chown,
      .ftruncate   = fuse_ftruncate,
      .fsync       = fuse_fsync,
      .fsyncdir    = fuse_fsyncdir,
      .getattr     = fuse_getattr,
      .getxattr    = fuse_getxattr,
      .ioctl       = fuse_ioctl,
      .listxattr   = fuse_listxattr,
      .mkdir       = fuse_mkdir,
      .mknod       = fuse_mknod,
      .open        = fuse_open,
      .opendir     = fuse_opendir,
      .read        = fuse_read,
      .readdir     = fuse_readdir,
      .readlink    = fuse_readlink,
      .release     = fuse_release,
      .releasedir  = fuse_releasedir,
      .removexattr = fuse_removexattr,
      .rename      = fuse_rename,
      .rmdir       = fuse_rmdir,
      .setxattr    = fuse_setxattr,
      .statfs      = fuse_statvfs,
      .symlink     = fuse_symlink,
      .truncate    = fuse_truncate,
      .unlink      = fuse_unlink,
      .utime       = fuse_utime, /* deprecated in 2.6 */
      .utimens     = fuse_utimens,
      .write       = fuse_write,
#if 0
      // not implemented
      .bmap        = fuse_bmap,
      .create      = fuse_create,
      .fallocate   = fuse_fallocate /* not in 2.6 */
      .fgetattr    = fuse_fgetattr,
      .flock       = fuse_flock,
      .flush       = fuse_flush,
      .getdir      = fuse_getdir, /* deprecated in 2.6 */
      .link        = fuse_link,
      .lock        = fuse_lock,
      .poll        = fuse_poll,
#endif
   };

   return fuse_main(argc, argv, &marfs_oper, NULL);
}
