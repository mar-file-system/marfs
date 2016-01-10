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
#include <time.h>
#include <sys/syscall.h>

// #include "marfs_fuse.h"
#define FUSE_USE_VERSION 26
#include <fuse.h>


// ---------------------------------------------------------------------------
// utilities
//
// These are some chunks of code used in more than one fuse function.
// Should probably be moved to another file, eventually.
// ---------------------------------------------------------------------------


typedef struct PerThreadContext {
   uid_t uid;                    /* original uid */
   gid_t gid;                    /* original gid */
   int   pushed;                 // detect double-push
   uid_t new_uid;
   gid_t new_gid;
} PerThreadContext;


#if 0

// We use local variables to hold pushed euid/egid.  These should be
// thread-local (i.e. on the stack), and therefore thread-safe.

#define PUSH_USER()                                                     \
   ENTRY();                                                             \
   PerThreadContext ctx;                                                \
   memset(&ctx, 0, sizeof(PerThreadContext));                           \
   __TRY0(push_user, &ctx)

#define POP_USER()                                                      \
   __TRY0(pop_user, &ctx);                                              \
   EXIT()


#elif 0

// once-per-thread dynamic allocation of storage to hold uid/gid for push/pop
// adapted from pthread_key_create(3P) manpage ...

static pthread_key_t  key;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;

static void delete_key(void* key_value) { free(key_value); }

static void make_key() { (void) pthread_key_create(&key, delete_key); }

#  define PUSH_USER()                                                   \
   ENTRY();                                                             \
   pthread_once(&key_once, make_key);                                   \
   PerThreadContext* ctx = (PerThreadContext*)pthread_getspecific(key); \
   if (! ctx) {                                                         \
      ctx = (PerThreadContext*)malloc(sizeof(PerThreadContext));        \
      memset(ctx, 0, sizeof(PerThreadContext));                         \
      pthread_setspecific(key, ctx);                                    \
   }                                                                    \
   __TRY0(push_user2, ctx)

#  define POP_USER()                                                    \
   __TRY0(pop_user2, ctx);                                              \
   EXIT()


#else

// using "saved" uid/gid to hold the state.
// using syscalls to get thread-safety, based on this:
// http://stackoverflow.com/questions/1223600/change-uid-gid-only-of-one-thread-in-linux

#define PUSH_USER()                                                     \
   ENTRY();                                                             \
   PerThreadContext ctx;                                                \
   memset(&ctx, 0, sizeof(PerThreadContext));                           \
   __TRY0(push_user3, &ctx)

#define POP_USER()                                                      \
   __TRY0(pop_user3, &ctx);                                             \
   EXIT()

#endif



// push_user()
//
//   Save current user info from syscall into saved_user
//   Set userid to requesting user (in fuse request structure)
//   return 0/negative for success/error
//
// NOTE: setuid() doesn't allow returning to priviledged user, from
//       unpriviledged-user.  For that, we apparently need the (BSD)
//       seteuid().
//
// NOTE: If seteuid() fails, and the problem is that we lack privs to call
//       seteuid(), this means *we* are running unpriviledged.  This could
//       happen if we're testing the FUSE mount as a non-root user.  In
//       this case, the <uid> argument should be the same as the <uid> of
//       the FUSE process, which we can extract from the fuse_context.
//       [We try the seteuid() first, for speed]
//
// NOTE: If push_user() fails (returns non-zero), pop_user() will not be
//       run.  So, push_user() should leave things as they were, if it's
//       going to fail.  If push_user() succeeds, pop_user() will attempt
//       to restore both saved values.  So, push_user() should push both
//       euid and egid, if it going to report success.

int push_user(PerThreadContext* ctx) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   int rc;

   //   LOG(LOG_INFO, "gid %u:  egid [%u](%u) -> (%u)\n",
   //       getgid(), ctx->gid, getegid(), fuse_get_context()->gid);
  LOG(LOG_INFO, "%u/%x @0x%lx  gid %u:  egid [%u](%u) -> (%u)\n",
      syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
      getgid(), ctx->gid, getegid(), fuse_get_context()->gid);

   ctx->gid = getegid();
   gid_t new_gid = fuse_get_context()->gid;
   rc = setegid(new_gid);
   if (rc == -1) {
      if ((errno == EACCES) && (new_gid == getgid())) {
         LOG(LOG_INFO, "failed (but okay)\n"); /* okay [see NOTE] */
      }
      else {
         LOG(LOG_ERR, "failed!\n");
         return -1;             // no changes were made
      }
   }

   //   LOG(LOG_INFO, "uid %u:  euid [%u](%u) -> (%u)\n",
   //       getuid(), ctx->uid, geteuid(), fuse_get_context()->uid);
   LOG(LOG_INFO, "%u/%x @0x%lx  uid %u:  euid [%u](%u) -> (%u)\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       getuid(), ctx->uid, geteuid(), fuse_get_context()->uid);

   ctx->uid = geteuid();
   uid_t new_uid = fuse_get_context()->uid;
   rc = seteuid(new_uid);
   if (rc == -1) {
      if ((errno == EACCES) && (new_uid == getuid())) {
         LOG(LOG_INFO, "failed (but okay)\n");
         return 0;              /* okay [see NOTE] */
      }

      // try to restore egid from before push
      if (! setegid(ctx->gid)) {
         LOG(LOG_ERR, "failed!\n");
         return -1;             // restored to initial conditions
      }

      else {
         LOG(LOG_ERR, "failed -- couldn't restore egid %d!\n", ctx->gid);
         exit(EXIT_FAILURE);  // don't leave thread stuck with wrong egid
         // return -1;
      }
   }

   return 0;
#else
#  error "No support for seteuid()/setegid()"
#endif
}


//  pop_user() changes the effective UID.  Here, we revert to the
//  "real" UID.
int pop_user(PerThreadContext* ctx) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
   int rc;

   //   LOG(LOG_INFO, "uid %u:  euid (%u) <- (%u)\n",
   //       getuid(), ctx->uid, geteuid());
   LOG(LOG_INFO, "%u/%x @0x%lx  uid %u:  euid (%u) <- (%u)\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       getuid(), ctx->uid, geteuid());

   uid_t  new_uid = ctx->uid;
   rc = seteuid(new_uid);
   if (rc == -1) {
      if ((errno == EACCES) && (new_uid == getuid())) {
         LOG(LOG_INFO, "failed (but okay)\n"); /* okay [see NOTE] */
      }
      else {
         LOG(LOG_ERR, "failed\n");
         exit(EXIT_FAILURE);    // don't leave thread stuck with wrong euid
         // return -1;
      }
   }


   //   LOG(LOG_INFO, "gid %u:  egid (%u) <- (%u)\n",
   //       getgid(), ctx->gid, getegid());
   LOG(LOG_INFO, "%u/%x @0x%lx  gid %u:  egid (%u) <- (%u)\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       getgid(), ctx->gid, getegid());


   gid_t  new_gid = ctx->gid;
   rc = setegid(new_gid);
   if (rc == -1) {
      if ((errno == EACCES) && (new_gid == getgid())) {
         LOG(LOG_INFO, "failed (but okay)\n");
         return 0;              /* okay [see NOTE] */
      }
      else {
         LOG(LOG_ERR, "failed!\n");
         exit(EXIT_FAILURE);    // don't leave thread stuck with wrong euid
         // return -1;
      }
   }

   return 0;
#else
#  error "No support for seteuid()/setegid()"
#endif
}


// ...........................................................................
// seteuid() / setegid() affect an entire process.
// setfsuid() / setfsgid() affect only the calling thread.

// setfs fns don't return error codes.  On success, they return the
// previous value.  On failure they return the new value.  If the
// argument is invalid, they return -1.

// failures in setfs* also don't set errno, but __TRY() assumes non-zero
// retrun has set errno, so we'll do it ourselves.

int push_user2(PerThreadContext* ctx) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   if (ctx->pushed) {
      LOG(LOG_ERR, "%u/%x @0x%lx double-push -> %u\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
          fuse_get_context()->uid);
      errno = EPERM;
      return -1;
   }

   LOG(LOG_INFO, "%u/%x @0x%lx  gid %u(%u) -> [%u]\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       getgid(), getegid(), fuse_get_context()->gid);

   gid_t old_gid = getegid();
   gid_t new_gid = fuse_get_context()->gid;
   gid_t set_gid = setfsgid(new_gid);

   if ((set_gid == new_gid) && (new_gid != old_gid)) {
      LOG(LOG_ERR, "%u/%x @0x%lx failed [%u]\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
          set_gid);
      setfsgid(old_gid);
      errno = EACCES;
      return -1;
   }
   else {
      ctx->gid     = old_gid;
      ctx->new_gid = new_gid;
   }

   LOG(LOG_INFO, "%u/%x @0x%lx  uid %u(%u) -> [%u]\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       getuid(), geteuid(), fuse_get_context()->uid);

   uid_t old_uid = geteuid();
   uid_t new_uid = fuse_get_context()->uid;
   uid_t set_uid = setfsuid(new_uid);

   if ((set_uid == new_uid) && (new_uid != old_uid)) {
      LOG(LOG_ERR, "%u/%x @0x%lx failed [%u]\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
          set_uid);
      setfsgid(old_gid);
      setfsuid(old_uid);
      errno = EACCES;
      return -1;
   }
   else {
      ctx->uid     = old_uid;
      ctx->new_uid = new_uid;
   }

   // allow detection of 2 pushes without a pop
   ctx->pushed = 1;
  
   return 0;

#else
#  error "No support for seteuid()/setegid()"
#endif
}


int pop_user2(PerThreadContext* ctx) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   int rc = 0;

   //  LOG(LOG_INFO, "gid %u:  egid (%u) <- (%u)\n",
   //      getgid(), ctx->gid, getegid());
   LOG(LOG_INFO, "%u/%x @0x%lx  gid [%u] <- %u(%u)[%u]\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       ctx->gid, getgid(), getegid(), ctx->new_gid);

   gid_t old_gid = fuse_get_context()->gid;
   gid_t new_gid = ctx->gid;
   gid_t set_gid = setfsgid(new_gid);

   if ((set_gid == new_gid) && (new_gid != old_gid)) {
      LOG(LOG_ERR, "%u/%x @0x%lx failed [%u]\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
          set_gid);
      rc = -1;
   }

   //  LOG(LOG_INFO, "uid %u:  euid (%u) <- (%u)\n",
   //      getuid(), ctx->uid, geteuid());
   LOG(LOG_INFO, "%u/%x @0x%lx  uid [%u] <- %u(%u)[%u]\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       ctx->uid, getuid(), geteuid(), ctx->new_uid);

   uid_t old_uid = fuse_get_context()->uid;
   uid_t new_uid = ctx->uid;
   uid_t set_uid = setfsuid(new_uid);

   if ((set_uid == new_uid) && (new_uid != old_uid)) {
      LOG(LOG_ERR, "%u/%x @0x%lx failed [%u]\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
          set_uid);
      rc = -1;
   }

   // allow detection of 2 pushes without a pop
   ctx->pushed = 0;

   if (rc)
      errno = EACCES;

   return rc;


#else
#  error "No support for seteuid()/setegid()"
#endif
}



// ...........................................................................
// Trying yet again.  There's a bug, or something in setfsgid(), regarding
// directories with gid=root.  Try calling setreseuid/setresdguid via
// direct syscalls to get thread safety.

int push_user3(PerThreadContext* ctx) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   int rc;

   // check for two pushes without a pop
   if (ctx->pushed) {
      LOG(LOG_ERR, "%u/%x @0x%lx double-push -> %u\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
          fuse_get_context()->uid);
      errno = EPERM;
      return -1;
   }

   // get current real/effective/saved gid
   gid_t old_rgid;
   gid_t old_egid;
   gid_t old_sgid;

   rc = syscall(SYS_getresgid, &old_rgid, &old_egid, &old_sgid);
   if (rc) {
      LOG(LOG_ERR, "%u/%x @0x%lx  getresgid() failed\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx);
      exit(EXIT_FAILURE);       // fuse should fail
   }

   // gid to push
   gid_t new_egid = fuse_get_context()->gid;

   // install the new egid, and save the current one
   LOG(LOG_INFO, "%u/%x @0x%lx  gid %u(%u) -> (%u)\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       old_rgid, old_egid, new_egid);

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
      LOG(LOG_ERR, "%u/%x @0x%lx  getresuid() failed\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx);
      exit(EXIT_FAILURE);       // fuse should fail
   }

   // uid to push
   gid_t new_euid = fuse_get_context()->uid;

   // install the new euid, and save the current one
   LOG(LOG_INFO, "%u/%x @0x%lx  uid %u(%u) -> (%u)\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       old_ruid, old_euid, new_euid);

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
#else
#  error "No support for seteuid()/setegid()"
#endif
}


//  pop_user() changes the effective UID.  Here, we revert to the
//  euid from before the push.
int pop_user3(PerThreadContext* ctx) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   int rc;

   uid_t old_ruid;
   uid_t old_euid;
   uid_t old_suid;

   rc = syscall(SYS_getresuid, &old_ruid, &old_euid, &old_suid);
   if (rc) {
      LOG(LOG_ERR, "%u/%x @0x%lx  getresuid() failed\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx);
      exit(EXIT_FAILURE);       // fuse should fail
   }

   LOG(LOG_INFO, "%u/%x @0x%lx  uid (%u) <- %u(%u)\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       old_suid, old_ruid, old_euid);

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
      LOG(LOG_ERR, "%u/%x @0x%lx  getresgid() failed\n",
          syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx);
      exit(EXIT_FAILURE);       // fuse should fail
   }

   LOG(LOG_INFO, "%u/%x @0x%lx  gid (%u) <- %u(%u)\n",
       syscall(SYS_gettid), (unsigned int)pthread_self(), (size_t)ctx,
       old_sgid, old_rgid, old_euid);

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

   return 0;
#else
#  error "No support for seteuid()/setegid()"
#endif
}




// --- wrappers just call the corresponding library-function, to support a
//     given fuse-function.  The library-functions are meant to be used by
//     both fuse and pftool, so they don't do seteuid(), and don't expect
//     any fuse-related structures.

#define WRAP(FNCALL)                                   \
   PUSH_USER();                                        \
   int fncall_rc = FNCALL;                             \
   POP_USER();                                         \
   if (fncall_rc < 0) {                                \
      LOG(LOG_ERR, "ERR %s, errno=%d '%s'\n",          \
          #FNCALL, errno, strerror(errno));            \
      return -errno;                                   \
   }                                                   \
   return fncall_rc /* caller provides semi */




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

   WRAP( marfs_access(path, mask) );
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

   WRAP( marfs_chmod(path, mode) );
}


int fuse_chown (const char* path,
                uid_t       uid,
                gid_t       gid) {

   WRAP( marfs_chown(path, uid, gid) );
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

   WRAP( marfs_ftruncate(path, length, (MarFS_FileHandle*)ffi->fh) );
}


// This is "stat()"
int fuse_getattr (const char*  path,
                  struct stat* stp) {

   WRAP( marfs_getattr(path, stp) );
}


// *** this may not be needed until we implement user xattrs in the fuse daemon ***
//
// Kernel calls this with key 'security.capability'
//
int fuse_getxattr (const char* path,
                   const char* name,
                   char*       value,
                   size_t      size) {

   WRAP( marfs_getxattr(path, name, value, size) );
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

   WRAP( marfs_mkdir(path, mode) );
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

   WRAP( marfs_mknod(path, mode, rdev) );
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



// A FileHandle is dynamically allocated in fuse_open().  We want to make
// sure any error-returns will deallocate the file-handle.  In C++, it's
// easy to get an object to do cleanup when it goes out of scope.  In C, we
// need to add some code before every return, to do any cleanup that might
// be needed before returning.
//


// RETURN() is used inside TRY(), which is the basis of all the test-macros
// defined in common.h.  So, we redefine that to add our cleanup checks.
#if 0
# undef RETURN
# define RETURN(VALUE)                            \
   do {                                           \
      LOG(LOG_INFO, "returning %d\n", (VALUE));   \
      free((MarFS_FileHandle*)ffi->fh);           \
      ffi->fh = 0;                                \
      return (VALUE);                             \
   } while(0)
#endif


// NOTE: stream_open() assumes the OS is in a pristine state.  fuse_open()
//       currently always allocates a fresh OS (inside the new FileHandle),
//       so that assumption is safe.  stream_close() doesn't wipe
//       everything clean, because we want some of that info (e.g. how much
//       data was written).  If you decide to start reusing FileHandles,
//       you should probably (a) assure they have been flushed/closed, and
//       (b) wipe them clean.  [See fuse_read(), which now performs a
//       distinct S3 request for every call, and reuses the ObjectStream
//       inside the FileHandle.]
int fuse_open (const char*            path,
               struct fuse_file_info* ffi) {

   PUSH_USER();

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
   PUSH_USER();

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

#if 0
# undef RETURN
# define RETURN(VALUE) return(VALUE)
#endif


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

   WRAP( marfs_readlink(path, buf, size) );
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

   PUSH_USER();

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

   PUSH_USER();

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

   WRAP( marfs_removexattr(path, name) );
}


int fuse_rename (const char* path,
                 const char* to) {

   WRAP( marfs_rename(path, to) );
}


// using looked up mdpath, do statxattr and get object name
int fuse_rmdir (const char* path) {

   WRAP( marfs_rmdir(path) );
}


int fuse_setxattr (const char* path,
                   const char* name,
                   const char* value,
                   size_t      size,
                   int         flags) {

   WRAP( marfs_setxattr(path, name, value, size, flags) );
}

// The OS seems to call this from time to time, with <path>=/ (and
// euid==0).  We could walk through all the namespaces, and accumulate
// total usage.  (Maybe we should have a top-level fsinfo path?)  But I
// guess we don't want to allow average users to do this.
int fuse_statvfs (const char*      path,
                 struct statvfs*  statbuf) {

   WRAP( marfs_statvfs(path, statbuf) );
}


// NOTE: <target> is given as a full path.  It might or might not be under
//     our fuse mount-point, but even if it is, we should just stuff
//     whatever <target> we get into the symlink.  If it is something under
//     a marfs mount, then fuse_readlink() will be called when the link is
//     followed.

int fuse_symlink (const char* target,
                  const char* linkname) {

   WRAP( marfs_symlink(target, linkname) );
}


// *** this may not be needed until we implement write in the fuse daemon ***
int fuse_truncate (const char* path,
                   off_t       size) {

   WRAP( marfs_truncate(path, size) );
}


int fuse_unlink (const char* path) {

   WRAP( marfs_unlink(path) );
}

// deprecated in 2.6
// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int fuse_utime(const char*     path,
               struct utimbuf* buf) {   

   WRAP( marfs_utime(path, buf) );
}

// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int fuse_utimens(const char*           path,
                 const struct timespec tv[2]) {   

   WRAP( marfs_utimens(path, tv) );
}


int fuse_write(const char*            path,
               const char*            buf,
               size_t                 size,
               off_t                  offset,
               struct fuse_file_info* ffi) {

#if 0
   WRAP( marfs_write(path, buf, size, offset, (MarFS_FileHandle*)ffi->fh) );
#else
   // EXPERIMENT: For large writes, the kernel will call us with <size> ==
   //   128k, and marfs_write will hand this off to the curl readfunc
   //   thread.  Normally, the curl readfunc gets called for increments of
   //   16k.  However, when using chunked-transfer-encoding, there is an
   //   overhead of 12 bytes for each transfer (for the CTE header), so the
   //   callback function actually gets called with for 8 times 16k-12,
   //   with an extra final call for 96 bytes.
   //
   //   If fuse will allow us to only move 16k-96, then maybe we can be
   //   more-efficient in our interactions with curl.
   //
   // RESULT: This buys us about ~%20 BW improvement.
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

   WRAP( marfs_create(path, mode, (MarFS_FileHandle*)ffi->fh) );
}


// obsolete, in fuse 2.6
int fuse_fallocate(const char*            path,
                   int                    mode,
                   off_t                  offset,
                   off_t                  length,
                   struct fuse_file_info* ffi) {

   WRAP( marfs_fallocate(path, mode, offset, length, (MarFS_FileHandle*)ffi->fh) );
}


// this is really fstat() ??
//
// Instead of using the <fd> (which is not yet implemented in our
// FileHandle), I'm calling fstat on the file itself.  Good enough?
int fuse_fgetattr(const char*            path,
                  struct stat*           st,
                  struct fuse_file_info* ffi) {

   WRAP( marfs_fgetattr(path, st, (MarFS_FileHandle*)ffi->fh) );
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

#ifdef STATIC_CONFIG
   if (read_config("~/marfs.config")) {
      LOG(LOG_ERR, "load_config() failed.  Quitting\n");
      return -1;
   }
#else
   if (read_configuration()) {
      LOG(LOG_ERR, "read_configuration() failed.  Quitting\n");
      return -1;
   }
#endif

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
