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



#include "push_user.h"

#include <sys/stat.h>
#include <stdlib.h>
#include <attr/xattr.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <utime.h>              /* for deprecated marfs_utime() */
#include <stdio.h>
#include <time.h>


// syscall(2) manpage says do this, but still getting "implicit decl" warnings
#ifndef _GNU_SOURCE
#  define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>


// fine, you want decls, here are decls ...
#ifndef _BSD_SOURCE
int syscall(int number, ...);
int getgrouplist(const char *user, gid_t group, gid_t *groups, int *ngroups); 
int setgroups(size_t size, const gid_t *list); 
#endif




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
//
// NOTE: seteuid/setegid are actually per-process, rather than per-thread
//       making them useless for fuse.  setfsuid/setfsgid are broken for
//       some other reason I forget now.  What you want is the syscalls
//       setresuid/setresgid, which are per-thread, and which afffect
//       fsuid/fsgid just as you would hope.
//
// NOTE: setgroups is also per-thread, which is also what you want.


int push_groups4(PerThreadContext* ctx, uid_t uid, gid_t gid) {

   TRY_DECLS();

   // check for two pushes without a pop
   if (ctx->pushed_groups) {
      LOG(LOG_ERR, "double-push (groups) -> %u\n", uid);
      errno = EPERM;
      return -1;
   }

   // save the group-list for the current process
   ctx->group_ct = getgroups(sizeof ctx->groups / sizeof (gid_t), ctx->groups);
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
   TRY0( setgroups(ngroups, groups) );

   ctx->pushed_groups = 1;   // so we can pop
   return 0;
}


int push_user4(PerThreadContext* ctx,
               uid_t             new_euid,
               gid_t             new_egid,
               int               push_groups) {

#  if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)

   int rc;

   // check for two pushes without a pop
   if (ctx->pushed) {
      LOG(LOG_ERR, "double-push -> %u\n", new_euid);
      errno = EPERM;
      return -1;
   }

   // --- maybe install the user's group-list (see comments, above)
   if (push_groups) {
      if (push_groups4(ctx, new_euid, new_egid)) {
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
      exit(EXIT_FAILURE);       // fuse should fail (?)
   }

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

   ctx->pushed = 1;             // prevent double-push

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

   TRY0( setgroups(ctx->group_ct, ctx->groups) );

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

   ctx->pushed = 0;
   return 0;

#  else
#  error "No support for seteuid()/setegid()"
#  endif
}
