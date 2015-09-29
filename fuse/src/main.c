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
// main
// ---------------------------------------------------------------------------


int main(int argc, char* argv[])
{
   size_t rc;                   /* used by "TRY" macros */

   INIT_LOG();
   LOG(LOG_INFO, "starting\n");

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
   //     it inside marfs_open(), for every S3 open, but it also means we
   //     don't know whether we really need it.
   //
   // ALSO: At start-up time, $USER is "root".  If we want per-user S3 IDs,
   //     then we would have to either (a) load them all now, and
   //     dynamically pick the one we want inside marfs_open(), or (b) call
   //     aws_read_config() inside marfs_open(), using the euid of the user
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
      .init        = marfs_init,
      .destroy     = marfs_destroy,

      .access      = marfs_access,
      .chmod       = marfs_chmod,
      .chown       = marfs_chown,
      .ftruncate   = marfs_ftruncate,
      .fsync       = marfs_fsync,
      .fsyncdir    = marfs_fsyncdir,
      .getattr     = marfs_getattr,
      .getxattr    = marfs_getxattr,
      .ioctl       = marfs_ioctl,
      .listxattr   = marfs_listxattr,
      .mkdir       = marfs_mkdir,
      .mknod       = marfs_mknod,
      .open        = marfs_open,
      .opendir     = marfs_opendir,
      .read        = marfs_read,
      .readdir     = marfs_readdir,
      .readlink    = marfs_readlink,
      .release     = marfs_release,
      .releasedir  = marfs_releasedir,
      .removexattr = marfs_removexattr,
      .rename      = marfs_rename,
      .rmdir       = marfs_rmdir,
      .setxattr    = marfs_setxattr,
      .statfs      = marfs_statfs,
      .symlink     = marfs_symlink,
      .truncate    = marfs_truncate,
      .unlink      = marfs_unlink,
      .utime       = marfs_utime, /* deprecated in 2.6 */
      .utimens     = marfs_utimens,
      .write       = marfs_write,
#if 0
      // not implemented
      .bmap        = marfs_bmap,
      .create      = marfs_create,
      .fallocate   = marfs_fallocate /* not in 2.6 */
      .fgetattr    = marfs_fgetattr,
      .flock       = marfs_flock,
      .flush       = marfs_flush,
      .getdir      = marfs_getdir, /* deprecated in 2.6 */
      .link        = marfs_link,
      .lock        = marfs_lock,
      .poll        = marfs_poll,
#endif
   };

   return fuse_main(argc, argv, &marfs_oper, NULL);
}
