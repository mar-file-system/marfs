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

// ---------------------------------------------------------------------------
// This is an experimental first-cut at the "pipetool".
// The idea is something like this:
//
//    tar -czvf - /scratch/mydir | pipetool /marfs/mydir.tgz
//
// So, we read from stdin, and write a single stream to some marfs-file
// named on the commmand-line.
// ---------------------------------------------------------------------------

#include "marfs_base.h"
#include "common.h"
#include "push_user.h"
#include "marfs_ops.h"

#include <aws4c.h>

#include <stdlib.h>

#include <stdio.h>
#include <string.h>
#include <errno.h>

// syscall(2) manpage says do this
#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>


int main(int argc, char* argv[])
{
   TRY_DECLS();

   if (argc != 2) {
      fprintf(stderr, "usage: %s <marfs_md_file>\n", argv[0]);
      exit(1);
   }
   const char* marfs_path = argv[1];

   INIT_LOG();
   LOG(LOG_INFO, "\n");
   LOG(LOG_INFO, "=== %s %s\n", argv[0], marfs_path);

   ///   // Not sure why, but I've seen machines where I'm logged in as root, and
   ///   // I run fuse in the background, and the process has an euid of some other user.
   ///   // This fixes that.
   ///   //
   ///   // NOTE: This also now *requires* that marfs fuse is always only run as root.
   ///   __TRY0( seteuid(0) );

   if (read_configuration()) {
      LOG(LOG_ERR, "read_configuration() failed.  Quitting\n");
      return -1;
   }
   else if (validate_config()) {
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

   ///   char* const user_name = (getenv("USER"));
   char* const user_name = "root";
   if (aws_read_config(user_name)) {
      // probably missing a line in ~/.awsAuth
      LOG(LOG_ERR, "aws-read-config for user '%s' failed\n", user_name);
      if (! config_fail_ok)
         exit(1);
   }


   ///   // make sure all support directories exist.  (See the config file.)
   ///   // This includes mdfs, fsinfo, the trash "scatter-tree", for all namespaces,
   ///   // plus a storage "scatter-tree" for any semi-direct repos.
   ///   __TRY0( init_mdfs() );


   // get current real/effective uid
   uid_t ruid = getuid();
   uid_t euid = geteuid();
   printf("ruid: %u, euid: %u\n", ruid, euid);

   // get current real/effective gid
   gid_t rgid = getgid();
   gid_t egid = getegid();
   printf("rgid: %u, egid: %u\n", rgid, egid);

#if 0
   // DEBUGGING

   int gdb=0;
   while (!gdb) {
      printf("waiting for gdb ...\n");
      sleep(4);
   }
#endif

   // Like pftool, marfspipe has to be SUID, because it needs to read
   // ~root/.awsAuth at start-up.  Now, we can de-escalate.
   PUSH_USER(1, ruid, rgid);
   printf("pushed euid:%d egid:%d\n", geteuid(), getegid());

   ///   // read from stdin
   ///   int in = open(info->post.md_path, O_RDONLY);
   ///   if (in == -1) {
   ///      LOG(LOG_ERR, "open(%s, O_RDONLY) [oct]%o failed\n",
   ///          info->post.md_path, new_mode);
   ///      return -1;
   ///   }

   // transfer-buffer
   ///   static const size_t BUF_SIZE = 1024 * 1024 * 16; // 16 MB
   ///   char buf[BUF_SIZE +1];

#define BUF_SIZE (1024 * 1024 * 16) /* 16 MB */
   char* buf = malloc(BUF_SIZE +1);
   if (! buf) {
      LOG(LOG_ERR, "couldn't allocate %d bytes\n", BUF_SIZE +1);
      exit(EXIT_FAILURE);
   }

   off_t  offset = 0;

   // write to marfs_file from command-line
   MarFS_FileHandle fh;
   memset(&fh, 0, sizeof(MarFS_FileHandle));

   ///   // if it doesn't exist, we have to call marfs_mknod() first.  BETTER:
   ///   // fix marfs_open() to handle O_CREAT, and let fuse be the place where
   ///   // we worry about whether the file exists already or not.
   ///   PathInfo* info = &fh.info;
   ///   strncpy(info->post.md_path, md_path, MARFS_MAX_MD_PATH); // use argv[1]
   ///   info->post.md_path[MARFS_MAX_MD_PATH -1] = 0;

   const char* sub_path = marfs_sub_path(marfs_path);
   TRY0( marfs_open(sub_path, &fh, (O_CREAT | O_WRONLY), 0) );

   struct timeval start;
   struct timeval end;

   gettimeofday(&start, NULL);
   while (1) {
      ssize_t read_ct = read(STDIN_FILENO, buf, BUF_SIZE);
      if (read_ct < 0) {
         LOG(LOG_ERR, "read failed: %s\n", strerror(errno));
         exit(EXIT_FAILURE);
      }
      else if (! read_ct)
         break;                 // EOF

      // copy to marfs file
      TRY_GE0( marfs_write(sub_path, buf, read_ct, offset, &fh) );
      offset += rc_ssize;
   }

   TRY0( marfs_release(sub_path, &fh) );
   gettimeofday(&end, NULL);

   struct timeval diff;
   diff.tv_sec = end.tv_sec - start.tv_sec;
   if (end.tv_usec < start.tv_usec) {
      diff.tv_sec  -= 1;
      diff.tv_usec  = start.tv_usec - end.tv_usec;
   }
   else 
      diff.tv_usec  = end.tv_usec - start.tv_usec;

   float elapsed = diff.tv_sec + ((float)diff.tv_usec / 1000000);
   float mib     = (float)offset/(1024 * 1024);

   printf("Copied %ld bytes (%5.2f MiB)\n", offset, mib);
   printf("%5.3f sec, %5.1f MiB/s\n", elapsed, mib/elapsed);

   // no need to re-escalate for shut-down.
   // POP_USER();
}
