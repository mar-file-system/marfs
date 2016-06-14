#ifndef MARFS_QUOTA_H
#define MARFS_QUOTA_H
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


#include <stdint.h>
#include <stddef.h>             // size_t
#include <sys/types.h>          // ino_t
#include <sys/stat.h>
#include <math.h>               // floorf
#include <gpfs_fcntl.h>
#include "marfs_base.h"
#include "aws4c.h"


// CHECK this and compare to Jeff's
#define MAX_FILESET_NAME_LEN 256

// Xattr info
#define MAX_MARFS_XATTR 3
#define MARFS_GC_XATTR_CNT 3
#define DAY_SECONDS 24*60*60

#define MAX_PACKED_NAME_SIZE 1024

#define TMP_LOCAL_FILE_LEN 1024 

struct marfs_xattr {
  char xattr_name[GPFS_FCNTL_XATTR_MAX_NAMELEN];
  char xattr_value[GPFS_FCNTL_XATTR_MAX_VALUELEN];
};

typedef struct Fileset_Info {
   char fileset_name[MARFS_MAX_NAMESPACE_NAME];
   // CHECK THIS 
//   char repo_name[MAX_FILESET_NAME_LEN];
   char repo_name[MARFS_MAX_NAMESPACE_NAME];
   char host[32];
} Fileset_Info;

typedef struct File_Info {
   char fileset_name[MARFS_MAX_NAMESPACE_NAME];
   FILE *outfd;
   FILE *packedfd;
   char packed_filename[TMP_LOCAL_FILE_LEN];
   unsigned int is_packed;
   unsigned int no_delete;
   MarFS_ObjType obj_type;
   unsigned int restart_found;
} File_Info;


int read_inodes(const char   *fnameP, 
                File_Info    *file_info_ptr, 
                int          fileset_id, 
                Fileset_Info *fileset_info_ptr, 
                size_t       rec_count, 
                unsigned int day_seconds);
int clean_exit(FILE                 *fd, 
               gpfs_iscan_t         *iscanP, 
               gpfs_fssnap_handle_t *fsP, 
               int                  terminate);
int get_xattr_value(struct     marfs_xattr *xattr_ptr, 
                    const char *desired_xattr, 
                    int        cnt);
int get_xattrs(gpfs_iscan_t *iscanP,
               const char   *xattrP,
               unsigned int xattrLen,
               const char   **marfs_xattr,
               int          max_xattr_count,
               struct       marfs_xattr *xattr_ptr, 
               FILE         *outfd);
void print_usage();
void init_records(Fileset_Info *fileset_info_buf, unsigned int record_count);
int dump_trash(struct marfs_xattr *xattr_ptr, 
               char               *md_path_ptr,  
               File_Info          *file_info_ptr, 
               MarFS_XattrPost    *post_xattr,
               MarFS_XattrPre     *pre_ptr);
int delete_object(char * object, File_Info *file_info_ptr,MarFS_XattrPre *pre_ptr, int is_mult);
int delete_file(char *filename, File_Info *file_info_ptr);
int process_packed(File_Info *file_info_ptr);
void print_current_time(File_Info *file_info_ptr);
int read_config_gc(Fileset_Info *fileset_info_ptr);
#endif

