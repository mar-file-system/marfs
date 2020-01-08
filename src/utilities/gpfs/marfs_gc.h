#ifndef MARFS_GC_H
#define MARFS_GC_H
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
#include <signal.h>

#include "marfs_base.h"
#include "common.h"             // marfs/fuse/src/common.h
#include "aws4c.h"
#include "hash_table.h"

#define PACKED_TABLE_SIZE 10000
#define REPACK_TABLE_SIZE 1000
#define QUEUE_MAX_DEFAULT 1000
#define NUM_THRDS_DEFAULT 16

// CHECK this and compare to Jeff's
#define MAX_FILESET_NAME_LEN 256

// Xattr info
#define MAX_MARFS_XATTR 3
#define MARFS_GC_XATTR_CNT 3
#define DAY_SECONDS 24*60*60

#define MAX_PACKED_NAME_SIZE 1024

#define TMP_LOCAL_FILE_LEN 1024 

#define VERB_FPRINTF(...)  if( run_info.verbose ) { fprintf(__VA_ARGS__); }

struct marfs_xattr {
  char xattr_name[GPFS_FCNTL_XATTR_MAX_NAMELEN];
  char xattr_value[GPFS_FCNTL_XATTR_MAX_VALUELEN];
};

typedef struct Fileset_Info {
   char fileset_name[MARFS_MAX_NAMESPACE_NAME];
   char repo_name[MARFS_MAX_NAMESPACE_NAME]; // MAX_FILESET_NAME_LEN?
   char host[32];
} Fileset_Info;

typedef struct Run_Info {
   // per-run settings
   //char          packed_filename[TMP_LOCAL_FILE_LEN];
   //FILE         *packedfd;
   FILE         *outfd;
   unsigned int  no_delete;                            /* from option 'n': dry run */
   char          target_ns[MARFS_MAX_NAMESPACE_NAME];  /* from option 'N' */
   size_t        target_ns_size;
   unsigned char verbose;
   unsigned char has_packed;
   unsigned long long deletes;
   unsigned long long warnings;
   unsigned long long errors;
   unsigned int  queue_max;
   unsigned int  queue_high_water;
} Run_Info;         

typedef struct File_Info {
   // per-file discoveries
   //char          fileset_name[MARFS_MAX_NAMESPACE_NAME];
   MarFS_ObjType obj_type;
   unsigned char  restart_found;
} File_Info;


int read_inodes(const char   *fnameP, 
                int          fileset_id, 
                Fileset_Info *fileset_info_ptr, 
                size_t       rec_count, 
                unsigned int day_seconds,
                hash_table_t* ht);
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
               struct       marfs_xattr *xattr_ptr);
void print_usage();
int  dump_trash(MarFS_FileHandle   *fh,
                File_Info          *file_info_ptr);
int  delete_object(MarFS_FileHandle *fh,
                   File_Info        *file_info_ptr,
                   int               is_mult);
int  delete_file(char *filename);
int  process_packed(hash_table_t* ht, hash_table_t* rt);
void print_current_time();
int  read_config_gc(Fileset_Info *fileset_info_ptr);

#endif

