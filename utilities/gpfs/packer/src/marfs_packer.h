#ifndef MARFS_PACKER_H
#define MARFS_PACKER_H
/*
 * This file is part of MarFS, which is released under the BSD license.
 *
 *
 * Copyright (c) 2015, Los Alamos National Security (LANS), LLC
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * -----
 *  NOTE:
 *  -----
 *  MarFS uses libaws4c for Amazon S3 object communication. The original version
 *  is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
 *  LANS, LLC added functionality to the original work. The original work plus
 *  LANS, LLC contributions is found at https://github.com/jti-lanl/aws4c.
 *
 *  GNU licenses can be found at <http://www.gnu.org/licenses/>.
 *
 *
 *  From Los Alamos National Security, LLC:
 *  LA-CC-15-039
 *
 *  Copyright (c) 2015, Los Alamos National Security, LLC All rights reserved.
 *  Copyright 2015. Los Alamos National Security, LLC. This software was produced
 *  under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
 *  Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
 *  the U.S. Department of Energy. The U.S. Government has rights to use,
 *  reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
 *  ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
 *  ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
 *  modified to produce derivative works, such modified software should be
 *  clearly marked, so as not to confuse it with the version available from
 *  LANL.
 *
 *  THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
 *  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 *  OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 *  */

#include <aws4c.h>
//#include <object_stream.h>
#include <marfs_base.h>

#define MAX_STACK_SIZE 1024


// This defines how many paths the treewalk will work on at a time
#define MAX_SCAN_FILE_COUNT 1024

#define MAX_PATH_LENGTH 1024
//#define MAX_SCAN_FILE_COUNT 5 

typedef struct MarFS_XattrPost2 {
   uint16_t           config_vers_maj; // redundant w/ config_vers in Pre?
   uint16_t           config_vers_min; // redundant w/ config_vers in Pre?
   MarFS_ObjType      obj_type;      // type of storage
   int                obj_offset;    // offset of file in the obj (Packed)
   int                chunks;        // (context-dependent.  See NOTE)
   int                chunk_info_bytes; // total size of chunk-info in MDFS file (Multi)
   CorrectInfo        correct_info;  // correctness info  (e.g. the computed checksum)
   EncryptInfo        encrypt_info;  // any info reqd to decrypt the data
   PostFlagsType      flags;
   char               md_path[MARFS_MAX_MD_PATH]; // full path to MDFS file
} MarFS_XattrPost2;

struct walk_path{
	int inode;
	char path[1024];
	char parent[1024];
};

struct marfs_inode {
	time_t atime;
	time_t ctime;
	time_t mtime;
        int inode;
        int size;
        int offset;
	char path[1024];
        //char url[MARFS_MAX_URL_SIZE];
//      struct MarFS_XattrPost mpost;   
        MarFS_XattrPre pre;
//        char pre[1215];
//      char post[1215];
//        MarFS_XattrPost2 post;
        MarFS_XattrPost post;
};
//typedef struct list_el item;
//typedef struct list_olist list;
//struct list_el {
typedef struct inode_lnklist {
	struct marfs_inode val;
	int count;
	//struct list_el * next;
	struct inode_lnklist *next;
} inode_lnklist;

//struct list_olist {
typedef struct obj_lnklist {
   //struct list_olist * next;
   struct obj_lnklist *next;
   int count;
   //struct item * val;
   struct inode_lnklist *val;
} obj_lnklist;

int get_objects(struct marfs_inode *unpacked, int unpacked_size, obj_lnklist*  packed, int *packed_size, int obj_size_max);
int pack_up(obj_lnklist *objects, MarFS_Repo* repo, MarFS_Namespace* ns);
int set_md(obj_lnklist *objects);
int set_xattrs(int inode, int xattr);
int setup_config();
int trash_inode(int inode); 
int push( struct walk_path stack[MAX_STACK_SIZE],int *top, struct walk_path *data);
int pop( struct walk_path stack[MAX_STACK_SIZE], int *top, struct walk_path *data);
void get_marfs_path(char * patht, char marfs[]);
void check_security_access(MarFS_XattrPre *pre);
void print_usage();
int walk_and_scan_control (char* top_level_path, size_t max_object_size,
                            size_t small_obj_size, const char* ns,
                            MarFS_Repo* repo, MarFS_Namespace* namespace,
                            uint8_t no_pack_flag);
//int get_inodes(const char *fnameP, int obj_size, struct marfs_inode *inode, int *marfs_inodeLen, const char* namespace, size_t small_obj_size, struct walk_path *paths);
int get_inodes(const char *fnameP, int obj_size, struct marfs_inode *inode,
               int *marfs_inodeLen, size_t *sum_size, const char* namespace,
               size_t small_obj_size, struct walk_path *paths);
int find_inode(size_t inode_number, struct walk_path *paths);
int pack_and_write(char* top_level_path, size_t max_object_size, MarFS_Repo* repo, 
                   MarFS_Namespace* namespace, const char *ns, size_t small_obj_size, 
                   struct walk_path *paths, uint8_t no_pack);
int parse_size_arg(char *input_size, uint64_t *out_value);
#endif

