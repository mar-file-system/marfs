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

       char* MARFS_POST_FORMAT2 = "ver.%03hu_%03hu/%c/off.%d/objs.%d/bytes.%d/corr.%016ld/crypt.%016ld/flags.%02hhX/mdfs.%s";
       //works char* MARFS_POST_FORMAT2 = "ver.%03hu_%03hu/%c/off.%d/objs.%d/bytes.%d/corr.%016ld/crypt.%016ld/flags.%02d/mdfs.%c";
       //char* MARFS_POST_FORMAT2 = "ver.%03hu_%03hu/%c/off.%d/objs.%d/bytes.%d/corr.%016d/crypt.%016d/flags.%02hhX/mdfs.%c";



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
        MarFS_XattrPost2 post;
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

int post_2_str2(char* post_str, size_t max_size, const MarFS_XattrPost2 *post);
int str_2_post2(MarFS_XattrPost2* post, const char* post_str);
int get_inodes(const char *fnameP, int obj_size, struct marfs_inode *inode, int *marfs_inodeLen, const char* namespace);
int get_objects(struct marfs_inode *unpacked, int unpacked_size, obj_lnklist*  packed, int *packed_size, int obj_size_max);
int pack_up(obj_lnklist *objects, MarFS_Repo* repo, MarFS_Namespace* ns);
int set_md(obj_lnklist *objects);
int set_xattrs(int inode, int xattr);
int setup_config();
int trash_inode(int inode); 
int fasttreewalk(char* path, int inode);
int push( struct walk_path stack[MAX_STACK_SIZE],int *top, struct walk_path *data);
int pop( struct walk_path stack[MAX_STACK_SIZE], int *top, struct walk_path *data);
void fasttreewalk2(char* path, int inode);
void get_marfs_path(char * patht, char marfs[]);
void check_security_access(MarFS_XattrPre *pre);
void print_usage();
