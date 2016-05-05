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

#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <gpfs.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <gpfs_fcntl.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <ctype.h>
#include <attr/xattr.h>
#include "marfs_base.h"
#include "common.h"
#include "marfs_ops.h"
#include "marfs_repack.h"


// 
//struct walk_path paths[10240000];
//struct walk_path paths[1024];
//
//
/******************************************************************************
* Name  main
* 
******************************************************************************/
int main(int argc, char **argv){
/********
        // I had to comment this out I was getting a seg fault with this allocation
        //struct marfs_inode unpacked[102400];
        struct marfs_inode unpacked[1024];
        int unpackedLen=0;
***********/
   int c;
   //char *rdir = NULL;
   // char *patht = NULL;
   char *fnameP = NULL;
   //unsigned int pack_obj_size = 1073741824;
   char *ns = NULL;

   //while ((c=getopt(argc,argv,"d:p:s:n:h")) != EOF) {
   while ((c=getopt(argc,argv,"d:n:")) != EOF) {
      switch(c) {
         case 'd': fnameP = optarg; break;
         case 'n': ns = optarg; break;
         default:
            exit(-1);
      }
   }
   if ( setup_config() == -1 ) {
      fprintf(stderr,"Error:  Initializing Configs and Aws failed, quitting!!\n");
      exit(-1);
   }
   MarFS_Namespace* namespace;
   namespace = find_namespace_by_name(ns);
        //MarFS_Repo* repo = namespace->iwrite_repo;
        // Find the correct repo - the one with the largest range
   MarFS_Repo* repo = find_repo_by_range(namespace, (size_t)-1);


   repack_objects *objs = NULL;
   find_repack_objects(fnameP, &objs);
   pack_objects(objs);
   //fprintf(stdout, "Check if objs\n");
   //while (objs) {
   //   fprintf(stdout, "objects in main = %s\n", objs->objid);
   //   objs=objs->next;
   //}
   return 0;
}
// find objects that need to be repacked and place in a dynamic link list
// that contains info about object
int find_repack_objects(char *fnameP, repack_objects **objects_ptr) {
   FILE *pipe_cat = NULL;
   FILE *pipe_grep = NULL;

   char obj_buf[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char obj_buf1[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char objid[MARFS_MAX_OBJID_SIZE];
   char cat_command[1024];
   char grep_command[1024];
   int file_count;

   int chunk_count;
   char filename[MARFS_MAX_MD_PATH];
   MarFS_XattrPre pre_struct;
   MarFS_XattrPre* pre_ptr = &pre_struct;
   int rc;
   MarFS_XattrPost post;
   char post_xattr[1024];


   obj_files *files, *files_head;
   repack_objects *objects, *objects_head;
   files_head=NULL;
   objects_head=NULL;




   // create command to cat and sort the list of objects that are packed
   sprintf(cat_command, "cat %s | awk '{print $1}' | sort | uniq", fnameP);

   // open STREAM for cat
   if (( pipe_cat = popen(cat_command, "r")) == NULL) {
      fprintf(stderr, "Error with popen\n");
      return(-1);
   }
   while(fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat)) {
      sscanf(obj_buf,"%s", objid);
      sprintf(grep_command, "cat %s | grep %s ", fnameP, objid);
      if (( pipe_grep = popen(grep_command, "r")) == NULL) {
         fprintf(stderr, "Error with popen\n");
         return(-1);
      }
      file_count = 0;
      while(fgets(obj_buf1, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_grep)) {
         sscanf(obj_buf1,"%s %s %d %s %s %s", objid, filename, &chunk_count,
                pre_ptr->host, pre_ptr->bucket, pre_ptr->objid);
         // Need to look into this - do we need a minimum value to repack?
         // same question as packer script
         file_count++;
         if (chunk_count <= 1) { 
            continue;
         }
         // if file_count == chunk_count, the files can be packed
         if (file_count <= chunk_count) {
            // Create object link list here
            //fprintf(stdout, "object = %s\n", objid);
            //fprintf(stdout, "file_count = %d chunk_count=%d\n", file_count, chunk_count);
            if ((files = (obj_files *)malloc(sizeof(obj_files)))==NULL) {
               fprintf(stderr, "Error allocating memory\n");
               return -1;
            }
            //fprintf(stdout, "%s %s\n", objid, filename);
            
            strcpy(files->filename, filename);


            if ((getxattr(files->filename, "user.marfs_post", &post_xattr, 1024) != -1)) {
               //fprintf(stdout, "xattr = %s\n", post_xattr);
               rc = str_2_post(&post, post_xattr, 1);
               files->offset = post.obj_offset;
               fprintf(stdout, "filename %s xattr = %s offset=%ld\n", files->filename, post_xattr,post.obj_offset);
            }
            files->next =  files_head;
            files_head = files;

         }

      }      
      if ((objects = (repack_objects *)malloc(sizeof(repack_objects)))==NULL) {
         fprintf(stderr, "Error allocating memory\n");
         return -1;
      }
      strcpy(objects->objid, objid);
      objects->pack_count = file_count;
      objects->chunk_count = chunk_count;
      objects->files_ptr = files_head;
      objects->next = objects_head;
      objects_head = objects;
      files_head=NULL;
   }
   //while (objects) {
   //   fprintf(stdout, "object = %s\n", objects->objid);
   //   fprintf(stdout, "file count = %ld chunks = %ld\n", objects->pack_count, objects->chunk_count);
   //  files = objects->files_ptr;
   //   while (files) {
   //      fprintf(stdout, "file = %s\n", files->filename);
   //	 files = files->next;
   //   }
   //   objects=objects->next;
   //}
   *objects_ptr=objects_head;
   return 0;
}


int pack_objects(repack_objects *objects)
{
   struct stat statbuf;
   char *path = "/";
//   repack_objects *objects; 

	 //struct stat statbuf;
   stat(path, &statbuf);
   size_t write_offset = 0;
   size_t obj_raw_size;
   size_t obj_size;
   size_t offset;
   //MarFS_XattrPre pre_struct;
   //MarFS_XattrPre* pre = &pre_struct;
   MarFS_XattrPre pre;
   IOBuf *nb = aws_iobuf_new();
   IOBuf *put_buf = aws_iobuf_new();
   char url[MARFS_MAX_XATTR_SIZE];
   char test_obj[1024];
   obj_files *files;
   int ret;
   char *data_ptr = NULL;


   // Also, if file_count =1 do i make uni or?
   //
   //
   while (objects) { 
      // need inner loop to get files for each object
      // If chunk_count == file count no need to pack
      // and garbage collection took care of it
      if (objects->chunk_count == objects->pack_count) {
         objects=objects->next;
         continue;
      }
      //No need to pack if only one file specified in xattr and only
      //one file found
      if (objects->chunk_count == 1 && objects->pack_count ==1 ) {
         objects=objects->next;
         continue;
      }
      // Not quite sure how this next condition could happen
      // TO DO:  make only one contion chunk_count > file_count
      // all others continue
      if (objects->pack_count > objects->chunk_count) {
         objects=objects->next;
         continue;
      }

      fprintf(stdout, "object = %s\n", objects->objid);
      fprintf(stdout, "file count = %ld chunks = %ld\n", objects->pack_count, objects->chunk_count);
      files = objects->files_ptr;
      write_offset = 0;
      ret=str_2_pre(&pre, objects->objid, NULL);
      sprintf(test_obj,"%s.test3",objects->objid);


      fprintf(stdout,"new object name =%s\n", test_obj);
  
      //aws_iobuf_reset(nb);

      while (files) {
         //fprintf(stdout, "file = %s offset=%ld\n", files->filename, files->offset);

         stat(files->filename, &statbuf);
         obj_raw_size = statbuf.st_size;
         obj_size = obj_raw_size + MARFS_REC_UNI_SIZE;
         //fprintf(stdout, "obj_size = %ld REC SIZE = %d\n", obj_size,MARFS_REC_UNI_SIZE);
         //write_offset+=obj_size;

         check_security_access(&pre);
         update_pre(&pre);
         s3_set_host(pre.host);
         //offset = objects->files_ptr->offset;
         offset = files->offset;
         //fprintf(stdout, "file %s will get re-written at offset %ld\n",
         //        files->filename, write_offset);

         // get object_data
         //aws_iobuf_extend_static(nb, data_ptr, obj_size);
         s3_set_byte_range(offset, obj_size);
         aws_iobuf_extend_static(nb, data_ptr, obj_size);
         fprintf(stdout, "going to get file %s from object %s at offset %ld\n", files->filename, objects->objid, offset);
         s3_get(nb,objects->objid);
         fprintf(stdout, "Read buffer write count = %ld  len = %ld\n", nb->write_count, nb->len);
         // may have to copy nb to a new buffer 
         // then write 
     

         //write data 
         // How do I specify offset?
         aws_iobuf_reset_hard(nb);
         fprintf(stdout, "going to write to object %s at offset %ld with size %ld\n", test_obj, write_offset,obj_size);
         aws_iobuf_append_static(nb, data_ptr, nb->len);
         //s3_set_byte_range(write_offset,obj_size);
         //s3_put(io_buf,test_obj);
         write_offset += obj_size; 
         //aws_iobuf_reset(nb);
	 files = files->next;
      }
      s3_put(nb,test_obj);
      aws_iobuf_reset_hard(nb);
      objects=objects->next;
   }
   return 0;
}


int update_meta()
{
  return 0;
}
/******************************************************************************
* Name setup_config
* This function reads the configuration file and initializes the configuration
* 
******************************************************************************/
int setup_config(){
        // read MarFS configuration file
        if (read_configuration()) {
           fprintf(stderr, "Error Reading MarFS configuration file\n");
           return(-1);
        }
        // Initialize MarFS xattr specs 
        init_xattr_specs();

        // Initialize aws
        aws_init();
        aws_reuse_connections(1);
        aws_set_debug(0);
        aws_read_config("root");
        return 0;
}

void check_security_access(MarFS_XattrPre *pre)
{
   if (pre->repo->security_method == SECURITYMETHOD_HTTP_DIGEST)
      s3_http_digest(1);

   if (pre->repo->access_method == ACCESSMETHOD_S3_EMC) {
      s3_enable_EMC_extensions(1);

      // For now if we're using HTTPS, I'm just assuming that it is without
      // validating the SSL certificate (curl's -k or --insecure flags). If
      // we ever get a validated certificate, we will want to put a flag
      // into the MarFS_Repo struct that says it's validated or not.
      if (pre->repo->ssl ) {
         s3_https( 1 );
         s3_https_insecure( 1 );
       }
   }
   else if (pre->repo->access_method == ACCESSMETHOD_SPROXYD) {
      s3_enable_Scality_extensions(1);
      s3_sproxyd(1);

      // For now if we're using HTTPS, I'm just assuming that it is without
      // validating the SSL certificate (curl's -k or --insecure flags). If
      // we ever get a validated certificate, we will want to put a flag
      // into the MarFS_Repo struct that says it's validated or not.
      if (pre->repo->ssl ) {
         s3_https( 1 );
         s3_https_insecure( 1 );
      }
   }
}
