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


/******************************************************************************
* Name  main
* 
* This utility, as currently written, repacks objects in the trash directory 
* due to mismatches between the chunk count specified in the trash file xattr
* and the actual number of files found in trash for the specified packed 
* object.
*
* The method for determining whether objects need to repacked is to read the 
* the file specied by the -d option.  This is typically the 
* tmp_packed_log file from garbage collection.  This file lists the packed 
* object name and associated files for the packed object.  It only lists those
* objects that could NOT reconcile with the file counts.  The format of this 
* file is:
*
* OBJECT_NAME  FILE_NAME  EXPECTED_FILE_COUNT  HOST BUCKED  OBJID   
*
* This utility reads the file and finds all files for the associated object
* The number of files found will be less than EXPECTED_FILE_COUNT (post xattr
* chunk count).  It then proceeds to read in the file objects and rewrite
* them back to a new object with offsets re-defined.  Xattrs are updated
* to reflect the new object, chunk_count, and offsets 
*
******************************************************************************/
int main(int argc, char **argv){
   int c;
   char *fnameP = NULL;
   char *outfile = NULL;
   //char *ns = NULL;

   while ((c=getopt(argc,argv,"d:o:h")) != EOF) {
      switch(c) {
         case 'd': fnameP = optarg; break;
         //case 'n': ns = optarg; break;
         case 'o': outfile = optarg; break;
         case 'h': print_usage();
         default:
            exit(-1);
      }
   }
   if (fnameP == NULL || outfile == NULL) {
      print_usage();
      exit(-1);
   }

   if ( setup_config() == -1 ) {
      fprintf(stderr,"Error:  Initializing Configs and Aws failed, quitting!!\n");
      exit(-1);
   }


   File_Handles file_info;
   File_Handles *file_status = &file_info;

   // open associated log file and packed log file
   if ((file_status->outfd = fopen(outfile,"w")) == NULL){
      fprintf(stderr, "Failed to open %s\n", outfile);
      exit(1);
   }
   strcpy(file_status->packed_log, fnameP);



   //MarFS_Namespace* namespace;
   //namespace = find_namespace_by_name(ns);
        //MarFS_Repo* repo = namespace->iwrite_repo;
        // Find the correct repo - the one with the largest range
   //MarFS_Repo* repo = find_repo_by_range(namespace, (size_t)-1);


   repack_objects *objs = NULL;
   find_repack_objects(file_status, &objs);
   pack_objects(file_status, objs);
   update_meta(file_status, objs);
   free_objects(objs);
   return 0;
}
/******************************************************************************
* Name find_repack_objects 
* 
* This function takes the output of the tmp_packed_log created by garbage
* collection and determines which objects are candidates for repacking.  The
* object information is retrieved and placed into an objects link list and a 
* files link list. 
* 
******************************************************************************/
int find_repack_objects(File_Handles *file_info, repack_objects **objects_ptr) {
   FILE *pipe_cat = NULL;
   FILE *pipe_grep = NULL;

   char obj_buf[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char obj_buf1[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char objid[MARFS_MAX_OBJID_SIZE];
   char cat_command[MAX_CMD_LENGTH];
   char grep_command[MAX_CMD_LENGTH];
   int file_count;

   int chunk_count;
   char filename[MARFS_MAX_MD_PATH];
   MarFS_XattrPre pre_struct;
   MarFS_XattrPre* pre_ptr = &pre_struct;
   int rc;
   MarFS_XattrPost post;
   char post_xattr[MARFS_MAX_XATTR_SIZE];


   obj_files *files, *files_head;
   repack_objects *objects, *objects_head;
   files_head=NULL;
   objects_head=NULL;




   // create command to cat and sort the list of objects that are packed
   sprintf(cat_command, "cat %s | awk '{print $1}' | sort | uniq", file_info->packed_log);

   // open STREAM for cat
   if (( pipe_cat = popen(cat_command, "r")) == NULL) {
      fprintf(stderr, "Error with popen\n");
      return(-1);
   }
   while(fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat)) {
      sscanf(obj_buf,"%s", objid);
      sprintf(grep_command, "cat %s | grep %s ", file_info->packed_log, objid);
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


            if ((getxattr(files->filename, "user.marfs_post", &post_xattr, MARFS_MAX_XATTR_SIZE) != -1)) {
               //fprintf(stdout, "xattr = %s\n", post_xattr);
               rc = str_2_post(&post, post_xattr, 1);
               files->original_offset = post.obj_offset;
               LOG(LOG_INFO, "filename %s xattr = %s offset=%ld\n", files->filename, post_xattr,post.obj_offset);
            }
            files->next =  files_head;
            files_head = files;

         }

      }      
      if (pclose(pipe_grep) == -1) {
         fprintf(stderr, "Error closing cat pipe in process_packed\n");
         return(-1);
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
   if (pclose(pipe_cat) == -1) {
      fprintf(stderr, "Error closing cat pipe in process_packed\n");
      return(-1);
   }
   *objects_ptr=objects_head;
   return 0;
}

/******************************************************************************
* Name  pack_objects 
* 
* This function traverses the object and file link lists and reads object 
* data for repacking into a new object.   
******************************************************************************/
int pack_objects(File_Handles *file_info, repack_objects *objects)
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
   char test_obj[2048];
   obj_files *files;
   int ret;
   char *obj_ptr;
   CURLcode s3_return;
   char pre_str[MARFS_MAX_XATTR_SIZE];


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

      LOG(LOG_INFO,"object = %s\n", objects->objid);
      LOG(LOG_INFO, "file count = %ld chunks = %ld\n", objects->pack_count, objects->chunk_count);
      files = objects->files_ptr;
      write_offset = 0;
      ret=str_2_pre(&pre, objects->objid, NULL);
      sprintf(test_obj,"%s.teste",objects->objid);

      //Make this a unique object since it derived from an existing object 
      pre.unique++;    


      LOG(LOG_INFO,"stdout,new object name =%s\n", test_obj);
  
      //aws_iobuf_reset(nb);

      while (files) {
         //fprintf(stdout, "file = %s offset=%ld\n", files->filename, files->offset);

         stat(files->filename, &statbuf);


         obj_raw_size = statbuf.st_size;
         obj_size = obj_raw_size + MARFS_REC_UNI_SIZE;
         files->size = obj_size;

         //fprintf(stdout, "obj_size = %ld REC SIZE = %d\n", obj_size,MARFS_REC_UNI_SIZE);
         //write_offset+=obj_size;
         if ((obj_ptr = (char *)malloc(obj_size))==NULL) {
            fprintf(stderr, "Error allocating memory\n");
            return -1;
         }

         check_security_access(&pre);
         update_pre(&pre);
         s3_set_host(pre.host);
         //offset = objects->files_ptr->offset;

         offset = files->original_offset;
         //fprintf(stdout, "file %s will get re-written at offset %ld\n",
         //        files->filename, write_offset);

         // get object_data
         s3_set_byte_range(offset, obj_size);
         aws_iobuf_extend_static(nb, obj_ptr, obj_size);
         LOG(LOG_INFO, "going to get file %s from object %s at offset %ld and size %ld\n", files->filename, objects->objid, offset, obj_size);
         fprintf(file_info->outfd, "Getting file %s from object %s at offset %ld and size %ld\n", files->filename, objects->objid, offset, obj_size);
         s3_return = s3_get(nb,objects->objid);
         check_S3_error(s3_return, nb, S3_GET);

         LOG(LOG_INFO, "Read buffer write count = %ld  len = %ld\n", nb->write_count, nb->len);
         // may have to copy nb to a new buffer 
         // then write 
     

         files->new_offset = write_offset;
         write_offset += obj_size; 
	 files = files->next;
         free(obj_ptr);
      }
      // create object string for put
      pre_2_str(pre_str, MARFS_MAX_XATTR_SIZE,&pre);

      strcpy(objects->new_objid, pre_str);
     
      LOG(LOG_INFO, "Going to write to object %s\n", pre_str);
      fprintf(file_info->outfd, "Writing file to object %s\n", pre_str);
      //s3_put(nb,test_obj);
      s3_put(nb,pre_str);
      check_S3_error(s3_return, nb, S3_PUT); 

      aws_iobuf_reset_hard(nb);
//      aws_iobuf_reset(nb);
      objects=objects->next;
   }
   return 0;
}
/******************************************************************************
* Name update_meta
*
* This function updates the xattrs to reflect the newly repacked object
* This includes a new object name and updated offsets and chunks in the 
* post xattr. 
* 
******************************************************************************/
int update_meta(File_Handles *file_info, repack_objects *objects)
{
  obj_files *files;
//  char pre[MARFS_MAX_XATTR_SIZE];

  MarFS_XattrPost post;
  MarFS_XattrPre pre;
  char post_str[MARFS_MAX_XATTR_SIZE];
  char post_xattr[MARFS_MAX_XATTR_SIZE];
  int rc;



  while(objects) {
     files = objects->files_ptr;
     rc=str_2_pre(&pre, objects->objid, NULL);
     while (files) {
        // Get the file post xattr
        // and update its elements based on the repack
        if ((getxattr(files->filename, "user.marfs_post", &post_xattr, MARFS_MAX_XATTR_SIZE) != -1)) {
           rc=str_2_post(&post, post_xattr, 1);
           post.chunks = objects->pack_count;
           post.obj_offset = files->new_offset;
           
           // convert the post xattr back to string so that the file xattr can
           // be re-written
           rc=post_2_str(post_str, MARFS_MAX_XATTR_SIZE, &post, pre.repo, 0);
           LOG(LOG_INFO, "Old xattr:       %s\n", post_xattr);
           LOG(LOG_INFO, "New post xattr:  %s\n", post_str);
           fprintf(file_info->outfd, "Updating %s objid xattr to %s\n", files->filename, objects->new_objid);
           fprintf(file_info->outfd, "Updating %s post xattr  to %s\n", files->filename, post_str);
           // Set the objid xattr to the new object name
           setxattr(files->filename, "user.marfs_objid", objects->new_objid, strlen(objects->new_objid)+1, 0);
           setxattr(files->filename, "user.marfs_post", post_str, strlen(post_str)+1, 0);
           //To do:
           // remove old object
        }
	files = files->next;
     }
     objects=objects->next;
  }
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
/******************************************************************************
* Name:  check_security_access
*
* 
******************************************************************************/
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
/******************************************************************************
* Name check_S3_error  
* 
* Check for s3 errors           
******************************************************************************/
int check_S3_error( CURLcode curl_return, IOBuf *s3_buf, int action )
{
  if ( curl_return == CURLE_OK ) {
    if (action == S3_GET || action == S3_PUT ) {
       if (s3_buf->code == HTTP_OK || s3_buf->code == HTTP_NO_CONTENT) {
          return(0);
       }
       else {
         fprintf(stderr, "Error, HTTP Code:  %d\n", s3_buf->code);
         return(s3_buf->code);
       }
    }
  }
  else {
    fprintf(stderr,"Error, Curl Return Code:  %d\n", curl_return);
    return(-1);
  }
  return(0);
}
/******************************************************************************
 * * Name free_objects
 * * 
 * * Free objects link list memory
 * ******************************************************************************/
void free_objects(repack_objects *objects)
{
   repack_objects *temp_obj=NULL;
   obj_files *temp_files;
   obj_files  *object;

   while((temp_obj=objects)!=NULL) {
      object = objects->files_ptr;
      while((temp_files=object) != NULL) {
         object = object->next;
         free(temp_files);
      }
      objects = objects->next;
      free(temp_obj);
   }
}

/******************************************************************************
 *  * Name:  print_usage
 *  ******************************************************************************/
void print_usage()
{
  fprintf(stderr,"Usage: ./marfs_repack -d packed_log_filename -o log_file [-h]\n\n");
  fprintf(stderr, "where -h = help\n\n");
}

