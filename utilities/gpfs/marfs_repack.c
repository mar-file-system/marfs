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
#include "utilities_common.h"


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
* "tmp_packed_log" file from garbage collection.  This file lists the packed 
* object name and associated files for the packed object.  It only lists those
* objects that could NOT reconcile with the file counts.  The format of this 
* file is:
*
* OBJECT_NAME  EXPECTED_FILE_COUNT  FILE_NAME
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
   //

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
* collection and determines which objects are candidates for repacking. 
*
* The criteria for repacking is:
*    object file count < post xattr chunk_count
* This implies that some of the files belonging to a packed object have
* been deleted, therefore the object no longer contains the full set
* of files it originally contained
*
* The process of repacking starts by finding all objects and files that
* are candidates and placing them into objects and file link lists
* respectively.  Each object * link list entry (node) contains a 
* link list of files that belong to the * new object. 
* 
******************************************************************************/
int find_repack_objects(File_Handles *file_info, 
                        repack_objects **objects_ptr) {

   FILE *pipe_cat = NULL;
   FILE *pipe_grep = NULL;

   char obj_buf[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char obj_buf1[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char objid[MARFS_MAX_OBJID_SIZE];
   char cat_command[MAX_CMD_LENGTH];
   char grep_command[MAX_CMD_LENGTH];
   char marfs_path[1024];
   const char* sub_path;
   int file_count;

   int chunk_count;
   char filename[MARFS_MAX_MD_PATH];

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
   // Now loop through all objects sorted by name in order to count number 
   // of files associated with that object
   while(fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat)) {
      sscanf(obj_buf,"%s", objid);
      sprintf(grep_command, "cat %s | grep %s ", file_info->packed_log, objid);
      if (( pipe_grep = popen(grep_command, "r")) == NULL) {
         fprintf(stderr, "Error with popen\n");
         return(-1);
      }
      file_count = 0;
      while(fgets(obj_buf1, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_grep)) {
         sscanf(obj_buf1,"%s %d %s", objid, &chunk_count, filename);
         // Need to look into this - do we need a minimum value to repack?
         // same question as packer script
         // Well for now this is not an issue because just trying to repack
         // so that garbage collection can delete the objects
         file_count++;
         if (chunk_count <= 1) { 
            continue;
         }
 
         // file count is the number of files associated with an object (from tmp_packed_log)
         // chunck count is the post xattr value that states how many files in packed
         // objecd
         // if file_count < chunk_count, the files can be packed
         if (file_count < chunk_count) {
            // Create the first files link list node
            if ((files = (obj_files *)malloc(sizeof(obj_files)))==NULL) {
               fprintf(stderr, "Error allocating memory\n");
               return -1;
            }
            
            strcpy(files->filename, filename);

            // Get info into MarFS_FileHandle structure because this is needed for 
            // further operations such as read with offset and write
            //
            memset(&files->fh, 0, sizeof(MarFS_FileHandle));
            
            // First convert md path to a fuse path
            get_marfs_path(filename, &marfs_path[0]);

            // now get fuse path minus mount
            sub_path = marfs_sub_path(marfs_path);

            // Fill in the file handle structure
            expand_path_info(&files->fh->info, sub_path);

            stat_xattrs(&files->fh->info);

            files->original_offset = files->fh->info.post.obj_offset;
            LOG(LOG_INFO, "filename %s offset=%ld\n", files->filename, files->fh->info.post.obj_offset);


            // adjust files link list pointers
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
      // Update objects structure entries based on file info
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
* This function traverses the object link list created in find_repack_objects
* and reads the corresponding file data.  That data is then written to a new
* object.  Because the old object had holes due to missing files, a new
* write offset is calculated.
*
******************************************************************************/
int pack_objects( File_Handles *file_info, 
                 repack_objects *objects)
{
   struct stat statbuf;
   char *path = "/";

   stat(path, &statbuf);
   size_t write_offset = 0;
   size_t obj_raw_size;
   size_t obj_size;
   //size_t offset;
   size_t unique;
   IOBuf *nb = aws_iobuf_new();
   //char test_obj[2048];
   obj_files *files;
   //int ret;
   //char *obj_ptr;
   //CURLcode s3_return;
   //char pre_str[MARFS_MAX_XATTR_SIZE];
   char marfs_path[1024];
   int flags;

   // Traverse object link list and find those that should be packed
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

      files->new_offset = write_offset;

      // Specify a new object being accessed 
      unique=0;

      // Each object has a files linked list.  Read each file 
      // at the offset calculated and write back to new object with
      // new offset.
      while (files) {
         // Get the associated Marfs file handle from from the linked list
         MarFS_XattrPre*  pre  = &files->fh->info.pre;

         // If new object increment unique to give it a new objid
         if (unique == 0) 
            pre->unique++;    
        
         // Need to make sure that objectSize
         // does not include recovery info TBD
         obj_raw_size = files->fh->objectSize;
         
         obj_size = obj_raw_size + MARFS_REC_UNI_SIZE;
         files->size = obj_size;

//********************
// Questions 
// correct path (fuse path)?
// set flags for open
// offset for read becomes 0, correct?
//
  
  
         char read_buf[1024];  
         size_t read_count;
         ssize_t write_count;
         
         flags = O_RDONLY; 
         get_marfs_path(files->filename, &marfs_path[0]);
         marfs_open_at_offset(marfs_path,
                              files->fh,
                              flags,
                              files->fh->info.post.obj_offset, 
                              obj_size);
         read_count = marfs_read(marfs_path, // Need recovery info as well
                                 read_buf,
                                 obj_size,
                                 0,
                                 files->fh);

         marfs_release (marfs_path, files->fh);
// Instead of reading more to I write now 
// This becomes a new object because pre.unique incremented
//
// DO I need to do anything special with flags?
// O_CREATE or O_APPEND?
// Need new flag with open or new function for recovery info
//
         marfs_open(marfs_path,
                    files->fh,
                    flags,  // WRITE
                    obj_size);
         write_count = marfs_write(marfs_path, // Need recovery info as well
                                   read_buf,
                                   obj_size,
                                   files->new_offset,
                                   files->fh);
         // This needs be moved outside loop
         // and I need an open write before while (files)
         // with O_CREATE and O_WRONLY
         // Jeff states I may need a special release with offset 
         // of last object
         //
         //marfs_release (marfs_path, files->fh);

         LOG(LOG_INFO, "Read buffer write count = %ld  len = %ld\n", nb->write_count, nb->len);
         // may have to copy nb to a new buffer 
         // then write 
     

         files->new_offset = write_offset;
         write_offset += obj_size; 
	 files = files->next;
      }
      objects=objects->next;
   } return 0;
}
/******************************************************************************
* Name update_meta
*
* This function updates the xattrs to reflect the newly repacked object
* This includes a new object name and updated offsets and chunks in the 
* post xattr. 
* 
******************************************************************************/
int update_meta(File_Handles *file_info, 
                repack_objects *objects)
{
  obj_files *files;
//  char pre[MARFS_MAX_XATTR_SIZE];

  char post_str[MARFS_MAX_XATTR_SIZE];
  char post_xattr[MARFS_MAX_XATTR_SIZE];
  //int rc;


  // Travers all repacked objects
  while(objects) {
     files = objects->files_ptr;
     //Traverse the object files
     while (files) {

        // retrive the MarFS_FileHandle information from the files linked
        // list

        //MarFS_XattrPre*  pre  = &files->fh->info.pre;
        MarFS_XattrPost* post = &files->fh->info.post;

        // Get the file post xattr
        // and update its elements based on the repack
        //if ((getxattr(files->filename, "user.marfs_post", &post_xattr, MARFS_MAX_XATTR_SIZE) != -1)) {
        //   rc=str_2_post(&post, post_xattr, 1);
           post->chunks = objects->pack_count;
           post->obj_offset = files->new_offset;
           

             
           save_xattrs(&files->fh->info, XVT_PRE | XVT_POST);

           // convert the post xattr back to string so that the file xattr can
           // be re-written
           //rc=post_2_str(post_str, MARFS_MAX_XATTR_SIZE, &post, pre.repo, 0);
           LOG(LOG_INFO, "Old xattr:       %s\n", post_xattr);
           LOG(LOG_INFO, "New post xattr:  %s\n", post_str);
           fprintf(file_info->outfd, "Updating %s objid xattr to %s\n", files->filename, objects->new_objid);
           fprintf(file_info->outfd, "Updating %s post xattr  to %s\n", files->filename, post_str);
           //To do:
           // remove old object
        //}
	files = files->next;
     }
     objects=objects->next;
  }
  return 0;
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

/******************************************************************************
* Name get_marfs_path
* This function, given a metadata path, determines the fuse mount path for a 
* file and returns it via the marfs pointer. 
* 
******************************************************************************/
void get_marfs_path(char * patht, char *marfs) {
  char *mnt_top = marfs_config->mnt_top;
  MarFS_Namespace *ns;
  NSIterator ns_iter;
  ns_iter = namespace_iterator();
  char the_path[MAX_PATH_LENGTH] = {0};
  char ending[MAX_PATH_LENGTH] = {0};
  int i;
  int index;

  while((ns = namespace_next(&ns_iter))){
    if (strstr(patht, ns->md_path)){

      // build path using mount point and md_path
      strcat(the_path, mnt_top);
      strcat(the_path, ns->mnt_path);

      for (i = strlen(ns->md_path); i < strlen(patht); i++){
        index = i - strlen(ns->md_path);
        ending[index] = *(patht+i);
      }

      ending[index+1] = '\0';
      strcat(the_path, ending);
      strcpy(marfs,the_path);
      break;
    }
  }
}
