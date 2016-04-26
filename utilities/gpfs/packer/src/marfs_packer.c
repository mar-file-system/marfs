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
#include <ctype.h>
#include <attr/xattr.h>
#include "marfs_base.h"
#include "common.h"
#include "marfs_ops.h"
#include "marfs_packer.h"



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
        char *ns = NULL;
        uint8_t no_pack_flag = 0;

        int min_file_cnt = 10;
        int max_file_cnt  =1024;
        int min_file_size = 1; 
        int max_file_size = 1048576;

        char *outf = NULL;
        pack_vars pack_elements;
        pack_vars *pack_elements_ptr =&pack_elements;

        //while ((c=getopt(argc,argv,"d:p:s:n:h")) != EOF) {
        while ((c=getopt(argc,argv,"d:n:o:lh")) != EOF) {
           switch(c) {
              case 'd': fnameP = optarg; break;
              case 'n': ns = optarg; break;
              case 'o' : outf = optarg; break;
              case 'l' : no_pack_flag = 1; break;
              case 'h': print_usage();
              default:
                 exit(-1);
           }
        }
        if (fnameP == NULL || ns == NULL || outf==NULL) {
           fprintf(stderr, "ERROR:  Must specify top level dir -d (gpfs fileset) ");
           fprintf(stderr, "and namespace -n and -o log_file\n\n");
           print_usage();
           exit(-1);
        }

        // Get rid of trailing / in gpfs path if one exists
        size_t path_len = strlen(fnameP);
        if (path_len > 0 && fnameP[path_len-1] == '/')
           fnameP[path_len -1] = '\0';

        // Read configuration and initialize aws
        if ( setup_config() == -1 ) {
           fprintf(stderr,"Error:  Initializing Configs and Aws failed, quitting!!\n");
           exit(-1);
        }

        MarFS_Namespace* namespace;
        namespace = find_namespace_by_name(ns);

        // Find the correct repo - the one with the largest range
        MarFS_Repo* repo = find_repo_by_range(namespace, (size_t)-1);
         
        pack_elements_ptr->max_object_size = repo->chunk_size;
        fprintf(stdout, "Setting pack size to config chunk size value: %zu\n", 
                pack_elements_ptr->max_object_size);

        // Get packing parameters from config or use defaults;
        if (repo->max_pack_file_count == 0){
           fprintf(stderr, "Config file has max_pack_file_count set to 0\n");
           fprintf(stderr, "No packing will be done\n");
           return -1;
        }
        else if (repo->max_pack_file_count == -1) {
           pack_elements_ptr->max_pack_file_count  = max_file_cnt;
           fprintf(stdout, "Setting max_pack_file_count to default value: %d\n", 
                   max_file_cnt);
        }
        else {
           pack_elements_ptr->max_pack_file_count  = repo->max_pack_file_count;
           fprintf(stdout, "Setting max_pack_file_count to config value: %zu\n", 
                   pack_elements_ptr->max_pack_file_count);
        }
        if (repo->min_pack_file_size == -1) {
           pack_elements_ptr->min_pack_file_size = min_file_size;
           fprintf(stdout, "Setting min_pack_file_size to default value: %d\n", 
                   min_file_size);
        }
        else {
           pack_elements_ptr->min_pack_file_size = repo->min_pack_file_size;
           fprintf(stdout, "Setting min_pack_file_size to config value: %zu\n", 
                   pack_elements_ptr->min_pack_file_size);
        }
        if (repo->max_pack_file_size == -1) {
           pack_elements_ptr->max_pack_file_size = max_file_size;
           fprintf(stdout, "Setting min_pack_file_size to default value: %d\n", 
                   max_file_size);
        }
        else {
           pack_elements_ptr->max_pack_file_size = repo->max_pack_file_size;
           fprintf(stdout, "Setting max_pack_file_size to config value: %zu\n", 
                   pack_elements_ptr->max_pack_file_size);
        }
        
        if (repo->min_pack_file_count == -1) {
           pack_elements_ptr->min_pack_file_count = min_file_cnt;
           fprintf(stdout, "Setting min_pack_file_couun to default value: %d\n", 
                   min_file_cnt);
        }
        else { 
           pack_elements_ptr->min_pack_file_count = repo->min_pack_file_count;
           fprintf(stdout, "Setting min_pack_file_count to config value: %zu\n", 
                   pack_elements_ptr->min_pack_file_count);
        }

        if ((pack_elements_ptr->outfd = fopen(outf, "w"))==NULL) {
           fprintf(stderr, "Failed to open %s\n", outf);
           exit(1);
        }

        // Start the process by first walking the directory tree to associate 
        // inodes with directory paths
        // Once the paths are established perform a inode scan to find candidates
        // for packing.  If objects found, pack, write and update xattrs.
        walk_and_scan_control (fnameP, ns, repo, namespace, no_pack_flag, 
                               pack_elements_ptr);
        //                        
        fclose(pack_elements_ptr->outfd);
        return 0;
}


  
/******************************************************************************
* Name 
* 
******************************************************************************/
MarFS_Repo_Ptr find_repo_by_name2( const char* name ) {

  MarFS_Repo*   repo = NULL;
  RepoIterator  it = repo_iterator();

	//printf("starting repo iterator\n");
  while ((repo = repo_next(&it))) {
    if ( ! strcmp( repo->name, name )) {
      return repo;
    }
  }
  return NULL;
}

/******************************************************************************
* Name  get_objects
* This function determines how many objects can be packed into a single object.
* It iterates through all the inodes found that meet the packing criteria and 
* creates link lists for all the inodes that fit in one object.  The target 
* object size is determine by obj_size_max which is passed into this function
* If more than one object is created in this function, the objects are joined
* in a link list as well.
* 
******************************************************************************/
int get_objects(struct marfs_inode *unpacked, int unpacked_size, 
		obj_lnklist**  packed, int *packed_size, pack_vars *packed_params){
	//int cur_size=0;	
	int sum_obj_size=0;	
	int i;
	int count=0;
	//int temp_count = 0;
        int obj_cnt = 0;
	inode_lnklist *sub_objects, *sub_obj_head;
	obj_lnklist *main_object, *main_obj_head;
        main_object = NULL;

	main_obj_head = NULL;
        sub_obj_head = NULL;
  


        // There are three conditions for packing objects
        // -objects add up to less than exact size of target object
        // -objects add up to the exact size of target object
        // -objects add up to > greater than size of target object
        //  and less than another target object      

        // Note this code can be consolidated with function calls or
        // MACROs but just trying to get functionality first.
        int need_main;
   
        // loop through all inodes found
	for (i = 0; i < unpacked_size; i++){
           LOG(LOG_INFO, "inode loop count = %d\n", i);
           sum_obj_size+=unpacked[i].size; 
           obj_cnt++;
           // add upp sizes of each object
           if (sum_obj_size < packed_params->max_object_size) {
              if ((sub_objects = (inode_lnklist *)malloc(sizeof(inode_lnklist)))==NULL) {
                 fprintf(stderr, "Error allocating memory\n");
                 return -1;
              }
              sub_objects->val = unpacked[i];
              sub_objects->next  = sub_obj_head;
              sub_obj_head = sub_objects;
              LOG(LOG_INFO, "adding sum size = %d\n", sum_obj_size);
              need_main = -1;
           }
            // check if sum is equal to or greater than target object
           else if (sum_obj_size >= packed_params->max_object_size) {
               // if the sum of the objects = the target file size
               // terminate the sub_objects link list 
               // and create a new main_object entry in the linked list
               if(sum_obj_size == packed_params->max_object_size) {
                  LOG(LOG_INFO, "Equal sum %d\n", sum_obj_size);
                  if ((sub_objects = (inode_lnklist *)malloc(sizeof(inode_lnklist)))==NULL) {
                     fprintf(stderr, "Error allocating memory\n");
                     return -1;
                  }
                  sub_objects->val = unpacked[i];
                  sub_objects->next  = sub_obj_head;
                  sub_obj_head = NULL;

                  // Add main object stuff here
                  if ((main_object = (obj_lnklist *)malloc(sizeof(obj_lnklist)))==NULL) {
                     fprintf(stderr, "Error allocating memory\n");
                     return -1;
                  }
                  main_object->val = sub_objects;
                  //main_object->count=obj_cnt;
                  main_object->next = main_obj_head;
                  main_obj_head = main_object;
                  //obj_cnt = 0;
                  count++;
                  sum_obj_size = 0;
                  need_main = 0;
               }
               // else the size is greater than the target object size so
               // terminate sub_object link list and create a new main object
               // link here.  
               // also start a new sub_object linked list
               else { 
                  //create a new main object
                  if ((main_object = (obj_lnklist *)malloc(sizeof(obj_lnklist)))==NULL) {
                     fprintf(stderr, "Error allocating memory\n");
                     return -1;
                  }
                  main_object->val = sub_objects;
                  //main_object->count= obj_cnt;
                  main_object->next = main_obj_head;
                  main_obj_head = main_object;
                  sub_obj_head=NULL;
                  count++;
                  sum_obj_size = 0;
                  //obj_cnt++;
                  if ((sub_objects = (inode_lnklist *)malloc(sizeof(inode_lnklist)))==NULL) {
                     fprintf(stderr, "Error allocating memory\n");
                     return -1;
                  }
                  sub_objects->val = unpacked[i];
                  sub_objects->next  = NULL;
                  sub_obj_head = sub_objects;

                  need_main = -1;
               }
           }
	}
        // done summing sizes, create target object if at least one not created or 
        // picking remainder from else above
        if (need_main == -1 && obj_cnt >= packed_params->min_pack_file_count) {
           if ((main_object = (obj_lnklist *)malloc(sizeof(obj_lnklist)))==NULL) {
              fprintf(stderr, "Error allocating memory\n");
              return -1;
           }
           main_object->val = sub_objects;
           //main_object->count=obj_cnt;
           main_object->next = main_obj_head;
           count++;
        }
        //else if (obj_cnt < packed_params->min_pack_file_count) {
        //    free_sub_objects(sub_objects);
        //}
        //main_object->next = NULL;

        if (obj_cnt < packed_params->min_pack_file_count) {
            free_sub_objects(sub_objects);
	    *packed_size = 0;
            fprintf(stderr, "Number of objects to pack does not minimum requirements %d\n", obj_cnt);
            return -1;
        }
        else {
	   *packed_size = count;
	   *packed = main_object;
           LOG(LOG_INFO, "main_object_count: %d\n", count);
           if (main_object->val == NULL) {
              LOG(LOG_INFO, "found NULL value even though inode scan found something\n");
              fprintf(stderr, "found NULL value even though inode scan found something\n");
              return -1;
           }
	   return 0;
        }
}
/******************************************************************************
* Name pack_up
* This function goes throught the main object link list and reads all data from
* the associated sub objects (using s3_get).
* It then writes the data to the new object using s3_put
******************************************************************************/
int pack_up(obj_lnklist *objects, MarFS_Repo* repo, MarFS_Namespace* ns)
{
   srand(time(NULL));
   int r = rand();
   int count = 0;
	
   IOBuf *nb = aws_iobuf_new();
   inode_lnklist *object;
   char url[MARFS_MAX_XATTR_SIZE];
   char pre_str[MARFS_MAX_XATTR_SIZE];
   CURLcode s3_return;

   // Iterated through the link list of packed objects
   while(objects){
      LOG(LOG_INFO,"outer while\n");
      // point to associated link sub_object link list
      object = objects->val;
      if (objects->val == NULL) {
         LOG(LOG_INFO, "NULL object\n");
         break;
      }
      MarFS_XattrPre packed_pre;
                
      // get the pre xattr and md_inode value
      packed_pre = object->val.pre;

      packed_pre.obj_type = OBJ_PACKED;
      count = 0;	

      // Now iterate through the sub_objects and update lengths, offset
      // type, namespace, repo
      while(object) {
         LOG(LOG_INFO, "Getting inode info %d\n", count);

         object->val.offset = nb->write_count;
         object->val.post.obj_offset = nb->write_count;
         object->val.post.obj_type = OBJ_PACKED;	

         //char url[MARFS_MAX_XATTR_SIZE];

         object->val.pre.ns = ns;
	 object->val.pre.repo = repo;
	 pre_2_str(url, MARFS_MAX_XATTR_SIZE, &object->val.pre);
         LOG(LOG_INFO, "unpacked url:=%s\n", url);

         check_security_access(&object->val.pre);
         update_pre(&object->val.pre);
         s3_set_host(object->val.pre.host);

         // get object_data
         s3_return = s3_get(nb,url);
         check_S3_error(s3_return, nb, S3_GET);

	 object->val.pre = packed_pre;
	 object->val.pre.obj_type = OBJ_PACKED;
         object = object->next ;
         count++;
      }
      LOG(LOG_INFO, "count = %d\n", count);
                
      packed_pre.ns = ns;	
      packed_pre.repo = repo;

      //char pre_str[MARFS_MAX_XATTR_SIZE];
      packed_pre.obj_type = 3;
      //packed_pre.obj_type = OBJ_PACKED;
      pre_2_str(pre_str, MARFS_MAX_XATTR_SIZE, &packed_pre);	
      r = rand();
      check_security_access(&packed_pre);
      update_pre(&packed_pre);
      LOG(LOG_INFO, "pre_str:=%s\n", pre_str);
      r = rand();

      // write data to new object
      s3_return = s3_put(nb,pre_str);
      check_S3_error(s3_return, nb, S3_PUT);
      //

      aws_iobuf_reset(nb);
      if (objects)
         objects->count = count;
      objects = objects->next;
      count=0;
   }
   return 0;
}

/******************************************************************************
* Name set_md
* This function updates the metadata associated with all objects that were 
* packed into a single object 
* 
******************************************************************************/
int set_md(obj_lnklist *objects, pack_vars *pack_params)
{
   //item * object;	
   inode_lnklist *object;
   int count = 0;
   char pre[MARFS_MAX_XATTR_SIZE];
   char post[MARFS_MAX_XATTR_SIZE];
   char *pre_ptr = &pre[0];
   ino_t obj_inode;
   time_t obj_md_ctime;
   time_t obj_ctime;
   char marfs_path[1024];
   char *path;
   struct stat statbuf;
   FILE *fp;

   LOG(LOG_INFO, "got to set md\n");
   while(objects){
      object = objects->val;
      while(object) {
         //char *path = object->val.path;
         path = object->val.path;
         LOG(LOG_INFO, "path=%s\n", path);
  
	 //struct stat statbuf;
         stat(path, &statbuf);
         if (statbuf.st_atime != object->val.atime || statbuf.st_mtime != object->val.mtime || statbuf.st_ctime != object->val.ctime){
            LOG(LOG_INFO, "changed path:=%s\n", path);
            LOG(LOG_INFO, "stat of path = %ld  stat from object = %ld\n", statbuf.st_atime, object->val.atime);
	 }
         else {
            // objid xattr will be set to match inode and ctime for first object in packed
            if (count == 0) {
               obj_inode =  object->val.pre.md_inode; 
               obj_md_ctime = object->val.pre.md_ctime;
               obj_ctime = object->val.pre.obj_ctime;
               count =1; 
               LOG(LOG_INFO, "md_inode = %ld\n", obj_inode);
            }
            // Not first object so retieve first object parameters
            else {
               object->val.pre.md_inode = obj_inode;
               object->val.pre.md_ctime = obj_md_ctime;
               object->val.pre.obj_ctime = obj_ctime;
            }

            // This is called to get the fuse mount
            // path to the file for removal process 
            // that follows
            get_marfs_path(path, &marfs_path[0]);

            object->val.pre.obj_type = OBJ_PACKED;
            object->val.post.chunks = objects->count;
            LOG(LOG_INFO, "set md count = %d\n", objects->count);
            pre_2_str(pre_ptr, MARFS_MAX_XATTR_SIZE, &object->val.pre);
            post_2_str(post, MARFS_MAX_XATTR_SIZE, &object->val.post,object->val.pre.repo,0);
                           
            // Remove the files via fuse mount
            if ((marfs_unlink(marfs_sub_path(marfs_path)))==-1)
               fprintf(stderr, "Error removing %s\n", marfs_path);
            LOG(LOG_INFO, "remove marfs_path:=%s\n", marfs_path);			
                               
            // Open new gpfs file and truncate to the corret size
            //FILE *fp;
            fp = fopen(object->val.path, "w+");
            fclose(fp);
            if ((truncate(object->val.path, object->val.size)) == -1) { 
               fprintf(stderr, "Error truncating gpfs file to correct size\n");
               return -1;    
            } 

            LOG(LOG_INFO, "count:=%d paths:=%s\n", objects->count, object->val.path);
            LOG(LOG_INFO, "obj_type:=%s\n", pre);
            LOG(LOG_INFO, "post: %s\n", post);
	    //getxattr(path,"user.marfs_objid",  value, MARFS_MAX_XATTR_SIZE);
            setxattr(path, "user.marfs_objid", pre, strlen(pre), 0);
            setxattr(path, "user.marfs_post", post, strlen(post), 0);
            fprintf(pack_params->outfd, "Packed file:  %s  into object:  %s\n", 
                    object->val.path, pre);
	 } // end else
         object = object->next ;
      } // endwhile object
      objects = objects->next;
      count = 0;
   } //end while objects

   return 0;
}


/******************************************************************************
* Name 
* 
******************************************************************************/
int set_xattrs(size_t inode, int xattr){
/* set the xattr stuff according to struct 
 * Don't forget new offsets, which will include size+recovery info
 * All new meta data ctime is all the same for each packed like object
 * */
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
* Name 
* 
******************************************************************************/
int trash_inode(size_t inode){                                                                                                                                                                                
/*
 *
 *   
 * Tie into alfreds trashing code
 *   
 */
        return 0;
}

/******************************************************************************
* Name push
* Use with fasttreewalk to build up directory paths while travesing dir tree 
******************************************************************************/
int push( struct walk_path stack[MAX_STACK_SIZE],int *top, struct walk_path *data)
{
	if( *top == MAX_STACK_SIZE -1 )
		return(-1);
	else
	{
		*top = *top + 1;
		stack[*top] = *data;
		return(1);
	} // else
} // push

/******************************************************************************
* Name pop
* Use with fasttreewalk to build up directory paths while travesing dir tree 
* 
******************************************************************************/
int pop( struct walk_path stack[MAX_STACK_SIZE], int *top, struct walk_path *data)
{
	if( *top == -1 )
		return(-1);
	else
	{
		*data = stack[*top];
		*top = *top - 1;
		return(1);
	} //else
} // pop


/******************************************************************************
* Name get_marfs_path
* This function, given a metadata path, determines the fuse mount path for a 
* file and returns it via the marfs pointer. 
* 
******************************************************************************/
//int get_marfs_path(char * patht, char *marfs[]){
void get_marfs_path(char * patht, char *marfs){
	char *mnt_top = marfs_config->mnt_top;
        MarFS_Namespace *ns;
//	printf("got to get_marfs_path step1\n");
        NSIterator ns_iter;
        ns_iter = namespace_iterator();
        char the_path[MAX_PATH_LENGTH] = {0};
        char ending[MAX_PATH_LENGTH] = {0};
        int i;
        int index;

        while((ns = namespace_next(&ns_iter))){
                if(strstr(patht, ns->md_path)){

                        // build path using mount point and md_path
                        strcat(the_path, mnt_top);
                        strcat(the_path, ns->mnt_path);

                        for(i = strlen(ns->md_path); i < strlen(patht); i++){
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
/******************************************************************************
* Name check_security_access 
* This function determines and sets appropriate security options for
* the target storage solution.
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
 * Name:  print_usage
******************************************************************************/
void print_usage()
{
  fprintf(stderr,"Usage: ./marfs_packer -d gpfs_path -n namespace -o log_file\
  [-l] [-h] \n\n");
  fprintf(stderr, "where -l = provide summary only - do not pack\n");
  fprintf(stderr, "where -h = help\n\n");
}

/******************************************************************************
* Name walk_and_scan_control 
* This function performs a gpfs tree walk in order to assoicate file paths 
* with inode numbers.  It works on a fixed amount of paths at a time.  After
* a set of paths are identified, pack_and_write is called to continue the 
* process. 
******************************************************************************/
int walk_and_scan_control (char* top_level_path, const char* ns,
                            MarFS_Repo* repo, MarFS_Namespace* namespace,
                            uint8_t no_pack, pack_vars *pack_params)
{
//   struct marfs_inode unpacked[1024]; // Chris originally had this set to 102400 but that caused problems
   struct walk_path dpath;
   struct walk_path rdpath = {0};
   int reg_file_cnt =0;
   int i;
   struct walk_path *paths;
   int ret;

   // To do 
   // dynamically create this using max_pack_file_count
   //struct walk_path paths[MAX_SCAN_FILE_COUNT];  
   if ((paths = (struct walk_path *)malloc(sizeof(struct walk_path)*pack_params->max_pack_file_count)) ==NULL) {
      fprintf(stderr, "Error allocating memory\n");   
      return(-1);
   }


   rdpath.inode = 3;
   strcpy(rdpath.path,top_level_path);

   // HAD TO SET THIS otherwise first pop was pulling garbage
   strcpy(rdpath.parent,"");
   //

   LOG(LOG_INFO,"parent:=%s\n", rdpath.path);

   //struct walk_path stack[max], data;
   struct walk_path stack[MAX_STACK_SIZE];
   //int top, option, reply;
   int top = 0;

   int rc = push(stack,&top, &rdpath);

   gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path(top_level_path);
   gpfs_ifile_t *file;
   const gpfs_direntx64_t *dirP;
   struct walk_path walkP;

   // This while loop watches the stack for entries.  Once the stack is empty,
   // the loop breaks
   while ( top >= 1){
      rc = pop(stack, &top,&walkP);
      file = gpfs_iopen64(fsP,walkP.inode, O_RDONLY, NULL, NULL);

      // This loop is responsible traversing the directory tree and pushing
      // new directories onto the stack.  At a count (MAX_SCAN_FILE_SIZE) it will
      // break away and try to do some packing.  The reason for this is the
      // possibly large number of inodes and the risk of running out
      // of memory due to path hashing that is based on inode numbers.
      while(1){
         rc = gpfs_ireaddir64(file, &dirP);
         if (dirP == NULL)
            break;
         dpath.inode = dirP->d_ino;
         strcpy(dpath.path,dirP->d_name);

         // If this is a directory
         if (dirP->d_type == GPFS_DE_DIR) {
            if (!(strcmp(dirP->d_name, ".") == 0) && !(strcmp(dirP->d_name, "..") == 0)){
               strcpy(dpath.parent, walkP.parent);
               strcat(dpath.parent, "/");
               strcat(dpath.parent, dpath.path);
               push(stack, &top, &dpath);
               strcpy(dpath.parent, walkP.path);
            }
         }
         // Else regular file
         else if(dirP->d_type == GPFS_DE_REG) {
            strcpy(dpath.parent, top_level_path);
            strcat(dpath.parent, walkP.parent);
            strcat(dpath.parent, "/");
            strcat(dpath.parent, dirP->d_name);

            // instead of using dpath.inode, use counter value and update inode and path in structure
            //paths[dpath.inode] = dpath;
            LOG(LOG_INFO, "Found regular file %s\n", dpath.parent);
            if ((strstr(dpath.parent, namespace->md_path))!=NULL) {
               paths[reg_file_cnt] = dpath;
               LOG(LOG_INFO, "found inode in desired namespace\n");
            //}

            reg_file_cnt++;
            LOG(LOG_INFO, "file count %d\n", reg_file_cnt);
            //if (reg_file_cnt == MAX_SCAN_FILE_COUNT ) {
            if (reg_file_cnt == pack_params->max_pack_file_count ) {
               ret = pack_and_write(top_level_path, repo, namespace, ns, &paths[0], no_pack, pack_params);
               if (ret == -1)
                  goto clean_and_free;        
               reg_file_cnt = 0;
            } // endif reg_file_cnt 
            }
         } // endif regular file
      } // endwhile finding directories and paths
      if (file) {
         gpfs_iclose(file);
      }
   }
   // If any leftovers, pack the rest now
   if (reg_file_cnt !=0) {
      // zero out remaining buffer
      //for ( i = reg_file_cnt; i < MAX_SCAN_FILE_COUNT; i++) {
      for ( i = reg_file_cnt; i < pack_params->max_pack_file_count; i++) {
          paths[i].inode = -1;
      }
      ret = pack_and_write(top_level_path, repo, namespace, ns, &paths[0], no_pack, pack_params);
   } 
clean_and_free:
   if (fsP) {
      gpfs_free_fssnaphandle(fsP);
   }
   free(paths);
   return 0;
}


/******************************************************************************
 * Name:  get_inodes 
 * This function performs a gpfs inode scan looking for candidate
 * objects for packing
******************************************************************************/
int get_inodes(const char *fnameP, struct marfs_inode *inode, 
               int *marfs_inodeLen, size_t *sum_size, const char* namespace, 
               struct walk_path *paths, pack_vars *pack_params)
{
   int counter = 0;
   const gpfs_iattr_t *iattrP;
   const char *xattrBP;
   unsigned int xattr_len;
   int rc;
   const char* nameP;
   const char* valueP;
   unsigned int valueLen;

   int ret;
   char fileset_name_buffer[MARFS_MAX_NAMESPACE_NAME];

   size_t inode_index;
   IOBuf *head_buf = aws_iobuf_new();
   char *object = NULL;
   int printable;
   int i;

   MarFS_XattrPost post;
   MarFS_XattrPre pre;

   // Setup for gpfs inode scan
   gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path(fnameP);
   if (fsP == NULL) {
      fprintf(stderr, "Error with gpfs_get_fssnaphandle\n");
      return(-1);
   }
   register gpfs_iscan_t *iscanP = gpfs_open_inodescan_with_xattrs(fsP, NULL, -1, NULL, NULL);
   if (iscanP == NULL) {
      fprintf(stderr, "Error opening inodescan\n");
      return(-1);
   }

   // While getting inodes
   while (1){
      //printf("in while\n");
      rc = gpfs_next_inode_with_xattrs(iscanP,0x7FFFFFFF,&iattrP,&xattrBP, &xattr_len);
      if ((iattrP == NULL) || (iattrP->ia_inode > 0x7FFFFFFF))
          break;
      if (iattrP->ia_inode != 3 && xattr_len > 0) {
         const char *xattrBufP = xattrBP;
         unsigned int xattrBufLen = xattr_len;
         //MarFS_XattrPre pre;

         gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid,
                              &fileset_name_buffer, MARFS_MAX_NAMESPACE_NAME);

         if (!strcmp(fileset_name_buffer, namespace)) {

            // Get xattrs associated with file
            while ((xattrBufP != NULL) && (xattrBufLen > 0)) {
               rc = gpfs_next_xattr(iscanP, &xattrBufP, &xattrBufLen, &nameP, &valueLen, &valueP);
               if (rc != 0) {
                  rc = errno;
                  fprintf(stderr, "gpfs_next_xattr: %s\n", strerror(rc));
                  return(-1);
               }
               // Found xattr?
               if (nameP == NULL)
                  break;

               // Make sure xattr value contains printable characters
               if (valueLen > 0) {
                  printable = 0;
                  if (valueLen > 1) {
                     printable = 1;
                     for (i = 0; i < (valueLen-1); i++)
                        if (!isprint(valueP[i]))
                           printable = 0;
                     if (printable) {
                        if (valueP[valueLen-1] == '\0')
                           valueLen -= 1;
                        else if (!isprint(valueP[valueLen-1]))
                           printable = 0;
                     }
                  }
               }


               LOG(LOG_INFO,"xattr length %d\n", valueLen);
               LOG(LOG_INFO,"%s %d\n", valueP, valueLen);
               // If object xattr - convert to pre structure
               if (strcmp(nameP, "user.marfs_objid") == 0){
                  //LOG(LOG_INFO,"%s %d\n", valueP, valueLen);
                  ret = str_2_pre(&pre, valueP, NULL);
                  object=strdup(valueP);
               }
               // else if post xattr - conver to post structure
               else if (strcmp(nameP, "user.marfs_post") == 0){
                  //MarFS_XattrPost post;
                  rc = str_2_post(&post, valueP, 1);

                  // Check if this is a relevant object for packing 
                  if (post.flags != POST_TRASH && iattrP->ia_size > 0 && 
                      iattrP->ia_size<=pack_params->max_pack_file_size && 
                      iattrP->ia_size>=pack_params->min_pack_file_size && 
                      post.obj_type == OBJ_UNI ){
                     
                     // Does this inode match an inode from the treewalk path discovery
                     if ((inode_index = find_inode(iattrP->ia_inode, paths, pack_params)) == -1) {
                        // Exit out of this
                        break;
                     }
                     // inode found in paths structure so place path in inode structure 
                     else {
                        // Verify that gpfs file size matches object size
                        check_security_access(&pre);
                        update_pre(&pre);
                        s3_set_host(pre.host);
                        s3_head(head_buf, object);
                        if (head_buf->contentLen != iattrP->ia_size + MARFS_REC_UNI_SIZE) {
                           fprintf(stderr, "Object Error on %s\n", inode[counter].path);
                           fprintf(stderr, "Read object of size %ld but metadata thinks size is %lld\n", head_buf->contentLen, iattrP->ia_size+MARFS_REC_UNI_SIZE);
                           fprintf(stderr, "Skipping this file\n");
                           aws_iobuf_reset_hard(head_buf);
                              break;
                        }
                        aws_iobuf_reset_hard(head_buf);

                        // Now fill inode structure with relevant scan info for packing
                        inode[counter].inode =  iattrP->ia_inode;
                        inode[counter].atime = iattrP->ia_atime.tv_sec;
                        inode[counter].mtime = iattrP->ia_mtime.tv_sec;
                        inode[counter].ctime = iattrP->ia_ctime.tv_sec;
                        //inode[counter].size = post.chunk_info_bytes;
                        inode[counter].size = iattrP->ia_size;

                        strcpy(inode[counter].path,paths[inode_index].parent);

                        //strcpy(inode[counter].path,paths[iattrP->ia_inode].parent);
                        LOG(LOG_INFO, "path assigned =%s\n", inode[counter].path);
                        LOG(LOG_INFO, "post md_path2 =%s\n", post.md_path);
                        inode[counter].post = post;
                        inode[counter].pre = pre;
                        //strcpy(inode[counter].pre,pre);
                        counter++;
                        *sum_size += iattrP->ia_size;

                        // if counter matches the number of paths found in treewalk
                        // might as well stop looking
                        //if (counter == MAX_SCAN_FILE_COUNT-1) {
                        if (counter == pack_params->max_pack_file_count-1) {
                           break;
                        }
                     }
                  }
               } // endif user.post
            } // endwhile 
         } // endif strcmp fileset name
      } // endif inode != 3
      if (object != NULL) {
         free(object);
         object = NULL;
      }
   } // endwhile
   *marfs_inodeLen = counter;
      gpfs_close_inodescan(iscanP);
   if (fsP) {
      gpfs_free_fssnaphandle(fsP);
   }
   return rc;
}

/******************************************************************************
* Name find_inode 
* This function scans the inode list structure to find a match
* This needs to be replaced by a btree search
* 
******************************************************************************/
int find_inode(size_t inode_number, struct walk_path *paths, pack_vars *pack_params) 
{
   int i;

   //for (i=0; i<MAX_SCAN_FILE_COUNT; i++) {
   for (i=0; i<pack_params->max_pack_file_count; i++) {
      if (inode_number == paths[i].inode) {
        LOG(LOG_INFO, "inode and parent: %d %s\n", paths[i].inode, paths[i].parent);
        return i;
      }
   }
   return -1;
}

/******************************************************************************
* Name pack_and_write
* This function is responsible for determining if objects can be packed based
* on a gpfs inode scan.  
* It calls:
* get_inodes - performs inode scan
* get_objects - creates link list of objects to pack
* pack_up - creates new packed object and writes it out
* set_md - update metadata 
******************************************************************************/
int pack_and_write(char* top_level_path, MarFS_Repo* repo, 
                   MarFS_Namespace* namespace, const char *ns, 
                   struct walk_path *paths, uint8_t no_pack,
                   pack_vars *pack_params)
{
  struct marfs_inode *unpacked;
   //struct marfs_inode unpacked[1024]; // Chris originally had this set to 102400 but that caused problems
   //struct marfs_inode unpacked[MAX_SCAN_FILE_COUNT]; // Chris originally had this set to 102400 but that caused problems
  if ((unpacked = (struct marfs_inode *)malloc(sizeof(struct marfs_inode)*pack_params->max_pack_file_count)) ==NULL) {
     fprintf(stderr, "Error allocating memory\n");
     return(-1);
  }

   int unpackedLen = 0;
   obj_lnklist    *packed=NULL;
   int packedLen = 0;
   int ret;
   size_t unpacked_sum_size = 0;
   int return_val = 0;

   // perform an inode scan and look for candidate objects for packing
   ret = get_inodes(top_level_path, unpacked, &unpackedLen, &unpacked_sum_size, ns, paths, pack_params);
   if (ret != 0){
      fprintf(stderr, "GPFS Inode Scan Failed, quitting!\n");
      free(unpacked);
      return -1;
   }
   if (no_pack) {
      if (unpackedLen >= pack_params->min_pack_file_count) {
      fprintf(stdout, "Objects will pack in size %ld  Found %d objects to pack with total size of %ld\n",
              pack_params->max_object_size, unpackedLen, unpacked_sum_size);
      fprintf(pack_params->outfd, "Objects will pack in size %ld  Found %d objects to pack with total size of %ld\n",
              pack_params->max_object_size, unpackedLen, unpacked_sum_size);
      fprintf(stdout, "Note:  total size does not include recovery info.\n");
      fprintf(pack_params->outfd, "Note:  total size does not include recovery info.\n");
      }
      else {
      fprintf(stdout, "Packing did not meet min_pack_file_count requirement of %zu\n", 
              pack_params->min_pack_file_count);
      }
      return_val=0;
   }
   // No potential packer objects found
   else if (unpackedLen == 0) {
      fprintf(stderr, "No valid packer objects found in this chunk - continuing with path \
chunking or Exiting now\n");
      //return -1;
      return_val=-1;
   }
    
   // Found objects that can be packed
   else {
      // create link-lists for objects found
      ret = get_objects(unpacked, unpackedLen, &packed, &packedLen, pack_params);
      if (ret == -1) {
         return_val = -1;
      }
      else {
         LOG(LOG_INFO, "%d %d\n", unpackedLen, packedLen);
         // repack small objects into larger object
         ret = pack_up(packed, repo, namespace);

         // Update metadata information
         set_md(packed, pack_params);
         free_objects(packed);
      }
   }
   free(unpacked);
   return return_val;
}
/******************************************************************************
* Name free_objects
* 
* Free objects link list memory
******************************************************************************/
void free_objects(obj_lnklist *objects)
{
   obj_lnklist *temp_obj=NULL;
   inode_lnklist *temp_inode;
   inode_lnklist *object;

   while((temp_obj=objects)!=NULL) {
      object = objects->val;
      while((temp_inode=object) != NULL) {
         object = object->next;
         free(temp_inode);
      }
      objects = objects->next;
      free(temp_obj);
   }
}
/******************************************************************************
* Name free_sub_objects
* 
* Free objects link list memory
******************************************************************************/
void free_sub_objects(inode_lnklist *sub_objects)
{
   inode_lnklist *temp_inode;
   inode_lnklist *object;

   object = sub_objects;
   while((temp_inode=object) != NULL) {
         object = object->next;
         free(temp_inode);
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

