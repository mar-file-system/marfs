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
#include "marfs_packer.h"
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

// TO DO
// make calls to marfs_base to do this work
/******************************************************************************
* Name 
* 
******************************************************************************/
int post_2_str2(char*                  post_str,
               size_t                 max_size,
               const MarFS_XattrPost2 *post){

   // config-version major and minor
   const int major = post->config_vers_maj;
   const int minor = post->config_vers_min;
   // putting the md_path into the xattr is really only useful if the marfs
   // file is in the trash, or is SEMI_DIRECT.  For other types of marfs
   // files, this md_path will be wrong as soon as the user renames it (or
   // a parent-directory) to some other path.  Therefore, one would never
   // want to trust it in those cases.  [Gary thought of an example where
   // several renames could get the path to point to the wrong file.]
   // So, let's only write it when it is needed and reliable.
   //
   // NOTE: Because we use the same xattr-field to point to the semi-direct
   //     file-system *OR* to the location of the file in the trash, we can
   //     not currently support moving semi-direct files to the trash.
   //     Deleting a semi-direct file must just delete it.
   
   //const char* md_path = "";
   ssize_t bytes_printed = snprintf(post_str, max_size,
                                    MARFS_POST_FORMAT2,
                                    //MARFS_POST_FORMAT,
                                    major, minor,
                                    encode_obj_type(post->obj_type),
                                    post->obj_offset,
                                    post->chunks,
                                    post->chunk_info_bytes,
                                    post->correct_info,
                                    post->encrypt_info,
                                    post->flags,
                                    post->md_path
                                    );
   if (bytes_printed < 0)
      return -1;                  // errno is set
   if (bytes_printed == max_size) {   /* overflow */
      return -1;
   }

   return 0;
}


// TO DO
// make calls to marfs_base to do this work
/******************************************************************************
* Name 
* 
******************************************************************************/
int str_2_post2(MarFS_XattrPost2* post, const char* post_str) {

   uint16_t major;
   uint16_t minor;
   char     obj_type_code;
   int scanf_size = sscanf(post_str, MARFS_POST_FORMAT2,
   //int scanf_size = sscanf(post_str, MARFS_POST_FORMAT,
                           &major, &minor,
                           &obj_type_code,
                           &post->obj_offset,
                           &post->chunks,
                           &post->chunk_info_bytes,
                           &post->correct_info,
                           &post->encrypt_info,
                           &post->flags,
                           (char*)&post->md_path); // might be empty
   if (scanf_size == EOF)
      return -1;                // errno is set
   else if (scanf_size < 9) {
      return -1;            /* ?? */
   }

   post->config_vers_maj = major;
   post->config_vers_min = minor;

   post->obj_type    = decode_obj_type(obj_type_code);
   return 0;
}

struct walk_path paths[10240000];
//struct walk_path paths[1024];
/******************************************************************************
* Name get_inodes
* This function uses an performs an inode scan of the gpfs file system.  It
* looks for MarFS type files and stores away the information in an array of
* struct inode.  The inode array has been pre-filled with file path info
* via the fasttreewalk function.
* 
******************************************************************************/
int get_inodes(const char *fnameP, int obj_size, struct marfs_inode *inode, int *marfs_inodeLen, const char* namespace){
	//gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path(fnameP);
	int counter = 0;
	const gpfs_iattr_t *iattrP;
	const char *xattrBP;
	unsigned int xattr_len;
	//register gpfs_iscan_t *iscanP = gpfs_open_inodescan_with_xattrs(fsP, NULL, -1, NULL, NULL);
	int rc;
	const char* nameP;
	const char* valueP;
	unsigned int valueLen;

        int i;
        int printable;
        int ret;
        char fileset_name_buffer[MARFS_MAX_NAMESPACE_NAME];

	gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path(fnameP);
	register gpfs_iscan_t *iscanP = gpfs_open_inodescan_with_xattrs(fsP, NULL, -1, NULL, NULL);
        while (1){
                rc = gpfs_next_inode_with_xattrs(iscanP,0x7FFFFFFF,&iattrP,&xattrBP, &xattr_len);
                if ((iattrP == NULL) || (iattrP->ia_inode > 0x7FFFFFFF))
                        break;
                if (iattrP->ia_inode != 3 && xattr_len > 0) {
			const char *xattrBufP = xattrBP;
			unsigned int xattrBufLen = xattr_len;
                        MarFS_XattrPre pre; 


                        gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid,
                                             &fileset_name_buffer, MARFS_MAX_NAMESPACE_NAME);
  
                        if (!strcmp(fileset_name_buffer, namespace)) {
                        while ((xattrBufP != NULL) && (xattrBufLen > 0)) {
                                rc = gpfs_next_xattr(iscanP, &xattrBufP, &xattrBufLen, &nameP, &valueLen, &valueP);
                                if (rc != 0) {
                                   rc = errno;
                                   fprintf(stderr, "gpfs_next_xattr: %s\n", strerror(rc));
                                   return(-1);
                                }
                                if (nameP == NULL)
                                   break;

                                // TEMP
                                // TEMP
                                if (iattrP->ia_inode >= 185811 && iattrP->ia_inode<=185815) {
                                // TEMP
                                // TEMP
				if (strcmp(nameP, "user.marfs_objid") == 0){
				//	strcpy(pre, valueP);
					//str_2_pre2(&pre, valueP, valueLen);

                                   if (valueLen > 0 ) {
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

                                       //for (i = 0; i < valueLen; i++) {
                                       //    if (printable) {
                                       //       xattr_ptr->xattr_value[i] = valueP[i];
                                       //    }
                                       //}
                                       //xattr_ptr->xattr_value[valueLen] = '\0';
                                       //xattr_ptr++;
                                   }

                                   printf("%s %d\n", valueP, valueLen);
				   ret = str_2_pre(&pre, valueP, NULL);
                                   
		//			printf("inode:=%d\n", iattrP->ia_inode);
					//printf("%s\n",valueP);
				}
                                if (strcmp(nameP, "user.marfs_post") == 0){
  /*                                     scanf_size = sscanf(valueP, MARFS_POST_FORMAT,
                                                                          &major, &minor,
                                                                          &obj_type_code,
                                                                          &obj_offset,
                                                                          &chunks,
                                                                          &chunk_info_bytes,
                                                                          &correct_info,
                                                                          &encrypt_info,
                                                                          &flags,
                                                                          &md_path);*/
					//MarFS_XattrPost2 post;
					MarFS_XattrPost post;
					//rc = str_2_post2(&post, valueP);
					rc = str_2_post(&post, valueP, 1);

					//if (post.flags != POST_TRASH && iattrP->ia_size <= obj_size && post.obj_type == OBJ_UNI && iattrP->ia_inode >= 422490 && iattrP->ia_inode <= 422694){
					//if (post.flags != POST_TRASH && iattrP->ia_size <= obj_size && post.obj_type == OBJ_UNI && iattrP->ia_inode >= 15200 && iattrP->ia_inode <= 15204){
					if (post.flags != POST_TRASH && iattrP->ia_size <= obj_size && iattrP->ia_size==41 && post.obj_type == OBJ_UNI && iattrP->ia_inode >=185811 && iattrP->ia_inode<=185815 ){
                                           //printf("%d\n", iattrP->ia_size);

					//if (post.flags != POST_TRASH && post.chunk_info_bytes <= obj_size && post.obj_type == OBJ_UNI){
						//printf("adding object now!!\n");
					//if (post.chunk_info_bytes != 0 && post.chunk_info_bytes != 48 && post.flags != POST_TRASH && post.chunk_info_bytes <= obj_size && post.obj_type == OBJ_UNI){
                                                //printf("inode:=%u obj_type_code:=%c obj_offset:=%d chunks:=%d chunk_info_bytes:=%d correct_info:=%d encrypt_info:=%d flags:=%d\n", iattrP->ia_inode, post.obj_type, post.obj_offset, post.chunks, post.chunk_info_bytes, post.correct_info, post.encrypt_info, post.flags);
                                                //get_iattrsx(iattrP->ia_inode);
                                                //
						inode[counter].inode = 	iattrP->ia_inode;
						inode[counter].atime = iattrP->ia_atime.tv_sec;
						inode[counter].mtime = iattrP->ia_mtime.tv_sec;
						inode[counter].ctime = iattrP->ia_ctime.tv_sec;
						//inode[counter].size = post.chunk_info_bytes;
						inode[counter].size = iattrP->ia_size;
					        strcpy(inode[counter].path,paths[iattrP->ia_inode].parent);
                                                printf("path assigned =%s\n", paths[iattrP->ia_inode].parent);
                                                printf("post md_path =%s\n", post.md_path);
						inode[counter].post = post;	
						inode[counter].pre = pre;
						//strcpy(inode[counter].pre,pre);
						counter++;
					}else{
						//printf("TRASH FOUND!\n");
					}
                                }
                                }
                        }
                        } // temp endif strcmp fileset name
                }
        }
	*marfs_inodeLen = counter;
        printf("counter value %d\n",counter);
        if (fsP) {
           gpfs_free_fssnaphandle(fsP);
        }
	gpfs_close_inodescan(iscanP);
	return rc;
}

//int get_objects(struct marfs_inode *unpacked, int unpacked_size, list*  packed, int *packed_size, int obj_size_max){
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
int get_objects(struct marfs_inode *unpacked, int unpacked_size, obj_lnklist*  packed, int *packed_size, int obj_size_max){
	//int cur_size=0;	
	int sum_obj_size=0;	
	int i;
	int count=0;
	//int temp_count = 0;
        int obj_cnt = 0;
	inode_lnklist *sub_objects, *sub_obj_head;
	obj_lnklist *main_object, *main_obj_head;
        main_object = NULL;

        // initial link list node creation
        //main_object = (obj_lnklist *)malloc(sizeof(obj_lnklist));
        //sub_objects = (inode_lnklist *)malloc(sizeof(inode_lnklist));
	main_obj_head = NULL;
        sub_obj_head = NULL;
  


        // There are three conditions for packing objects
        // -objects add up to less than exact size of target object
        // -objects add up to the exact size of target object
        // -objects add up to > greater than size of target object
        //  and less than another target object      

        // Note this code can be consolidated with function calls or
        // MACROs but just trying to get functionality first.
        int need_main = -1;
   
        // loop through all inodes found
	for (i = 0; i < unpacked_size; i++){
           printf("i = %d\n", i);
           sum_obj_size+=unpacked[i].size; 
           obj_cnt++;
           // add upp sizes of each object
           if (sum_obj_size < obj_size_max) {
              sub_objects = (inode_lnklist *)malloc(sizeof(inode_lnklist));
              sub_objects->val = unpacked[i];
              sub_objects->next  = sub_obj_head;
              sub_obj_head = sub_objects;
              printf("sum size = %d\n", sum_obj_size);
           }
            // check if sum is equal to or greater than target object
           else if (sum_obj_size >= obj_size_max) {
               // if the sum of the objects = the target file size
               // terminate the sub_objects link list 
               // and create a new main_object entry in the linked list
               if(sum_obj_size == obj_size_max) {
                  printf("Equal %d\n", sum_obj_size);
                  sub_objects = (inode_lnklist *)malloc(sizeof(inode_lnklist));
                  sub_objects->val = unpacked[i];
                  sub_objects->next  = sub_obj_head;
                  sub_obj_head = NULL;

                  // Add main object stuff here
                  main_object = (obj_lnklist *)malloc(sizeof(obj_lnklist));
                  main_object->val = sub_objects;
                  main_object->count=obj_cnt;
                  main_object->next = main_obj_head;
                  main_obj_head = main_object;
                  obj_cnt = 0;
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
                  printf("XX\n");
                  main_object = (obj_lnklist *)malloc(sizeof(obj_lnklist));
                  main_object->val = sub_objects;
                  main_object->count= obj_cnt;
                  main_object->next = main_obj_head;
                  main_obj_head = main_object;
                  sub_obj_head=NULL;
                  count++;
                  sum_obj_size = 0;
                  obj_cnt++;
                  sub_objects = (inode_lnklist *)malloc(sizeof(inode_lnklist));
                  sub_objects->val = unpacked[i];
                  sub_objects->next  = NULL;
                  sub_obj_head = sub_objects;

                  need_main = -1;
               }
           }
	}
        // done summing sizes, create target object if at least one not created or 
        // picking remainder from else above
        if (need_main == -1) {
           printf("YY\n");
           main_object = (obj_lnklist *)malloc(sizeof(obj_lnklist));
           main_object->val = sub_objects;
           main_object->count=obj_cnt;
           main_object->next = main_obj_head;
           count++;
        }
        //main_object->next = NULL;


	*packed_size = count;
	*packed = *main_object;
        printf("sub object count %d\n", packed->count);
        printf("main object count %d\n", count);
        if (packed->val == NULL) {
           printf("NULL value\n");
        }
	return 0;
}
//int pack_up(list * objects, MarFS_Repo* repo, MarFS_Namespace* ns){
/******************************************************************************
* Name pack_up
* This function goes throught the main object link list and reads all data from
* the associated sub objects (using s3_get).
* It then writes the data to the new object using s3_put
******************************************************************************/
int pack_up(obj_lnklist *objects, MarFS_Repo* repo, MarFS_Namespace* ns){
	//printf("let's packup!\n");
//	char *pre;
        //s3_set_host("10.10.0.1:81/");
	srand(time(NULL));
	int r = rand();
	int count = 0;
	
	IOBuf *nb = aws_iobuf_new();
	//item *object;
	inode_lnklist *object;
//	char newpre[10240];
//	struct ObjectStream * get;
//	struct ObjectStream * put;
	//list * objtemp;
//	obj_lnklist *objtemp;
//	printf("ptr location:=%d", &objects);
        // Iterated through the link list of packed objects
	while(objects){
		
		printf("outer step1\n");
                // point to associated link sub_object link list
                object = objects->val;
		if(objects->val == NULL) {
		   printf("NULL object\n");
			break;
                }
		MarFS_XattrPre packed_pre;
                
                // get the pre xattr and md_inode value
		packed_pre = object->val.pre;
		packed_pre.md_inode = r;

                // get the post xattr for the sub_object and set the object
                // type to PACKED
		//MarFS_XattrPost2 packed_post;
		MarFS_XattrPost packed_post;
		packed_post = object->val.post;
		packed_post.obj_type = OBJ_PACKED;	
		packed_pre.obj_type = OBJ_PACKED;
		count = 0;	

                // Now iterate through the sub_objects and update lengths, offset
                // type, namespace, repo
                while(object) {
                        printf("Getting inode info %d\n", count);
			object->val.offset = nb->write_count;
                        printf("nb length = %ld\n", nb->write_count);
			object->val.post.obj_offset = nb->write_count;
			object->val.post.obj_type = OBJ_PACKED;	
                        char url[MARFS_MAX_XATTR_SIZE];
			object->val.pre.ns = ns;
	                object->val.pre.repo = repo;
	                pre_2_str(url, MARFS_MAX_XATTR_SIZE, &object->val.pre);
			printf("unpacked url:=%s\n", url);
			//MarFS_XattrPost2 unpacked_post ;
			MarFS_XattrPost unpacked_post ;
			unpacked_post = object->val.post;

			//char the_post[MARFS_MAX_XATTR_SIZE];

//			post_2_str2(the_post, MARFS_MAX_XATTR_SIZE, &unpacked_post);
                        check_security_access(&object->val.pre);
			update_pre(&object->val.pre);
                        s3_set_host(object->val.pre.host);

                        // get object_data
			s3_get(nb,url);

                        // ********Why is this repeated here?
			object->val.pre = packed_pre;
			object->val.pre.ns = ns;
			object->val.pre.repo = repo;
			object->val.pre.obj_type = 3;
			//object->val.pre.obj_type = OBJ_PACKED;
                        // ********Why is this repeated here?
			object = object->next ;
			count++;
                }
                printf("count = %d\n", count);
                
		packed_post.chunks = count;
		packed_post.chunk_info_bytes = nb->len;
		//char post_str[MARFS_MAX_XATTR_SIZE];
		//post_2_str2(post_str, MARFS_MAX_XATTR_SIZE, &packed_post);
		packed_pre.ns = ns;	
		packed_pre.repo = repo;

		char pre_str[MARFS_MAX_XATTR_SIZE];
		packed_pre.obj_type = 3;
		//packed_pre.obj_type = OBJ_PACKED;
		pre_2_str(pre_str, MARFS_MAX_XATTR_SIZE, &packed_pre);	
		r = rand();
                check_security_access(&packed_pre);
		update_pre(&packed_pre);
		printf("pre_str:=%s\n", pre_str);
		r = rand();

		// write data to new object
		s3_put(nb,pre_str);
		//

                aws_iobuf_reset(nb);
		if (objects)
			objects->count = count;
		objects = objects->next;
		count=0;
        }
	return 0;
}


//int set_md(list * objects){
/******************************************************************************
* Name set_md
* This function updates the metadata associated with all objects that were 
* packed into a single object 
* 
******************************************************************************/
int set_md(obj_lnklist *objects){
	printf("got to set md\n");
	//item * object;	
	inode_lnklist *object;
        //int count = 0;
	char pre[MARFS_MAX_XATTR_SIZE];
	char post[MARFS_MAX_XATTR_SIZE];
        char *pre_ptr = &pre[0];
	printf("got to set md2\n");
        while(objects){
                object = objects->val;
                while(object) {
			char *path = object->val.path;
			  printf("path=%s\n", path);
  
			struct stat statbuf;
			stat(path, &statbuf);
			if (statbuf.st_atime != object->val.atime || statbuf.st_mtime != object->val.mtime || statbuf.st_ctime != object->val.ctime){
				printf("changed path:=%s\n", path);
                                printf("stat of path = %ld  stat from object = %ld\n", statbuf.st_atime, object->val.atime);
			}else{
				char marfs_path[1024];
                                char *marfs_path_ptr = &marfs_path[0];

				// This is called to get the fuse mount
				// path to the file for removal process 
				// that follows
				get_marfs_path(path, marfs_path_ptr);

				object->val.pre.obj_type = OBJ_PACKED;
				object->val.post.chunks = objects->count;
                                printf("set md count = %d\n", objects->count);
				//pre_2_str(&pre, MARFS_MAX_XATTR_SIZE, &object->val.pre);
				pre_2_str(pre_ptr, MARFS_MAX_XATTR_SIZE, &object->val.pre);
				//post_2_str2(post, MARFS_MAX_XATTR_SIZE, &object->val.post);
				post_2_str(post, MARFS_MAX_XATTR_SIZE, &object->val.post,object->val.pre.repo,0);
                           
                                // Remove the files via fuse mount
				remove(marfs_path);
				printf("remove marfs_path:=%s\n", marfs_path_ptr);			
				FILE *fp;
				fp = fopen(object->val.path, "w+");
				fclose(fp);
                                if ((truncate(object->val.path, object->val.size)) == -1) { 
                                   fprintf(stderr, "Error truncating gpfs file to correct size\n");
                                   return -1;    
                                } 
                                // TO DO 
                                // How does xattr inode value get set for packed object?
                                // Ask Jeff

				printf("count:=%d paths:=%s\n", objects->count, object->val.path);
				printf("obj_type:=%s\n", pre);
                                printf("post: %s\n", post);
				//getxattr(path,"user.marfs_objid",  value, MARFS_MAX_XATTR_SIZE);
				setxattr(path, "user.marfs_objid", pre, strlen(pre), 0);
				setxattr(path, "user.marfs_post", post, strlen(post), 0);
			}
                        object = object->next ;
                }
                objects = objects->next;
        }

        return 0;
}


/******************************************************************************
* Name 
* 
******************************************************************************/
int set_xattrs(int inode, int xattr){
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
int trash_inode(int inode){                                                                                                                                                                                
/*
 *
 *   
 * Tie into alfreds trashing code
 *   
 */
        return 0;
}

/******************************************************************************
* Name 
* 
******************************************************************************/
/**************
int fasttreewalk(char* path, int inode){
        gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path("/gpfs/marfs-gpfs");

	gpfs_ifile_t *file;
	if(inode == -1)
		inode = 3;
	file = gpfs_iopen64(fsP,inode, O_RDONLY, NULL, NULL);
	gpfs_direntx64_t *dirP;
	int rc;
	while(1){
		rc = gpfs_ireaddir64(file, &dirP); 
		if(rc != 0){
			return -1;
		}
		if(dirP == NULL){
			return -1;
		}
	        if ((strcmp(dirP->d_name, ".") == 0) || (strcmp(dirP->d_name, "..") == 0))
	          continue;
		char *new_path;
		new_path = malloc(sizeof(char) * (strlen(path) + strlen(dirP->d_name)+16));
		strcpy(new_path, path);
		strcat(new_path, "/");
		strcat(new_path, dirP->d_name);
		if(dirP->d_type == 4){
			fasttreewalk(new_path, dirP->d_ino);
		}else if(dirP->d_type == 8){
			//printf("%s\n", new_path);	
		}
	}
return 0;
}
****************/

//#define max 1024
//struct walk_path stack[max], data;
//int top, option, reply;

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
* Name fasttreewalk2
* This function uses gpfs API calls to walk t a directory tree
* 
******************************************************************************/
void fasttreewalk2(char* path, int inode){
	struct walk_path dpath;
	struct walk_path rdpath;
	rdpath.inode = 3;
	strcpy(rdpath.path,path);

        // HAD TO SET THIS otherwise first pop was pulling garbage
	strcpy(rdpath.parent,"");
        //

	printf("parent:=%s\n", rdpath.path);
	printf("path passed :=%s\n", path);

        //struct walk_path stack[max], data;
        struct walk_path stack[MAX_STACK_SIZE];
        //int top, option, reply;
        int top;

	int rc = push(stack,&top, &rdpath);

	gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path(path);
        gpfs_ifile_t *file;
        const gpfs_direntx64_t *dirP;
	struct walk_path walkP;

	while ( top >= 1){
		rc = pop(stack, &top,&walkP);
        	file = gpfs_iopen64(fsP,walkP.inode, O_RDONLY, NULL, NULL);
                 
	        while(1){
	                rc = gpfs_ireaddir64(file, &dirP);
			if(dirP == NULL)
				break;
			dpath.inode = dirP->d_ino;
			strcpy(dpath.path,dirP->d_name);
                        //printf("after readdir %s\n", dirP->d_name);

                        if(dirP->d_type == GPFS_DE_DIR) {
			//if(dirP->d_type == 4){
				if (!(strcmp(dirP->d_name, ".") == 0) && !(strcmp(dirP->d_name, "..") == 0)){
					strcpy(dpath.parent, walkP.parent);
					strcat(dpath.parent, "/");
					strcat(dpath.parent, dpath.path);
					push(stack, &top, &dpath);
					strcpy(dpath.parent, walkP.path);
				}
                        } else if(dirP->d_type == GPFS_DE_REG) {
			//}else if (dirP->d_type == 8){
					strcpy(dpath.parent, path);
					strcat(dpath.parent, walkP.parent);
					strcat(dpath.parent, "/");
					strcat(dpath.parent, dirP->d_name);
                                        printf("found path %s\n", dpath.parent);
					paths[dpath.inode] = dpath;
                                        //if ((strstr(dpath.parent, "atorrez"))!=NULL) {
					//   paths[dpath.inode] = dpath;
                                        //}
			}
		}
                if (file) {
                   gpfs_iclose(file);
                }
	}
        if (fsP) {
           gpfs_free_fssnaphandle(fsP);
        }
    
}
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
        char the_path[1024000];
	char empty[1024] = {0};
	strcpy(marfs, empty);
        //while(ns = namespace_next(&ns_iter)){
        while((ns = namespace_next(&ns_iter))){
                if(strstr(patht, ns->md_path)){
                        // TO DO 
                        // NOT SURE WHY THIS IS HERE 
        		//char the_path[1024000];
        		//
			strcpy(the_path, empty);
                        strcat(the_path, mnt_top);
                        strcat(the_path, ns->mnt_path);

                        char ending[1024];
                        int i;
                        for(i = strlen(ns->md_path); i < strlen(patht); i++){
                                ending[i-strlen(ns->md_path)] = *(patht+i);
                        }
                        strcat(the_path, ending);
                        strcat(marfs,the_path);
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
void print_usage()
{

  return;
}
/******************************************************************************
* Name  main
* 
******************************************************************************/
int main(int argc, char **argv){
	//char  *patht = "/gpfs/gpfs_test/atorrez/mdfs/cphoffma/small/dir115/file14";

	//const char * fnameP = "/gpfs/marfs-gpfs";
	//int max_object_size = 1073741824; //1GiB
	//int max_object_size = 7721; //1GiB
	int max_object_size = 190; //1GiB
	//int max_object_size = 107374182400; //1GiB
	//int max_object_size = 38; //1GiB
	//int max_object_size = 314572800; //300 MB
//	int max_object_size = 107374182; //100 MB
       
        // I had to comment this out I was getting a seg fault with this allocation
	//struct marfs_inode unpacked[102400];
	struct marfs_inode unpacked[1024];
	int unpackedLen=0;

        int c;
        //char *rdir = NULL;
        char *patht = NULL;
        char *fnameP = NULL;
        char *fnameP_1 = NULL;
        int pack_object_size;
        char *ns = NULL; 
        int ret;
        int sole_obj_size = 1048576;

        while ((c=getopt(argc,argv,"d:p:s:n:h")) != EOF) {
           switch(c) {
              case 'd': fnameP = optarg; break;
              case 'p': patht = optarg; break;
              case 's': pack_object_size = atoi(optarg);  break;
              case 'n': ns = optarg; break;
              case 'h': print_usage();
              default:
                 exit(0);
           }
        }

        // Read configuration and initialize aws
	if ( setup_config() == -1 ) {
           fprintf(stderr,"Initializing Configs and Aws failed, quitting!!\n");
           return -1;
        }

	MarFS_Namespace* namespace;
	namespace = find_namespace_by_name(ns);
	MarFS_Repo* repo = namespace->iwrite_repo;

       // TESTING THIS METHOD
        walk_and_scan_control (fnameP, max_object_size, sole_obj_size, ns, repo, namespace);
        return 0;
       // TESTING THIS METHOD



        // Use gpfs API to walk the tree and hash paths via inode number 
	fasttreewalk2(fnameP, -1);

        // TRY THIS
        //fnameP_1 = namespace->md_path;
	//fasttreewalk2(fnameP_1, -1);
        // TRY THIS
 
        // Perform gpfs inode scan 
	ret = get_inodes(fnameP,max_object_size, unpacked, &unpackedLen, ns);
	if (ret != 0){
		printf("GPFS Inode Scan Failed, quitting!\n");
		return -1;
	}
        if (unpackedLen == 0) {
           printf("No valid packer objects found, Exiting now\n");
           return -1;
        }
        else {
	   printf("mnt_top:=%s\n", marfs_config->mnt_top);

	   //MarFS_Namespace* namespace;
	   //namespace = find_namespace_by_name(ns);
	   //MarFS_Repo* repo = namespace->iwrite_repo;


           // TO DO
           // figure out how and why namespace is used 
           // NOT sure why it needs to be specified because read namespace 
           // should be target namespace or am I confused? 
           //	
	   //Declare object type for linklist that will contain all packed objects
	   obj_lnklist packed;

	   int packedLen = 0;
        
           // Identify all all objects that can be packed
	   ret = get_objects(unpacked, unpackedLen, &packed, &packedLen, max_object_size);
           if (packed.val == NULL) {
              printf("NULL value\n");
           }
           printf("%d %d\n", unpackedLen, packedLen);

           // repack small objects into larger object
	   ret = pack_up(&packed, repo, namespace);
   
           // Update metadata information
	   set_md(&packed);
        }
	return ret;
}

/******************************************************************************
* Name walk_and_scan_control 
* This function uses gpfs API calls to walk t a directory tree
* 
******************************************************************************/
int walk_and_scan_control (char* top_level_path, size_t max_object_size, 
                            size_t sole_obj_size, const char* ns,
                            MarFS_Repo* repo, MarFS_Namespace* namespace)
{

//   struct marfs_inode unpacked[1024]; // Chris originally had this set to 102400 but that caused problems
//   int unpackedLen = 0;
//   obj_lnklist    packed;
//   int packedLen = 0;
   struct walk_path dpath;
   struct walk_path rdpath;
   int reg_file_cnt =0;
   int ret;


   rdpath.inode = 3;
   strcpy(rdpath.path,top_level_path);

   // HAD TO SET THIS otherwise first pop was pulling garbage
   strcpy(rdpath.parent,"");
   //

   printf("parent:=%s\n", rdpath.path);
   // TRYING THIS
   // Only look for namespace entries
   //top_level_path = namespace->md_path;
   //

   //struct walk_path stack[max], data;
   struct walk_path stack[MAX_STACK_SIZE];
   //int top, option, reply;
   int top;

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

         if (dirP->d_type == GPFS_DE_DIR) {
            if (!(strcmp(dirP->d_name, ".") == 0) && !(strcmp(dirP->d_name, "..") == 0)){
               strcpy(dpath.parent, walkP.parent);
               strcat(dpath.parent, "/");
               strcat(dpath.parent, dpath.path);
               push(stack, &top, &dpath);
               strcpy(dpath.parent, walkP.path);
            }
         }
         else if(dirP->d_type == GPFS_DE_REG) {
            strcpy(dpath.parent, top_level_path);
            strcat(dpath.parent, walkP.parent);
            strcat(dpath.parent, "/");
            strcat(dpath.parent, dirP->d_name);

            // instead of using dpath.inode, use counter value and update inode and path in structure
            //paths[dpath.inode] = dpath;
            printf("%s\n", dpath.parent);
            if ((strstr(dpath.parent, namespace->md_path))!=NULL) {
               paths[reg_file_cnt] = dpath;
               printf("found inode\n");
            //}

            reg_file_cnt++;
            printf("file count %d\n", reg_file_cnt);
            //if (reg_file_cnt == MAX_SCAN_FILE_COUNT ) {
            if (reg_file_cnt == 5 ) {
               pack_and_write(top_level_path, max_object_size, repo, namespace, ns);
/**********
               ret = get_inodes_new(top_level_path, max_object_size, unpacked, &unpackedLen, ns);
               if (ret != 0){
                  printf("GPFS Inode Scan Failed, quitting!\n");
                  return -1;
               }
               // No potential packer objects fount
               if (unpackedLen == 0) {
                  printf("No valid packer objects found, Exiting now\n");
                  return -1;
               }
               // Found objects that can be packed
               else {
                  ret = get_objects(unpacked, unpackedLen, &packed, &packedLen, max_object_size);
                  if (packed.val == NULL)
                     printf("NULL value\n"); 
                  printf("%d %d\n", unpackedLen, packedLen);
                  // repack small objects into larger object
                  ret = pack_up(&packed, repo, namespace);

                  // Update metadata information
                  set_md(&packed);
                  reg_file_cnt = 0;
               }
**********/
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
      pack_and_write(top_level_path, max_object_size, repo, namespace, ns);
   } 
   if (fsP) {
      gpfs_free_fssnaphandle(fsP);
   }
   return 0;
}


/********
int get_inodes_new(const char *fnameP, int obj_size, struct marfs_inode *inode, int *marfs_inodeLen, const char* namespace)
{
   return 0;
}
*********/

int get_inodes_new(const char *fnameP, int obj_size, struct marfs_inode *inode, int *marfs_inodeLen, const char* namespace)
{
   int counter = 0;
   const gpfs_iattr_t *iattrP;
   const char *xattrBP;
   unsigned int xattr_len;
   int rc;
   const char* nameP;
   const char* valueP;
   unsigned int valueLen;

   int i;
   int printable;
   int ret;
   char fileset_name_buffer[MARFS_MAX_NAMESPACE_NAME];

   gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path(fnameP);
   register gpfs_iscan_t *iscanP = gpfs_open_inodescan_with_xattrs(fsP, NULL, -1, NULL, NULL);

   size_t inode_index;

   // Based on paths found (hashed by inode number) determine min and max inode
   // search parameters
   //size_t inode_start = (inode_offset-1) * MAX_WALK_PATHS;
   //size_t inode_end =   inode_offset * (MAX_WALK_PATHS-1);
 

   // While getting inodes
   while (1){
      //printf("in while\n");
      rc = gpfs_next_inode_with_xattrs(iscanP,0x7FFFFFFF,&iattrP,&xattrBP, &xattr_len);
      if ((iattrP == NULL) || (iattrP->ia_inode > 0x7FFFFFFF))
          break;
      if (iattrP->ia_inode != 3 && xattr_len > 0) {
         const char *xattrBufP = xattrBP;
         unsigned int xattrBufLen = xattr_len;
         MarFS_XattrPre pre;

         gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid,
                              &fileset_name_buffer, MARFS_MAX_NAMESPACE_NAME);

         //if (!strcmp(fileset_name_buffer, namespace && iattrP->ia_node >= inode_start && iattrP->ia_inode <= inode_end)) {

            // Get xattrs associated with file
            while ((xattrBufP != NULL) && (xattrBufLen > 0)) {
               rc = gpfs_next_xattr(iscanP, &xattrBufP, &xattrBufLen, &nameP, &valueLen, &valueP);
               if (rc != 0) {
                  rc = errno;
                  fprintf(stderr, "gpfs_next_xattr: %s\n", strerror(rc));
                  return(-1);
               }
               if (nameP == NULL)
                  break;

               // TEMP
               // TEMP
               if (iattrP->ia_inode >= 185646 && iattrP->ia_inode<=185652) {
               // TEMP
               // TEMP
                  if (strcmp(nameP, "user.marfs_objid") == 0){
/********
                     if (valueLen > 0 ) {
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
**********/

                     printf("%s %d\n", valueP, valueLen);
                     ret = str_2_pre(&pre, valueP, NULL);

                  }
                  if (strcmp(nameP, "user.marfs_post") == 0){
/******
                    if (valueLen > 0 ) {
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
*********/
// MarFS_XattrPost2 post;
                     MarFS_XattrPost post;
                     //printf("Post = %s\n", valueP);
                     //rc = str_2_post2(&post, valueP);
                     rc = str_2_post(&post, valueP, 1);
                     //printf("post md_path1 =%s\n", post.md_path);
                     //printf("post flags =%d\n", post.flags);
                     // TEMP
                     // TEMP
                     if (post.flags != POST_TRASH && iattrP->ia_size <= obj_size && iattrP->ia_size==40 && post.obj_type == OBJ_UNI && iattrP->ia_inode >=185646 && iattrP->ia_inode<=185652 ){
                     // TEMP
                     // TEMP

                     //if (post.flags != POST_TRASH && post.chunk_info_bytes <= obj_size && post.obj_type == OBJ_UNI){
                          //printf("adding object now!!\n");
                          //get_iattrsx(iattrP->ia_inode);
                          //





                        // IMPORTANT
                        //
                        // NEED to see if current inode exists in array of paths
                        // if so update path
                        // call find_inode  which returns an index to paths array

                        inode[counter].inode =  iattrP->ia_inode;
                        inode[counter].atime = iattrP->ia_atime.tv_sec;
                        inode[counter].mtime = iattrP->ia_mtime.tv_sec;
                        inode[counter].ctime = iattrP->ia_ctime.tv_sec;
                        //inode[counter].size = post.chunk_info_bytes;
                        inode[counter].size = iattrP->ia_size;
                        
                        if ((inode_index = find_inode(iattrP->ia_inode)) == -1) {
                           // Exit out of this
                           break;
                        }
                        else {
                           strcpy(inode[counter].path,paths[inode_index].parent);
                        }
                        

                        //strcpy(inode[counter].path,paths[iattrP->ia_inode].parent);
                        printf("path assigned =%s\n", inode[counter].path);

                        printf("post md_path2 =%s\n", post.md_path);
                        inode[counter].post = post;
                        inode[counter].pre = pre;
                        //strcpy(inode[counter].pre,pre);
                        counter++;
                     }
                     else{
                        //printf("TRASH FOUND!\n");
                     }
                  } // endif user.post
               } // endif ia_inode
            } // endwhile 
         //} // endif strcmp fileset name
      } // endif inode != 3
      //if ( counter == MAX_SCAN_FILE_COUNT) {
      //   printf ("Hit maximum count for upacked objects returning from inode scan\n"); 
      //   break;
       
 
   } // endwhile
   *marfs_inodeLen = counter;
   printf("counter value %d\n",counter);
   if (fsP) {
      gpfs_free_fssnaphandle(fsP);
   }
   gpfs_close_inodescan(iscanP);
   return rc;
}


int find_inode(size_t inode_number) 
{
   int i;

   for (i=0; i<MAX_SCAN_FILE_COUNT; i++) {
      if (inode_number == paths[i].inode) {
        printf("looking for inode %ld  getting %ld\n", inode_number, paths[i].inode);
        printf("inode and parent: %s\n", paths[i].parent);
        return i;
      }
   }
   return -1;
}


int pack_and_write(char* top_level_path, size_t max_object_size, MarFS_Repo* repo, MarFS_Namespace* namespace, const char *ns)
{
   struct marfs_inode unpacked[1024]; // Chris originally had this set to 102400 but that caused problems
   int unpackedLen = 0;
   obj_lnklist    packed;
   int packedLen = 0;
   int ret;

   ret = get_inodes_new(top_level_path, max_object_size, unpacked, &unpackedLen, ns);
   if (ret != 0){
      printf("GPFS Inode Scan Failed, quitting!\n");
      return -1;
   }
   // No potential packer objects fount
   if (unpackedLen == 0) {
      printf("No valid packer objects found, Exiting now\n");
      return -1;
   }
   // Found objects that can be packed
   else {
      ret = get_objects(unpacked, unpackedLen, &packed, &packedLen, max_object_size);
      if (packed.val == NULL)
         printf("NULL value\n");
      printf("%d %d\n", unpackedLen, packedLen);
      // repack small objects into larger object
      ret = pack_up(&packed, repo, namespace);

      // Update metadata information
      set_md(&packed);
   }
   return 0;
}
