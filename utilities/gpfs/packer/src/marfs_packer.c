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
#include "marfs_packer.h"
/******************************************************************************
* Name 
* 
******************************************************************************/
MarFS_Repo_Ptr find_repo_by_name2( const char* name ) {

  MarFS_Repo*   repo = NULL;
  RepoIterator  it = repo_iterator();

	//printf("starting repo iterator\n");
  while (repo = repo_next(&it)) {
    if ( ! strcmp( repo->name, name )) {
      return repo;
    }
  }
  return NULL;
}

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
   
   const char* md_path = "";
   ssize_t bytes_printed = snprintf(post_str, max_size,
                                    //MARFS_POST_FORMAT2,
                                    MARFS_POST_FORMAT,
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

/******************************************************************************
* Name 
* 
******************************************************************************/



int stub_str_2_pre2(MarFS_XattrPre*    pre,
              const char*        pre_str, // i.e. an xattr-value
              const struct stat* st) {

   // parse bucket and objid separately
   int read_count;

   read_count = sscanf(pre_str, MARFS_PRE_RD_FORMAT,
                       pre->bucket,
                       pre->objid);
   if (read_count == EOF){       // errno is set (?)
      return -1;
}
   if (read_count != 2) {
      return -1;
   }


   uint16_t  major;
   uint16_t  minor;

   char      obj_type;          /* ignored, see NOTE above init_pre() */
   char      compress;
   char      correct;
   char      encrypt;

   ino_t     md_inode;

   size_t    chunk_size;
   size_t    chunk_no;

   char      md_ctime_str[MARFS_DATE_STRING_MAX];
   char      obj_ctime_str[MARFS_DATE_STRING_MAX];
   uint8_t   unique;

   // --- parse bucket components

   // NOTE: We put repo first, because it seems less-likely we'll want a
   //       dot in a repo-name, than in a namespace, and we're using dot as
   //       a delimiter.  It will still be easy to construct
   //       delimiter-based S3 commands, to search for all entries with a
   //       given namespace, in a known repo.
   char  repo_name[MARFS_MAX_REPO_NAME];

   read_count = sscanf(pre->bucket, MARFS_BUCKET_RD_FORMAT,
                       repo_name);

   if (read_count == EOF) {     // errno is set (?)
      printf(" parsing bucket '%s'\n", pre->bucket);
      return -1;
   }
   else if (read_count != 1) {
      printf("parsed %d items from '%s'\n", read_count, pre->bucket);
      return -1;
   }

   // --- parse "obj-id" components (i.e. the part below bucket)

   // Holds namespace-name from obj-id, so we can decode_namespace(), then
   // find the corresponding namespace, for Pre.ns.  Do we ever care about
   // this?  The only reason we need it is because update_pre() uses it to
   // re-encode the bucket string.
   char  ns_name[MARFS_MAX_NAMESPACE_NAME];
   
   read_count = sscanf(pre->objid, MARFS_OBJID_RD_FORMAT,
                       ns_name,
                       &major, &minor,
                       &obj_type, &compress, &correct, &encrypt,
                       &md_inode,
                       md_ctime_str, obj_ctime_str, &unique,
                       &chunk_size, &chunk_no);

   if (read_count == EOF) {       // errno is set (?)
      printf("EOF parsing objid '%s'\n", pre->objid);
      return -1;
   }
   else if (read_count != 13) {
     //printf("parsed %d items from '%s'\n", read_count, pre->objid);
      return -1;
   }

   // --- conversions and validation

   // find repo from repo-name
   MarFS_Repo* repo = find_repo_by_name2(repo_name);
   if (! repo) {
//      printf("couldn't find repo '%s'\n", repo_name);
  //    printf("couldn't find repo '%s'\n", repo_name);

	//printf("return step 8\n");
//     return -1;
   }

   // find namespace from namespace-name
   //
   //   if (decode_namespace(ns_name, ns_name)) {
   //      errno = EINVAL;
   //      return -1;
   //   }
   MarFS_Namespace* ns = find_namespace_by_name(ns_name);
   if ( ! ns) {
   //   printf("couldn't find namespace '%s'\n", ns_name);
//	printf("return step 8\n");
//      return -1;
   }

   // should we believe the inode in the obj-id, or the one in caller's stat struct?
   //
   // NOTE: Packed objects (if they contain more than one logical object)
   //     can't possibly have the correct inode in their object-ID, in all
   //     cases.  But we don't want them to have all-zeros, either, because
   //     then they wouldn't be reliably-unique.  Therefore, they are built
   //     with an indoe from one of their members, but it won't match the
   //     inode of the others.
/*   if (st
       && (md_inode != st->st_ino)
       && (decode_obj_type(obj_type) != OBJ_PACKED)) {
      LOG(LOG_ERR, "non-packed obj, but MD-inode %ju != st->st_ino %ju \n",
          (uintmax_t)md_inode, (uintmax_t)st->st_ino);
	printf("return step 9\n");
      return -1;
   }
*/
   // parse encoded time-stamps
   time_t  md_ctime;
   time_t  obj_ctime;
   if (str_to_epoch(&md_ctime, md_ctime_str, MARFS_DATE_STRING_MAX)) {
      printf("error converting string '%s' to Pre.md_ctime\n", md_ctime_str);
      return -1;
   }
   //printf("ob_ctime_str:=%s\n", obj_ctime_str);
   if (str_to_epoch(&obj_ctime, obj_ctime_str, MARFS_DATE_STRING_MAX)) {
      LOG(LOG_ERR, "error converting string '%s' to Pre.obj_ctime\n", obj_ctime_str);
      return -1;
   }


   // --- fill in fields in Pre
   pre->config_vers_maj = major;
   pre->config_vers_min = minor;

   pre->md_ctime     = md_ctime;
   pre->obj_ctime    = obj_ctime;
   pre->unique       = unique;

   pre->obj_type     = decode_obj_type(obj_type);
   pre->compression  = decode_compression(compress);
   pre->correction   = decode_correction(correct);
   pre->encryption   = decode_encryption(encrypt);

   pre->ns           = ns;
   pre->repo         = repo;
   pre->chunk_size   = chunk_size;
   pre->chunk_no     = chunk_no;
   pre->md_inode     = md_inode; /* NOTE: from object-ID, not st->st_ino  */

   // va/fidate version
/*   if ((   major != marfs_config->version_major)
       || (minor != marfs_config->version_minor)) {

	printf("return step 17\n");
      LOG(LOG_ERR, "xattr vers '%d.%d' != config %d.%d\n",
          major, minor,
          marfs_config->version_major, marfs_config->version_minor);
	printf("return step 12\n");
      return -1;
   }*/

   return 0;
}

/******************************************************************************
* Name 
* 
******************************************************************************/
int str_2_post2(MarFS_XattrPost2* post, const char* post_str) {

   uint16_t major;
   uint16_t minor;
   char     obj_type_code;
   //int scanf_size = sscanf(post_str, MARFS_POST_FORMAT2,
   int scanf_size = sscanf(post_str, MARFS_POST_FORMAT,
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
* Name 
* 
******************************************************************************/
int get_inodes(const char *fnameP, int obj_size, struct marfs_inode *inode, int *marfs_inodeLen){
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
                                if (iattrP->ia_inode >= 246233 && iattrP->ia_inode<=246237) {
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
					MarFS_XattrPost2 post;
					rc = str_2_post2(&post, valueP);
					//if (post.flags != POST_TRASH && iattrP->ia_size <= obj_size && post.obj_type == OBJ_UNI && iattrP->ia_inode >= 422490 && iattrP->ia_inode <= 422694){
					//if (post.flags != POST_TRASH && iattrP->ia_size <= obj_size && post.obj_type == OBJ_UNI && iattrP->ia_inode >= 15200 && iattrP->ia_inode <= 15204){
					if (post.flags != POST_TRASH && iattrP->ia_size <= obj_size && iattrP->ia_size==40 && post.obj_type == OBJ_UNI && iattrP->ia_inode >=246233 && iattrP->ia_inode<=246237 ){
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
                }
        }
	*marfs_inodeLen = counter;
        printf("counter value %d\n",counter);
	gpfs_close_inodescan(fsP);
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
	int temp_count = 0;
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
	char *pre;
        //s3_set_host("10.10.0.1:81/");
	srand(time(NULL));
	int r = rand();
	int count = 0;

	
	IOBuf *nb = aws_iobuf_new();
	//item *object;
	inode_lnklist *object;
	char newpre[10240];
	struct ObjectStream * get;
	struct ObjectStream * put;
	//list * objtemp;
	obj_lnklist *objtemp;
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
		MarFS_XattrPost2 packed_post;
		packed_post = object->val.post;
		packed_post.obj_type = OBJ_PACKED;	
		packed_pre.obj_type = OBJ_PACKED;
		count = 0;	

                // Now iterate through the sub_objects and update lengths, offset
                // type, namespace, repo
                while(object) {
                        printf("Getting inode info %d\n", count);
			object->val.offset = nb->len;
                        printf("nb length = %d\n", nb->len);
			object->val.post.obj_offset = nb->len;
			object->val.post.obj_type = OBJ_PACKED;	
                        char url[MARFS_MAX_XATTR_SIZE];
			object->val.pre.ns = ns;
	                object->val.pre.repo = repo;
	                pre_2_str(url, MARFS_MAX_XATTR_SIZE, &object->val.pre);
			printf("unpacked url:=%s\n", url);
			MarFS_XattrPost2 unpacked_post ;
			unpacked_post = object->val.post;
			char the_post[MARFS_MAX_XATTR_SIZE];
//			post_2_str2(the_post, MARFS_MAX_XATTR_SIZE, &unpacked_post);
                        check_security_access(&object->val.pre);
			update_pre(&object->val.pre);
                        s3_set_host(&object->val.pre.host);

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
        int count = 0;
	char pre[MARFS_MAX_XATTR_SIZE];
	char post[MARFS_MAX_XATTR_SIZE];
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
			}else{
				char marfs_path[1024];
				get_marfs_path(path, &marfs_path);
				object->val.pre.obj_type = OBJ_PACKED;
				object->val.post.chunks = objects->count;
				pre_2_str(&pre, MARFS_MAX_XATTR_SIZE, &object->val.pre);
				post_2_str2(post, MARFS_MAX_XATTR_SIZE, &object->val.post);
				remove(marfs_path);
				printf("remove marfs_path:=%s\n", marfs_path);			
				FILE *fp;
				fp = fopen(object->val.path, "w+");
				fclose(fp);
				printf("count:=%d paths:=%s\n", objects->count, object->val.path);
				printf("obj_type:=%s\n", pre);
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
* Name 
* 
******************************************************************************/
int setup_config(){
        int rc = read_configuration();
	init_xattr_specs();
	printf("config rc:%d\n", rc);
        aws_init();
//	aws_read_config("root");
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

#define max 1024
//struct walk_path stack[max], data;
//int top, option, reply;

/******************************************************************************
* Name 
* 
******************************************************************************/
int push( struct walk_path stack[max],int *top, struct walk_path *data)
{
	if( *top == max -1 )
		return(-1);
	else
	{
		*top = *top + 1;
		stack[*top] = *data;
		return(1);
	} // else
} // push

/******************************************************************************
* Name 
* 
******************************************************************************/
int pop( struct walk_path stack[max], int *top, struct walk_path *data)
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
* 
******************************************************************************/
int fasttreewalk2(char* path, int inode){
	struct walk_path dpath;
	struct walk_path rdpath;
	rdpath.inode = 3;
	strcpy(rdpath.path,path);

        // HAD TO SET THIS otherwise first pop was pulling garbage
	strcpy(rdpath.parent,"");
        //

	printf("parent:=%s\n", rdpath.path);

        struct walk_path stack[max], data;
        int top, option, reply;

	int rc = push(stack,&top, &rdpath);

	gpfs_fssnap_handle_t *fsP = gpfs_get_fssnaphandle_by_path(path);
        gpfs_ifile_t *file;
        gpfs_direntx64_t *dirP;
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

                        //if(dirP->d_type == GPFS_DE_DIR)
			if(dirP->d_type == 4){
				if (!(strcmp(dirP->d_name, ".") == 0) && !(strcmp(dirP->d_name, "..") == 0)){
					strcpy(dpath.parent, walkP.parent);
					strcat(dpath.parent, "/");
					strcat(dpath.parent, dpath.path);
					push(stack, &top, &dpath);
					strcpy(dpath.parent, walkP.path);
				}
                        //if(dirP->d_type == GPFS_DE_REG)
			}else if (dirP->d_type == 8){
					strcpy(dpath.parent, path);
					strcat(dpath.parent, walkP.parent);
					strcat(dpath.parent, "/");
					strcat(dpath.parent, dirP->d_name);
					paths[dpath.inode] = dpath;
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
* Name 
* 
******************************************************************************/
int get_marfs_path(char * patht, char marfs[]){
	char *mnt_top = marfs_config->mnt_top;
        MarFS_Namespace *ns;
//	printf("got to get_marfs_path step1\n");
        NSIterator ns_iter;
        ns_iter = namespace_iterator();
        char the_path[1024000];
	char empty[1024] = {0};
	strcpy(marfs, empty);
        while(ns = namespace_next(&ns_iter)){
                if(strstr(patht, ns->md_path)){
        		char the_path[1024000];
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
        char *rdir = NULL;
        char *patht = NULL;
        char *fnameP = NULL;
        int pack_object_size;
        char *ns = NULL; 
        int ret;

        while ((c=getopt(argc,argv,"d:p:s:n:h")) != EOF) {
           switch(c) {
              case 'd': fnameP = optarg; break;
              case 'p': patht = optarg; break;
              case 's': pack_object_size;  break;
              case 'n': ns = optarg; break;
              case 'h': print_usage();
              default:
                 exit(0);
           }
        }

        // TO DO
        // This needs error checking OR really setup_config needs error checking
	ret = setup_config();
        // TO DO

	fasttreewalk2(fnameP, -1);
	int rc = get_inodes(fnameP,max_object_size, unpacked, &unpackedLen);
	if (rc != 0){
		printf("GPFS Inode Scan Failed, quitting!\n");
		return -1;
	}
        if (unpackedLen == 0) {
           printf("No valid packer objects found, Exiting now\n");
           return -1;
        }

	////rc = setup_config();
	//get_marfs_path(patht);
	printf("mnt_top:=%s\n", marfs_config->mnt_top);
	MarFS_Namespace* namespace;
	namespace = find_namespace_by_name(ns);
	MarFS_Repo* repo = namespace->iwrite_repo;
	
	if (rc != 0){
		printf("Initializing Configs and Aws failed, quitting!!\n");
		return -2;
	}
	//list packed;
	obj_lnklist packed;
	int packedLen = 0;
	rc = get_objects(unpacked, unpackedLen, &packed, &packedLen, max_object_size);
        if (packed.val == NULL) {
           printf("NULL value\n");
        }
        printf("%d %d\n", unpackedLen, packedLen);
	rc = pack_up(&packed, repo, namespace);
	set_md(&packed);

	return rc;
}

