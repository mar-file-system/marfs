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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <gpfs.h>
#include <ctype.h>
#include <unistd.h>
#include "marfs_gc.h"
#include "aws4c.h"

/******************************************************************************
* This program scans the inodes looking specifically at trash filesets.  It will
* determine if post xattr and gc.path defined and if so, gets the objid xattr
* and deletes those objects.  The gc.path file is also deleted.
*
******************************************************************************/

char    *ProgName;
int debug = 0;

/*****************************************************************************
Name: main

*****************************************************************************/

int main(int argc, char **argv) {
   //FILE *outfd;
   //FILE *packedfd;
   char *outf = NULL;
   char *rdir = NULL;
   char *packed_log = NULL;
   char packed_filename[MAX_PACKED_NAME_SIZE];
   //unsigned int uid = 0;
   int fileset_id = -1;
   int  c;
   unsigned int fileset_count = 1;
   extern char *optarg;
   unsigned int time_threshold_sec=0;
 
   //fileset_stat *fileset_stat_ptr;
   fileset_info *fileset_info_ptr;
//   char * fileset_name = "root,proja,projb";
//   char  fileset_name[] = "root,proja,projb";
//   char  fileset_name[] = "project_a,projb,root";
//   char  fileset_name[] = "project_a,root,projb";
//   char  fileset_name[] = "trash";
//   char  fileset_name[] = "project_c";
//   char  fileset_name[] = "trash";
//   char  fileset_name[] = "project_c";
   char *fileset = NULL;

   if ((ProgName = strrchr(argv[0],'/')) == NULL)
      ProgName = argv[0];
   else
      ProgName++;

   while ((c=getopt(argc,argv,"d:f:t:p:ho:")) != EOF) {
      switch (c) {
         case 'd': rdir = optarg; break;
         case 'o': outf = optarg; break;
         case 't': time_threshold_sec=atoi(optarg) * DAY_SECONDS; break;
         case 'f': fileset = optarg; break; 
         case 'p': packed_log = optarg; break;
         case 'h': print_usage();
         default:
            exit(0);
      }
   }
   
   if (rdir == NULL || 
       outf == NULL || 
       fileset == NULL ) {
      fprintf(stderr,"%s: no directory (-d) or output file name (-o) or fileset (-f) specified\n\n",ProgName);
      print_usage();
      exit(1);
   }

   // Structure contains info about output log and temproray packed list file for trashing
   // packed type objects
   file_info file_stat_info;
   file_info *file_status = &file_stat_info;

   /*
    *  This code assumes is set up for multiple filesets if the need ever arises but
    *  so I am going to leave it in for now.  
    *  Currently a struture containing one fileset
   */
   // Get list of filesets and count

   //fileset_stat_ptr = (fileset_stat *) malloc(sizeof(*fileset_stat_ptr)*fileset_count);
   fileset_info_ptr = (fileset_info *) malloc(sizeof(*fileset_info_ptr)*fileset_count);
   if (fileset_info_ptr == NULL ) {
      fprintf(stderr,"Memory allocation failed\n");
      exit(1);
   }
   init_records(fileset_info_ptr, fileset_count);
   aws_init();
   //strcpy(fileset_stat_ptr[0].fileset_name, fileset_name);
   strcpy(fileset_info_ptr[0].fileset_name, fileset);

   if (packed_log == NULL) {
      strcpy(packed_filename,"./tmp_packed_log");
      packed_log = packed_filename;
   }

   file_status->outfd = fopen(outf,"w");
   file_status->packedfd = fopen(packed_log, "w");
   strcpy(file_status->packed_filename, packed_log);
   file_status->is_packed=0;

   // Add filsets to structure so that inode scan can update fileset info
   //ec = read_inodes(rdir,outfd,fileset_id,fileset_stat_ptr,fileset_count,time_threshold_sec);

   // NEED to get these from config when the parser is ready
   aws_read_config("atorrez");
   //s3_set_host ("10.140.0.17:9020");
   s3_set_host ("10.135.0.22:81");
   //s3_set_host ("10.135.0.21:81");

   read_inodes(rdir,file_status,fileset_id,fileset_info_ptr,fileset_count,time_threshold_sec);
   if (file_status->is_packed) {
      fclose(file_status->packedfd);
      process_packed(file_status);
   }
   fclose(file_status->outfd);

   //unlink(packed_log);
   return (0);   
}

/***************************************************************************** 
Name: init_records 

*****************************************************************************/
void init_records(fileset_info *fileset_info_buf, unsigned int record_count)
{
   memset(fileset_info_buf, 0, (size_t)record_count * sizeof(fileset_info)); 
}

/***************************************************************************** 
Name: print_usage 

*****************************************************************************/
void print_usage()
{
   fprintf(stderr,"Usage: %s -d gpfs_path -o ouput_log_file -f fileset name [-p packed_tmp_file] [-t time_threshold-days] [-h] \n",ProgName);
}



/***************************************************************************** 
Name:  get_xattr_value

This function, given the name of the attribute, returns the associated value.

*****************************************************************************/
int get_xattr_value(struct marfs_xattr *xattr_ptr, const char *desired_xattr, int cnt) {

   int i;
   int ret_value = -1;

   for (i=0; i< cnt; i++) {
      //printf("XX %s %s\n", xattr_ptr->xattr_name, desired_xattr);
      if (!strcmp(xattr_ptr->xattr_name, desired_xattr)) {
         return(i);
      }
      else {
         xattr_ptr++;
      }
   }
   return(ret_value);
}

/***************************************************************************** 
Name:  get_xattrs

This function fills the xattr struct with all xattr key value pairs

*****************************************************************************/
int get_xattrs(gpfs_iscan_t *iscanP,
                 const char *xattrP,
                 unsigned int xattrLen,
                 const char **marfs_xattr,
                 int max_xattr_count,
                 struct marfs_xattr *xattr_ptr, FILE *outfd) {
   int rc;
   int i;
   const char *nameP;
   const char *valueP;
   unsigned int valueLen;
   const char *xattrBufP = xattrP;
   unsigned int xattrBufLen = xattrLen;
   int printable;
   int xattr_count =0;

   /*  Loop through attributes */
   while ((xattrBufP != NULL) && (xattrBufLen > 0)) {
      rc = gpfs_next_xattr(iscanP, &xattrBufP, &xattrBufLen,
                          &nameP, &valueLen, &valueP);
      if (rc != 0) {
         rc = errno;
         fprintf(stderr, "gpfs_next_xattr: %s\n", strerror(rc));
         return(-1);
      }
      if (nameP == NULL)
         break;



      //Determine if found a marfs_xattr by comparing our list of xattrs
      //to what the scan has found
      for ( i=0; i < max_xattr_count; i++) {
         if (!strcmp(nameP, marfs_xattr[i])) {
            strcpy(xattr_ptr->xattr_name, nameP);
            xattr_count++;
         }
      }

/******* NOT SURE ABOUT THIS JUST YET
      Eliminate gpfs.dmapi attributes for comparision
        Internal dmapi attributes are filtered from snapshots 
      if (printCompare > 1)
      {
         if (strncmp(nameP, "gpfs.dmapi.", sizeof("gpfs.dmapi.")-1) == 0)
            continue;
      }
***********/
    
      if (valueLen > 0 && xattr_count > 0 ) {
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

         for (i = 0; i < valueLen; i++) {
            if (printable) {
              xattr_ptr->xattr_value[i] = valueP[i]; 
            }
         }
         xattr_ptr->xattr_value[valueLen] = '\0'; 
         xattr_ptr++;
      }
      //xattr_ptr++;
   } // endwhile
   return(xattr_count);
}

/***************************************************************************** 
Name:  clean_exit

This function closes gpfs-related inode information and file handles

*****************************************************************************/

int clean_exit(FILE *fd, gpfs_iscan_t *iscanP, gpfs_fssnap_handle_t *fsP, int terminate) {
   if (iscanP)
      gpfs_close_inodescan(iscanP); /* close the inode file */
   if (fsP)
      gpfs_free_fssnaphandle(fsP); /* close the filesystem handle */
   if (terminate) {
      fclose(fd);
      exit(0);
   }
   else 
      return(0);
}

/***************************************************************************** 
Name: read_inodes 

This function opens an inode scan in order to provide size/block information
as well as file extended attribute information

*****************************************************************************/
int read_inodes(const char *fnameP, file_info *file_info_ptr, int fileset_id,fileset_info *fileset_info_ptr, size_t rec_count, unsigned int day_seconds) {
   int rc = 0;
   const gpfs_iattr_t *iattrP;
   const char *xattrBP;
   unsigned int xattr_len; 
   register gpfs_iscan_t *iscanP = NULL;
   gpfs_fssnap_handle_t *fsP = NULL;
   struct marfs_xattr mar_xattrs[MAX_MARFS_XATTR];
   struct marfs_xattr *xattr_ptr = mar_xattrs;
   int xattr_count;
   char fileset_name_buffer[32];

   const char *marfs_xattrs[] = {"user.marfs_post","user.marfs_objid"};
   int post_index=0;
   int objid_index=1;
   int marfs_xattr_cnt = MARFS_GC_XATTR_CNT;
   int trash_status;
   //unsigned int fileset_index = 0;


//   const char *xattr_objid_name = "user.marfs_objid";
//   const char *xattr_post_name = "user.marfs_post";
   MarFS_XattrPost post;
   //const char *xattr_post_name = "user.a";
  
   int early_exit =0;
   int xattr_index;
   char *md_path_ptr;

   //outfd = fopen(onameP,"w");

   /*
    *  Get the unique handle for the filesysteme
   */
   if ((fsP = gpfs_get_fssnaphandle_by_path(fnameP)) == NULL) {
      rc = errno;
      fprintf(stderr, "%s: line %d - gpfs_get_fshandle_by_path: %s\n", 
              ProgName,__LINE__,strerror(rc));
      early_exit = 1;
      clean_exit(file_info_ptr->outfd, iscanP, fsP, early_exit);
   }

   /*
    *  Open the inode file for an inode scan with xattrs
   */
  //if ((iscanP = gpfs_open_inodescan(fsP, NULL, NULL)) == NULL) {
   if ((iscanP = gpfs_open_inodescan_with_xattrs(fsP, NULL, -1, NULL, NULL)) == NULL) {
      rc = errno;
      fprintf(stderr, "%s: line %d - gpfs_open_inodescan: %s\n", 
      ProgName,__LINE__,strerror(rc));
      early_exit = 1;
      clean_exit(file_info_ptr->outfd, iscanP, fsP, early_exit);
   }


   while (1) {
      rc = gpfs_next_inode_with_xattrs(iscanP,0x7FFFFFFF,&iattrP,&xattrBP,&xattr_len);
      //rc = gpfs_next_inode(iscanP, 0x7FFFFFFF, &iattrP);
      if (rc != 0) {
         rc = errno;
         fprintf(stderr, "gpfs_next_inode: %s\n", strerror(rc));
         early_exit = 1;
         clean_exit(file_info_ptr->outfd, iscanP, fsP, early_exit);
      }
      // Are we done?
      if ((iattrP == NULL) || (iattrP->ia_inode > 0x7FFFFFFF))
         break;

      // Determine if invalid inode error 
      if (iattrP->ia_flags & GPFS_IAFLAG_ERROR) {
         fprintf(stderr,"%s: invalid inode %9d (GPFS_IAFLAG_ERROR)\n", ProgName,iattrP->ia_inode);
         continue;
      } 

      // If fileset_id is specified then only look for those inodes and xattrs
      if (fileset_id >= 0) {
         if (fileset_id != iattrP->ia_filesetid){
            continue; 
         }
      }

      // Print out inode values to output file
      // This is handy for debug at the moment
      if (iattrP->ia_inode != 3) {	/* skip the root inode */
         if (debug) { 
            fprintf(file_info_ptr->outfd,"%u|%lld|%lld|%d|%d|%u|%u|%u|%u|%u|%lld|%d\n",
            iattrP->ia_inode, iattrP->ia_size,iattrP->ia_blocks,iattrP->ia_nlink,iattrP->ia_filesetid,
            iattrP->ia_uid, iattrP->ia_gid, iattrP->ia_mode,
            iattrP->ia_atime.tv_sec,iattrP->ia_mtime.tv_sec, iattrP->ia_blocks, iattrP->ia_xperm );
         }
         gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid, &fileset_name_buffer, 32); 
         if (!strcmp(fileset_name_buffer,fileset_info_ptr[0].fileset_name)) {



         // Do we have extended attributes?
         // This will be modified as time goes on - what xattrs do we care about
            if (iattrP->ia_xperm == 2 && xattr_len >0 ) {
               xattr_ptr = &mar_xattrs[0];
               //if ((xattr_count = get_xattrs(iscanP, xattrBP, xattr_len, xattr_post_name, xattr_objid_name, xattr_ptr, outfd)) > 0) {
               if ((xattr_count = get_xattrs(iscanP, xattrBP, xattr_len, marfs_xattrs, marfs_xattr_cnt, xattr_ptr, file_info_ptr->outfd)) > 0) {
                  xattr_ptr = &mar_xattrs[0];
                  //if ((xattr_index=get_xattr_value(xattr_ptr, xattr_post_name, xattr_count)) != -1 ) { 
                  if ((xattr_index=get_xattr_value(xattr_ptr, marfs_xattrs[post_index], xattr_count)) != -1 ) { 
                     xattr_ptr = &mar_xattrs[xattr_index];
                     if (debug)
                        printf("post xattr name = %s value = %s count = %d index=%d\n",xattr_ptr->xattr_name, xattr_ptr->xattr_value, xattr_count,xattr_index);
                     if ((parse_post_xattr(&post, xattr_ptr))) {
                         fprintf(stderr,"Error getting post xattr\n");
                         continue;
                     }
                  }
                  //str_2_post(&post, xattr_ptr); 
                  // Talk to Jeff about this filespace used not in post xattr
                  if (debug) 
                     printf("found post chunk info bytes %zu\n", post.chunk_info_bytes);
                  //if (!strcmp(post.gc_path, "")){
                  if (post.flags != POST_TRASH){
                     if (debug) 
			// why would this ever happen?  if in trash gc_path should be non-null
                        //printf("gc_path is NULL\n");
                        printf("trash flag not set??\n");
                  } 
                  // else trash
                  else {
                     time_t now = time(0);
                   
                     // Check if older than X days (specified by user arg)
                     if (now-day_seconds > iattrP->ia_atime.tv_sec) {
                        if (debug)
                           printf("found trash\n");

                        md_path_ptr = &post.md_path[0];

                        xattr_ptr = &mar_xattrs[0];


                        // Get objid xattr
                        if ((xattr_index=get_xattr_value(xattr_ptr, marfs_xattrs[objid_index], xattr_count)) != -1) { 
                           xattr_ptr = &mar_xattrs[xattr_index];
                           if (debug) {
                              printf("objid xattr name = %s xattr_value =%s\n",xattr_ptr->xattr_name, xattr_ptr->xattr_value);
                              printf("remove file: %s  remove object:  %s\n", md_path_ptr, xattr_ptr->xattr_value); 
                           }
                           
                           // Deterimine if object type is packed.  If so we must complete scan
                           // to determine if all files exist for the object
                           if (post.obj_type == OBJ_PACKED) {
                              fprintf(file_info_ptr->packedfd,"%s %s %zu\n", xattr_ptr->xattr_value, md_path_ptr, post.chunks);
                              file_info_ptr->is_packed = 1;
                           }
                           // TO DO:
                           // Figure out how to get userid and host IP.  In xattr??
                           //   -- userid will be a fixed name according to Jeff/Gary
                           //   -- hostIP can be found in namespace namespace or repo info
                           //   So I will need to make some calls config file parser/routines to get this
                           //   But how do I link objectid to correct config table info?
                           //   Maybe I can pass gc.path or object Id back to parser routines 
                           //   and they will pass back host and userid?
                           //
                           // move s3 functions to separate function
                           //aws_init();
                           //Call find namespace to get username
                           //userid = find_namespace() 
                           //aws_read_config(userid);
                           ////aws_read_config("atorrez");
                           //hostname = find_host(namespace
                           //s3_set_host (hostname);
                           //s3_set_host ("10.140.0.17:9020");

                           // FOR SPROXYD
                           ////s3_set_host ("10.135.0.22:81");
                           // FOR SPROXYD
                           //IOBuf * bf = aws_iobuf_new();
                            // do not have to set bucket if part of the path
                           //s3_set_bucket("atorrez");
                           //

                           // Not checking return because log has error message and want to keep running 
                           // even if errors exists on certain objects or files
                           else {
                              trash_status = dump_trash(xattr_ptr, md_path_ptr, file_info_ptr, &post);
                           }
                        }
                     }
                  }
               }
            }
         }
      }
   } // endwhile
   clean_exit(file_info_ptr->outfd, iscanP, fsP, early_exit);
   return(rc);
}

/***************************************************************************** 
Name: parse_post_xattr 

 parse an xattr-value string into a MarFS_XattrPost

*****************************************************************************/
int parse_post_xattr(MarFS_XattrPost* post, struct marfs_xattr * post_str) {

   int   major;
   int   minor;

   char  obj_type_code;
   if (debug)
      printf("%s\n", post_str->xattr_value);
   // --- extract bucket, and some top-level fields
   int scanf_size = sscanf(post_str->xattr_value, MARFS_POST_FORMAT,
                           &major, &minor,
                           &obj_type_code,
                           &post->obj_offset,
                           &post->chunks,
                           &post->chunk_info_bytes,
                           &post->correct_info,
                           &post->encrypt_info,
                           &post->flags,
                           (char*)&post->md_path);
   if (scanf_size == EOF || scanf_size < 9)
      return -1;   
   post->obj_type = decode_obj_type(obj_type_code);
   return 0;
}


/***************************************************************************** 
Name: dump_trash 

 This function deletes the object file as well as gpfs metadata files
*****************************************************************************/
int dump_trash(struct marfs_xattr *xattr_ptr, char *md_path_ptr, 
               file_info *file_info_ptr, MarFS_XattrPost *post_xattr)
{
   int return_value =0;

   char object_name[MARFS_MAX_OBJID_SIZE];
   char *obj_name_ptr;
   int i;
   int delete_obj_status;

   //If multi type file then delete all objects associated with file
   if (post_xattr->obj_type == OBJ_MULTI) {
      for (i=0; i < post_xattr->chunks; i++ ) {
         obj_name_ptr = strrchr(xattr_ptr->xattr_value, '.');
         obj_name_ptr++;
         *obj_name_ptr='\0'; 
         sprintf(object_name, "%s%d",xattr_ptr->xattr_value,i);
         if ((delete_obj_status=delete_object(object_name,file_info_ptr)) != 0) {
            fprintf(file_info_ptr->outfd, "s3_delete error (HTTP Code:  %d) on object %s\n", delete_obj_status,xattr_ptr->xattr_value);
            return_value = -1;
         }
         else {
            fprintf(file_info_ptr->outfd, "deleted object %s\n", object_name);
         }
      } 
   }

   // else UNI BUT NEED to implemented other formats as developed 
   else if (post_xattr->obj_type == OBJ_UNI) {
      strncpy(object_name, xattr_ptr->xattr_value, strlen(xattr_ptr->xattr_value));
      if ((delete_obj_status=delete_object(object_name, file_info_ptr))  != 0 ) {
         fprintf(file_info_ptr->outfd, "s3_delete error (HTTP Code:  %d) on object %s\n", delete_obj_status,xattr_ptr->xattr_value);
         return_value = -1;
      }
      else {
         fprintf(file_info_ptr->outfd, "deleted object %s\n", object_name);
      }
   }

   // Delete trash files
   // Only delete if no error deleting object
   if (!return_value) {
      delete_file(md_path_ptr, file_info_ptr); 
   }
   return(return_value);
}

/***************************************************************************** 
Name: delete_object 

 This function deletes the object and returns -1 if error, 0 if successful
*****************************************************************************/
int delete_object(char * object, file_info *file_info_ptr)
{
   //
   int return_val;
   CURLcode s3_return;
   IOBuf * obj_buffer = aws_iobuf_new();

   print_current_time(file_info_ptr);

   s3_return = s3_delete( obj_buffer, object );
   return_val=check_S3_error(s3_return, obj_buffer, S3_DELETE);
   
   return(return_val);

   //if (rv != 0) 
   //   return -1;
   //else 
   //   return 0;
}

/***************************************************************************** 
Name: delete_file 

this function deletes the gpfs files assoiated with an object
*****************************************************************************/
int delete_file(char *filename, file_info *file_info_ptr)
{
   int return_value = 0;
   // NEED TO FIGURE OUT SIZE FOR THIS
   char path_file[4096];
   sprintf(path_file,"%s.path",filename);
   print_current_time(file_info_ptr);

   if ((unlink(filename) == -1)) {
      fprintf(file_info_ptr->outfd,"Error removing file %s\n",filename);
      return_value = -1;
   }
   else {
      fprintf(file_info_ptr->outfd,"deleted file %s\n",filename);
   }
   print_current_time(file_info_ptr);
   if ((unlink(path_file) == -1)) {
      fprintf(file_info_ptr->outfd,"Error removing file %s\n",path_file);
      return_value = -1;
   }
   else {
      fprintf(file_info_ptr->outfd,"deleted file %s\n",path_file);
   }
   return(return_value);
}

/***************************************************************************** 
Name: print_current_file 

This function prints current time to the log entry
*****************************************************************************/

void print_current_time(file_info *file_info_ptr)
{
   char time_string[20];
   struct tm *time_info;
   
   time_t now = time(0);
   time_info = localtime(&now);
   strftime(time_string, sizeof(time_string), "%Y-%m-%d %H:%M:%S", time_info);
   fprintf(file_info_ptr->outfd, "%s ", time_string);
}


/***************************************************************************** 
Name: process_packed

This function determines which objects and files should be deleted when
the object type is PACKED.  Packed implies multiple files packed into an 
object so objects and files cannot be deleted if all the files do not
exist for a particular object.  The function is passed in a filename that
contains a list of all PACKED entries found in the scan.
The format of the files is:

object_name file_name total_file_count

This function sorts by object name then proceeds to count how many files
exists for that object_name.  If the files counted equal the total file_count,
the object and files are deleted.  Ohterwise they are left in place and a 
repack utility will be run on the trash directory.
*****************************************************************************/

int process_packed(file_info *file_info_ptr)
{
   FILE *pipe_cat;
   FILE *pipe_grep;

   char obj_buf[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char file_buf[MARFS_MAX_MD_PATH];
   char objid[MARFS_MAX_OBJID_SIZE];
   char last_objid[MARFS_MAX_OBJID_SIZE];
   char grep_command[MARFS_MAX_MD_PATH+MAX_PACKED_NAME_SIZE+64];
   char cat_command[MAX_PACKED_NAME_SIZE];
   int obj_return;
   int df_return;

   int chunk_count;
   char filename[MARFS_MAX_MD_PATH];
   int count = 0;
   
   // create command to cat and sort the list of objects that are packed
   sprintf(cat_command, "cat %s | sort", file_info_ptr->packed_filename);
   
   // open STREAM for cat
   if (( pipe_cat = popen(cat_command, "r")) == NULL) {
      fprintf(stderr, "Error with popen\n");
      return(-1); 
   }
   // Get the first line and sscanf into object_name, filenames, and chunk cnt
   fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat);
   sscanf(obj_buf,"%s %s %d", objid, filename, &chunk_count);

   // Keep track of of when objid name changes 
   // count is used to see how many files have been counted
   // so that a check of whether all files are accounted for in the packed
   // object
   strcpy(last_objid, objid);
   count++;

   //In a loop get all lines from the file
   while(fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat)) {
      sscanf(obj_buf,"%s %s %d", objid, filename, &chunk_count);
      // if objid the same - keep counting files
      if (!strcmp(last_objid,objid)) {
         count++;
         // If file count == chuck count - all files accounted for
         // delete objec
         if (chunk_count == count) {

            if ((obj_return=delete_object(objid, file_info_ptr)) != 0) 
               fprintf(file_info_ptr->outfd, "s3_delete error (HTTP Code:  %d) on object %s\n", obj_return, objid);
            else 
              fprintf(file_info_ptr->outfd, "deleted object %s\n", objid);

            sprintf(grep_command,"grep %s %s | awk '{print $2}' ", objid, file_info_ptr->packed_filename);
            //Now open pipe to find all files associated with object 
            if (( pipe_grep = popen(grep_command, "r")) == NULL) {
               fprintf(stderr, "Error with popen\n");
               return(-1); 
            }
            //Delete files
            while(fgets(file_buf, MARFS_MAX_MD_PATH, pipe_grep)) {
               sscanf(file_buf,"%s",filename);
               df_return = delete_file(filename, file_info_ptr);
            }
         }
      }

      //current object did not match last one so on to next object and new count
      else {
         strcpy(last_objid, objid);
         count = 1;
      }

   }
   if (df_return == -1 || obj_return == -1)
      return(-1);
   if ((pclose(pipe_cat) == -1) || (pclose(pipe_grep) == -1)) {
      printf("Error closing pipes\n");
      return(-1);
   }
       
   else 
      return(0);
}


/***************************************************************************** 
 *
*****************************************************************************/
int check_S3_error( CURLcode curl_return, IOBuf *s3_buf, int action )
{
  if ( curl_return == CURLE_OK ) {
    //if (action == S3_CREATE || action == S3_STAT ) {
    //  if (s3_buf->code != HTTP_OK) {
    //    printf("Error, HTTP Code:  %d\n", s3_buf->code);
    //    return(-1);
    //  }
    //}
    if (action == S3_DELETE) {
    //else {// action == S3_DELETE
       if (s3_buf->code == HTTP_OK || s3_buf->code == HTTP_NO_CONTENT) {
          return(0);
       }
       else {
         printf("Error, HTTP Code:  %d\n", s3_buf->code);
         return(s3_buf->code);
       }
    }
  }
  else {
    printf("Error, Curl Return Code:  %d\n", curl_return);
    return(-1);
  }
  return(0);
}

