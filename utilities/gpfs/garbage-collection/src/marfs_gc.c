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
   FILE *outfd;
   int ec;
   char *outf = NULL;
   char *rdir = NULL;
   //unsigned int uid = 0;
   int fileset_id = -1;
   int  c;
   unsigned int fileset_count = 1;
   extern char *optarg;
   fileset_stat *fileset_stat_ptr;
//   char * fileset_name = "root,proja,projb";
//   char  fileset_name[] = "root,proja,projb";
//   char  fileset_name[] = "project_a,projb,root";
//   char  fileset_name[] = "project_a,root,projb";
//   char  fileset_name[] = "trash";
//   char  fileset_name[] = "project_c";
   char  fileset_name[] = "trash";

   if ((ProgName = strrchr(argv[0],'/')) == NULL)
      ProgName = argv[0];
   else
      ProgName++;

   while ((c=getopt(argc,argv,"d:f:ho:u:")) != EOF) {
      switch (c) {
         case 'd': rdir = optarg; break;
         case 'o': outf = optarg; break;
         //case 'u': uid = atoi(optarg); break;
         case 'h': print_usage();
         default:
            exit(0);
      }
   }
   
   if (rdir == NULL || outf == NULL) {
      fprintf(stderr,"%s: no directory (-d) or output file name (-o) specified\n",ProgName);
      exit(1);
   }
   /*
    * Now assuming that the config file has a list of filesets (either name or path or both).
    * I will make a call to get a list of filesets and the count.
    * I will use the count to malloc space for a array of strutures count size.
    * I will malloc an array of ints count size that will map fileset id to array index 
   */
   // Get list of filesets and count

   int *fileset_id_map;
   fileset_id_map = (int *) malloc(sizeof(int)*fileset_count); 
   fileset_stat_ptr = (fileset_stat *) malloc(sizeof(*fileset_stat_ptr)*fileset_count);
   if (fileset_stat_ptr == NULL || fileset_id_map == NULL) {
      fprintf(stderr,"Memory allocation failed\n");
      exit(1);
   }
   init_records(fileset_stat_ptr, fileset_count);
   aws_init();
   strcpy(fileset_stat_ptr[0].fileset_name, fileset_name);

   outfd = fopen(outf,"w");
   // Add filsets to structure so that inode scan can update fileset info
   ec = read_inodes(rdir,outfd,fileset_id,fileset_stat_ptr,fileset_count);
   return (0);   
}

/***************************************************************************** 
Name: init_records 

*****************************************************************************/
void init_records(fileset_stat *fileset_stat_buf, unsigned int record_count)
{
   memset(fileset_stat_buf, 0, (size_t)record_count * sizeof(fileset_stat)); 
}

/***************************************************************************** 
Name: print_usage 

*****************************************************************************/
void print_usage()
{
   fprintf(stderr,"Usage: %s -d gpfs_path -o ouput_log_file [-f fileset_id]\n",ProgName);
}



/***************************************************************************** 
Name:  get_xattr_value

This function, given the name of the attribute, returns the associated value.

*****************************************************************************/
int get_xattr_value(struct marfs_xattr *xattr_ptr, const char *desired_xattr, int cnt) {

   int i;
   int ret_value = -1;

   for (i=0; i< cnt; i++) {
      printf("XX %s %s\n", xattr_ptr->xattr_name, desired_xattr);
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
   fclose(fd);
   if (terminate) 
      exit(0);
   else 
      return(0);
}

/***************************************************************************** 
Name: read_inodes 

This function opens an inode scan in order to provide size/block information
as well as file extended attribute information

*****************************************************************************/
int read_inodes(const char *fnameP, FILE *outfd, int fileset_id,fileset_stat *fileset_stat_ptr, size_t rec_count) {
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


  // NEED TO FIGURE OUT SIZE FOR THIS
   char path_file[4096];



//   const char *xattr_objid_name = "user.marfs_objid";
//   const char *xattr_post_name = "user.marfs_post";
   MarFS_XattrPost post;
   //const char *xattr_post_name = "user.a";
  
   int early_exit =0;
   int xattr_index;
   char *gc_path_ptr;

   //outfd = fopen(onameP,"w");

   /*
    *  Get the unique handle for the filesysteme
   */
   if ((fsP = gpfs_get_fssnaphandle_by_path(fnameP)) == NULL) {
      rc = errno;
      fprintf(stderr, "%s: line %d - gpfs_get_fshandle_by_path: %s\n", 
              ProgName,__LINE__,strerror(rc));
      early_exit = 1;
      clean_exit(outfd, iscanP, fsP, early_exit);
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
      clean_exit(outfd, iscanP, fsP, early_exit);
   }


   while (1) {
      rc = gpfs_next_inode_with_xattrs(iscanP,0x7FFFFFFF,&iattrP,&xattrBP,&xattr_len);
      //rc = gpfs_next_inode(iscanP, 0x7FFFFFFF, &iattrP);
      if (rc != 0) {
         rc = errno;
         fprintf(stderr, "gpfs_next_inode: %s\n", strerror(rc));
         early_exit = 1;
         clean_exit(outfd, iscanP, fsP, early_exit);
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
            fprintf(outfd,"%u|%lld|%lld|%d|%d|%u|%u|%u|%u|%u|%lld|%d\n",
            iattrP->ia_inode, iattrP->ia_size,iattrP->ia_blocks,iattrP->ia_nlink,iattrP->ia_filesetid,
            iattrP->ia_uid, iattrP->ia_gid, iattrP->ia_mode,
            iattrP->ia_atime.tv_sec,iattrP->ia_mtime.tv_sec, iattrP->ia_blocks, iattrP->ia_xperm );
         }
         gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid, &fileset_name_buffer, 32); 
         if (!strcmp(fileset_name_buffer,fileset_stat_ptr[0].fileset_name)) {



         // Do we have extended attributes?
         // This will be modified as time goes on - what xattrs do we care about
            if (iattrP->ia_xperm == 2 && xattr_len >0 ) {
               xattr_ptr = &mar_xattrs[0];
               //if ((xattr_count = get_xattrs(iscanP, xattrBP, xattr_len, xattr_post_name, xattr_objid_name, xattr_ptr, outfd)) > 0) {
               if ((xattr_count = get_xattrs(iscanP, xattrBP, xattr_len, marfs_xattrs, marfs_xattr_cnt, xattr_ptr, outfd)) > 0) {
                  xattr_ptr = &mar_xattrs[0];
                  //if ((xattr_index=get_xattr_value(xattr_ptr, xattr_post_name, xattr_count)) != -1 ) { 
                  if ((xattr_index=get_xattr_value(xattr_ptr, marfs_xattrs[post_index], xattr_count)) != -1 ) { 
                     xattr_ptr = &mar_xattrs[xattr_index];
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
                  if (!strcmp(post.gc_path, "")){
                     if (debug) 
			// why would this ever happen?  if in trash gc_path should be non-null
                        printf("gc_path is NULL\n");
                  } 
                  // else trash
                  else {
                     printf("found trash\n");
                     gc_path_ptr = &post.gc_path[0];
                     //create string for *.path file that needs removal also
                     sprintf(path_file,"%s.path",gc_path_ptr);
                     printf("%s\n",marfs_xattrs[post_index]);
                     printf("path_file is %s\n", path_file);
                     xattr_ptr = &mar_xattrs[0];
                     if ((xattr_index=get_xattr_value(xattr_ptr, marfs_xattrs[objid_index], xattr_count)) != -1) { 
                        xattr_ptr = &mar_xattrs[xattr_index];
                        printf("objid xattr name = %s xattr_value =%s\n",xattr_ptr->xattr_name, xattr_ptr->xattr_value);
                        printf("remove file: %s  remove object:  %s\n", gc_path_ptr, xattr_ptr->xattr_value); 
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
                        aws_read_config("atorrez");
                        //hostname = find_host(namespace
                        //s3_set_host (hostname);
                        s3_set_host ("10.140.0.17:9020");
                        IOBuf * bf = aws_iobuf_new();
                         // do not have to set bucket if part of the path
                        //s3_set_bucket("atorrez");
                        //

                        trash_status = dump_trash(bf, xattr_ptr, gc_path_ptr, &path_file[0], outfd);

                     }

                  }
               }
            }
         }
      }
   } // endwhile
   clean_exit(outfd, iscanP, fsP, early_exit);
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
                           &post->num_objects,
                           &post->chunk_info_bytes,
                           &post->correct_info,
                           &post->encrypt_info,
                           (char*)&post->gc_path);

   if (scanf_size == EOF || scanf_size < 9)
      return -1;   
   return 0;
}


/***************************************************************************** 
Name: dump_trash 

 This function deletes the object file as well as gpfs metadata files
*****************************************************************************/
int dump_trash(IOBuf * bf, struct marfs_xattr *xattr_ptr, char *gc_path_ptr, 
               char *path_file_ptr, FILE *outfd) 
{
   int return_value =0;
   int rv;
   char time_string[20];
   struct tm *time_info;
   
   time_t now = time(0);
   time_info = localtime(&now);
   strftime(time_string, sizeof(time_string), "%Y-%m-%d %H:%M:%S", time_info);
 
   // Delete object
   fprintf(outfd, "%s ", time_string);
   rv = s3_delete( bf, xattr_ptr->xattr_value);
   if (rv != 0) {
      fprintf(outfd, "s3_delete error on object %s\n", xattr_ptr->xattr_value);
      return_value = -1;
   }
   else {
      fprintf(outfd, "deleted object %s\n", xattr_ptr->xattr_value);
   }

   // Delete trash file
   fprintf(outfd, "%s ", time_string);
   if ((unlink(gc_path_ptr) == -1)) {
      fprintf(outfd,"Error removing file %s\\n",gc_path_ptr);
      return_value = -1;
   }
   else {
      fprintf(outfd,"deleted file %s\n",gc_path_ptr);
   }

   // Delete trash path file
   fprintf(outfd, "%s ", time_string);
   if ((unlink(path_file_ptr) == -1)) {
      fprintf(outfd,"Error removing file %s\n",path_file_ptr);
      return_value = -1;
   }
   else {
      fprintf(outfd,"deleted file %s\n",path_file_ptr);
   }
   return(return_value);
}
