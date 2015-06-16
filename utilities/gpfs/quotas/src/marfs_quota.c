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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <gpfs.h>
#include <ctype.h>
#include <unistd.h>
#include "marfs_quota.h"

/******************************************************************************
* This program reads gpfs inodes and extended attributes in order to provide
* a total size value to the fsinfo file.  It is meant to run as a regularly
* scheduled batch job.
*
* Features to be added/to do:
* 
* 1) Need to interface with config parser
*  to:
*   get list of filesets for scanning
*   get count of filesets for scanning
* 2) Use new xattr to determine what fileset trash belongs
*    We currently do not have a way to determine this once files are in trash
*    alternatively, use a file as Gary suggested that has path to orignal
*
* 3) OTHER
*  determine extended attributes that we care about
*     passed in as args or hard coded?
*  determine arguments to main 
*  block (512 bytes) held by file
*  I consider this a perpetual process  in that we will find more and more
*  info we want to store in fsinfo as  the project proceeds.  But 
*  all info from attr and xattr is handy!
*
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
   unsigned int fileset_count = 3;
   extern char *optarg;
   fileset_stat *fileset_stat_ptr;
//   char * fileset_name = "root,proja,projb";
//   char  fileset_name[] = "root,proja,projb";
//   char  fileset_name[] = "project_a,projb,root";
   char  fileset_name[] = "project_a,root,project_b";
//   char  fileset_name[] = "root";
   char * indv_fileset_name; 
   int i;
   unsigned int fileset_scan_count = 0;
   unsigned int fileset_scan_index = 0; 


   if ((ProgName = strrchr(argv[0],'/')) == NULL)
      ProgName = argv[0];
   else
      ProgName++;

   while ((c=getopt(argc,argv,"c:d:f:hi:o:u:")) != EOF) {
      switch (c) {
         case 'c': fileset_scan_count =  atoi(optarg); break;
         case 'd': rdir = optarg; break;
         case 'f': fileset_id = atoi(optarg); break;
         case 'i': fileset_scan_index = atoi(optarg); break;
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
    * Things are becoming more clear now about how this will work with the parser
    * The parser will return a link-list. Jeff has created some support functions
    * that may be used to get the fileset from the returned link list.  It would
    * be ideal for the filesets to be in a comma seperated list that I can 
    * tokenize.
    *
    * Based on the number of filesets, this program will have two new optional
    * arguments  -c for number of filesets to scan and * -i index for where to 
    * start.  filesets are contained in an array of struct
    *
    *  so array element [0] --> fileset_1
    *                   [1] --> fileset_2
    *                   [2] --> fileset_3
    *                   [3[ --> fileset_4 
    *
    * so -c 2 -i 1 implies look for 2 filesets in scan and work on fileset_2 and 
    * fileset_3 (-i 1 implies [1] which is fileset_2).
    * If -c and -i are not specified the program defaults to looking for all filesets
    * defined in the array of structures.
    *  
    * If -c 0 is passed, the program will report total number of filesets to stdout
    * This info can be used to determine if running multiple instances on different 
    * filesets is desirable.
    * If -c is non-zero than -i must be given
    *
    * Sample Scenario
    *
    * ./marfs_quota -c 0 -d /path/to/top/level/mount/fileset
    * number of filesets:  10
    * on node 1
    * ./marfs_quota -c 2 -i 0 -d /path/tot/top/level/mount -o fsinfo.log
    * This will run the scan looing for the first two filesets starting at index 0 (from list of
    * filesets created from calling parser)
    * on node 2
    * ./marfs_quota -c 8 -i 2 -d /path/tot/top/level/mount -o fsinfo.log
    * Scan looking for remaining filesetso
    *
    * Or run looking for all filesets 
    * ./marfs_quota -c 10 -i 0 -d /path/tot/top/level/mount -o fsinfo.log
    *
    *
   */
   // Get list of filesets and count
   // When parser implemented 
   // get filesecount before doing any of this
   if ( fileset_scan_count == 0 ) {
      // Call parser get link list and count filesets
      // fileset_count = fileset_scan_count;
      fileset_count = 3; 
      // TEMP for now until I get parser integrated
      fileset_scan_count = fileset_count;
      // TEMP for now until I get parser integrated
   } 

   if ((fileset_scan_count > fileset_count) ||
       (fileset_scan_count + fileset_scan_index > fileset_count)) {

      fprintf(stderr, "Trying to scan more filesets than exist\n");
      print_usage();
      exit(1);
   }  
    
   fileset_stat_ptr = (fileset_stat *) malloc(sizeof(*fileset_stat_ptr)*fileset_count);
   if (fileset_stat_ptr == NULL ) {
      fprintf(stderr,"Memory allocation failed\n");
      exit(1);
   }
   init_records(fileset_stat_ptr, fileset_count);

   // now copy info into structure
   //
   // This code is dependent on what the parser returns.  I may need to do a strdup before 
   // tokeninzing.  I also require the count from the parser so that I can malloc the 
   // correct amount of filset structures.  I could always count filesets before I malloc 
   // if need be
   if (debug) 
      printf("Going to tokenize\n");
   indv_fileset_name = strtok(fileset_name,",");
   if (debug) 
      printf("%s\n", indv_fileset_name);
   i =0;
   while (indv_fileset_name != NULL) {
      if (debug) 
         printf("%s\n", indv_fileset_name);
      strcpy(fileset_stat_ptr[i].fileset_name, indv_fileset_name);
      indv_fileset_name = strtok(NULL,","); 
      i++;
   }
   if (debug) 
      printf("Filsets count = %d\n", fileset_count);
  
   outfd = fopen(outf,"w");

    


   // Add filsets to structure so that inode scan can update fileset info
   ec = read_inodes(rdir, outfd, fileset_id, fileset_stat_ptr, fileset_scan_count,fileset_scan_index);
//   fprintf(outfd,"small files = %llu\n medium files = %llu\n large_files = %llu\n",
//          histo_size_ptr->small_count, histo_size_ptr->medium_count, histo_size_ptr->large_count);
   free(fileset_stat_ptr);
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
   fprintf(stderr,"Usage: %s -d gpfs_path -o ouput_log_file [-c fileset_count] [-i start_index] [-f fileset_id]\n",ProgName);
   fprintf(stderr, "NOTE: -c and -i are optional.  Default behavior will be to try to match all filesets defined in config\n");
   fprintf(stderr, "See README for information\n");
}

/***************************************************************************** 
Name: fill_size_histo 

This function counts file sizes based on small, medium, and large for 
the purposes of displaying a size histogram.
*****************************************************************************/
static void fill_size_histo(const gpfs_iattr_t *iattrP, fileset_stat *fileset_buffer, int index)
{

   if (iattrP->ia_size < SMALL_FILE_MAX) 
     fileset_buffer[index].small_count+= 1;
   
   else if (iattrP->ia_size < MEDIUM_FILE_MAX) 
     fileset_buffer[index].medium_count+= 1;
   else
     fileset_buffer[index].large_count+= 1;
}

/***************************************************************************** 
Name:  get_xattr_value

This function, given the name of the attribute returns the associated value.

*****************************************************************************/
int get_xattr_value(struct marfs_xattr *xattr_ptr, const char *desired_xattr, int cnt, FILE *outfd) {

   int i;
   int ret_value = -1;

   for (i=0; i< cnt; i++) {
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
                 //const char * xattr_1,
                 //const char * xattr_2,
                 //const char * xattr_3,
                 struct marfs_xattr *xattr_ptr) {
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

      // find marfs xattrs we care about by comaring our list of xattrs
      // to what the scan has found
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
      // Now get associated value 
      // Determine if printible characters
      if (valueLen > 0 && xattr_count > 0) {
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

         //create string from xattr value char array
         for (i = 0; i < valueLen; i++) {
            if (printable) {
              xattr_ptr->xattr_value[i] = valueP[i]; 
            }
         }
         xattr_ptr->xattr_value[valueLen] = '\0'; 
         xattr_ptr++;
      }
      //xattr_ptr++;
      //xattr_count++;
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
int read_inodes(const char *fnameP, FILE *outfd, int fileset_id,fileset_stat *fileset_stat_ptr, size_t rec_count, size_t offset_start) {
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
   int last_struct_index = -1;
   unsigned int struct_index;
   unsigned int last_fileset_id = -1;


   // Defined xattrs as an array of const char strings with defined indexs
   const char *marfs_xattrs[] = {"user.marfs_post","user.marfs_objid","user.marfs_restart"};
   int post_index=0;
   //int objid_index=1;
   //int restart_index=2;
   int marfs_xattr_cnt = MARFS_QUOTA_XATTR_CNT;

   MarFS_XattrPost post;
   //const char *xattr_post_name = "user.a";
  
   int early_exit =0;
   int xattr_index;

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

         /*
         At this point determine if the last inode fileset name matches this one.  if not,
         call a function to determine which struct index contains that fileset
         the function will return an index and this function will update appropriate 
         fields.
         */
         if (debug) 
            printf("%d %d\n", last_fileset_id, iattrP->ia_filesetid);
         if (last_fileset_id != iattrP->ia_filesetid) {
            gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid, &fileset_name_buffer, 32); 
            struct_index = lookup_fileset(fileset_stat_ptr,rec_count,offset_start,fileset_name_buffer);
            if (struct_index == -1) 
               continue;
            last_struct_index = struct_index;
            last_fileset_id = iattrP->ia_filesetid;
         }
         fileset_stat_ptr[last_struct_index].sum_size+=iattrP->ia_size;
         fileset_stat_ptr[last_struct_index].sum_file_count+=1;
         if (debug) 
            printf("%d size = %llu file size sum  = %zu\n", last_struct_index,iattrP->ia_size,fileset_stat_ptr[last_struct_index].sum_size);
         fill_size_histo(iattrP, fileset_stat_ptr, last_fileset_id); 

         // Do we have extended attributes?
         // This will be modified as time goes on - what xattrs do we care about
         
         if (iattrP->ia_xperm == 2 && xattr_len >0 ) {
            xattr_ptr = &mar_xattrs[0];
            // get marfs xattrs and associated values
            if ((xattr_count = get_xattrs(iscanP, xattrBP, xattr_len, marfs_xattrs, marfs_xattr_cnt, xattr_ptr)) > 0) {
               xattr_ptr = &mar_xattrs[0];
               // Get post xattr value
               if ((xattr_index=get_xattr_value(xattr_ptr, marfs_xattrs[post_index], xattr_count, outfd)) != -1 ) {
                   xattr_ptr = &mar_xattrs[xattr_index];
                   //if (debug)
                   //fprintf(outfd,"post xattr name = %s value = %s count = %d\n",xattr_ptr->xattr_name, xattr_ptr->xattr_value, xattr_count);
               }
               // scan into post xattr structure
               if (str_2_post(&post, xattr_ptr)) {
                  continue;             
               }
               if (debug) 
                  printf("found post chunk info bytes %zu\n", post.chunk_info_bytes);
               fileset_stat_ptr[last_struct_index].sum_filespace_used += post.chunk_info_bytes;
               // Determine if file in trash directory
               if (!strcmp(post.gc_path, "")){
                  fprintf(outfd,"gc_path is NULL\n");
                  if (debug) 
                     printf("gc_path is NULL\n");
               } 
               // Is trash
               else {
                  //if(debug)
                  //fprintf(outfd,"index = %d   %llu\n", last_struct_index, iattrP->ia_size);
                  fileset_stat_ptr[last_struct_index].sum_trash += iattrP->ia_size;
                  /*
                    Code needed here in order to determine trash per fileset 
                    xattr_index=get_xattr_value(xattr_ptr, marfs_xattrs[objid_index], xattr_count, outfd)  
                    xattr_ptr = &mar_xattrs[xattr_index]
                    str_2_pre to get filespace name - this should be fileset name
                        use Jeff's code as a model no need to do all the parsing that he does
                    index = lookup_fileset(fileset_stat_ptr,rec_count,offset_start,fileset_name_buffer); 
                    fileset_stat_ptr[index].sum_trash += iattrP->ia_size;
                  */
               }
            }
         }
      }
   } // endwhile
   write_fsinfo(outfd, fileset_stat_ptr, rec_count, offset_start);
   clean_exit(outfd, iscanP, fsP, early_exit);
   return(rc);
}

/***************************************************************************** 
Name: lookup_fileset 

This function attempts to match the passed in fileset name to the structure
element (array of structures ) containing that same fileset name.  The index 
for the matching element is returned. 
*****************************************************************************/
int lookup_fileset(fileset_stat *fileset_stat_ptr, size_t rec_count, size_t offset_start, char *inode_fileset)
{
   int index = offset_start;
   int comp_res;
  

   // search the array of structures for matching fileset name
   do 
   {
      index++;
   } while ((comp_res = strcmp(fileset_stat_ptr[index-1].fileset_name, inode_fileset)) && index <= rec_count);
   // found a match
   if (!comp_res) {
      return(index-1);
   }
   // no match return -1
   else { 
      return(-1);
   }
}   


/***************************************************************************** 
Name: str_2_post 

 parse an xattr-value string into a MarFS_XattrPost

*****************************************************************************/
int str_2_post(MarFS_XattrPost* post, struct marfs_xattr * post_str) {

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

   if (scanf_size == EOF)
      return -1;                // errno is set
   else if (scanf_size < 9) {
      errno = EINVAL;
      return -1;            /* ?? */
   }
   return 0;
}


/***************************************************************************** 
Name: write_fsinfo 

This function prints various fileset information to the fs_info file

*****************************************************************************/
void write_fsinfo(FILE* outfd, fileset_stat* fileset_stat_ptr, size_t rec_count, size_t index_start)
{
   size_t i;
 
   for (i=index_start; i < rec_count+index_start; i++) {
      fprintf(outfd,"[%s]\n", fileset_stat_ptr[i].fileset_name);
      fprintf(outfd,"total_file_count:  %zu\n", fileset_stat_ptr[i].sum_file_count);
      fprintf(outfd,"total_size:  %zu\n", fileset_stat_ptr[i].sum_size);
      fprintf(outfd,"trash_size:  %zu\n", fileset_stat_ptr[i].sum_trash);
      fprintf(outfd,"adjusted_size:  %zu\n", fileset_stat_ptr[i].sum_size - fileset_stat_ptr[i].sum_trash);
   }
}

