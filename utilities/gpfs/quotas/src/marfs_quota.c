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
#include <getopt.h>
#include <sys/types.h>
#include "marfs_quota.h"
#include "marfs_configuration.h"


/******************************************************************************
* This program reads gpfs inodes and extended attributes in order to provide
* a summary of file counts, file sizes, and trash info for each fileset
* This information is written to a file specified by the -o option. 
* 
*
******************************************************************************/
char    *ProgName;

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
   unsigned int fileset_count = 4;
   extern char *optarg;
   Fileset_Stats *fileset_stat_ptr = NULL;
   int fileset_scan_count = -1;
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
            print_usage();
            exit(0);
      }
   }
   
   if (rdir == NULL || outf == NULL) {
      fprintf(stderr,"%s: no directory (-d) or output file name (-o)\
 specified\n",ProgName);
      exit(1);
   }

   /*
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
    * If -c and -i are not specified the program defaults to looking for all 
    * filesets defined in the array of structures.
    *  
    * If -c 0 is passed, the program will report total number of filesets to 
    * stdout.  This info can be used to determine if running multiple instances
    * on different filesets is desirable.
    * If -c is non-zero than -i must be given
    *
    * Sample Scenario - running multiple instances
    *
    * ./marfs_quota -c 0 -d /path/to/top/level/mount/fileset
    * number of filesets:  10
    * on node 1
    * ./marfs_quota -c 2 -i 0 -d /path/tot/top/level/mount -o fsinfo.log
    * This will run the scan looing for the first two filesets starting at 
    * index 0 (from list of * filesets created from calling parser)
    * on node 2
    * ./marfs_quota -c 8 -i 2 -d /path/tot/top/level/mount -o fsinfo.log
    * Scan looking for remaining filesetso
    *
    *
    * Or run looking for all filesets 
    * ./marfs_quota -c 10 -i 0 -d /path/tot/top/level/mount -o fsinfo.log
    * Or, simply
    * ./marfs_quota -d /path/tot/top/level/mount -o fsinfo.log
    *
    *
    *
   */

   if ( fileset_scan_count <= 0 ) {
      fileset_stat_ptr = read_config(&fileset_count); 

      if (fileset_stat_ptr == NULL ) {
         fprintf(stderr,"Problem with reading of configuration file\n");
         exit(1);
      }

      // User wants to know how many filesets exists
      // print and exit
      if ( fileset_scan_count == 0 ) {
         fprintf(stderr, "fileset count = %d\n", fileset_count);
         exit(0);
      }
      fileset_scan_count = fileset_count;
   }
   else {
     //do below checks here since only applies when user provides these parameters
      if ((fileset_scan_count > fileset_count) ||
         (fileset_scan_count + fileset_scan_index > fileset_count)) {

         fprintf(stderr, "Trying to scan more filesets than exist\n");
         print_usage();
         exit(1);
      }  
   }
    
   // open the user defined log file
   outfd = fopen(outf,"w");
   if (outfd == NULL) {
      fprintf(stderr, "Error opening log file\n");
      exit(1);
   }

   // Add filsets to structure so that inode scan can update fileset info
   ec = read_inodes(rdir, outfd, fileset_id, fileset_stat_ptr, 
                    fileset_scan_count,fileset_scan_index);
   //free(myNamespaceList);
   free(fileset_stat_ptr);
   return (0);   
}

/***************************************************************************** 
Name: init_records 
This function initializes all running counts and sizes in the Fileset_Stats
structure.  This is the structure that contains namespace/fileset counts 
and sizes
*****************************************************************************/
void init_records(Fileset_Stats *fileset_stat_buf, unsigned int record_count)
{
   //Cannot use memset because read_config has already populated
   //namespace_name so just zero out counts.
   int i;
   for (i=0; i< record_count; i++) {
      fileset_stat_buf[i].sum_size=0;
      fileset_stat_buf[i].sum_blocks=0;
      fileset_stat_buf[i].sum_filespace_used=0;
      fileset_stat_buf[i].sum_file_count=0;
      fileset_stat_buf[i].sum_trash=0;
      fileset_stat_buf[i].sum_trash_file_count=0;
      fileset_stat_buf[i].adjusted_size=0;
      fileset_stat_buf[i].small_count=0;
      fileset_stat_buf[i].medium_count=0;
      fileset_stat_buf[i].large_count=0;
      fileset_stat_buf[i].small_count=0;
      fileset_stat_buf[i].obj_type.uni_count=0;
      fileset_stat_buf[i].obj_type.multi_count=0;
      fileset_stat_buf[i].obj_type.packed_count=0;
      fileset_stat_buf[i].sum_restart_size=0;
      fileset_stat_buf[i].sum_restart_file_count=0;
   }
}

/***************************************************************************** 
Name: print_usage 

*****************************************************************************/
void print_usage()
{
   fprintf(stderr,"Usage: %s -d gpfs_path -o ouput_log_file [-c fileset_count]\
 [-i start_index] [-f fileset_id]\n",ProgName);
   fprintf(stderr, "NOTE: -c and -i are optional.  Default behavior will be\
 to try to match all filesets defined in config\n");
   fprintf(stderr, "See README for information\n");
}

/***************************************************************************** 
Name: fill_size_histo 

This function counts file sizes based on small, medium, and large for 
the purposes of displaying a size histogram.
*****************************************************************************/
static void fill_size_histo(const         gpfs_iattr_t *iattrP, 
                            Fileset_Stats *fileset_buffer, 
                            int           index)
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

This function, given the name of the xattr, returns the associated index
for the value.

*****************************************************************************/
int get_xattr_value(Marfs_Xattr *xattr_ptr, 
                    const char *desired_xattr, 
                    int cnt, FILE *outfd) {

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
                 Marfs_Xattr *xattr_ptr) {
   int rc;
   int i;
   const char *nameP;
   const char *valueP;
   unsigned int valueLen;
   const char *xattrBufP = xattrP;
   unsigned int xattrBufLen = xattrLen;
   int printable;
   int xattr_count =0;
   int desired_xattr = 0;

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
            desired_xattr =1;
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
      if (desired_xattr) {
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
          desired_xattr=0;
      }
   } // endwhile
   return(xattr_count);
}

/***************************************************************************** 
Name:  clean_exit

This function closes gpfs-related inode information and file handles

*****************************************************************************/

int clean_exit(FILE                 *fd, 
               gpfs_iscan_t         *iscanP, 
               gpfs_fssnap_handle_t *fsP, 
               int                  terminate) {
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
int read_inodes(const char    *fnameP, 
                FILE          *outfd, 
                int           fileset_id, 
                Fileset_Stats *fileset_stat_ptr, 
                size_t        rec_count, 
                size_t        offset_start) {
   int rc = 0;
   const gpfs_iattr_t *iattrP;
   const char *xattrBP;
   unsigned int xattr_len; 
   register gpfs_iscan_t *iscanP = NULL;
   gpfs_fssnap_handle_t *fsP = NULL;
   Marfs_Xattr mar_xattrs[MAX_MARFS_XATTR];
   Marfs_Xattr *xattr_ptr = mar_xattrs;
   int xattr_count;
   char fileset_name_buffer[MARFS_MAX_NAMESPACE_NAME];

   int last_struct_index = -1;
   unsigned int struct_index;
   int fileset_trash_index = 0;
   int trash_index = -1;
 
   unsigned int last_fileset_id = -1;


   // Defined xattrs as an array of const char strings with defined indexs
   // Change MARFS_QUOTA_XATTR_CNT in marfs_gc.h if the number of xattrs
   // changes
   int marfs_xattr_cnt = MARFS_QUOTA_XATTR_CNT;
   const char *marfs_xattrs[] = {"user.marfs_post","user.marfs_objid",
                                 "user.marfs_restart"};
   int post_index=0;
   int restart_index=2;

   MarFS_XattrPost post;
  
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
      clean_exit(outfd, iscanP, fsP, early_exit);
   }

   /*
    *  Open the inode file for an inode scan with xattrs
   */
  //if ((iscanP = gpfs_open_inodescan(fsP, NULL, NULL)) == NULL) {
   if ((iscanP = gpfs_open_inodescan_with_xattrs(fsP, NULL, -1, NULL, NULL)) 
        == NULL) {
      rc = errno;
      fprintf(stderr, "%s: line %d - gpfs_open_inodescan: %s\n", 
      ProgName,__LINE__,strerror(rc));
      early_exit = 1;
      clean_exit(outfd, iscanP, fsP, early_exit);
   }


   while (1) {
      rc = gpfs_next_inode_with_xattrs(iscanP,0x7FFFFFFF,&iattrP,&xattrBP,
                                       &xattr_len);
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
         fprintf(stderr,"%s: invalid inode %9d (GPFS_IAFLAG_ERROR)\n", 
                 ProgName,iattrP->ia_inode);
         continue;
      } 

      // If fileset_id is specified then only look for those inodes and xattrs
      if (fileset_id >= 0) {
         if (fileset_id != iattrP->ia_filesetid){
            continue; 
         }
      }

      // This is handy for debug at the moment
      if (iattrP->ia_inode != 3) {	/* skip the root inode */
         //LOG(LOG_INFO, "%u|%lld|%lld|%d|%d|%u|%u|%u|%u|%u|%lld|%d\n",
         //   iattrP->ia_inode, iattrP->ia_size,iattrP->ia_blocks,
         //   iattrP->ia_nlink,iattrP->ia_filesetid, iattrP->ia_uid, 
         //   iattrP->ia_gid, iattrP->ia_mode, iattrP->ia_atime.tv_sec,
         //   iattrP->ia_mtime.tv_sec, iattrP->ia_blocks, iattrP->ia_xperm );

         /*
         At this point determine if the last inode fileset name matches this one.  if not,
         call a function to determine which struct index contains that fileset
         the function will return an index and this function will update appropriate 
         fields.
         */
         if (last_fileset_id != iattrP->ia_filesetid) {
            //gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid, &fileset_name_buffer, 32); 
            gpfs_igetfilesetname(iscanP, iattrP->ia_filesetid, 
                                 &fileset_name_buffer, MARFS_MAX_NAMESPACE_NAME); 
            struct_index = lookup_fileset(fileset_stat_ptr,rec_count,
                                          offset_start,fileset_name_buffer);
            
             LOG(LOG_INFO, "scan fileset = %s\n", fileset_name_buffer);
            if (struct_index == -1) 
               continue;
            last_struct_index = struct_index;
            last_fileset_id = iattrP->ia_filesetid;
         }

         // Do we have extended attributes?
         // This will be modified as time goes on - what xattrs do we care about
         if (iattrP->ia_xperm & GPFS_IAXPERM_XATTR && xattr_len >0 ) {
            xattr_ptr = &mar_xattrs[0];
            // get marfs xattrs and associated values
            if ((xattr_count = get_xattrs(iscanP, xattrBP, xattr_len, 
                                          marfs_xattrs, marfs_xattr_cnt, 
                                          xattr_ptr)) > 0) {
               xattr_ptr = &mar_xattrs[0];

               // Get post xattr value
               if ((xattr_index=get_xattr_value(xattr_ptr, 
                                                marfs_xattrs[post_index],
                                                xattr_count, outfd)) != -1 ) {
                   xattr_ptr = &mar_xattrs[xattr_index];

                   LOG(LOG_INFO, "post xattr name = %s value = %s count = %d\n",
                   xattr_ptr->xattr_name, xattr_ptr->xattr_value, xattr_count);
               }
               // If restart xattr keep stats on this as well
               else if ((xattr_index=get_xattr_value(xattr_ptr,
                                                marfs_xattrs[restart_index],
                                                xattr_count, outfd)) != -1 ) {
                  fileset_stat_ptr[last_struct_index].sum_restart_size+=iattrP->ia_size;
                  fileset_stat_ptr[last_struct_index].sum_restart_file_count+=1;
               }

               // scan into post xattr structure
               // if error parsing this xattr, skip and continue
               if (parse_post_xattr(&post, xattr_ptr) == -1) {
                  continue;             
               }
               fileset_stat_ptr[last_struct_index].sum_size+=iattrP->ia_size;
               fileset_stat_ptr[last_struct_index].sum_file_count+=1;
               LOG(LOG_INFO, "struct index = %d size = %llu file size sum\
                   = %zu\n", last_struct_index,
               iattrP->ia_size,fileset_stat_ptr[last_struct_index].sum_size);
               fill_size_histo(iattrP, fileset_stat_ptr, last_fileset_id); 

               // Determine obj_type and update counts
               update_type(&post, fileset_stat_ptr, last_struct_index);

               LOG(LOG_INFO,"found post chunk info bytes %zu\n", post.chunk_info_bytes);
               fileset_stat_ptr[last_struct_index].sum_filespace_used += \
                                post.chunk_info_bytes;

               /* Determine if file in trash directory
               * if this is trash there are a few steps here
               *
               *  1)  Read post xattr to determine which fileset the trash came from
               *  2)  Update the fileset structure with a trash size count
               *  3)  First sum the trash in this fileset
               *
               */
               if ( post.flags & POST_TRASH ) {
                  xattr_ptr = &mar_xattrs[0];
                  md_path_ptr = &post.md_path[0];
                  fileset_trash_index = lookup_fileset_path(fileset_stat_ptr, 
                                                    rec_count, &trash_index, 
                                                    md_path_ptr);
                  if (fileset_trash_index == -1) {
                     fprintf(stderr, "Error finding .path file for %s", 
                             fileset_stat_ptr->fileset_name);
                     fprintf(stderr, "md_path =%s\n", 
                             md_path_ptr);
                     continue;
                  }
                  else {
                     fileset_stat_ptr[fileset_trash_index].sum_trash += iattrP->ia_size;
                     fileset_stat_ptr[fileset_trash_index].sum_trash_file_count += 1;
                     if (trash_index != -1) {
                        fileset_stat_ptr[trash_index].sum_size += iattrP->ia_size;
                        fileset_stat_ptr[trash_index].sum_file_count += 1;

                     } 
                  }
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
int lookup_fileset(Fileset_Stats *fileset_stat_ptr, 
                   size_t        rec_count, 
                   size_t        offset_start, 
                   char          *inode_fileset) {

   int index = offset_start;
   int comp_res;
  

   // search the array of structures for matching fileset name
   do 
   {
      index++;
   } while ((comp_res = strcmp(fileset_stat_ptr[index-1].fileset_name, 
             inode_fileset)) && index <= rec_count);
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
//Name: str_2_post 
Name: parse_post_xattr

 parse an xattr-value string into a MarFS_XattrPost

*****************************************************************************/
int parse_post_xattr (MarFS_XattrPost* post, Marfs_Xattr * post_str) {

   uint16_t   major;
   uint16_t   minor;

   char  obj_type_code;
   LOG(LOG_INFO,"%s\n", post_str->xattr_value);

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

   if (scanf_size == EOF || scanf_size < 9) {
      return -1;                
   }
   post->obj_type = decode_obj_type(obj_type_code);
   return 0;
}


/***************************************************************************** 
Name: write_fsinfo 

This function prints various fileset information to the fs_info file

*****************************************************************************/
void write_fsinfo(FILE*         outfd, 
                  Fileset_Stats *fileset_stat_ptr, 
                  size_t        rec_count, 
                  size_t        index_start)
{
   size_t i;
   int GIB = 1024*1024*1024;
 
   for (i=index_start; i < rec_count+index_start; i++) {
      fprintf(outfd,"[%s]\n", fileset_stat_ptr[i].fileset_name);
      fprintf(outfd,"total_file_count:    %zu\n", \
              fileset_stat_ptr[i].sum_file_count);
      fprintf(outfd,"total_size:          %zu (%zuG)\n", fileset_stat_ptr[i].sum_size,\
               fileset_stat_ptr[i].sum_size/GIB);
      fprintf(outfd,"uni count:           %zu\n", \
              fileset_stat_ptr[i].obj_type.uni_count);
      fprintf(outfd,"multi count:         %zu\n", \
              fileset_stat_ptr[i].obj_type.multi_count);
      fprintf(outfd,"packed count:        %zu\n", \
              fileset_stat_ptr[i].obj_type.packed_count);
      fprintf(outfd,"restart_file_count:  %zu\n", \
              fileset_stat_ptr[i].sum_restart_file_count);
      fprintf(outfd,"restart_size:        %zu\n", fileset_stat_ptr[i].sum_restart_size);
      fprintf(outfd,"trash_file_count:    %zu\n", \
              fileset_stat_ptr[i].sum_trash_file_count);
      fprintf(outfd,"trash_size:          %zu\n\n", fileset_stat_ptr[i].sum_trash);
   }
   trunc_fsinfo(outfd, fileset_stat_ptr, rec_count, index_start);
}
/***************************************************************************** 
Name: truncate_fsinfo 

This function truncates the fsinfo file to the total size determined for 
a particular namespace.  The fsinfo path is contained in the configuration
file.

*****************************************************************************/
int trunc_fsinfo(FILE*         outfd, 
                 Fileset_Stats *fileset_stat_ptr, 
                 size_t        rec_count, 
                 size_t        index_start)
{
   int ret;
   int i;

   //  Go through all namespaces/filesets scanned
   for (i=index_start; i < rec_count+index_start; i++) {
      // Do not truncate fsinfo file if trash
      if (strcmp(fileset_stat_ptr[i].fileset_name, "trash")) {
         // do trunc
         ret = truncate(fileset_stat_ptr[i].fsinfo_path, 
                        fileset_stat_ptr[i].sum_size);
         if (ret == -1) {
            fprintf(stderr, "Unable to truncate %s to %zu in namespace %s\n",
                   fileset_stat_ptr[i].fsinfo_path, 
                   fileset_stat_ptr[i].sum_size, 
                   fileset_stat_ptr[i].fileset_name); 
            fprintf(outfd, "Unable to truncate %s to %zu in namespace %s\n",
                   fileset_stat_ptr[i].fsinfo_path, 
                   fileset_stat_ptr[i].sum_size, 
                   fileset_stat_ptr[i].fileset_name); 
         }
         else {
            LOG(LOG_INFO, "Truncated file %s to size %zu\n", 
                fileset_stat_ptr[i].fsinfo_path, 
                fileset_stat_ptr[i].sum_size);
         }
      }
   }
   return 0;
}



/****************************************************************************** 
 * Name: update_type 
 *
 * This function keeps a running count of the object types for final log 
 * reporting 
 * ***************************************************************************/
void update_type(MarFS_XattrPost * xattr_post, 
                 Fileset_Stats * fileset_stat_ptr, 
                 int index)
{
   switch(xattr_post->obj_type) {
      case OBJ_UNI :
         fileset_stat_ptr[index].obj_type.uni_count +=1;
         break;
      case OBJ_MULTI :
         fileset_stat_ptr[index].obj_type.multi_count +=1;
         break;
      case OBJ_PACKED :
         fileset_stat_ptr[index].obj_type.packed_count +=1;
         break;
      default:
         fprintf(stderr, "obj_type undefined: %d\n", xattr_post->obj_type);
   }
}

/****************************************************************************** 
 * Name:  lookup_fileset_path
 *
 * This function uses the md_path_ptr (path to gpfs meta data file) and adds
 * the ".path" extension so that this function cat determine the fileset
 * name (namespace) by reading the *.path file

*** ***************************************************************************/
int lookup_fileset_path(Fileset_Stats *fileset_stat_ptr, size_t rec_count, 
                        int *trash_index, char *md_path_ptr)
{
  FILE *pipe_cat;
  int i, index = -1;
  
  char path_file[MAX_PATH_LENGTH];  
  char cat_command[MAX_PATH_LENGTH];
  char path[MAX_PATH_LENGTH];

   // Add .path to the filename and cat the file to get path
   sprintf(path_file,"%s.path", md_path_ptr);
   sprintf(cat_command,"cat %s", path_file);
   if ((pipe_cat = popen(cat_command,"r")) == NULL) {
      fprintf(stderr, "No path file found\n");
      return(-1);
   }
   fgets(path, MAX_PATH_LENGTH, pipe_cat);
   if (pclose(pipe_cat) == -1) {
     fprintf(stderr, "Error closing .path pipe\n");
   }
   //path variable  now contains original path from .path file
   //now iterate throuh filesets and see if any of them
   //match in the path
   //This may need additional work if we come up with a
   //standard
   //:


   // search the array of structures for matching fileset name
   //printf("path = %s\n", path);
   for (i = 0; i < rec_count; i++) {
       //printf("AAAAAA %s %s\n", fileset_stat_ptr[i].fileset_name,path);
       //printf("fileset = %s\n", fileset_stat_ptr[i].fileset_name);
       if(strstr(path,fileset_stat_ptr[i].fileset_name) != NULL && index == -1 ) {
          index=i;
       }
       // Get trash index so that quota stats may reflect all trash found
       if(strstr("trash", fileset_stat_ptr[i].fileset_name) != NULL) {
          *trash_index = i;
       }
   } 
   return(index);
}
/******************************************************************************
 * Name read_config
 * This function reads the config file in order to extract a list of 
 * namespaces.  Each namespace is added to an array of namespaces in order
 * to track various information on each of the namespaces.
 * This function also reads the fsinfo path from the config file and assigns
 * it to the corresponsing namespace structure element.
 *
******************************************************************************/
//int read_config(Fileset_Stats *fileset_struct)
Fileset_Stats *read_config(unsigned int *count)
{
   int i = 0;
   //MarFS_Config_Ptr marfs_config;
   MarFS_Namespace_Ptr namespacePtr;
   Fileset_Stats *fileset_stat_ptr = NULL;
   Fileset_Stats *fileset_struct = NULL;

   // Read the the config
   if ( read_configuration()) {
      fprintf(stderr, "Error Reading MarFS configuration\n");
      return(&fileset_stat_ptr[0]);
   }
   // Iterate through namespaces and malloc structure memory for namespace found
   NSIterator nit = namespace_iterator();
   while (( namespacePtr = namespace_next (&nit)) != NULL ) {
      i++;
      fileset_struct = (Fileset_Stats *) realloc(fileset_stat_ptr, 
                                                 sizeof(Fileset_Stats) * i);
      if (fileset_struct != NULL ) {
         fileset_stat_ptr = fileset_struct;
   //      init_records(fileset_stat_ptr, 1);        
   //      printf("address = %zu\n", fileset_stat_ptr);
      }
      else {
         fprintf(stderr, "Error allocating memory for fileset\n");
         return(&fileset_stat_ptr[0]);
         //return(-1);
      }
      //Initialize structure element namespace and fsinfopath
      strcpy(fileset_stat_ptr[i-1].fileset_name, namespacePtr->name);
      strcpy(fileset_stat_ptr[i-1].fsinfo_path, namespacePtr->fsinfo_path);
      LOG(LOG_INFO, "fileset name = %s index = %d\n", fileset_stat_ptr[i-1].fileset_name, i);
      LOG(LOG_INFO, "fsinfo_path  = %s\n", fileset_stat_ptr[i-1].fsinfo_path);
   } 
   // Now add one more structure entry for trash
   i++;
   if ((fileset_struct = (Fileset_Stats *) realloc(fileset_stat_ptr, 
                                           sizeof(Fileset_Stats) * i)) == NULL) {
      fprintf(stderr, "Error allocating memory for fileset\n");
   }
   else { 
      fileset_stat_ptr = fileset_struct;
      strcpy(fileset_stat_ptr[i-1].fileset_name, "trash");
   }
   init_records(fileset_stat_ptr, i);        
   *count = i;
   return(&fileset_stat_ptr[0]);
}
