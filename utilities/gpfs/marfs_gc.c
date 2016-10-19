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
#include "marfs_configuration.h"
#include "common.h"
#include "utilities_common.h"

/******************************************************************************
* This program scans the inodes looking specifically at trash filesets.  It will
* determine if post xattr and gc.path defined and if so, gets the objid xattr
* and deletes those objects.  The gc.path file is also deleted.
*
******************************************************************************/

char    *ProgName;

/*****************************************************************************
Name: main

*****************************************************************************/

int main(int argc, char **argv) {
   //FILE *outfd;
   //FILE *packedfd;
   char *outf = NULL;
   char *rdir = NULL;
   char *packed_log = NULL;
   char *aws_user_name = NULL;
   char packed_filename[MAX_PACKED_NAME_SIZE];
   //unsigned int uid = 0;
   int fileset_id = -1;
   int  c;
   unsigned int fileset_count = 1;
   extern char *optarg;
   unsigned int time_threshold_sec=0;
   unsigned int delete_flag = 0;
 
   Fileset_Info *fileset_info_ptr;

   if ((ProgName = strrchr(argv[0],'/')) == NULL)
      ProgName = argv[0];
   else
      ProgName++;

   while ((c=getopt(argc,argv,"d:t:p:hno:u:")) != EOF) {
      switch (c) {
         case 'd': rdir = optarg; break;
         case 'o': outf = optarg; break;
         case 't': time_threshold_sec=atoi(optarg) * DAY_SECONDS; break;
         case 'p': packed_log = optarg; break;
         case 'u': aws_user_name = optarg; break;
         case 'n': delete_flag = 1; break;
         case 'h': print_usage();
         default:
            exit(0);
      }
   }
   
   if (rdir == NULL || 
       outf == NULL || 
       aws_user_name == NULL ) {

      fprintf(stderr,"%s: no directory (-d) or output file name (-o) or\
 aws user name (-u) specified\n\n",ProgName);
      print_usage();
      exit(1);
   }

   // Structure contains info about output log and temproray packed list file 
   // for trashing packed type objects
   File_Info file_stat_info;
   File_Info *file_status = &file_stat_info;

   // Read the configuation file
   if (read_configuration()) { 
      fprintf(stderr, "Error Reading MarFS configuration file\n");
      return(-1);
   }

   // must call validate_configuration. this is where the mdal is
   // actually actually_initialized.
   if(validate_configuration()) {
      fprintf(stderr, "MarFS configuration not valid\n");
      return(-1);
   }

   // Create structure containing fileset information
   fileset_info_ptr = (Fileset_Info *) malloc(sizeof(*fileset_info_ptr));
   if (fileset_info_ptr == NULL ) {
      fprintf(stderr,"Memory allocation failed\n");
      exit(1);
   }
   init_records(fileset_info_ptr, fileset_count);
   aws_init();

   if (packed_log == NULL) {
      strcpy(packed_filename,"./tmp_packed_log");
      packed_log = packed_filename;
   }

   // open associated log file and packed log file
   if ((file_status->outfd = fopen(outf,"w")) == NULL){ 
      fprintf(stderr, "Failed to open %s\n", outf);
      exit(1);
   }
   if ((file_status->packedfd = fopen(packed_log, "w"))==NULL){ 
      fprintf(stderr, "Failed to open %s\n", packed_log);
      exit(1);
   }
   strcpy(file_status->packed_filename, packed_log);
   file_status->is_packed=0;
   file_status->no_delete = delete_flag;

   // Configure aws for the user specified on command line
   aws_read_config(aws_user_name);

   read_inodes(rdir,file_status,fileset_id,fileset_info_ptr,
               fileset_count,time_threshold_sec);

   if (file_status->is_packed) {
      fclose(file_status->packedfd);
      process_packed(file_status);
   }
   fclose(file_status->outfd);

   //TEMP TEMP TEMP FOR DEBUG
   //unlink(packed_log);
   return (0);   
}

/***************************************************************************** 
Name: init_records 

*****************************************************************************/
void init_records(Fileset_Info *fileset_info_buf, unsigned int record_count)
{
   memset(fileset_info_buf, 0, (size_t)record_count * sizeof(Fileset_Info)); 
}

/***************************************************************************** 
Name: print_usage 

*****************************************************************************/
void print_usage()
{
   fprintf(stderr,"Usage: %s -d gpfs_path -o ouput_log_file -u aws_user_name\
 [-p packed_tmp_file] [-t time_threshold-days] [-n] [-h] \n\n",ProgName);
   fprintf(stderr, "where -n = no delete and -h = help\n\n");
}



/***************************************************************************** 
Name:  get_xattr_value

This function, given the name of the desired xattr, returns a 
ptr to the structure element containing that xattr value

*****************************************************************************/
int get_xattr_value(struct     marfs_xattr *xattr_ptr, 
                    const char *desired_xattr, 
                    int        cnt) {

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



      //Determine if found a marfs_xattr by comparing our list of xattrs
      //to what the scan has found
      for ( i=0; i < max_xattr_count; i++) {
         if (!strcmp(nameP, marfs_xattr[i])) {
            strcpy(xattr_ptr->xattr_name, nameP);
            xattr_count++;
            desired_xattr = 1;
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
      if (desired_xattr) { 
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
         desired_xattr = 0;
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
               int                  terminate) 
{
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

The functions uses the gpfs API inode scan capabilities to gather information
about the targe file system.  In this case, we are scanning filesets to find
files that have MarFS-based xattrs that give information about the files and
objects that are to be deleted.  Once the files and objects that are to be
deleted are found, files and objects are deleted based on the type of object.
The method for trashing based on object type is given below:

UNI - single gpfs file and object are deleted
MULTI = singe gpfs file and multiple objects are deleted 
PACKED = scan makes a list (>file) that is post processed to determine if 
         all gpfs files exist for the packed object.  If so, delete all
 

*****************************************************************************/
int read_inodes(const char *fnameP, 
                File_Info *file_info_ptr, 
                int fileset_id,
                Fileset_Info *fileset_info_ptr, 
                size_t rec_count, 
                unsigned int day_seconds) {


   int rc = 0;
   const gpfs_iattr_t *iattrP;
   const char *xattrBP;
   unsigned int xattr_len; 
   register gpfs_iscan_t *iscanP = NULL;
   gpfs_fssnap_handle_t *fsP = NULL;
   struct marfs_xattr mar_xattrs[MAX_MARFS_XATTR];
   struct marfs_xattr *xattr_ptr = mar_xattrs;
   int xattr_count;

#if 0
   MarFS_XattrPre  pre_struct;
   MarFS_XattrPre* pre = &pre_struct;

   MarFS_XattrPost post_struct;
   MarFS_XattrPre* post = &post_struct;
#else
   MarFS_FileHandle fh;
   memset(&fh, 0, sizeof(MarFS_FileHandle));

   MarFS_XattrPre*  pre  = &fh.info.pre;
   MarFS_XattrPost* post = &fh.info.post;
#endif

   // initialize a list of known MarFS xattrs, used by stat_xattrs(), etc.
   init_xattr_specs();

   // Defined xattrs as an array of const char strings with defined indexs
   // Change MARFS_QUOTA_XATTR_CNT in marfs_gc.h if the number of xattrs
   // changes.  Order matters here so make sure array elements match
   // index definitions
   int marfs_xattr_cnt = MARFS_GC_XATTR_CNT;
   const char *marfs_xattrs[] = {"user.marfs_post",
                                 "user.marfs_objid",
                                 "user.marfs_restart"};

   int post_index=0;
   int objid_index=1;
   int restart_index=2;
   //
   //

   int trash_status;

   int early_exit =0;
   int xattr_index;
   char *md_path_ptr;
   const struct stat* st = NULL;


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
   if ((iscanP = gpfs_open_inodescan_with_xattrs(fsP, NULL, -1, NULL, NULL)) 
           == NULL) {
      rc = errno;
      fprintf(stderr, "%s: line %d - gpfs_open_inodescan: %s\n", 
      ProgName,__LINE__,strerror(rc));
      early_exit = 1;
      clean_exit(file_info_ptr->outfd, iscanP, fsP, early_exit);
   }


   while (1) {
      rc = gpfs_next_inode_with_xattrs(iscanP,
                                       0x7FFFFFFF,
                                       &iattrP, 
                                       &xattrBP,
                                       &xattr_len);
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

      // Print out inode values to output file
      // This is handy for debug at the moment
      if (iattrP->ia_inode != 3) {	/* skip the root inode */
 
         // This log commented out due to amount of inodes dumped
         //LOG(LOG_INFO,"%u|%lld|%lld|%d|%d|%u|%u|%u|%u|%u|%lld|%d\n",
         //   iattrP->ia_inode, iattrP->ia_size,iattrP->ia_blocks,
         //   iattrP->ia_nlink,iattrP->ia_filesetid,
         //   iattrP->ia_uid, iattrP->ia_gid, iattrP->ia_mode,
         //   iattrP->ia_atime.tv_sec,iattrP->ia_mtime.tv_sec, 
         //   iattrP->ia_blocks, iattrP->ia_xperm );

/**********
 * Removing this for now - no need to verify that this is the trash fileset 
         gpfs_igetfilesetname(iscanP, 
                              iattrP->ia_filesetid, 
                              &fileset_name_buffer, 
                              MARFS_MAX_NAMESPACE_NAME); 
         //if (!strcmp(fileset_name_buffer,fileset_info_ptr[0].fileset_name)) {
         if (!strcmp(fileset_name_buffer,fileset_info_ptr->fileset_name)) {
************/



         // Do we have extended attributes?
         // This will be modified as time goes on - what xattrs do we care about
         if (iattrP->ia_xperm & GPFS_IAXPERM_XATTR && xattr_len >0 ) {
            // Got ahead and get xattrs then deterimine if it is an 
            // an actual xattr we are looking for.  If so,
            // check if it specifies the file is trash.
            if ((xattr_count = get_xattrs(iscanP, xattrBP, xattr_len, 
                                          marfs_xattrs, marfs_xattr_cnt, 
                                          &mar_xattrs[0], 
                                          file_info_ptr->outfd)) > 0) {

               // marfs_xattrs has a list of xattrs found.  Look for POST
               // first, because that tells us if file is in the trash.
               // POST xattr for files in the trash includes the md_path.
               if ((xattr_index=get_xattr_value(&mar_xattrs[0], 
                    marfs_xattrs[post_index], xattr_count)) != -1 ) { 
                    xattr_ptr = &mar_xattrs[xattr_index];
                  LOG(LOG_INFO,"post xattr name = %s value = %s \
                      count = %d index=%d\n", xattr_ptr->xattr_name, \
                      xattr_ptr->xattr_value, xattr_count,xattr_index);
                  if ((str_2_post(post, xattr_ptr->xattr_value, 1))) {
                      fprintf(stderr,"Error parsing  post xattr for inode %d\n",
                      iattrP->ia_inode);
                      continue;
                  }
               }
               else {
                  fprintf(stderr,"Error:  Not finding post xattr for inode %d\n", 
                          iattrP->ia_inode);
               }
               LOG(LOG_INFO, "found post chunk info bytes %zu\n", 
                   post->chunk_info_bytes);

               // Is this trash?
               if (post->flags & POST_TRASH){
                  time_t now = time(0);
                   
                  // Check if older than X days (specified by user arg)
                  if (now-day_seconds > iattrP->ia_atime.tv_sec) {
                     LOG(LOG_INFO, "Found trash\n");
                     md_path_ptr = &post->md_path[0];

                     // Get OBJID xattr (aka "pre")
                     if ((xattr_index=get_xattr_value(&mar_xattrs[0], 
                          marfs_xattrs[objid_index], xattr_count)) != -1) { 
                          xattr_ptr = &mar_xattrs[xattr_index];
                        LOG(LOG_INFO, "objid xattr name = %s xattr_value =%s\n",
                            xattr_ptr->xattr_name, xattr_ptr->xattr_value);
                        LOG(LOG_INFO, "remove file: %s  remove object:  %s\n",
                            md_path_ptr, xattr_ptr->xattr_value); 

                        // Going to get the repo name now from the objid xattr
                        // To do this, must call marfs str_2_pre to parse out
                        // the bucket name which include repo name
                        //fprintf(stderr,"going to call str_2_pre %s\n",xattr_ptr->xattr_value);
                        if (str_2_pre(pre, xattr_ptr->xattr_value, st) != 0) {
                           LOG(LOG_ERR, "str_2_pre failed\n");
                           continue;
                        }

                        // Now check if we have RESTART xattr which may imply
                        // OBJ_FUSE or OBJ_Nto1 which requires 
                        // special handling in dump_trash
                        file_info_ptr->restart_found = 0;
                        if ((xattr_index=get_xattr_value(&mar_xattrs[0],
                             marfs_xattrs[restart_index], xattr_count)) != -1 ) {
                                file_info_ptr->restart_found = 1;
                        }

                        check_security_access(pre);

                        // Deterimine if object type is packed.  If so we 
                        // must complete scan to determine if all files 
                        // exist for the object.  So, just add the particulars to a file,
                        // which we will later review.
                        //
                        // TBD: update_pre() is only needed to compute the
                        //      randomized host, which is unused until we
                        //      actually delete.  Might be better to skip
                        //      writing this field into the file, and let
                        //      process_packed() compute it as needed.
                        if (post->obj_type == OBJ_PACKED) {
                           update_pre(pre);
                           fprintf(file_info_ptr->packedfd,"%s %zu %s\n", 
                                   xattr_ptr->xattr_value, post->chunks, md_path_ptr );
                           file_info_ptr->is_packed = 1;
                        }


                        // Not checking return because log has error message
                        // and want to keep running even if errors exists on 
                        // certain objects or files
                        else {
                           fake_filehandle_for_delete_inits(&fh);
                           trash_status = dump_trash(&fh, xattr_ptr, file_info_ptr);
                        } // endif dump trash
                     } // endif objid xattr 
                  } // endif days old check
               } // endif post xattr specifies trash
            } // endif get xattrs
         } // endif extended attributes
/******
 * Removing this because no need for checking if trash fileset
         }
******/
      }
   } // endwhile
   clean_exit(file_info_ptr->outfd, iscanP, fsP, early_exit);
   return(rc);
}

/***************************************************************************** 
Name: dump_trash 

 This function deletes the object file as well as gpfs metadata files
 This is only called for non-packed MarFS files.
*****************************************************************************/
int dump_trash(MarFS_FileHandle   *fh,
               struct marfs_xattr *xattr_ptr, // object-ID
               File_Info          *file_info_ptr)
{
   int return_value =0;
   int i;
   int delete_obj_status;
   int multi_flag = 0;

   // PathInfo        *info        = &fh->info;
   MarFS_XattrPre  *pre_ptr     = &fh->info.pre;
   MarFS_XattrPost *post_ptr    = &fh->info.post;
   char            *md_path_ptr =  fh->info.post.md_path;


   // This is the case where a file in trash has a RESTART xattr and the
   // PRE obj_type = N to 1.  It exists because pftool did not complete the N
   // to 1 creation.  It's possible that only a few chunks out of a huge
   // potential number were successfully written.  We determine which
   // chunks were written and delete only those associated objects.
   //
   // NOTE: The obj-type in the POST xattr is the final correct
   //     object-type, but it will be MULTI both for the pftool-failure
   //     case, and the fuse-failure case.  fuse would only write chunks
   //     sequentially.  The pre xattr lets us distinguish the two cases.
   if (file_info_ptr->restart_found
       && (pre_ptr->obj_type == OBJ_Nto1)) {

#if 0
      // COMMENTED OUT.  This will slow everything down.  We already parsed
      // the xattrs we got from the inode scan.  They are in fh->info.pre,
      // and fh->info.post.  If we call this, it will do a stat() of the MD
      // file, which will hurt performance.

      if (stat_xattrs(info)) {    // parse all xattrs for the MD file
         fprintf(stderr, "stat_xattrs() failed for MD file: '%s'\n", md_path_ptr);
         return -1;
      }
#endif


      // assure the MD is open for reading
      //fh->flags |= FH_READING;
      // 
      // Now call with flag (0) to open for reading
      if (open_md(fh, 0)) {
         fprintf(stderr, "open_md() failed for MD file: '%s'\n", md_path_ptr);
         return -1;
      }
      
      // read chunkinfo and create object name so that it can be deleted
      MultiChunkInfo chunk_info;
      while (! read_chunkinfo(fh, &chunk_info)) {
         if (chunk_info.chunk_data_bytes != 0 ) {

            pre_ptr->chunk_no = chunk_info.chunk_no;
            update_pre(pre_ptr);
            update_url(&fh->os, &fh->info);
            if ((delete_obj_status = delete_object(fh, file_info_ptr, multi_flag))) {
               fprintf(file_info_ptr->outfd,
                       "s3_delete error (HTTP Code: %d) on object %s\n",
                       delete_obj_status, fh->os.url); // xattr_ptr->xattr_value
               return_value = -1;
            }
            else if (!file_info_ptr->no_delete) {
               fprintf(file_info_ptr->outfd, "deleted object %s\n",
                       fh->os.url); // object_name
            }
         }
      }
      close_md(fh);
   }
   // If multi type file then delete all objects associated with file
   else if (post_ptr->obj_type == OBJ_MULTI) {
      for (i=0; i < post_ptr->chunks; i++ ) {
         multi_flag = 1;
         pre_ptr->chunk_no = i;
         update_pre(pre_ptr);
         update_url(&fh->os, &fh->info);
         if ((delete_obj_status = delete_object(fh, file_info_ptr, multi_flag))) {
            fprintf(file_info_ptr->outfd,
                    "s3_delete error (HTTP Code: %d) on object %s\n",
                    delete_obj_status, fh->os.url); // xattr_ptr->xattr_value
            return_value = -1;
         }
         else {
            if (!file_info_ptr->no_delete)
               fprintf(file_info_ptr->outfd, "deleted object %s\n",
                       fh->os.url);
         }
      }
   }

   // else UNI BUT NEED to implemented other formats as developed 
   else if (post_ptr->obj_type == OBJ_UNI) {
      update_pre(pre_ptr);
      update_url(&fh->os, &fh->info);
      if ((delete_obj_status = delete_object(fh, file_info_ptr, multi_flag))) {
         fprintf(file_info_ptr->outfd,
                 "s3_delete error (HTTP Code:  %d) on object %s\n",
                 delete_obj_status, fh->os.url); // xattr_ptr->xattr_value
         return_value = -1;
      }
      else if (!file_info_ptr->no_delete) {
            // update_url(&fh->os, &fh->info); // redundant
            fprintf(file_info_ptr->outfd, "deleted object %s\n",
                    fh->os.url); // object_name
      }
   }

   // Need to implement semi-direct here.  In this case the obj_type will not
   // have that information.  I will have to rely on the config parser to 
   // determine the protocol from the RepoAccessProto structure.  I would 
   // not delete objects anymore, I would delete files so hopefully I could
   // use delete file as is.  

   // Delete trash files
   // Only delete if no error deleting object
   if (!return_value) {
      delete_file(md_path_ptr, file_info_ptr); 
   }
   else {
      fprintf(file_info_ptr->outfd, "Object error no file deletion of %s\n", md_path_ptr);
   }
   return(return_value);
}

/***************************************************************************** 
Name: delete_object 

 This function deletes the object whose ID is in fh->pre and returns -1 if
 error, 0 if successful
*****************************************************************************/
int delete_object(MarFS_FileHandle *fh,
                  File_Info        *file_info_ptr,
                  int               is_multi)
{
   //
   int             return_val = 0;
   MarFS_XattrPre* pre_ptr = &fh->info.pre;
   const char*     object = fh->os.url;
   int             rc;

   print_current_time(file_info_ptr);

   // update_pre(&fh->info.pre);
   // update_url(&fh->os, &fh->info);

   if (file_info_ptr->restart_found && pre_ptr->obj_type == OBJ_Nto1) {
      if ( file_info_ptr->no_delete )
         fprintf(file_info_ptr->outfd,"ID'd incomplete Nto1 multi object %s\n", object);
      // else delete object
      else 
         // s3_return = s3_delete( obj_buffer, object );
         rc = delete_data(fh);
   }
   else if (file_info_ptr->restart_found && pre_ptr->obj_type == OBJ_FUSE) {
      if ( file_info_ptr->no_delete )
         fprintf(file_info_ptr->outfd,"ID'd incomplete FUSE multi object %s\n", object);
      // else delete object
      else 
         // s3_return = s3_delete( obj_buffer, object );
         rc = delete_data(fh);
   }
   else if ( is_multi )
      // Check if no delete just print to log
      if ( file_info_ptr->no_delete )
         fprintf(file_info_ptr->outfd,"ID'd multi object %s\n", object);
      // else delete object
      else 
         // s3_return = s3_delete( obj_buffer, object );
         rc = delete_data(fh);
   else 
      // Check if no delete just print to log
      if ( file_info_ptr->no_delete )
         fprintf(file_info_ptr->outfd,"ID' uni object %s\n", pre_ptr->objid);
      // else delete object
      else 
         // s3_return = s3_delete( obj_buffer, pre_ptr->objid );
         rc = delete_data(fh);
      
   if ( file_info_ptr->no_delete )
      return_val = 0;
   else if (rc)
      // return_val=check_S3_error(s3_return, obj_buffer, S3_DELETE);
      return_val=check_S3_error(fh->os.op_rc, &fh->os.iob, S3_DELETE);

   return(return_val);
}

/***************************************************************************** 
Name: delete_file 

this function deletes the gpfs files assoiated with an object
*****************************************************************************/
int delete_file(char *filename, File_Info *file_info_ptr)
{
   int return_value = 0;
   char path_file[MARFS_MAX_MD_PATH];
   sprintf(path_file,"%s.path",filename);
   print_current_time(file_info_ptr);
  
   // Check if no_delete option so that no files or objects deleted 
   if (file_info_ptr->no_delete) {
      fprintf(file_info_ptr->outfd,"ID'd file %s\n", filename);
      fprintf(file_info_ptr->outfd,"ID'd path file %s\n", path_file);
      return(return_value);
   }

   // Delete files
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

void print_current_time(File_Info *file_info_ptr)
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
The format of the files is space delimited as follows:

OBJECT_NAME TOTAL_FILE_COUNT FILENAME  

This function sorts by object name then proceeds to count how many files
exists for that object_name.  If the files counted equal the total file_count,
the object and files are deleted.  Ohterwise they are left in place and a 
repack utility will be run on the trash directory.
*****************************************************************************/

int process_packed(File_Info *file_info_ptr)
{
   FILE *pipe_cat = NULL;
   FILE *pipe_grep = NULL;
   FILE *pipe_wc = NULL;

   char obj_buf[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
   char file_buf[MARFS_MAX_MD_PATH];
   char wc_buf[64];
   char objid[MARFS_MAX_OBJID_SIZE];
   char last_objid[MARFS_MAX_OBJID_SIZE];
   char grep_command[MARFS_MAX_MD_PATH+MAX_PACKED_NAME_SIZE+64];
   char cat_command[MAX_PACKED_NAME_SIZE];
   char wc_command[MAX_PACKED_NAME_SIZE];
   int obj_return;
   int df_return;

   int chunk_count;
   char filename[MARFS_MAX_MD_PATH];
   int count = 0;
   int multi_flag = 0;
   int wc_count;
   
   // create command to cat and sort the list of objects that are packed
   sprintf(cat_command, "cat %s | sort", file_info_ptr->packed_filename);
   
   // open STREAM for cat
   if (( pipe_cat = popen(cat_command, "r")) == NULL) {
      fprintf(stderr, "Error with popen\n");
      return(-1); 
   }
   // Get the first line and sscanf into object_name, chunk_cnt, and filename
   fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat);

   sscanf(obj_buf,"%s,%d,%s", objid, &chunk_count, filename);

   // Keep track of of when objid name changes 
   // count is used to see how many files have been counted
   // so that a check of whether all files are accounted for in the packed
   // object
   strcpy(last_objid, objid);
   count++;

   // In a loop get all lines from the file
   // Example line (with 'xx' instead of IP-addr octets, and altered MD paths):
   //
   // proxy1/bparc/ver.001_001/ns.development/P___/inode.0000016572/md_ctime.20160526_124320-0600_1/obj_ctime.20160526_124320-0600_1/unq.0/chnksz.40000000/chnkno.0 403
   // /gpfs/fileset/trash/development.0/8/7/3/uni.255.trash_0000016873_20160526_125809-0600_1
   //
   while(fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat)) {
      // if objid the same - keep counting files
      sscanf(obj_buf,"%s %d %s", objid, &chunk_count, filename);
      if (!strcmp(last_objid,objid) || chunk_count==1) {
         count++;
         // printf("count: %d chunk_count: %d\n", count, chunk_count);
         // If file count == chuck count - all files accounted for
         // delete objec
         if (chunk_count == count || chunk_count==1) {
            // So make sure that the chunk_count (from xattr) matches the number
            // of files found 
            sprintf(wc_command, "grep %s %s | wc -l", objid, file_info_ptr->packed_filename);
            if (( pipe_wc = popen(wc_command, "r")) == NULL) {
               fprintf(stderr, "Error with popen\n");
               return(-1); 
            }
            while(fgets(wc_buf,1024, pipe_wc)) {
               sscanf(wc_buf, "%d", &wc_count);
            }
            if (wc_count != chunk_count) {
               fprintf(stderr, "object %s has %d files while chunk count is %d\n",
                       objid, wc_count, chunk_count);
               continue;
            }
            pclose(pipe_wc);

            // If we get here, counts are matching up
            // Go ahead and start deleting
            MarFS_FileHandle fh;
            if (fake_filehandle_for_delete(&fh, objid, filename)) {
               fprintf(file_info_ptr->outfd,
                       "failed to create filehandle for %s %s\n",
                       objid, filename);
               continue;
            }

            update_pre(&fh.info.pre);
            update_url(&fh.os, &fh.info);
            // Delete object first
            if ((obj_return = delete_object(&fh, file_info_ptr, multi_flag)) != 0) 
               fprintf(file_info_ptr->outfd, "s3_delete error (HTTP Code: \
                       %d) on object %s\n", obj_return, objid);

            else if ( file_info_ptr->no_delete) 
               fprintf(file_info_ptr->outfd, "ID'd packed object %s\n", objid);

            // Get metafile name from tmp_packed_log and delelte 
            else 
               fprintf(file_info_ptr->outfd, "deleted object %s\n", objid);

            sprintf(grep_command,"grep %s %s | awk '{print $3}' ", 
                    objid, file_info_ptr->packed_filename);
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

   if (pclose(pipe_cat) == -1) {
      fprintf(stderr, "Error closing cat pipe in process_packed\n");
      return(-1);
   }
   else if (pipe_grep != NULL) {
      if (pclose(pipe_grep) == -1) {
         fprintf(stderr, "Error closing grep pipe in process_packed\n");
         return(-1);
      }
      else
         return(0);
   }
   else 
      return(0);
}
/******************************************************************************
 * Name read_config_gc
 * This function reads the config file in order to extract the object hostname
 * associated with the current gpfs file (from the inode scan) 
 *
******************************************************************************/
int read_config_gc(Fileset_Info *fileset_info_ptr)
{
   MarFS_Repo_Ptr repoPtr;

   //Find the correct repo so that the hostname can be determined
   LOG(LOG_INFO, "fileset repo name = %s\n", fileset_info_ptr->repo_name);
   if ((repoPtr = find_repo_by_name(fileset_info_ptr->repo_name)) == NULL) {
      fprintf(stderr, "Repo %s not found in configuration\n", 
              fileset_info_ptr->repo_name);
      return(-1);
   }
   else {
      strcpy(fileset_info_ptr->host, repoPtr->host);
      LOG(LOG_INFO, "fileset name = %s/n", fileset_info_ptr->host);
      return(0);
   }
}
