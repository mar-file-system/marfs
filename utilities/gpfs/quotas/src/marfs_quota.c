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
*  determine extended attributes that we care about
*     passed in as args or hard coded?
*  determine arguments to main 
*  inode total count
*  block (512 bytes) held by file
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
   unsigned int uid = 0;
   int  c;
   extern char *optarg;
   struct histogram size_histo;
   struct histogram *histo_size_ptr = &size_histo;


   if ((ProgName = strrchr(argv[0],'/')) == NULL)
      ProgName = argv[0];
   else
      ProgName++;

   while ((c=getopt(argc,argv,"d:ho:u:")) != EOF) {
      switch (c) {
         case 'd': rdir = optarg; break;
         case 'o': outf = optarg; break;
         case 'u': uid = atoi(optarg); break;
         case 'h': print_usage();
         default:
            exit(0);
      }
   }

   if (rdir == NULL || outf == NULL) {
      fprintf(stderr,"%s: no directory (-d) or output file name (-o) specified\n",ProgName);
      exit(1);
   }
   histo_size_ptr->small_count=0;
   histo_size_ptr->medium_count=0;
   histo_size_ptr->large_count=0;
   outfd = fopen(outf,"w");

   ec = read_inodes(rdir,outfd, histo_size_ptr,uid);
   fprintf(outfd,"small files = %llu\n medium files = %llu\n large_files = %llu\n",
          histo_size_ptr->small_count, histo_size_ptr->medium_count, histo_size_ptr->large_count);
   exit(ec);
}



/***************************************************************************** 
Name: print_usage 

*****************************************************************************/
void print_usage()
{
   fprintf(stderr,"Usage: %s -d gpfs_path -o ouput_log_file [-u uid]\n",ProgName);
}

/***************************************************************************** 
Name: fill_size_histo 

This function counts file sizes based on small, medium, and large for 
the purposes of displaying a size histogram.
*****************************************************************************/

static void fill_size_histo(const gpfs_iattr_t *iattrP, struct histogram *histogram_ptr)
{
   if (iattrP->ia_size < SMALL_FILE_MAX) 
     histogram_ptr->small_count+= 1;
   
   else if (iattrP->ia_size < MEDIUM_FILE_MAX) 
     histogram_ptr->medium_count+= 1;
   else
     histogram_ptr->large_count+= 1;
}

/***************************************************************************** 
Name:  get_attr_value

This function, given the name of the attribute returns the associated value.

*****************************************************************************/
int get_xattr_value(gpfs_iscan_t *iscanP,
                 const char *xattrP,
                 unsigned int xattrLen,
                 char * desired_xattr,
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

      // keep track of how many xattrs found 
      xattr_count++;
      strcpy(xattr_ptr->xattr_name, nameP);

/******* NOT SURE ABOUT THIS JUST YET
      Eliminate gpfs.dmapi attributes for comparision
        Internal dmapi attributes are filtered from snapshots 
      if (printCompare > 1)
      {
         if (strncmp(nameP, "gpfs.dmapi.", sizeof("gpfs.dmapi.")-1) == 0)
            continue;
      }
***********/
    
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

         for (i = 0; i < valueLen; i++) {
            if (printable) {
              xattr_ptr->xattr_value[i] = valueP[i]; 
            }
         }
         xattr_ptr->xattr_value[valueLen] = '\0'; 
      }
      xattr_ptr++;
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
int read_inodes(const char *fnameP, FILE *outfd, struct histogram *histo_ptr, unsigned int uid) {
   int rc = 0;
   const gpfs_iattr_t *iattrP;
   const char *xattrBP;
   unsigned int xattr_len; 
   register gpfs_iscan_t *iscanP = NULL;
   gpfs_fssnap_handle_t *fsP = NULL;
//  int printCompare;
   unsigned long long int sum_size = 0;
   struct marfs_xattr mar_xattrs[MAX_MARFS_XATTR];
   struct marfs_xattr *xattr_ptr = mar_xattrs;
   int i, xattr_count;
  
   int early_exit =0;

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

      // If userid is specified then only look for those inodes and xattrs
      if (uid > 0) {
         if (uid != iattrP->ia_uid){
            continue; 
         }
      }

      // Print out inode values to output file
      // This is handy for debug at the moment
      if (iattrP->ia_inode != 3) {	/* skip the root inode */
         fprintf(outfd,"%u|%lld|%lld|%d|%u|%u|%u|%u|%u|%lld|%d\n",
         iattrP->ia_inode, iattrP->ia_size,iattrP->ia_blocks,iattrP->ia_nlink,
         iattrP->ia_uid, iattrP->ia_gid, iattrP->ia_mode,
         iattrP->ia_atime.tv_sec,iattrP->ia_mtime.tv_sec, iattrP->ia_blocks, iattrP->ia_xperm );

         sum_size += iattrP->ia_size;
         fill_size_histo(iattrP, histo_ptr);
      }

      // Do we have extended attributes?
      // This will be modified as time goes on - what xattrs do we care about
      if (iattrP->ia_xperm == 2 && xattr_len >0 ) {
         xattr_ptr = &mar_xattrs[0];
         if ((xattr_count = get_xattr_value(iscanP, xattrBP, xattr_len, "user.a", xattr_ptr)) > 0) {
            xattr_ptr = &mar_xattrs[0];
            for (i = 0; i < xattr_count; i++) {
               fprintf(outfd,"xattr name:   %s   xattr value:  %s\n", xattr_ptr->xattr_name, xattr_ptr->xattr_value); 
               xattr_ptr++;
            }
         }
      }
   } // endwhile
   fprintf(outfd,"file size sum  = %llu\n", sum_size);
   clean_exit(outfd, iscanP, fsP, early_exit);
   return(rc);
}

