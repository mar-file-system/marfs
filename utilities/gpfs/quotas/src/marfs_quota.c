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
   char  fileset_name[] = "root";
   char * indv_fileset_name; 
   int i;


   if ((ProgName = strrchr(argv[0],'/')) == NULL)
      ProgName = argv[0];
   else
      ProgName++;

   while ((c=getopt(argc,argv,"d:f:ho:u:")) != EOF) {
      switch (c) {
         case 'd': rdir = optarg; break;
         case 'f': fileset_id = atoi(optarg); break;
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
   ec = read_inodes(rdir, outfd, fileset_id, fileset_stat_ptr, fileset_count);
//   fprintf(outfd,"small files = %llu\n medium files = %llu\n large_files = %llu\n",
//          histo_size_ptr->small_count, histo_size_ptr->medium_count, histo_size_ptr->large_count);
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
Name:  get_attr_value

This function, given the name of the attribute returns the associated value.

*****************************************************************************/
int get_xattr_value(gpfs_iscan_t *iscanP,
                 const char *xattrP,
                 unsigned int xattrLen,
                 const char * desired_xattr,
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
      if (!strcmp(nameP, desired_xattr)) {
         strcpy(xattr_ptr->xattr_name, nameP);
          xattr_count++;
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
   int last_struct_index = -1;
   unsigned int struct_index;
   unsigned int last_fileset_id = -1;

   //const char *xattr_objid_name = "user.marfs_objid";
   const char *xattr_post_name = "user.marfs_post";
   MarFS_XattrPost post;
   //const char *xattr_post_name = "user.a";
  
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
            struct_index = lookup_fileset(fileset_stat_ptr,rec_count,fileset_name_buffer);
            if (struct_index == -1) 
               continue;
            last_struct_index = struct_index;
            last_fileset_id = iattrP->ia_filesetid;
         }
         fileset_stat_ptr[last_struct_index].sum_size+=iattrP->ia_size;
         fileset_stat_ptr[last_struct_index].sum_file_count+=1;
         if (debug) 
            printf("%d size = %llu file size sum  = %llu\n", last_struct_index,iattrP->ia_size,fileset_stat_ptr[last_struct_index].sum_size);
         fill_size_histo(iattrP, fileset_stat_ptr, last_fileset_id); 

         // Do we have extended attributes?
         // This will be modified as time goes on - what xattrs do we care about
         
         if (iattrP->ia_xperm == 2 && xattr_len >0 ) {
            xattr_ptr = &mar_xattrs[0];
            if ((xattr_count = get_xattr_value(iscanP, xattrBP, xattr_len, xattr_post_name, xattr_ptr)) > 0) {
               xattr_ptr = &mar_xattrs[0];
               str_2_post(&post, xattr_ptr); 
               // Talk to Jeff about this filespace used not in post xattr
               if (debug) 
                  printf("found post chunk info bytes %zu\n", post.chunk_info_bytes);
               fileset_stat_ptr[last_struct_index].sum_filespace_used += post.chunk_info_bytes;
               if (!strcmp(post.gc_path, "0")){
                  if (debug) 
                     printf("gc_path is NULL\n");
               } 
               else {
                  fileset_stat_ptr[last_struct_index].sum_trash += iattrP->ia_size;
                  fileset_stat_ptr[last_struct_index].adjusted_size = fileset_stat_ptr[last_struct_index].sum_size - iattrP->ia_size; 
               }
            }
         }
      }
   } // endwhile
   write_fsinfo(outfd, fileset_stat_ptr, rec_count);
   clean_exit(outfd, iscanP, fsP, early_exit);
   return(rc);
}

/***************************************************************************** 
Name: lookup_fileset 

This function attempts to match the passed in fileset name to the structure
element (array of structures ) containing that same fileset name.  The index 
for the matching element is returned. 
*****************************************************************************/
int lookup_fileset(fileset_stat *fileset_stat_ptr, size_t rec_count,char *inode_fileset)
{
   int index = 0;
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
   else if (scanf_size != 8) {
      errno = EINVAL;
      return -1;            /* ?? */
   }
   return 0;
}


/***************************************************************************** 
Name: write_fsinfo 

This function prints various fileset information to the fs_info file

*****************************************************************************/
void write_fsinfo(FILE* outfd, fileset_stat* fileset_stat_ptr, size_t rec_count)
{
   size_t i;
 
   for (i=0; i < rec_count; i++) {
      fprintf(outfd,"[%s]\n", fileset_stat_ptr[i].fileset_name);
      fprintf(outfd,"total_file_count:  %zu\n", fileset_stat_ptr[i].sum_file_count);
      fprintf(outfd,"total_size:  %llu\n", fileset_stat_ptr[i].sum_size);
      fprintf(outfd,"trash_size:  %zu\n", fileset_stat_ptr[i].sum_trash);
      fprintf(outfd,"adjusted_size:  %zu\n", fileset_stat_ptr[i].adjusted_size);
   }
}





