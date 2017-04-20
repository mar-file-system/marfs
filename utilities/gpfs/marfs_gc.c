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

typedef struct payload_file_struct {
   char* md_path;
   size_t size;
   time_t md_ctime;
   struct payload_file_struct* next;
} *ht_file;

typedef struct payload_header_struct {
   size_t chunks;
   MarFS_Namespace_Ptr ns;
   time_t md_ctime;
   struct payload_file_struct* files;
   struct payload_file_struct* tail;
} *ht_header;

typedef struct payload_package_struct {
   MarFS_Namespace_Ptr ns;
   size_t chunks;
   char* md_path;
   time_t md_ctime;
} *ht_package;
//
//typedef struct rpayload_object_struct {
//   size_t file_count;
//   size_t obj_size;
//   time_t min_ctime;
//   time_t max_ctime;
//   ht_file files;
//   struct rpayload_object_struct* next;
//} *rt_object;
//
//typedef struct rpayload_header_struct {
//   MarFS_Namespace_Ptr ns;
//   rt_object objects;
//} *rt_header;

typedef struct delete_return_struct {
   unsigned int successes;
   unsigned int failures;
} delete_return;

typedef struct delete_entry_struct {
   File_Info          file_info;
   MarFS_FileHandle   fh;
   struct delete_entry_struct* next;
} delete_entry;

struct delete_queue_struct {
   pthread_mutex_t    queue_lock;
   pthread_cond_t     queue_empty;
   pthread_cond_t     queue_full;
   delete_entry*      head;
   delete_entry*      tail;
   int                num_items;
   int                num_consumers;
   char               work_done; // flag to indicate the producer is done.
   pthread_t*         consumer_threads;
} delete_queue;

Run_Info run_info; //global run settings

// ---- Repack structs ----
//
//typedef struct File_Handles {
//   FILE *outfd;
//   char packed_log[MAX_PATH_LENGTH];
//} File_Handle;
//
//typedef struct obj_files {
//  char filename[MAX_PATH_LENGTH];
//  size_t original_offset;
//  size_t size;
////  size_t new_offset;
//  char pre_xattr[MARFS_MAX_XATTR_SIZE];
//  MarFS_FileHandle *fh;
//  struct obj_files *next;
//} obj_files;
//
//typedef struct repack_objects {
//  size_t pack_count;
////  size_t chunk_count;
////  char objid[MARFS_MAX_XATTR_SIZE];
//  char new_objid[MARFS_MAX_XATTR_SIZE];
//  repack_file files;
//  struct repack_objects *next;
//} repack_objects;
//
//
//int find_repack_objects(File_Handles *file_info, repack_objects **objects);
//int pack_objects(File_Handles *file_info, repack_objects *objects);
//int update_meta(File_Handles *file_info, repack_objects *objects);
//void get_marfs_path(char * patht, char *marfs);
//void free_objects(repack_objects *objects);
//void print_usage();
//
// ---- End Repack Structs ----
  

/******************************************************************************
* This program scans the inodes looking specifically at trash filesets.  It will
* determine if post xattr and gc.path defined and if so, gets the objid xattr
* and deletes those objects.  The gc.path file is also deleted.
*
******************************************************************************/

char    *ProgName;
MarFS_Repo_Ptr repoPtr;

void start_threads( int num_threads );
void stop_workers();

/*****************************************************************************
Name: main

*****************************************************************************/

int main(int argc, char **argv) {
   //FILE *outfd;
   //FILE *packedfd;
   char *outf = NULL;
   char *rdir = NULL;
//   char *packed_log = NULL;
   char *aws_user_name = NULL;
//   char packed_filename[MAX_PACKED_NAME_SIZE];
   //unsigned int uid = 0;
   int fileset_id = -1;
   int  c;
   unsigned int thread_count = 16;
   unsigned int fileset_count = 1;
   extern char *optarg;
   unsigned int time_threshold_sec=0;
   unsigned int delete_flag = 0;
   unsigned char repack_flag = 0;
   char*        target_ns = NULL;
 
   Fileset_Info *fileset_info_ptr;

   if ( (ProgName = strrchr( argv[0], '/' )) == NULL)
      ProgName = argv[0];
   else
      ProgName++;

   while ((c=getopt(argc,argv, "d:t:T:p:hnro:u:f:N:")) != EOF) {
      switch (c) {
         case 'd': rdir = optarg; break;
         case 'o': outf = optarg; break;
         case 't': thread_count = atoi(optarg); break;
         case 'T': time_threshold_sec=atoi(optarg) * DAY_SECONDS; break;
//         case 'p': packed_log = optarg; break;
         case 'u': aws_user_name = optarg; break;
         case 'n': delete_flag = 1; break;
         case 'r': repack_flag = 1; break;
         case 'f': fileset_id = atoi(optarg); break;
         case 'N': target_ns = optarg; break;

         default:
         case 'h': print_usage();
            exit(0);
      }
   }
   
   if (rdir == NULL || 
       outf == NULL || 
       aws_user_name == NULL ) {

      fprintf(stderr, "%s: no directory (-d) or output file name (-o) or\
 aws user name (-u) specified\n\n",ProgName);
      print_usage();
      exit(1);
   }

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
      fprintf(stderr, "Memory allocation failed\n");
      exit(1);
   }
   init_records(fileset_info_ptr, fileset_count);
   aws_init();

//   if (packed_log == NULL) {
//      strcpy(packed_filename,"./tmp_packed_log");
//      packed_log = packed_filename;
//   }

   // open associated log file and packed log file
   if ((run_info.outfd = fopen(outf, "w")) == NULL){ 
      fprintf(stderr, "Failed to open %s\n", outf);
      exit(1);
   }
//   if ((run_info.packedfd = fopen(packed_log, "w"))==NULL){ 
//      fprintf(stderr, "Failed to open %s\n", packed_log);
//      exit(1);
//   }
//   strcpy(run_info.packed_filename, packed_log);
   run_info.has_packed=0;
   run_info.no_delete = delete_flag;
   run_info.deletes = 0;
   run_info.max_queue_depth = 0;

   if (target_ns) {
     size_t target_ns_size = strlen(target_ns);
     if (target_ns_size >= MARFS_MAX_NAMESPACE_NAME) {
      fprintf(stderr, "target_ns name is longer than %u chars\n", MARFS_MAX_NAMESPACE_NAME);
      exit(1);
     }
     run_info.target_ns_size = target_ns_size;
     strncpy(run_info.target_ns, target_ns, MARFS_MAX_NAMESPACE_NAME-1);
   }

   // Configure aws for the user specified on command line
   aws_read_config(aws_user_name);

   // GRAN
   // Create a hash table for packed files
   hash_table_t* ht;
   if ( (ht = malloc( sizeof( struct hash_table ) )) == NULL ) {
      fprintf( stderr, "Failed to allocate hash table\n" );
      exit(1);
   }
   if ( ht_init( ht, PACKED_TABLE_SIZE ) == NULL ) {
      fprintf( stderr, "Failed to initialize hash table\n" );
      exit(1);
   }

   hash_table_t* rt = NULL;
   if ( repack_flag ) {
      if ( (rt = malloc( sizeof( struct hash_table ) )) == NULL ) {
         fprintf( stderr, "Failed to allocate repack hash table\n" );
         exit(1);
      }
      if ( ht_init( rt, REPACK_TABLE_SIZE ) == NULL ) {
         fprintf( stderr, "Failed to initialize repack hash table\n" );
         exit(1);
      }  
   }

   printf( "INFO: initializing %u threads...", thread_count );
   fflush( stdout );
   // initialize threads and work-queue
   start_threads(thread_count);
   printf( "done\n" );

   fprintf(run_info.outfd, "\nPhase 1: processing inodes in order\n");
   read_inodes(rdir,fileset_id,fileset_info_ptr,
               fileset_count,time_threshold_sec,ht);

//   fclose(run_info.packedfd);
   if (run_info.has_packed) {
      fprintf(run_info.outfd, "\nPhase 2: processing packed files\n");
      process_packed( ht, rt );
   }

   // join with all threads
   stop_workers();

   fclose(run_info.outfd);
   free( fileset_info_ptr );
   free( ht->table );
   free( ht );
   if( rt ) {
      free( rt->table );
      free( rt );
   }

   //TEMP TEMP TEMP FOR DEBUG
   //unlink(packed_log);
   return (0); 
}


/***************************************************************************** 
 * Name: queue_delete
 *
 * *****************************************************************************/
void queue_delete( delete_entry* del_ent ) {
   if(pthread_mutex_lock(&delete_queue.queue_lock)) {
      fprintf(stderr, "ERROR: producer failed to acquire queue lock.\n");
      exit(-1); // a bit unceremonious
   }

   printf( "Queue_Depth = %d\n", delete_queue.num_items );
   if( delete_queue.num_items > run_info.max_queue_depth )
      run_info.max_queue_depth = delete_queue.num_items;

   while(delete_queue.num_items == QUEUE_MAX)
      pthread_cond_wait(&delete_queue.queue_full, &delete_queue.queue_lock);

   if( delete_queue.num_items != 0 ) { delete_queue.tail->next = del_ent; }
   else { delete_queue.head = del_ent; }
   delete_queue.tail = del_ent;
   del_ent->next = NULL;
   delete_queue.num_items++;
   run_info.deletes++;

   // wake any consumers waiting for an item to be added
   pthread_cond_signal(&delete_queue.queue_empty);

   pthread_mutex_unlock(&delete_queue.queue_lock);
   return;
}


/***************************************************************************** 
 * Name: obj_destroyer
 *
 * *****************************************************************************/
void* obj_destroyer( void* arg ) {
   delete_return* stats = malloc( sizeof( struct delete_return_struct ) );
   if( ! stats ) {
      fprintf( stderr, "obj_destroyer: failed to allocate space for a return struct\n" );
      return NULL;
   }

   while ( 1 ) {
      if( pthread_mutex_lock( &delete_queue.queue_lock ) ) {
         fprintf( stderr, "ERROR: consumer failed to aquire the queue lock\n" );
         return NULL;
      }

      // wait for the queue to fill or for the producer to finish
      while(!delete_queue.num_items && !delete_queue.work_done)
         pthread_cond_wait(&delete_queue.queue_empty, &delete_queue.queue_lock);

      // if all work has been completed, terminate
      if(delete_queue.num_items == 0 && delete_queue.work_done) {
         pthread_mutex_unlock(&delete_queue.queue_lock);
         pthread_exit( stats );
      }

      //retrieve an item from the queue
      delete_entry* del_ent = delete_queue.head; //we should be guaranteed this is valid
      delete_queue.head = del_ent->next;
      delete_queue.num_items--;
      if( delete_queue.num_items < 1 ) {
         delete_queue.tail = delete_queue.head;
      }

      // signal the producer to add to the queue, in case it was sleeping
      pthread_cond_signal(&delete_queue.queue_full);

      // exit the critical-section
      pthread_mutex_unlock( &delete_queue.queue_lock );

      int obj_return;
      if( del_ent->file_info.obj_type != OBJ_PACKED ) {
         fake_filehandle_for_delete_inits( &(del_ent->fh) );
         if ( (obj_return = dump_trash( &(del_ent->fh), &(del_ent->file_info) )) ) {
            fprintf(run_info.outfd, "delete error (obj_destroyer): \
               %d: on object %s\n", obj_return, del_ent->fh.info.pre.objid );
            fprintf(stderr, "delete error (obj_destroyer): \
               %d: on object %s\n", obj_return, del_ent->fh.info.pre.objid );
            stats->failures++;
         }
         else {
            fprintf( stderr, "INFO: deleted object -- %s\n", del_ent->fh.info.pre.objid );
            stats->successes++;
         }
      }
      else {
         // Delete object first
         if ( (obj_return = delete_object( &(del_ent->fh), &(del_ent->file_info), 0)) != 0 ) {
            fprintf(run_info.outfd, "delete error (obj_destroyer): \
               %d: on object %s\n", obj_return, del_ent->fh.info.pre.objid );
            fprintf(stderr, "delete error (obj_destroyer): \
               %d: on object %s\n", obj_return, del_ent->fh.info.pre.objid );
            stats->failures++;
         }
         // Get metafile name from tmp_packed_log and delelte 
         else {
            fprintf( stderr, "INFO: deleted packed object -- %s\n", del_ent->fh.info.pre.objid );
            stats->successes++;
         }

      }

      free( del_ent );
   }

}


/***************************************************************************** 
 * Name: start_threads
 *
 * *****************************************************************************/
void start_threads(int num_threads) {

  delete_queue.consumer_threads = calloc(num_threads, sizeof(pthread_t));
  if(delete_queue.consumer_threads == NULL) {
    perror("start_threads: calloc()");
    exit(-1);
  }

  pthread_mutex_init(&delete_queue.queue_lock, NULL);
  pthread_cond_init(&delete_queue.queue_empty, NULL);
  pthread_cond_init(&delete_queue.queue_full, NULL);

  delete_queue.num_items     = 0;
  delete_queue.work_done     = 0;
  delete_queue.head          = NULL;
  delete_queue.tail          = NULL;
  delete_queue.num_consumers = num_threads;

  int i;
  for(i = 0; i < num_threads; i++) {
    pthread_create(&delete_queue.consumer_threads[i], NULL, obj_destroyer, NULL);
  }
}


/***************************************************************************** 
 * Name: stop_workers
 *
 * *****************************************************************************/
void stop_workers() {
   pthread_mutex_lock(&delete_queue.queue_lock);
   delete_queue.work_done = 1;
   pthread_cond_broadcast(&delete_queue.queue_empty);
   pthread_mutex_unlock(&delete_queue.queue_lock);

   delete_return* status = NULL;

   unsigned long failures = 0;
   unsigned long successes = 0;
   int i;
   for(i = 0; i < delete_queue.num_consumers; i++) {
      pthread_join(delete_queue.consumer_threads[i], (void**)&status);
      if( status ) {
         failures += status->failures;
         successes += status->successes;
         free( status );
         status = NULL;
      }
      else {
         fprintf( stderr, "THREAD_ERROR: master process recieved a NULL status from delete_thread %d\n", i+1 );
      }
   }

   printf(  "----- RUN TOTALS -----\n" \
            "Total Attempted Deletes = %*llu\n" \
            "Successful Deletes =      %*lu\n" \
            "Failed Deletes =          %*lu\n", \
            10, run_info.deletes, 
            10, successes, 
            10, failures );
   printf(  " ( Max Work-Queue Depth = %d )\n", run_info.max_queue_depth );
}



/***************************************************************************** 
Name: init_records 

*****************************************************************************/
void init_records(Fileset_Info *fileset_info_buf, unsigned int record_count)
{
   memset(fileset_info_buf, 0, (size_t)record_count * sizeof(Fileset_Info)); 
}

/******************************************************************************
 * Name read_config_gc
 * This function reads the config file in order to extract the object hostname
 * associated with the current gpfs file (from the inode scan) 
 *
 ******************************************************************************/
//int read_config_gc(Fileset_Info *fileset_info_ptr)
//{
//
//   //Find the correct repo so that the hostname can be determined
//   LOG(LOG_INFO, "fileset repo name = %s\n", fileset_info_ptr->repo_name);
//   if ((repoPtr = find_repo_by_name(fileset_info_ptr->repo_name)) == NULL) {
//      fprintf(stderr, "Repo %s not found in configuration\n", 
//              fileset_info_ptr->repo_name);
//      return -1;
//   }
//   else {
//      strcpy(fileset_info_ptr->host, repoPtr->host);
//      LOG(LOG_INFO, "fileset name = %s/n", fileset_info_ptr->host);
//      return 0;
//   }
//}


/***************************************************************************** 
Name: print_usage 

*****************************************************************************/
void print_usage()
{
  fprintf(stderr, "Usage: %s\n", ProgName);
  fprintf(stderr, "  -d gpfs_path\n");
  fprintf(stderr, "  -o ouput_log_file\n");
  fprintf(stderr, "  -u aws_user_name\n");
  fprintf(stderr, "\n");
  fprintf(stderr, " [-t num_threads  (to be used for deletions, default = 16)]" );
  fprintf(stderr, " [-f fileset_id              (of trash fileset, for speed)]\n");
  fprintf(stderr, " [-N target_namespace          (in object-ID / PRE xattrs)]\n");
  fprintf(stderr, " [-p packed_tmp_file]\n");
  fprintf(stderr, " [-T time_threshold-days]\n");
  fprintf(stderr, " [-n]               (dry run)\n");
  fprintf(stderr, " [-h]               (help)\n");
  fprintf(stderr, "\n");
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
                 struct marfs_xattr *xattr_ptr ) {
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
         fprintf(stderr, "ERROR: gpfs_next_xattr -- %s\n", strerror(rc));
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
Name:  update_payload

This acts as the update function for hash table payload-insertion, creating
a linked list of payload entries.

*****************************************************************************/

void update_payload( void* new, void** old){
   ht_package NP = (ht_package) new;
   ht_header OP = *((ht_header*) old);

   if ( OP == NULL ) {
      if( (OP = malloc( sizeof( struct payload_header_struct ) )) == NULL ) {
         fprintf( stderr, "ERROR: update_payload -- failed to allocate a payload header struct\n" );
         exit( -1 );
      }

      if ( (OP->files = malloc( sizeof( struct payload_file_struct ) )) == NULL ) {
         fprintf( stderr, "ERROR: update_payload -- failed to allocate a payload file struct\n" );
         exit( -1 );
      }

      OP->ns = NP->ns;
      OP->md_ctime = NP->md_ctime;
      OP->chunks = NP->chunks;
      OP->files->md_path = NP->md_path;
      OP->files->md_ctime = 0;
      OP->files->size = 0;
      OP->files->next = NULL;
      OP->tail = OP->files;
      *old = OP;
   }
   else {
      ht_file* tmp = &(OP->tail->next);

      //generate a new element at the tail
      if ( (*tmp = malloc( sizeof( struct payload_file_struct ) )) == NULL ) {
         fprintf( stderr, "ERROR: update_payload -- failed to allocate a payload file struct\n" );
         exit( -1 );
      }

      (*tmp)->md_path = NP->md_path;
      (*tmp)->md_ctime = 0;
      (*tmp)->size = 0;
      (*tmp)->next = NULL;
      OP->tail = (*tmp);

      if ( OP->chunks != NP->chunks ) {
         fprintf( stderr, "WARNING: update_payload -- xattr file count of %zd for %s does not match expected %zd (from %s)\n", NP->chunks, NP->md_path, OP->chunks, OP->files->md_path );
      }
      if ( OP->ns != NP->ns ) {
         fprintf( stderr, "WARNING: update_payload -- xattr MarFS Namespace for %s does not match expected from %s\n", NP->md_path, OP->files->md_path );
      }
      if ( OP->md_ctime != NP->md_ctime ) {
         fprintf( stderr, "WARNING: update_payload -- xattr md_ctime for %s does not match expected from %s\n", NP->md_path, OP->files->md_path );
      }
   }
}


/***************************************************************************** 
Name:  split_insert

This is an internal helper function which acts to insert a list of files into the 
repacker payload structures

*****************************************************************************/
//rt_object split_insert( rt_object match, ht_header list_head, size_t opt_count, size_t max_obj ) {
//      size_t file_count = 0;
//      size_t obj_size = 0;
//      struct stat st;
//
//      if( list_head->files == NULL )
//         return NULL;
//
//      ht_file* cur = &(match->files);
//      while( *cur != NULL  &&  (*cur)->md_ctime < list_head->md_ctime ) {
//         obj_size += (*cur)->size;
//         if( (*cur)->size  == 0 ) {
//            fprintf( stderr, "split_insert: WARNING: hit file with unexpected 0 size \"%s\"\n", (*cur)->md_path );
//         }
//         file_count++;
//         cur = &((*cur)->next);
//      }
//
//      list_head->tail = *cur;
//      *cur = list_head->files;
//      while( *cur != NULL  &&  (obj_size + (*cur)->size) <= max_obj  &&  (file_count + 1) <= opt_count ) {
//         file_count++;
//         if ( stat( (*cur)->md_path, &st ) != 0 ) {
//            fprintf( stderr, "split_insert: failed to stat md_path %s\n", (*cur)->md_path );
//         }
//         (*cur)->md_ctime = list_head->md_ctime;
//         (*cur)->size = st.st_size;
//         obj_size += st.st_size;
//         cur = &((*cur)->next);
//      }
//      
//      list_head->files = *cur;
//      if( *cur == NULL ) {
//         list_head->tail = NULL;
//         return NULL;
//      }
//
//      *cur = NULL;
//      if( match->next == NULL ) {
//         if( (match->next = malloc( sizeof( struct rpayload_object_struct ) )) == NULL ) {
//            fprintf( stderr, "split_insert: failed to allocate space for a new object structure\n" );
//            exit( -1 );
//         }
//         match->next->next=NULL;
//         match->next->obj_size = 0;
//         match->next->file_count = 0;
//         match->next->files = NULL;
//         match->next->min_ctime = list_head->md_ctime;
//         match->next->max_ctime = list_head->md_ctime;
//      }
//
//      return match->next;
//}


/***************************************************************************** 
Name:  repack_update_payload

This acts as the update function for repack hash table payload-insertion.

*****************************************************************************/
//void repack_update_payload( void* new, void** old ) {
//   // When called, new should be a pointer to the payload for the original GC hash-table 
//   // while old is a pointer to the pointer to the payload for a repack hash-table entry
//   ht_header NP = (ht_header) new;
//   rt_header OP = *((rt_header *) old);
//
//   // if this is the first object for this directory, simply populate structures
//   if ( OP == NULL ) {
//      struct stat st;
//      if( (OP = malloc( sizeof( struct rpayload_header_struct ) )) == NULL ) {
//         fprintf( stderr, "repack_update_payload: failed to allocate a repack payload header struct\n" );
//         exit( -1 );
//      }
//      if( (OP->objects = malloc( sizeof( struct rpayload_object_struct ) )) == NULL ) {
//         fprintf( stderr, "repack_update_payload: failed to allocate a repack payload object struct\n" );
//         exit( -1 );
//      }
//
//      OP->objects->next = NULL;
//      OP->objects->obj_size = 0;
//      OP->objects->file_count = 0;
//      OP->objects->files = NP->files; //still need to set size and ctime for this list
//      OP->ns = NP->ns;
//      time_t md_ctime = NP->md_ctime;
//      OP->objects->min_ctime = md_ctime;
//      OP->objects->max_ctime = md_ctime;
//      
//      ht_file nfile = OP->objects->files;
//      while ( nfile != NULL ) { //set size and ctime for all files
//         nfile->md_ctime = md_ctime;
//         OP->objects->file_count++;
//         if ( stat( nfile->md_path, &st ) != 0 ) {
//            fprintf( stderr, "repack_update_payload: failed to stat md_path %s\n", nfile->md_path );
//            continue;
//         }
//         nfile->size = st.st_size;
//         OP->objects->obj_size += st.st_size;
//      }
//   }
//   else { //otherwise, we need to insert according to md_ctime
//      ssize_t min_pack_count = OP->ns->iwrite_repo->min_pack_file_count;
//      ssize_t max_pack_count = OP->ns->iwrite_repo->max_pack_file_count;
//      ssize_t min_pack_size = OP->ns->iwrite_repo->min_pack_file_size;
//      ssize_t max_pack_size = OP->ns->iwrite_repo->max_pack_file_size;
//      size_t obj_size = OP->ns->iwrite_repo->chunk_size;
//      size_t opt_count;
//
//      if ( max_pack_count == 0 ) {
//         fprintf( stderr, "repack_update_payload: ERROR: repacking objects for repo \"%s\" but max_pack_file_count == 0\n", NP->ns->iwrite_repo->name );
//         exit( -1 );
//      }
//      if( max_pack_count == -1  ||  (max_pack_count*max_pack_size) > obj_size ) {
//         // if count is unbounded or otherwise excessive, limit optimal count to a value that will allow squeezing in excess files
//         opt_count = ( ( obj_size - (min_pack_count*max_pack_size) ) / max_pack_size );
//      }
//      else {
//         opt_count = max_pack_count - min_pack_count;
//         if( opt_count > max_pack_count ) //unnecessary? Only hit if min_pack_count < 0
//            opt_count = max_pack_count;
//      }
//
//      if( opt_count < min_pack_count )
//         opt_count = min_pack_count;
//
//      printf( "opt_count = %zu,  Repo: %s - min_pc=%zd, max_pc=%zd, min_ps=%zd, max_ps=%zd\n", opt_count, NP->ns->iwrite_repo->name, min_pack_count, max_pack_count, min_pack_size, max_pack_size );
//
//      rt_object matchobj = OP->objects;
//      rt_object prevobj = NULL;
//      while( matchobj != NULL  &&  matchobj->max_ctime < NP->md_ctime ) {
//         prevobj = matchobj;
//         matchobj = matchobj->next;
//      }
//
//      if( matchobj == NULL )
//         matchobj = prevobj;
//
//      while( (matchobj = split_insert( matchobj, NP, opt_count, NP->ns->iwrite_repo->chunk_size)) != NULL ) {
//         ; //NO-OP
//      }
//
//   }
//}


/***************************************************************************** 
Name: print_current_file 

This function prints current time to the log entry
*****************************************************************************/
void print_current_time()
{
   char time_string[21];
   struct tm *time_info;
   
   time_t now = time(0);
   time_info = localtime(&now);
   strftime(time_string, sizeof(time_string), "%Y-%m-%d %H:%M:%S", time_info);
   fprintf(run_info.outfd, "%s  ", time_string);
}


void print_delete_preamble() {
   print_current_time();
   if ( run_info.no_delete )
      fprintf(run_info.outfd, "INFO: ID'd ");
   else
      fprintf(run_info.outfd, "deleting ");
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
int read_inodes(const char   *fnameP, 
                int           fileset_id,
                Fileset_Info *fileset_info_ptr, 
                size_t rec_count, 
                unsigned int day_seconds,
                hash_table_t* ht) {

   int rc = 0;
   unsigned int prog_count = 0;
   unsigned int tmp_prog;
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

   int early_exit =0;
   int xattr_index;
   char *md_path_ptr;
   const struct stat* st = NULL;


   printf( "Scanning Inodes." );
   fflush( stdout );
   char sep_char = '\n';

   /*
    *  Get the unique handle for the filesysteme
   */
   if ((fsP = gpfs_get_fssnaphandle_by_path(fnameP)) == NULL) {
      rc = errno;
      fprintf(stderr, "%s: line %d - gpfs_get_fshandle_by_path: %s\n", 
              ProgName,__LINE__,strerror(rc));
      early_exit = 1;
      clean_exit(run_info.outfd, iscanP, fsP, early_exit);
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
      clean_exit(run_info.outfd, iscanP, fsP, early_exit);
   }

   struct payload_package_struct p_struct;
   ht_package payload = &p_struct;

   delete_entry *del_ent = NULL;

   while (1) {

      MarFS_XattrPre*  pre;
      MarFS_XattrPost* post;

      if ( ! del_ent ) {
         del_ent = calloc( 1, sizeof( struct delete_entry_struct ) );
         if( del_ent == NULL ) {
            fprintf( stderr, "ERROR: read_inodes: failed to allocate a delete entry struct\n" );
            exit( -1 );
         }

         pre  = &(del_ent->fh.info.pre);
         post = &(del_ent->fh.info.post);
      }


      rc = gpfs_next_inode_with_xattrs(iscanP,
                                       0x7FFFFFFF,
                                       &iattrP, 
                                       &xattrBP,
                                       &xattr_len);
      //rc = gpfs_next_inode(iscanP, 0x7FFFFFFF, &iattrP);
      if (rc != 0) {
         rc = errno;
         fprintf(stderr, "ERROR: gpfs_next_inode: %s\n", strerror(rc));
         early_exit = 1;
         clean_exit(run_info.outfd, iscanP, fsP, early_exit);
      }
      // Are we done?
      if ((iattrP == NULL) || (iattrP->ia_inode > 0x7FFFFFFF))
         break;

      // Print progress indicator
      tmp_prog = iattrP->ia_inode / 100000;
      if ( prog_count < tmp_prog ) { prog_count = tmp_prog; printf( "." ); sep_char = '\n'; fflush( stdout ); }

      // Determine if invalid inode error 
      if (iattrP->ia_flags & GPFS_IAFLAG_ERROR) {
         fprintf(stderr, "%sERROR: read_inodes: invalid inode %9d (GPFS_IAFLAG_ERROR)\n", 
               sep_char, ProgName,iattrP->ia_inode);
         sep_char = '\0';
         continue;
      } 

      // If fileset_id is specified then only look for those inodes and xattrs
      // The new use for this is to provide the file-set-ID of trash on the command-line.
      // This allows us to avoid even considering anything not in trash.
      if (fileset_id >= 0) {
         if (fileset_id != iattrP->ia_filesetid) {
           LOG(LOG_INFO, "skipping inode %u (fset: %d)\n",
               iattrP->ia_inode, iattrP->ia_filesetid);
            continue;
         }
      }

      // Print out inode values to output file
      // This is handy for debug at the moment
      if (iattrP->ia_inode != 3) {	/* skip the root inode */
         
         // This log commented out due to amount of inodes dumped
         //LOG(LOG_INFO, "%u|%lld|%lld|%d|%d|%u|%u|%u|%u|%u|%lld|%d\n",
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
                        &mar_xattrs[0])) > 0) {

               // marfs_xattrs has a list of xattrs found.  Look for POST
               // first, because that tells us if file is in the trash.
               // POST xattr for files in the trash includes the md_path.
               if ((xattr_index=get_xattr_value(&mar_xattrs[0], 
                           marfs_xattrs[post_index], xattr_count)) != -1 ) { 
                  xattr_ptr = &mar_xattrs[xattr_index];
                  LOG(LOG_INFO, "post xattr name = %s value = %s \
                        count = %d index=%d\n", xattr_ptr->xattr_name, \
                        xattr_ptr->xattr_value, xattr_count,xattr_index);
                  if ((str_2_post(post, xattr_ptr->xattr_value, 1))) {
                     fprintf(stderr, "%cError parsing  post xattr for inode %d\n",
                           sep_char, iattrP->ia_inode);
                     sep_char = '\0';
                     continue;
                  }
               }
               else if ( (xattr_index = get_xattr_value(&mar_xattrs[0], marfs_xattrs[restart_index], xattr_count)) ){
                  fprintf(stderr, "%cWARNING: failed to find post or restart xattr for inode %d\n", 
                          sep_char, iattrP->ia_inode);
                  sep_char = '\0';
                  continue;
               }
               else {
                  fprintf( stderr, "%cINFO: found restart (%s) but no post xattr for inode %d\n", 
                     sep_char, mar_xattrs[xattr_index].xattr_value, iattrP->ia_inode );
                  sep_char = '\0';
                  continue;
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

                        // Going to get the repo name now from the objid xattr
                        // To do this, must call marfs str_2_pre to parse out
                        // the bucket name which include repo name
                        if (str_2_pre(pre, xattr_ptr->xattr_value, st) != 0) {
                           LOG(LOG_ERR, "str_2_pre failed\n");
                           continue;
                        }

                        // command-line option 'N' allows us to ignore everything
                        // except trashed files that have a certain namespace in their
                        // object-ID.
                        if (run_info.target_ns_size
                            && strcmp(run_info.target_ns, pre->ns->name)) {
                          LOG(LOG_INFO, "skipping inode %u (namespace: %s)\n",
                              iattrP->ia_inode, pre->ns->name);
                          continue;
                        }

                        // Now check if we have RESTART xattr which may imply
                        // OBJ_FUSE or OBJ_Nto1 which requires 
                        // special handling in dump_trash
                        unsigned char restart_found = 0;
                        if ((xattr_index=get_xattr_value(&mar_xattrs[0],
                             marfs_xattrs[restart_index], xattr_count)) != -1 ) {
                                restart_found = 1;
                        }

                        check_security_access(pre);

                        // file has passed all the checks.  We would delete it.
                        // Actually, no.  It hasn't passed the packed checks.
                        LOG(LOG_INFO, "remove file: %s  remove object:  %s\n",
                            md_path_ptr, xattr_ptr->xattr_value); 

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
                        // GRAN
                        //   fprintf(run_info->packedfd,"%s %zu %s\n", 
                        //      xattr_ptr->xattr_value, post->chunks, md_path_ptr );
                           int len_mdpath = strlen( md_path_ptr ) + 1;

                           if ( (payload->md_path = malloc( sizeof( char ) * len_mdpath )) == NULL ) {
                              rc = errno;
                              fprintf(stderr, "marfs_gc: failed to allocate space for a path string\n");
                              early_exit = 1;
                              clean_exit(run_info.outfd, iscanP, fsP, early_exit);
                           }
                           if ( (payload->md_path = strncpy( payload->md_path, md_path_ptr, len_mdpath )) == NULL ) {
                              fprintf( stderr, "marfs_gc: failure of strncpy for packed file md_path\n" );
                              rc = errno;
                              early_exit = 1;
                              clean_exit(run_info.outfd, iscanP, fsP, early_exit);
                           }
                           
                           payload->chunks = post->chunks;
                           payload->ns = (MarFS_Namespace_Ptr) pre->ns;
                           payload->md_ctime = pre->md_ctime;

                           ht_insert_payload( ht, xattr_ptr->xattr_value, payload, &update_payload );
                           run_info.has_packed = 1;
                        }


                        // Not checking return because log has error message
                        // and want to keep running even if errors exist on 
                        // certain objects or files
                        else {
                           del_ent->file_info.restart_found = restart_found;
                           del_ent->file_info.obj_type = post->obj_type;

                           queue_delete( del_ent );
                           //fake_filehandle_for_delete_inits(&fh);
                           //trash_status = dump_trash(&fh, file_info_ptr);
                           del_ent = NULL; //remove reference to the entry, so another will be allocated
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
   if ( del_ent )
      free( del_ent );

   printf( "%c", sep_char );

   clean_exit(run_info.outfd, iscanP, fsP, early_exit);
   return(rc);
}

/***************************************************************************** 
Name: dump_trash 

 This function deletes the object file as well as gpfs metadata files
 This is only called for non-packed MarFS files.
*****************************************************************************/
int dump_trash(MarFS_FileHandle   *fh,
               File_Info          *file_info_ptr)
{
   int    return_value =0;
   int    delete_obj_status;
   int    multi_flag = 0;
   size_t i;

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

      multi_flag = 1;

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
               print_current_time();
               fprintf(run_info.outfd,
                       "s3_delete error (HTTP Code: %d) on object %s\n",
                       delete_obj_status, fh->os.url); // xattr_ptr->xattr_value
               return_value = -1;
            }
         }
      }
      close_md(fh);
   }
   // If multi type file then delete all objects associated with file
   else if (post_ptr->obj_type == OBJ_MULTI) {
      multi_flag = 1;

      for (i=0; i < post_ptr->chunks; i++ ) {
         pre_ptr->chunk_no = i;
         update_pre(pre_ptr);
         update_url(&fh->os, &fh->info);

         if ((delete_obj_status = delete_object(fh, file_info_ptr, multi_flag))) {
            print_current_time();
            fprintf(run_info.outfd,
                    "s3_delete error (HTTP Code: %d) on object %s\n",
                    delete_obj_status, fh->os.url); // xattr_ptr->xattr_value
            return_value = -1;
         }
      }
   }

   // else UNI, but need to implement other formats, as they are developed 
   else if (post_ptr->obj_type == OBJ_UNI) {
      update_pre(pre_ptr);
      update_url(&fh->os, &fh->info);

      if ((delete_obj_status = delete_object(fh, file_info_ptr, multi_flag))) {
         print_current_time();
         fprintf(run_info.outfd,
                 "s3_delete error (HTTP Code:  %d) on object %s\n",
                 delete_obj_status, fh->os.url); // xattr_ptr->xattr_value
         return_value = -1;
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
      delete_file(md_path_ptr); 
   }
   else {
      print_current_time();
      fprintf(run_info.outfd, "Object error no file deletion of %s\n", md_path_ptr);
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

   // update_pre(&fh->info.pre);
   // update_url(&fh->os, &fh->info);


   // timestamp, plus "ID'd " or "deleting "
   print_delete_preamble();

   if (file_info_ptr->restart_found && pre_ptr->obj_type == OBJ_Nto1) {
      fprintf(run_info.outfd, "chunk %zd of incomplete Nto1 multi object %s\n", pre_ptr->chunk_no, object);
      if (! run_info.no_delete ) {
         // s3_return = s3_delete( obj_buffer, object );
         rc = delete_data(fh);
      }
   }
   else if (file_info_ptr->restart_found && pre_ptr->obj_type == OBJ_FUSE) {
      fprintf(run_info.outfd, "incomplete FUSE multi object %s\n", object);
      if (! run_info.no_delete ) {
         // s3_return = s3_delete( obj_buffer, object );
         rc = delete_data(fh);
      }
   }
   else if ( is_multi ) {
      fprintf(run_info.outfd, "chunk %zd of multi object %s\n", pre_ptr->chunk_no, object);
      if (! run_info.no_delete ) {
         // s3_return = s3_delete( obj_buffer, object );
         rc = delete_data(fh);
      }
   }
   else if (pre_ptr->obj_type == OBJ_PACKED) {
      fprintf(run_info.outfd, "packed object %s\n", pre_ptr->objid);
      if (! run_info.no_delete ) {
         // s3_return = s3_delete( obj_buffer, pre_ptr->objid );
         rc = delete_data(fh);
      }
   }
   else {
      fprintf(run_info.outfd, "uni object %s\n", pre_ptr->objid);
      if (! run_info.no_delete ) {
         // s3_return = s3_delete( obj_buffer, pre_ptr->objid );
         rc = delete_data(fh);
      }
   }
      

   if ( run_info.no_delete )
      return_val = 0;
   else if (rc)
      // return_val=check_S3_error(s3_return, obj_buffer, S3_DELETE);
      return_val=check_S3_error(fh->os.op_rc, &fh->os.iob, S3_DELETE);


   return(return_val);
}

/***************************************************************************** 
Name: delete_file 

this function deletes the gpfs files associated with an object
*****************************************************************************/
int delete_file( char *filename )
{
   int return_value = 0;
   char path_file[MARFS_MAX_MD_PATH];
   sprintf(path_file, "%s.path",filename);

   // Delete MD-file (unless '-n')
   print_delete_preamble();
   fprintf(run_info.outfd, "  MD-file   %s\n", filename);

   if ((! run_info.no_delete)
       && ((unlink(filename) == -1))) {
      print_current_time();
      fprintf(run_info.outfd, "Error removing MD-file %s\n", filename);
      return_value = -1;
   }

   // Delete path-file (unless '-n')
   print_delete_preamble();
   fprintf(run_info.outfd, "  path-file %s\n", path_file);

   if ((! run_info.no_delete)
       && (unlink(path_file) == -1)) {
      fprintf(run_info.outfd, "Error removing path-file %s\n", path_file);
      return_value = -1;
   }

   return(return_value);
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
int process_packed(hash_table_t* ht, hash_table_t* rt)
{
   int obj_return;
   int df_return;
   unsigned int tmp_prog;
   unsigned int prog_count = 0;
   unsigned int ent_count = 0;

   ht_entry_t* entry = NULL;

   // In a loop get all lines from the file
   // Example line (with 'xx' instead of IP-addr octets, and altered MD paths):
   //
   // proxy1/bparc/ver.001_001/ns.development/P___/inode.0000016572/md_ctime.20160526_124320-0600_1/obj_ctime.20160526_124320-0600_1/unq.0/chnksz.40000000/chnkno.0 403
   // /gpfs/fileset/trash/development.0/8/7/3/uni.255.trash_0000016873_20160526_125809-0600_1
   //
   ht_file Ptmp;
   ht_file file;

   printf( "\nProcessing Packed Files." );
   fflush( stdout );
   char sep_char = '\n'; //silly little whitespace to make progress output look better

   while ( (entry = ht_traverse_and_destroy( ht, entry )) != NULL ) {
      
      ent_count++;
      tmp_prog = ent_count / 10;
      if( prog_count < tmp_prog ) { prog_count = tmp_prog; printf( "." ); sep_char='\n'; fflush( stdout ); }
      
      //The var 'p_header' is the head of a linked list of file info for this object
      ht_header p_header = (ht_header) entry->payload;      
      file = p_header->files;

      if (entry->value != p_header->chunks) {

         // TODO check min_pack_file_count to determine canidacy for repack
         if ( rt  &&  ( entry->value < p_header->chunks ) ) {
            fprintf( stderr, "%cINFO: repack canidate found, but repack is not implemented\n", sep_char );
            sep_char = '\0';

            // Get the post xattr to determine offset in the objec
//            if ((getxattr(file->md_path, "user.marfs_post", &post_xattr, MARFS_MAX_XATTR_SIZE) != -1)) {
//               //fprintf(stdout, "xattr = %s\n", post_xattr);
//               rc = str_2_post(&post, post_xattr, 1);
//               files->original_offset = post.obj_offset;
//               LOG(LOG_INFO, "filename %s xattr = %s offset=%ld\n", files->filename, post_xattr,post.obj_offset);
//            }

            // TODO insert into rt table
         }
         else {
//            if ( entry->value < p_header->chunks ) 
//               fprintf(stderr, "INFO: found repack canidate -- object %s has %d files while chunk count is %zd\n",
//                  entry->key, entry->value, p_header->chunks);
            if ( entry->value > p_header->chunks ) {
               fprintf(stderr, "%cWARNING: potential faulty 'marfs_post' xattr -- object %s has %d files while chunk count is %zd  MD-example = %s\n",
                  sep_char, entry->key, entry->value, p_header->chunks, file->md_path);
               sep_char = '\0';
            }

            //free the payload list
            while ( file != NULL ) {
               Ptmp = file;
               file = file->next;
               free( Ptmp->md_path );
               free( Ptmp );
//               if( file )
//                  fprintf(stderr, "REPEAT: object %s has %d files while chunk count is %zd   MD = %s\n",
//                     entry->key, entry->value, p_header->chunks, file->md_path);
            }
            free( p_header );
         }
         continue;
      }

      if ( run_info.no_delete ) {
         fprintf(run_info.outfd, "ID'd packed object %s\n", entry->key);
         continue;
      }

      // If we get here, counts are matching up
      // Go ahead and start deleting
      delete_entry* del_ent = calloc( 1, sizeof( struct delete_entry_struct ) );
      if ( del_ent == NULL ) {
         fprintf( stderr, "ERROR: process_packed: failed to allocate space for a delete entry struct\n" );
         exit( -1 );  //if we fail to allocate memory, just terminate
      }
      del_ent->file_info.obj_type = OBJ_PACKED;
      del_ent->file_info.restart_found = 0; //assume no restart?  This was done before, but seems odd.

      if ( fake_filehandle_for_delete( &(del_ent->fh), entry->key, file->md_path ) ) {
         fprintf(run_info.outfd,
                 "ERROR: failed to create filehandle for %s %s\n",
                 entry->key, file->md_path);
         continue;
      }

      update_pre( &(del_ent->fh.info.pre) );
      update_url( &(del_ent->fh.os), &(del_ent->fh.info) );

      queue_delete( del_ent );

      Ptmp = NULL;
      while ( file != NULL ) {
         //Delete files
         if ( delete_file( file->md_path ) == -1 )
            df_return = -1;

         // free payload struct and referenced strings
         Ptmp = file;
         file = file->next;
         free( Ptmp->md_path );
         free( Ptmp );
      }
      free( p_header );

   }

   printf( "%c", sep_char );

   if (df_return == -1 || obj_return == -1) {
      return(-1);
   }
   return(0);
}

//  ---------   Begin Repacker Code ----------
//
//#include <errno.h>
//#include <stdlib.h>
//#include <fcntl.h>
//#include <gpfs.h>
//#include <stdio.h>
//#include <string.h>
//#include <time.h>
//#include <stdlib.h>
//#include <gpfs_fcntl.h>
//#include <sys/types.h>
//#include <attr/xattr.h>
//#include <ctype.h>
//#include <attr/xattr.h>
//#include "marfs_base.h"
//#include "common.h"
//#include "marfs_ops.h"
//#include "marfs_repack.h"
//#include "utilities_common.h"
//
//
///******************************************************************************
//* Name  main
//* 
//* This utility, as currently written, repacks objects in the trash directory 
//* due to mismatches between the chunk count specified in the trash file xattr
//* and the actual number of files found in trash for the specified packed 
//* object.
//*
//* The method for determining whether objects need to repacked is to read the 
//* the file specied by the -d option.  This is typically the 
//* "tmp_packed_log" file from garbage collection.  This file lists the packed 
//* object name and associated files for the packed object.  It only lists those
//* objects that could NOT reconcile with the file counts.  The format of this 
//* file is:
//*
//* OBJECT_NAME  EXPECTED_FILE_COUNT  FILE_NAME
//*
//* This utility reads the file and finds all files for the associated object
//* The number of files found will be less than EXPECTED_FILE_COUNT (post xattr
//* chunk count).  It then proceeds to read in the file objects and rewrite
//* them back to a new object with offsets re-defined.  Xattrs are updated
//* to reflect the new object, chunk_count, and offsets 
//*
//******************************************************************************/
//int main(int argc, char **argv){
//   int c;
//   char *fnameP = NULL;
//   char *outfile = NULL;
//   //char *ns = NULL;
//
//   while ((c=getopt(argc,argv,"d:o:h")) != EOF) {
//      switch(c) {
//         case 'd': fnameP = optarg; break;
//         //case 'n': ns = optarg; break;
//         case 'o': outfile = optarg; break;
//         case 'h': print_usage();
//         default:
//            exit(-1);
//      }
//   }
//   if (fnameP == NULL || outfile == NULL) {
//      print_usage();
//      exit(-1);
//   }
//
//   if ( setup_config() == -1 ) {
//      fprintf(stderr,"Error:  Initializing Configs and Aws failed, quitting!!\n");
//      exit(-1);
//   }
//
//
//   File_Handles file_info;
//   File_Handles *file_status = &file_info;
//
//   // open associated log file and packed log file
//   if ((file_status->outfd = fopen(outfile,"w")) == NULL){
//      fprintf(stderr, "Failed to open %s\n", outfile);
//      exit(1);
//   }
//   strcpy(file_status->packed_log, fnameP);
//
//
//
//   //MarFS_Namespace* namespace;
//   //namespace = find_namespace_by_name(ns);
//        //MarFS_Repo* repo = namespace->iwrite_repo;
//        // Find the correct repo - the one with the largest range
//   //MarFS_Repo* repo = find_repo_by_range(namespace, (size_t)-1);
//   //
//
//   repack_objects *objs = NULL;
//   find_repack_objects(file_status, &objs);
//   pack_objects(file_status, objs);
//   update_meta(file_status, objs);
//   free_objects(objs);
//   return 0;
//}
//
//
///******************************************************************************
//* Name find_repack_objects 
//* 
//* This function takes the output of the tmp_packed_log created by garbage
//* collection and determines which objects are candidates for repacking. 
//*
//* The criteria for repacking is:
//*    object file count < post xattr chunk_count
//* This implies that some of the files belonging to a packed object have
//* been deleted, therefore the object no longer contains the full set
//* of files it originally contained
//*
//* The process of repacking starts by finding all objects and files that
//* are candidates and placing them into objects and file link lists
//* respectively.  Each object * link list entry (node) contains a 
//* link list of files that belong to the * new object. 
//* 
//******************************************************************************/
//int find_repack_objects(File_Handles *file_info, repack_objects **objects_ptr) {
//   FILE *pipe_cat = NULL;
//   FILE *pipe_grep = NULL;
//
//   char obj_buf[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
//   char obj_buf1[MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64];
//   char objid[MARFS_MAX_OBJID_SIZE];
//   char cat_command[MAX_CMD_LENGTH];
//   char grep_command[MAX_CMD_LENGTH];
//   int file_count;
//
//   int chunk_count;
//   char filename[MARFS_MAX_MD_PATH];
//   int rc;
//   MarFS_XattrPost post;
//   char post_xattr[MARFS_MAX_XATTR_SIZE];
//
//
//   obj_files *files, *files_head;
//   repack_objects *objects, *objects_head;
//   files_head=NULL;
//   objects_head=NULL;
//
//
//   // create command to cat and sort the list of objects that are packed
//   sprintf(cat_command, "cat %s | awk '{print $1}' | sort | uniq", file_info->packed_log);
//
//   // open STREAM for cat
//   if (( pipe_cat = popen(cat_command, "r")) == NULL) {
//      fprintf(stderr, "Error with popen\n");
//      return(-1);
//   }
//   // Now loop through all objects sorted by name in order to count number 
//   // of files associated with that object
//   while(fgets(obj_buf, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_cat)) {
//      sscanf(obj_buf,"%s", objid);
//      sprintf(grep_command, "cat %s | grep %s ", file_info->packed_log, objid);
//      if (( pipe_grep = popen(grep_command, "r")) == NULL) {
//         fprintf(stderr, "Error with popen\n");
//         return(-1);
//      }
//      file_count = 0;
//      while(fgets(obj_buf1, MARFS_MAX_MD_PATH+MARFS_MAX_OBJID_SIZE+64, pipe_grep)) {
//         sscanf(obj_buf1,"%s %d %s", objid, &chunk_count, filename);
//         // Need to look into this - do we need a minimum value to repack?
//         // same question as packer script
//         file_count++;
//         if (chunk_count <= 1) { 
//            continue;
//         }
// 
//         // file count is the number of files associated with an object (from tmp_packed_log)
//         // chunck count is the post xattr value that states how many files in packed
//         // objecd
//         // if file_count < chunk_count, the files can be packed
//         if (file_count < chunk_count) {
//            // Create object link list here
//            //fprintf(stdout, "object = %s\n", objid);
//            //fprintf(stdout, "file_count = %d chunk_count=%d\n", file_count, chunk_count);
//            if ((files = (obj_files *)malloc(sizeof(obj_files)))==NULL) {
//               fprintf(stderr, "Error allocating memory\n");
//               return -1;
//            }
//            //fprintf(stdout, "%s %s\n", objid, filename);
//            
//            strcpy(files->filename, filename);
//
//
//            // Get the post xattr to determine offset in the objec
//            if ((getxattr(files->filename, "user.marfs_post", &post_xattr, MARFS_MAX_XATTR_SIZE) != -1)) {
//               //fprintf(stdout, "xattr = %s\n", post_xattr);
//               rc = str_2_post(&post, post_xattr, 1);
//               files->original_offset = post.obj_offset;
//               LOG(LOG_INFO, "filename %s xattr = %s offset=%ld\n", files->filename, post_xattr,post.obj_offset);
//            }
//            // adjust files link list pointers
//            files->next =  files_head;
//            files_head = files;
//         }
//
//      }      
//      if (pclose(pipe_grep) == -1) {
//         fprintf(stderr, "Error closing cat pipe in process_packed\n");
//         return(-1);
//      }
//      if ((objects = (repack_objects *)malloc(sizeof(repack_objects)))==NULL) {
//         fprintf(stderr, "Error allocating memory\n");
//         return -1;
//      }
//      // Update objects structure entries based on file info
//      strcpy(objects->objid, objid);
//      objects->pack_count = file_count;
//      objects->chunk_count = chunk_count;
//      objects->files_ptr = files_head;
//      objects->next = objects_head;
//      objects_head = objects;
//      files_head=NULL;
//   }
//   if (pclose(pipe_cat) == -1) {
//      fprintf(stderr, "Error closing cat pipe in process_packed\n");
//      return(-1);
//   }
//   *objects_ptr=objects_head;
//   return 0;
//}
//
//
///******************************************************************************
//* Name  pack_objects 
//* 
//* This function traverses the object link list created in find_repack_objects
//* and reads the corresponding file data.  That data is then written to a new
//* object.  Because the old object had holes due to missing files, a new
//* write offset is calculated.
//*
//******************************************************************************/
//int pack_objects( File_Handles *file_info, 
//                 repack_objects *objects)
//{
//   struct stat statbuf;
//   char *path = "/";
//
//   stat(path, &statbuf);
//   size_t write_offset = 0;
//   size_t obj_raw_size;
//   size_t obj_size;
//   //size_t offset;
//   size_t unique;
//   IOBuf *nb = aws_iobuf_new();
//   //char test_obj[2048];
//   obj_files *files;
//   //int ret;
//   //char *obj_ptr;
//   //CURLcode s3_return;
//   //char pre_str[MARFS_MAX_XATTR_SIZE];
//   char marfs_path[1024];
//   int flags;
//
//   // Traverse object link list and find those that should be packed
//   while (objects) { 
//      // need inner loop to get files for each object
//      // If chunk_count == file count no need to pack
//      // and garbage collection took care of it
//      if (objects->chunk_count == objects->pack_count) {
//         objects=objects->next;
//         continue;
//      }
//      //No need to pack if only one file specified in xattr and only
//      //one file found
//      if (objects->chunk_count == 1 && objects->pack_count ==1 ) {
//         objects=objects->next;
//         continue;
//      }
//      // Not quite sure how this next condition could happen
//      // TO DO:  make only one contion chunk_count > file_count
//      // all others continue
//      if (objects->pack_count > objects->chunk_count) {
//         objects=objects->next;
//         continue;
//      }
//
//      LOG(LOG_INFO,"object = %s\n", objects->objid);
//      LOG(LOG_INFO, "file count = %ld chunks = %ld\n", objects->pack_count, objects->chunk_count);
//      files = objects->files_ptr;
//      write_offset = 0;
//
//      files->new_offset = write_offset;
//
//      // Specify a new object being accessed 
//      unique=0;
//
//      // Each object has a files linked list.  Read each file 
//      // at the offset calculated and write back to new object with
//      // new offset.
//      while (files) {
//         // Get the associated Marfs file handle from from the linked list
//         MarFS_XattrPre*  pre  = &files->fh->info.pre;
//
//         // If new object increment unique to give it a new objid
//         if (unique == 0) 
//            pre->unique++;    
//        
//         // Need to make sure that objectSize
//         // does not include recovery info TBD
//         obj_raw_size = files->fh->objectSize;
//         
//         obj_size = obj_raw_size + MARFS_REC_UNI_SIZE;
//         files->size = obj_size;
//
////********************
//// Questions 
//// correct path (fuse path)?
//// set flags for open
//// offset for read becomes 0, correct?
////
//  
//  
//         char read_buf[1024];  
//         size_t read_count;
//         ssize_t write_count;
//         
//         flags = O_RDONLY; 
//         get_marfs_path(files->filename, &marfs_path[0]);
//         marfs_open_at_offset(marfs_path,
//                              files->fh,
//                              flags,
//                              files->fh->info.post.obj_offset, 
//                              obj_size);
//         read_count = marfs_read(marfs_path, //TODO Need recovery info as well
//                                 read_buf,
//                                 obj_size,
//                                 0,
//                                 files->fh);
//
//         marfs_release (marfs_path, files->fh);
//// Instead of reading more to I write now 
//// This becomes a new object because pre.unique incremented
////
//// DO I need to do anything special with flags?
//// O_CREATE or O_APPEND?
//// Need new flag with open or new function for recovery info
////
//         marfs_open(marfs_path,
//                    files->fh,
//                    flags,  // WRITE
//                    obj_size);
//         write_count = marfs_write(marfs_path, //TODO Need recovery info as well
//                                   read_buf,
//                                   obj_size,
//                                   files->new_offset,
//                                   files->fh);
//         // This needs be moved outside loop
//         // and I need an open write before while (files)
//         // with O_CREATE and O_WRONLY
//         // Jeff states I may need a special release with offset 
//         // of last object
//         //
//         //marfs_release (marfs_path, files->fh);
//
//         LOG(LOG_INFO, "Read buffer write count = %ld  len = %ld\n", nb->write_count, nb->len);
//         // may have to copy nb to a new buffer 
//         // then write 
//     
//
//         files->new_offset = write_offset;
//         write_offset += obj_size; 
//	 files = files->next;
//      }
//      objects=objects->next;
//   } return 0;
//}
//
///******************************************************************************
//* Name update_meta
//*
//* This function updates the xattrs to reflect the newly repacked object
//* This includes a new object name and updated offsets and chunks in the 
//* post xattr. 
//* 
//******************************************************************************/
//int update_meta(File_Handles *file_info, 
//                repack_objects *objects)
//{
//  obj_files *files;
////  char pre[MARFS_MAX_XATTR_SIZE];
//
//  char post_str[MARFS_MAX_XATTR_SIZE];
//  char post_xattr[MARFS_MAX_XATTR_SIZE];
//  //int rc;
//
//
//  // Travers all repacked objects
//  while(objects) {
//     files = objects->files_ptr;
//     //Traverse the object files
//     while (files) {
//
//        // retrive the MarFS_FileHandle information from the files linked
//        // list
//
//        //MarFS_XattrPre*  pre  = &files->fh->info.pre;
//        MarFS_XattrPost* post = &files->fh->info.post;
//
//        // Get the file post xattr
//        // and update its elements based on the repack
//        //if ((getxattr(files->filename, "user.marfs_post", &post_xattr, MARFS_MAX_XATTR_SIZE) != -1)) {
//        //   rc=str_2_post(&post, post_xattr, 1);
//           post->chunks = objects->pack_count;
//           post->obj_offset = files->new_offset;
//           
//
//             
//           save_xattrs(&files->fh->info, XVT_PRE | XVT_POST);
//
//           // convert the post xattr back to string so that the file xattr can
//           // be re-written
//           //rc=post_2_str(post_str, MARFS_MAX_XATTR_SIZE, &post, pre.repo, 0);
//           LOG(LOG_INFO, "Old xattr:       %s\n", post_xattr);
//           LOG(LOG_INFO, "New post xattr:  %s\n", post_str);
//           fprintf(file_info->outfd, "Updating %s objid xattr to %s\n", files->filename, objects->new_objid);
//           fprintf(file_info->outfd, "Updating %s post xattr  to %s\n", files->filename, post_str);
//           //To do:
//           // remove old object
//        //}
//	files = files->next;
//     }
//     objects=objects->next;
//  }
//  return 0;
//}
//
///******************************************************************************
// * * Name free_objects
// * * 
// * * Free objects link list memory
// * ******************************************************************************/
//void free_objects(repack_objects *objects)
//{
//   repack_objects *temp_obj=NULL;
//   obj_files *temp_files;
//   obj_files  *object;
//
//   while((temp_obj=objects)!=NULL) {
//      object = objects->files_ptr;
//      while((temp_files=object) != NULL) {
//         object = object->next;
//         free(temp_files);
//      }
//      objects = objects->next;
//      free(temp_obj);
//   }
//}
//
///******************************************************************************
// *  * Name:  print_usage
// *  ******************************************************************************/
//void print_usage()
//{
//  fprintf(stderr,"Usage: ./marfs_repack -d packed_log_filename -o log_file [-h]\n\n");
//  fprintf(stderr, "where -h = help\n\n");
//}
//
///******************************************************************************
//* Name get_marfs_path
//* This function, given a metadata path, determines the fuse mount path for a 
//* file and returns it via the marfs pointer. 
//* 
//******************************************************************************/
//void get_marfs_path(char * patht, char *marfs) {
//  char *mnt_top = marfs_config->mnt_top;
//  MarFS_Namespace *ns;
//  NSIterator ns_iter;
//  ns_iter = namespace_iterator();
//  char the_path[MAX_PATH_LENGTH] = {0};
//  char ending[MAX_PATH_LENGTH] = {0};
//  int i;
//  int index;
//
//  while((ns = namespace_next(&ns_iter))){
//    if (strstr(patht, ns->md_path)){
//
//      // build path using mount point and md_path
//      strcat(the_path, mnt_top);
//      strcat(the_path, ns->mnt_path);
//
//      for (i = strlen(ns->md_path); i < strlen(patht); i++){
//        index = i - strlen(ns->md_path);
//        ending[index] = *(patht+i);
//      }
//
//      ending[index+1] = '\0';
//      strcat(the_path, ending);
//      strcpy(marfs,the_path);
//      break;
//    }
//  }
//}
//
