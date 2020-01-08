/*
 * This file is part of MarFS, which is released under the BSD license.
 * Copyright (c) 2015, Los Alamos National Security (LANS), LLC
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
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
 * -----
 *  NOTE:
 *  -----
 *  MarFS uses libaws4c for Amazon S3 object communication. The original version
 *  is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
 *  LANS, LLC added functionality to the original work. The original work plus
 *  LANS, LLC contributions is found at https://github.com/jti-lanl/aws4c.
 *  GNU licenses can be found at <http://www.gnu.org/licenses/>.
 *  From Los Alamos National Security, LLC:
 *  LA-CC-15-039
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
 *
 * ------------------------------------------------------------------------------
 *
 *  This is a simple program intended to produce the path of the Multi-Component 
 *  object referenced by a given MarFS file path/offset.
 *
 *  Original Author: Lei Cao (github branch: rdma_merge_leiBranchOrigin)
 *  Edited by gransom
 *
 *  */

#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

// As we'll be calling some internal MarFS functions directly, include the actual source
#include "common.c"
#include "dal.c"

void show_usage(char* prog_name)
{
   fprintf(stderr, "Usage: %s [option] <PATH>\n", prog_name);
   fprintf(stderr, "\n");
   fprintf(stderr, "\toptions:\n");
   fprintf(stderr, "\t\t-h                     help\n");
   fprintf(stderr, "\t\t-c [ <chunk number> ]  chunk number of the object to reference\n");
   fprintf(stderr, "\t\t-o [ <offset> ]        offset within the file at wich to refernce the object (overrides '-c')\n");
   fprintf(stderr, "\n");
}

int main(int argc, char* argv[])
{
   int    c;
   int    usage = 0;
   char*  path = NULL;
   int    chunk_no = 0;
   curl_off_t offset = 0;
   while((c = getopt(argc, argv, "hc:o:")) != -1)
   {
      switch(c)
      {
         case 'h':
            usage = 1;
            break;

         case 'c':
            chunk_no = strtol(optarg, NULL, 10);
            break;

         case 'o':
            offset = strtol(optarg, NULL, 10);
            break;

         default:
            usage = 1;
            break;
      }
   }

   for ( c = optind; c < argc; c++ ) {
      if ( path == NULL ) {
         // just reference, we don't expect to modify this path
         path = argv[c];
      }
      else {
         printf( "WARNING: ignoring unrecognized arg: \"%s\"\n", argv[c] );
      }
   }
   
   if (usage || path == NULL)
   {
      show_usage(argv[0]);
      return 0;
   }

   struct stat st;

   //first need to read config
   if (read_configuration())
   {
      printf("ERROR: Reading Marfs configuration failed\n");
      return -1;
   }

   if (init_xattr_specs())
   {
      printf("ERROR: init_xattr_specs failed\n");
      return -1;
   }

   printf("Full Path    -- %s\n", path );

   // verify that this is actually a marfs path
   const char* marfs_path = marfs_sub_path(path);
   if (! marfs_path) {
      printf("ERROR: path '%s' doesn't appear to be a MarFS path.  "
             "Check config-file?\n", path);
      return -1;
   }

   printf("MarFS Path   -- %s\n", marfs_path );

   //first we check if the file exists
   int mode = marfs_getattr(marfs_path, &st);
   if (mode) {
      printf("ERROR: couldn't stat marfs-path '%s': %s\n",
             marfs_path, strerror(errno));
      return -1;
   }

   MarFS_FileHandle fh;
   memset(&fh, 0, sizeof(fh));

   int rc = marfs_open(marfs_path, &fh, O_RDONLY, 0);
   if (rc) {
      printf("ERROR: failed to open marfs file: \"%s\" (%s)\n", marfs_path, strerror(errno) );
      return -1;
   }

   printf("File Size    -- %zd\n", fh.info.st.st_size );

   // if an offset was specified, determine the appropriate chunk_no
   if ( offset ) {
      if ( chunk_no ) {
         printf( "WARNING: ignoring '-c' specification of %d in favor of offset %zd\n", chunk_no, offset );
      }
      chunk_no = offset / ( (&fh)->info.pre.repo->chunk_size - MARFS_REC_UNI_SIZE );
   }
   // specified offset has precendence over chunk_no
   else if ( chunk_no ) {
      offset = ( (&fh)->info.pre.repo->chunk_size - MARFS_REC_UNI_SIZE ) * chunk_no;
   }

   // check again, explicitly setting the chunk if necessary
   if ( chunk_no ) {
      //manually adjust chunk_no in the file handle
      (&fh)->info.pre.chunk_no = chunk_no;
      update_pre(&(fh.info.pre));
   }

   printf("Object Chunk -- %d\n", chunk_no );
   printf("File Offset  -- %zd\n", offset );

   // just for user reference, check if our current offset will take us beyond the file end
   if ( fh.info.st.st_size < offset ) {
      printf( "WARNING: The current offset appears to be beyond the logical end of the file, performing lookup anyway\n" );
   }

   // manually initialize the MC context info
   rc = init_data( &fh );
   if (rc) {
      printf("ERROR: failed to initialize marfs file-handle for data access: \"%s\" (%s)\n", marfs_path, strerror(errno) );
      marfs_release( "required for some reason, even though it's never used", &fh );
      return -1;
   }

   // trigger an update of the MC object path
   rc  = DAL_OP(update_object_location, &fh);
   if (rc) {
      printf("ERROR: failed to update obj location info for marfs file-handle: \"%s\" (%s)\n", marfs_path, strerror(errno) );
      marfs_release( "required for some reason, even though it's never used", &fh );
      return -1;
   }

#if 0  // MC_SOCKETS should be allowed, too!
   // before we reach into the context structs directly, make sure this is actually a MC obj
   if ( strncmp( fh.dal_handle.dal->name, "MC", 3 ) ) {
      printf("ERROR: marfs file-handle does not indicate the expected DAL name of 'MC': \"%s\"\n", fh.dal_handle.dal->name );
      marfs_release( "required for some reason, even though it's never used", &fh );
      return -1;
   }
#endif

   // pull the path out of the open file handle
   printf("Object Path  -- %s\n", MC_CONTEXT(&(fh.dal_handle.ctx))->path_template );

   // close the file handle
   rc = marfs_release( "required for some reason, even though it's never used", &fh );
   if (rc) {
      printf("ERROR: failed to release marfs file-handle: \"%s\" (%s)\n", marfs_path, strerror(errno) );
      return -1;
   }

   return 0;
}
