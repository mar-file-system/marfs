#ifndef __MARFS_COPYRIGHT_H__
#define __MARFS_COPYRIGHT_H__

/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#endif

#include "marfs_auto_config.h"
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <pwd.h>
#include <errno.h>


#define PROGNAME "marfs-verifyconf"
#define OUTPREFX PROGNAME ": "


int main(int argc, const char** argv) {
   errno = 0; // init to zero (apparently not guaranteed)
   char* config_path = getenv( "MARFS_CONFIG_PATH" ); // check for config env var
   char* ns_path = ".";
   char* user_name = NULL;
   char mdalcheck = 0;
   char necheck = 0;
   char recurse = 0;
   char fix = 0;

   // parse all position-independent arguments
   char pr_usage = 0;
   int c;
   while ((c = getopt(argc, (char* const*)argv, "c:n:u:mdrfah")) != -1) {
      switch (c) {
      case 'c':
         config_path = optarg;
         break;
      case 'n':
         ns_path = optarg;
         break;
      case 'u':
         user_name = optarg;
         break;
      case 'm':
         mdalcheck = 1;
         break;
      case 'd':
         necheck = 1;
         break;
      case 'r':
         recurse = 1;
         break;
      case 'f':
         fix = 1;
         break;
      case 'a':
         mdalcheck = 1;
         necheck = 1;
         recurse = 1;
         fix = 1;
         break;
      case '?':
         printf( OUTPREFX "ERROR: Unrecognized cmdline argument: \'%c\'\n", optopt );
      case 'h':
         pr_usage = 1;
         break;
      default:
         printf("ERROR: Failed to parse command line options\n");
         return -1;
      }
   }

   // check if we need to print usage info
   if (pr_usage) {
      printf(OUTPREFX "Usage info --\n");
      printf(OUTPREFX "%s [-c configpath] [-n namespace] [-u username] [-m] [-d] [-r] [-f] [-a] [-h]\n", PROGNAME);
      printf(OUTPREFX "   -c : Path of the MarFS config file ( will use MARFS_CONFIG_PATH env var, if omitted )\n");
      printf(OUTPREFX "   -n : NS target to be verified ( will assume rootNS, \".\", if omitted )\n");
      printf(OUTPREFX "   -u : Username to switch to prior to verification\n");
      printf(OUTPREFX "   -m : Verify the MDAL security of encoutered namespaces\n");
      printf(OUTPREFX "   -d : Verify the DAL / LibNE Ctxt of encoutered namespaces\n");
      printf(OUTPREFX "   -r : Recurse through subspaces of the target NS\n");
      printf(OUTPREFX "   -f : Attempt to correct encountered problems ( otherwise, just note and complain )\n");
      printf(OUTPREFX "   -a : Equivalent to specifying '-m', '-d', '-r', and '-f'\n");
      printf(OUTPREFX "   -h : Print this usage info\n");
      return -1;
   }

   // verify that a config was defined
   if (config_path == NULL) {
      printf(OUTPREFX "ERROR: no config path defined ( '-c' arg or MARFS_CONFIG_PATH env var )\n");
      return -1;
   }

   // potentially change user ID
   if ( user_name ) {
      // lookup user info
      struct passwd* pswd = getpwnam( user_name );
      if ( pswd == NULL ) {
         printf(OUTPREFX "ERROR: Failed to lookup info for target user: \"%s\"\n", user_name);
         return -1;
      }
      if ( geteuid() != pswd->pw_uid ) {
         // switch to the target GID + UID
         printf(OUTPREFX "Switching to user \"%s\" (UID : %lu  |  GID : %lu)\n", user_name, (unsigned long)pswd->pw_uid, (unsigned long)pswd->pw_gid);
         if ( setgid( pswd->pw_gid ) ) {
            printf(OUTPREFX "ERROR: Failed to perform setgid call to target group\n");
            return -1;
         }
         if ( setuid( pswd->pw_uid ) ) {
            printf(OUTPREFX "ERROR: Failed to perform setuid call to target user\n");
            return -1;
         }
      }
   }
   else if ( geteuid() == 0 ) {
      printf(OUTPREFX "ERROR: To perform verification as 'root', run with a '-u root' argument.\n");
      return -1;
   }

   // read in the marfs config
   pthread_mutex_t erasurelock;
   if ( pthread_mutex_init( &erasurelock, NULL ) ) {
      printf( OUTPREFX "ERROR: failed to initialize erasure lock\n" );
      return -1;
   }
   marfs_config* config = config_init(config_path,&erasurelock);
   if (config == NULL) {
      printf(OUTPREFX "ERROR: Failed to initialize config: \"%s\" ( %s )\n",
         config_path, strerror(errno));
      if ( errno == ENOENT  ||  errno == EACCES ) {
         if ( errno == ENOENT ) {
            printf(OUTPREFX "       Recommendation -- It is very likely that this is caused by missing MDAL and/or DAL \n"
                   OUTPREFX "                         root paths ( or by an entirely missing config file ).\n" );
         }
         else {
            printf(OUTPREFX "       Recommendation -- It is very likely that this is caused by MDAL and/or DAL root \n"
                   OUTPREFX "                         paths ( or their parent paths ) with restrictive permissions.\n" );
         }
         printf(OUTPREFX "                         Look for the '<ns_root>' and '<sec_root>' paths in your MarFS \n"
                OUTPREFX "                         Config file.  Those paths must exist and be accessible to the \n"
                OUTPREFX "                         running user.\n" );
      }
      else {
         printf(OUTPREFX "       Recommendation -- Try building with 'config' debugging enabled ( '--enable-debugCONFIG' \n"
                OUTPREFX "                         argument to the autoconf 'configure' binary at the root of this repo )\n"
                OUTPREFX "                         to get a more explicit indication of why this has failed\n" );
      }
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // verify the config
   int verres = config_verify(config, ns_path, mdalcheck, necheck, recurse, fix);
   if ( config_term(config) ) {
      printf(OUTPREFX "WARNING: Failed to properly terminate MarFS config ( %s )\n", strerror(errno));
   }
   if ( verres < 0 ) {
      printf(OUTPREFX "ERROR: Failed to verify config: \"%s\" ( %s )\n",
         config_path, strerror(errno));
   }
   else if ( verres ) {
      printf(OUTPREFX "WARNING: %d Uncorrected Config Errors Remain\n", verres);
   }
   else {
      printf(OUTPREFX "Config Verified\n");
   }
   pthread_mutex_destroy(&erasurelock);
   return verres;
}

