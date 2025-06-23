#ifndef __MARFS_COPYRIGHT_H__
#define __MARFS_COPYRIGHT_H__

/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
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
   int flags = CFG_OWNERCHECK;

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
         flags |= CFG_MDALCHECK;
         break;
      case 'd':
         flags |= CFG_DALCHECK;
         break;
      case 'r':
         flags |= CFG_RECURSE;
         break;
      case 'f':
         flags |= CFG_FIX;
         break;
      case 'a':
         flags |= CFG_MDALCHECK;
         flags |= CFG_DALCHECK;
         flags |= CFG_RECURSE;
         flags |= CFG_FIX;
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
   int verres = config_verify(config, ns_path, flags);

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

