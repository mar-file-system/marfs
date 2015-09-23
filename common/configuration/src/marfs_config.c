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
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include <unistd.h>             // getopt
#include <stdlib.h>             // exit
#include <stdio.h>

// #include "logging.h"
#include "marfs_configuration.h"



int
show_usage(char* prog_name) {
   fprintf(stderr, "Usage: %s [option]\n", prog_name);
   fprintf(stderr, "\n");
   fprintf(stderr, "\toptions:\n");
   fprintf(stderr, "\t\t-h                     help\n");
   fprintf(stderr, "\t\t-r [ <repo_name> ]     show named Repo (or all repo-names)\n");
   fprintf(stderr, "\t\t-n [ <ns_name> ]       show named NS   (or all NS-names)\n");
   fprintf(stderr, "\t\t-t [ <setting-name> ]  show named top-level parm (or all)\n");
   fprintf(stderr, "\n");
   fprintf(stderr, "\tCan't use more than one of -r, -n, -t\n");
   fprintf(stderr, "\tUse an empty-string to \n");
}


int main( int argc, char *argv[] ) {
   int    c;
   int    digit_optind = 0;

   int    usage = 0;
   int    err = 0;
   int    repo_opt = 0;
   char*  repo_name = NULL;
   int    ns_opt = 0;
   char*  ns_name = NULL;
   int    top_opt = 0;
   char*  top_name = NULL;

   char** name_ptr = NULL;

   // NOTE: We avoid using the GNU-specific extension that supports
   //     optional getopt args.  Instead, because there's only one option
   //     allowed, we can assume that anything following (i.e. the
   //     appearence of a non-option argument) is an optional argument.
   while ( (c = getopt(argc, argv, "hrnt")) != -1) {

      //              printf("\toptarg:      '%s'\n", optarg);
      //              printf("\toptind:       %d\n", optind);
      //              printf("\tthis_optind:  %d\n", optind);
      //              printf("\tdigit_optind: %d\n", digit_optind);

      switch (c) {

      case 'h':
         usage = 1;
         break;

      case 'n':
         // printf ("option n with value '%s'\n", optarg);
         ns_opt = 1;
         // ns_name = optarg;
         name_ptr = &ns_name;
         break;

      case 'r':
         // printf ("option r with value '%s'\n", optarg);
         repo_opt = 1;
         // repo_name = optarg;
         name_ptr = &repo_name;
         break;

      case 't':
         // printf ("option t with value '%s'\n", optarg);
         top_opt = 1;
         // top_name = optarg;
         name_ptr = &top_name;
         break;

      case '?':
         // getopt returns '?' when there is a problem.  In this case it
         // also prints, e.g. "getopt_test: illegal option -- z"
         fprintf(stderr, "unrecognized option '%s'\n", argv[optind -1]);
         usage = 1;
         err = 1;
         break;

      default:
         usage = 1;
         err = 1;
         fprintf(stderr, "?? getopt returned character code 0%o ??\n", c);
      }
   }

   //   printf("\n\ndone parsing\n");
   //   printf("\toptind: %d\n", optind);
   //   printf("\targc:   %d\n", argc);

   if (usage) {
      show_usage(argv[0]);
      return err;
   }
   else if ((repo_opt + ns_opt + top_opt) > 1) {
      show_usage(argv[0]);
      return -1;
   }
   else if ((repo_opt + ns_opt + top_opt) == 0) {
      show_usage(argv[0]);
      return -1;
   }


   // --- repatriate a single non-option argument with a provided option
   if (optind == (argc -1)) {
      *name_ptr = argv[optind];
   }
   else if (optind != argc) {
      //      printf ("non-option ARGV-elements: ");
      //      while (optind < argc)
      //         printf ("%s ", argv[optind++]);
      //      printf ("\n");
      show_usage(argv[0]);
      return -1;
   }




   if (read_configuration()) {
      fprintf( stderr, "ERROR: Reading MarFS configuration failed.\n" );
      return -1;
   }


   // option:  -r [ repo_name ]
   if (repo_opt) {
      MarFS_Repo* repo;

      if (repo_name) {
         // --- show contents of named repo
         repo = find_repo_by_name(repo_name);
         if (!repo) {
            fprintf(stderr, "repo '%s' not found\n", repo_name);
            return -1;
         }
         debug_repo(repo);
      }
      else {
         // --- show names of all repos
         RepoIterator rit = repo_iterator();
         while (( repo = repo_next( &rit )) != NULL ) {
            printf("%s\n", repo->name);
         }
      }
      
      return 0;
   }


   // option:  -n [ ns_name ]
   if (ns_opt) {
      MarFS_Namespace* ns;

      if (ns_name) {
         // --- show contents of named namespace
         ns = find_namespace_by_name(ns_name);
         if (ns)
            debug_namespace(ns);
         else {
            // maybe the "name" was really a mount path?
            ns = find_namespace_by_mnt_path(ns_name);
            if (ns)
               debug_namespace(ns);
            else {
               fprintf(stderr, "ns '%s' not found\n", ns_name);
               return -1;
            }
         }
      }
      else {
         // --- show names of all namespaces
         NSIterator nit = namespace_iterator();
         while (( ns = namespace_next( &nit )) != NULL ) {
            printf("%s\n", ns->name);
         }
      }
      
      return 0;
   }


   // option:  -t [ top-level-config-option-name ]
   if (top_opt) {
      if (! top_name) {
         // --- list of all top-level options
         printf("mnt_top\n");
         printf("version\n");
         printf("name\n");
      }
      else if (!strncmp(top_name, "mnt_top", 7))
         printf("%s\n", marfs_config->mnt_top);
      else if (!strncmp(top_name, "name", 4))
         printf("%s\n", marfs_config->name);
      else if (!strncmp(top_name, "version", 7))
         printf("%d.%d\n", marfs_config->version_major, marfs_config->version_minor);
      else {
         fprintf(stderr, "unrecognized top-level config-option: '%s'\n", top_name);
         return -1;
      }

      return 0;
   }



   return 0;
}
