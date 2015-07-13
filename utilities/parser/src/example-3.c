/*
 * 
 *   example-3.c
 *
 *   XML config file parser example 2
 *
 *   Ron Croonenberg rocr@lanl.gov
 *   High Performance Computing (HPC-3)
 *   Los Alamos National Laboratory
 *
 *
 *   6-8-2015: initial start
 *  
 *
 *
 */

//
//    This example code is based on the ./config/config-2 example configuration
//    This example code is based on the ./config/config-2 example configuration
//    This example code is based on the ./config/config-2 example configuration
//    This example code is based on the ./config/config-2 example configuration
//    This example code is based on the ./config/config-2 example configuration
//

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "parse-inc/config-structs.h"

#include "confpars-structs.h"
#include "confpars.h"
#include "parse-types.h"



main(int argc, char *argv[])
{
struct config *config;
struct line h_page, pseudo_h, fld_nm_lst;
int idx = 0;

config = (struct config *)malloc(sizeof(struct config));


memset(&h_page,     0x00, sizeof(struct line));							// don't want any pointers on the loose
memset(&pseudo_h,   0x00, sizeof(struct line));							// ditto
memset(&fld_nm_lst, 0x00, sizeof(struct line));							// ditto


if (argc == 2) {
   parseConfigFile(argv[1], CREATE_STRUCT_PATHS, &h_page, &fld_nm_lst, config, VERBOSE);	// create the structure paths for parsing/populating verbose
   freeHeaderFile(h_page.next);									// free the header page

   // play with the config structire a bit
   printf("repo name: %s\n", config->namespace[0].repo[0].name);
   printf("repo name: %s\n", config->namespace[0].repo[1].name);
   printf("repo name: %s\n", config->namespace[1].repo[0].name);

   // a different way			(not being implemented yet)
//   while (idx >= 0) {
//      idx = findByMember(idx, 1, "name");
//printf("%d\n", idx);
//      idx++;
//      }

   freeConfigStructContent(config);								// free the config structure
   }
else
   printf("Usage: ./example-2 <config file>\n");
}
