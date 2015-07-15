/*
 * 
 *   example-2.c
 *
 *   XML config file parser example 2
 *
 *   Ron Croonenberg rocr@lanl.gov
 *   High Performance Computing (HPC-3)
 *   Los Alamos National Laboratory
 *
 *
 *   06-08-2015:        initial start rocr@lanl.gov
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

config = (struct config *)malloc(sizeof(struct config));


memset(&h_page,     0x00, sizeof(struct line));							// don't want any pointers on the loose
memset(&pseudo_h,   0x00, sizeof(struct line));							// ditto
memset(&fld_nm_lst, 0x00, sizeof(struct line));							// ditto


if (argc == 2) {
   parseConfigFile(argv[1], CREATE_STRUCT_PATHS, &h_page, &fld_nm_lst, config, QUIET);		// create the structure paths for parsing/populating
   freeHeaderFile(h_page.next);									// free the header page

   // play with the config structire a bit
   printf("repo name: %s\n", config->namespace[0].repo[0].name);
   printf("repo name: %s\n", config->namespace[0].repo[1].name);
   printf("repo name: %s\n", config->namespace[1].repo[0].name);

   freeConfigStructContent(config);								// free the contents of the config structure
   free(config);											// free the config structure itself
   }
else
   printf("Usage: ./example-2 <config file>\n");
}
