/*
 * 
 *   mainpars.c
 *
 *   main for config file parser 
 *
 *   Ron Croonenberg rocr@lanl.gov
 *   High Performance Computing (HPC-3)
 *   Los Alamos National Laboratory
 *
 *
 *   6-8-2015:
 *    - initial start 
 *
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#ifdef DATAPARSE
#include "parse-inc/config-structs.h"
#endif

#include "confpars-structs.h"
#include "confpars.h"
#include "parse-types.h"



main(int argc, char *argv[])
{
struct line h_page, pseudo_h, fld_nm_lst;
int task;

#ifdef DATAPARSE
struct config *config;
config = (struct config *)malloc(sizeof(struct config));
#endif


memset(&h_page,     0x00, sizeof(struct line));							// don't want any pointers on the loose
memset(&pseudo_h,   0x00, sizeof(struct line));							// ditto
memset(&fld_nm_lst, 0x00, sizeof(struct line));							// ditto

//task = DISPLAY;
task = CREATE_STRUCT;

if (argc == 2) {
#ifdef DATAPARSE
   //parseConfigFile(argv[1], CREATE_STRUCT,      &h_page, &fld_nm_lst, config, VERBOSE);	// This example show the "raw" structure of what we parse
   //listHeaderFile(&h_page, NO_ORDER);								// not very usefull to the end user.
   //freeHeaderFile(h_page.next);								// free the header page
   //memset(&h_page, 0x00, sizeof(struct line));						// clear struct
   //freeHeaderFile(fld_nm_lst.next);								// free field names list
   //memset(&fld_nm_lst, 0x00, sizeof(struct line));						// clear struct

   parseConfigFile(argv[1],    CREATE_STRUCT_PATHS, &h_page, &fld_nm_lst, config, QUIET);	// create the structure paths for parsing/populating
   freeHeaderFile(h_page.next);									// free the header page
   freeConfigStructContent(config);								// free the config structure
#else
   parseConfigFile(argv[1], CREATE_STRUCT, &h_page, &fld_nm_lst, VERBOSE);			// parse prep work this parses the XML
   pseudo_h.next = listHeaderFile(&h_page, DECONSTRUCT);					// create C structures from parsed XML into pseudo headers
   listHeaderFile(&pseudo_h, GEN_PARSE_STRUCTS);						// creates the actual structures and generates confpars-structs.h for the parser
//   listHeaderFile(&pseudo_h, NO_ORDER);                                                               // display C structures derived from XML

   freeHeaderFile(h_page.next);									// free the header page
   memset(&h_page, 0x00, sizeof(struct line));
   freeHeaderFile(fld_nm_lst.next);								// free field names list
   memset(&fld_nm_lst, 0x00, sizeof(struct line));
   freeHeaderFile(pseudo_h.next);								// free the pseudo headers
   memset(&pseudo_h, 0x00, sizeof(struct line));

   parseConfigFile(argv[1],    CREATE_STRUCT_PATHS, &h_page, &fld_nm_lst, VERBOSE);		// parse for access paths
   listHeaderFile(&fld_nm_lst, GEN_STRUCT_SWITCH);						// generate access switch
   freeHeaderFile(fld_nm_lst.next);
#endif
   }
else
#ifdef DATAPARSE
   printf("Usage: ./dataparse <config file>\n");
#else
   printf("Usage: ./confparse <config file>\n");
#endif


//listHeaderFile(&h_page, NO_ORDER_DEBUG);
//listHeaderFile(&h_page, NO_ORDER);
}
