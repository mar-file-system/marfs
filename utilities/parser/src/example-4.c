/*
 * 
 *   example-4.c
 *
 *   XML config file parser example 2
 *
 *   Ron Croonenberg rocr@lanl.gov
 *   High Performance Computing (HPC-3)
 *   Los Alamos National Laboratory
 *
 *
 *   06-30-2015:	initial start rocr@lanl.gov
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
//    It would work on another config file, but just returns NULL every time.
//

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "parse-inc/config-structs.h"

#include "confpars-structs.h"
#include "confpars.h"
#include "parse-types.h"
#include "parsedata.h"


main(int argc, char *argv[])
{
struct line h_page, pseudo_h, fld_nm_lst;							// for internal use
struct config *config;										// always need one of these

memset(&h_page,     0x00, sizeof(struct line));							// clear header page
memset(&pseudo_h,   0x00, sizeof(struct line));							// clear pseudo headers
memset(&fld_nm_lst, 0x00, sizeof(struct line));							// clear field names list

// User part starts here, the statements above are ALWAYS needed

struct namespace **myNamespaceList, *myNamespace;                                                             
struct repo **myRepoList, *myRepo;
int i;

config = (struct config *)malloc(sizeof(struct config));					// create our config structure

if (argc == 2) {
   parseConfigFile(argv[1], CREATE_STRUCT_PATHS, &h_page, &fld_nm_lst, config, QUIET);		// create the structure paths for parsing/populating verbose
   freeHeaderFile(h_page.next);									// free the header page

   // play with the config structire a bit
   printf("My name spaces: \n");
   i = 0;
   myNamespaceList = (struct namespace **)listObjByName("namespace", config);			// get the list of pointers to namespaces
   while (myNamespaceList[i] != (struct namespace *)NULL) {
      myNamespace = (struct namespace *)myNamespaceList[i];
      printf("\t%s\n", myNamespace->name);
      i++;
      }
   free(myNamespaceList);
   printf("\n");

   // play some more
   printf("My repos: \n");
   i = 0;
   myRepoList = (struct repo **)listObjByName("repo", config);					// get list of pointers to repos
   while (myRepoList[i] != (struct repo *)NULL){
      myRepo = (struct repo *)myRepoList[i];
      printf("\t%s\n", myRepo->name);
      i++;
      }
   free(myRepoList);

   freeConfigStructContent(config);								// free the config structure
   }
else
   printf("Usage: ./example-2 <XML config file>\n");
}
