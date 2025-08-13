/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "dal/dal.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>

#define DATASIZE 50000

int main(int argc, char **argv)
{
  if (geteuid() != 0)
  {
    printf("error: must be run as root\n");
    return 0;
  }

  errno = 0;
  if ( mkdir( "./top_dir", 0755 )  &&  errno != EEXIST ) {
    printf("failed to create './top_dir' subdir\n" );
    return -1;
  }

  errno = 0;
  if ( mkdir( "./top_dir/sec_root", 0700 )  &&  errno != EEXIST ) {
    printf("failed to create './top_dir/sec_root' subdir\n" );
    return -1;
  }

  xmlDoc *doc = NULL;
  xmlNode *root_element = NULL;

  /*
   * this initialize the library and check potential ABI mismatches
   * between the version it was compiled for and the actual shared
   * library used.
   */
  LIBXML_TEST_VERSION

  /*parse the file and get the DOM */
  doc = xmlReadFile("./testing/verify_config.xml", NULL, XML_PARSE_NOBLANKS);

  if (doc == NULL)
  {
    printf("error: could not parse file %s\n", "./dal/testing/verify_config.xml");
    return -1;
  }

  /*Get the root element node */
  root_element = xmlDocGetRootElement(doc);

  // Initialize a posix dal instance
  DAL_location maxloc = {.pod = 1, .block = 1, .cap = 1, .scatter = 1};
  DAL dal = init_dal(root_element, maxloc);

  /* Free the xml Doc */
  xmlFreeDoc(doc);
  /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
  xmlCleanupParser();

  // check that initialization succeeded
  if (dal == NULL)
  {
    printf("error: failed to initialize DAL: %s\n", strerror(errno));
    return -1;
  }

  int ret = 0;
  if ((ret = dal->verify(dal->ctxt, 1)))
  {
    printf("error: failed to initially verify DAL: %d issues detected\n", ret);
    return -1;
  }

  printf("deleting directory \"pod1/block1/cap1/scatter1/\"\n");
  if (rmdir("./top_dir/sec_root/pod1/block1/cap1/scatter1/"))
  {
    printf("error: failed to delete \"pod1/block1/cap1/scatter1/\" (%s)\n", strerror(errno));
    return -1;
  }

  if (!(ret = dal->verify(dal->ctxt, 0)))
  {
    printf("error: verify failed to detect a missing bucket\n");
    return -1;
  }

  if ((ret = dal->verify(dal->ctxt, 1)))
  {
    printf("error: failed to verify and fix DAL: %d issues detected\n", ret);
    return -1;
  }

  // Free the DAL
  if (dal->cleanup(dal))
  {
    printf("error: failed to cleanup DAL\n");
    return -1;
  }

  // cleanup all created subdirs (ignoring errors)
  int p = 0;
  int b = 0;
  int c = 0;
  int s = 0;
  char tgtdir[1024] = {0};
  for ( ; p < 2; p++ ) {
    for( b = 0; b < 2; b++ ) {
      for( c = 0; c < 2; c++ ) {
        for( s = 0; s < 2; s++ ) {
          snprintf( tgtdir, 1024, "./top_dir/sec_root/pod%d/block%d/cap%d/scatter%d", p, b, c, s );
          rmdir( tgtdir );
        }
        snprintf( tgtdir, 1024, "./top_dir/sec_root/pod%d/block%d/cap%d", p, b, c );
        rmdir( tgtdir );
      }
      snprintf( tgtdir, 1024, "./top_dir/sec_root/pod%d/block%d", p, b );
      rmdir( tgtdir );
    }
    snprintf( tgtdir, 1024, "./top_dir/sec_root/pod%d", p );
    rmdir( tgtdir );
  }
  rmdir( "./top_dir/sec_root" );
  rmdir( "./top_dir" );

  return 0;
}
