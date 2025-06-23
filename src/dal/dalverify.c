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

#include "../marfs_auto_config.h"
#if defined(DEBUG_ALL) || defined(DEBUG_NE)
#define DEBUG
#endif
#define preFMT "%s: "

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "dal.h"

int main(int argc, const char **argv)
{
  // make sure we're root
  if (geteuid() != 0)
  {
    printf("error: must be run as root\n");
    return -1;
  }

  if (argc != 6)
  {
    printf("usage: %s config_file pod_size block_size cap_size scatter_size\n", argv[0]);
    return -1;
  }

  // set our maximum location from the given arguments
  DAL_location maxloc = {.pod = atoi(argv[2]) - 1, .block = atoi(argv[3]) - 1, .cap = atoi(argv[4]) - 1, .scatter = atoi(argv[5]) - 1};
  if (maxloc.pod < 0 || maxloc.block < 0 || maxloc.cap < 0 || maxloc.scatter < 0)
  {
    printf("error: invalid maximum location. pod: %d block: %d cap: %d scatter: %d\n", maxloc.pod, maxloc.block, maxloc.cap, maxloc.scatter);
    return -1;
  }

  xmlDoc *doc = NULL;
  xmlNode *root_element = NULL;

  /*
   * this initializes the library and check potential ABI mismatches
   * between the version it was compiled for and the actual shared
   * library used.
   */
  LIBXML_TEST_VERSION

  /*parse the file and get the DOM */
  doc = xmlReadFile(argv[1], NULL, XML_PARSE_NOBLANKS);

  if (doc == NULL)
  {
    printf("error: could not parse file %s\n", argv[1]);
    return -1;
  }

  /*Get the root element node */
  root_element = xmlDocGetRootElement(doc);

  // Initialize a posix dal instance
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

  // verify the DAL and attempt to fix any issues
  int ret = 0;
  printf("Preparing to verify dal with maximum location: pod: %d block: %d cap: %d scatter: %d\n", maxloc.pod, maxloc.block, maxloc.cap, maxloc.scatter);
  if ((ret = dal->verify(dal->ctxt, 1)))
  {
    printf("error: failed to verify DAL: %d issues detected\n", ret);
    return -1;
  }

  dal->cleanup(dal);

  printf("successfully verified DAL\n");
  return 0;
}
