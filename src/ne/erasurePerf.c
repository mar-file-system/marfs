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
#include <sys/time.h>
#include <libgen.h>
#include <string.h>

#include "ne.h"

int main(int argc, const char **argv)
{
  if (argc < 3 || argc > 5)
  {
    printf("usage: %s N E [O] [partsz]\n", argv[0]);
    return -1;
  }

  // Parse erasure info
  ne_erasure epat = {.N = atoi(argv[1]), .E = atoi(argv[2]), .O = 1, .partsz = 1024};
  if (argc >= 4)
  {
    epat.O = atoi(argv[3]);
  }
  if (argc >= 5)
  {
    epat.partsz = atoi(argv[4]);
  }

  // Form config file path
  printf("%s\n", argv[0]);
  char *d_name = dirname(strdup(argv[0]));
  char *config_path = malloc(sizeof(char) * (strlen(d_name) + 28));
  if (config_path == NULL)
  {
    return -1;
  }
  if (sprintf(config_path, "%s/../testing/perf_config.xml", d_name) < 0)
  {
    return -1;
  }

  ne_location loc = {.pod = 0, .cap = 0, .scatter = 0};

  int iosz = 943718;

  xmlDoc *doc = NULL;
  xmlNode *root_element = NULL;

  /*
   * this initialize the library and check potential ABI mismatches
   * between the version it was compiled for and the actual shared
   * library used.
   */
  LIBXML_TEST_VERSION

  /*parse the file and get the DOM */
  doc = xmlReadFile(config_path, NULL, XML_PARSE_NOBLANKS);

  if (doc == NULL)
  {
    printf("error: could not parse file %s\n", config_path);
    return -1;
  }

  //Get the root element node
  root_element = xmlDocGetRootElement(doc);

  // Initialize libne
  ne_ctxt ctxt = ne_init(root_element, loc, epat.N + epat.E, NULL);
  if (ctxt == NULL)
  {
    printf("ERROR: Failed to initialize ne_ctxt!\n");
    return -1;
  }

  // Initialize a zeroed out buffer to read from
  void *iobuff = calloc(1, iosz);
  if (iobuff == NULL)
  {
    printf("ERROR: Failed to allocate space for an iobuffer!\n");
    return -1;
  }

  // Time a write operation
  struct timeval beg;
  gettimeofday(&beg, NULL);
  ne_handle handle = ne_open(ctxt, "", loc, epat, NE_WRALL);
  if (handle == NULL)
  {
    printf("ERROR: Failed to open a write handle!\n");
    return -1;
  }
  if (ne_write(handle, iobuff, iosz) != iosz)
  {
    printf("ERROR: Unexpected return value from ne_write!\n");
    return -1;
  }
  if (ne_close(handle, NULL, NULL) < 0)
  {
    printf("ERROR: Failure of ne_close!\n");
    return -1;
  }
  struct timeval end;
  gettimeofday(&end, NULL);
  double wr_time = (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6;

  // Time a read operation
  gettimeofday(&beg, NULL);
  handle = ne_open(ctxt, "", loc, epat, NE_RDONLY);
  if (handle == NULL)
  {
    printf("ERROR: Failed to open a read handle!\n");
    return -1;
  }
  if (ne_read(handle, iobuff, iosz) != iosz)
  {
    printf("ERROR: Unexpected return value from ne_read!\n");
    return -1;
  }
  if (ne_close(handle, NULL, NULL) < 0)
  {
    printf("ERROR: Failure of ne_close!\n");
    return -1;
  }
  gettimeofday(&end, NULL);
  double rd_time = (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6;

  ne_state *sref = calloc(1, sizeof(ne_state));

  // Time a read operation that verifies every block
  gettimeofday(&beg, NULL);
  handle = ne_open(ctxt, "", loc, epat, NE_RDALL);
  if (handle == NULL)
  {
    printf("ERROR: Failed to open a verify handle!\n");
    return -1;
  }
  if (ne_read(handle, iobuff, iosz) != iosz)
  {
    printf("ERROR: Unexpected return value from ne_read!\n");
    return -1;
  }
  if (ne_close(handle, NULL, sref) < 0)
  {
    printf("ERROR: Failure of ne_close!\n");
    return -1;
  }
  gettimeofday(&end, NULL);
  double ver_time = (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6;

  // Time a rebuild operation
  gettimeofday(&beg, NULL);
  handle = ne_open(ctxt, "", loc, epat, NE_REBUILD);
  if (handle == NULL)
  {
    printf("ERROR: Failed to open a verify handle!\n");
    return -1;
  }
  if (ne_seed_status(handle, sref))
  {
    printf("ERROR: Unexpected return value from ne_seed_status!\n");
    return -1;
  }
  if (ne_rebuild(handle, NULL, NULL) < 0)
  {
    printf("ERROR: Unexpected return value from ne_rebuild!\n");
    return -1;
  }
  if (ne_close(handle, NULL, NULL) < 0)
  {
    printf("ERROR: Failure of ne_close!\n");
    return -1;
  }
  gettimeofday(&end, NULL);
  double reb_time = (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6;

  // Output results
  printf("write time: %.6f, read time: %.6f, verify time: %.6f, rebuild time: %.6f\n", wr_time, rd_time, ver_time, reb_time);

  ne_term(ctxt);

  /* Free the xml Doc */
  xmlFreeDoc(doc);
  /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
  xmlCleanupParser();

  return 0;
}
