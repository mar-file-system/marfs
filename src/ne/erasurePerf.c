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

#include "../erasureUtils_auto_config.h"
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
