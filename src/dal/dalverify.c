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
