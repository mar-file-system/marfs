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
