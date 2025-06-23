/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "dal/dal.h"
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>

int main(int argc, char **argv)
{

  xmlDoc *doc = NULL;
  xmlNode *root_element = NULL;

  /*
   * this initialize the library and check potential ABI mismatches
   * between the version it was compiled for and the actual shared
   * library used.
   */
  LIBXML_TEST_VERSION

  /*parse the file and get the DOM */
  doc = xmlReadFile("./testing/rec_config.xml", NULL, XML_PARSE_NOBLANKS);

  if (doc == NULL)
  {
    printf("error: could not parse file %s\n", "./dal/testing/rec_config.xml");
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

  // Open, write to, and set meta info for a specific block
  void *writebuffer = calloc(10, 1024);
  if (writebuffer == NULL)
  {
    printf("error: failed to allocate write buffer\n");
    return -1;
  }
  BLOCK_CTXT block = dal->open(dal->ctxt, DAL_WRITE, maxloc, "obj");
  if (block == NULL)
  {
    printf("error: failed to open block context for write: %s\n", strerror(errno));
    return -1;
  }
  if (dal->put(block, writebuffer, (10 * 1024)))
  {
    printf("warning: put did not return expected value\n");
  }
  meta_info meta_val = { .N = 3, .E = 1, .O = 3, .partsz = 4096, .versz = 1048576, .blocksz = 10485760, .crcsum = 1234567, .totsz = 7654321 };
  if (dal->set_meta(block, &meta_val))
  {
    printf("warning: set_meta did not return expected value\n");
  }
  if (dal->close(block))
  {
    printf("error: failed to close block write context: %s\n", strerror(errno));
    return -1;
  }

  // Open the same block for read and verify all values
  void *readbuffer = malloc(sizeof(char) * 10 * 1024);
  if (readbuffer == NULL)
  {
    printf("error: failed to allocate read buffer\n");
    return -1;
  }
  block = dal->open(dal->ctxt, DAL_READ, maxloc, "obj");
  if (block == NULL)
  {
    printf("error: failed to open block context for read: %s\n", strerror(errno));
    return -1;
  }
  if (dal->get(block, readbuffer, (10 * 1024), 0) != (10 * 1024))
  {
    printf("warning: get did not return expected value\n");
  }
  if (memcmp(writebuffer, readbuffer, (10 * 1024)))
  {
    printf("warning: retrieved data does not match written!\n");
  }
  meta_info readmeta;
  if (dal->get_meta(block, &readmeta))
  {
    printf("warning: get_meta returned an unexpected value\n");
  }
  if (cmp_minfo(&meta_val, &readmeta))
  {
    printf("warning: retrieved meta value does not match written!\n");
  }
  if (dal->close(block))
  {
    printf("error: failed to close block read context: %s\n", strerror(errno));
    return -1;
  }

  // Obtain random data to write before aborting
  void *randbuffer = calloc(10, 1024);
  if (randbuffer == NULL)
  {
    printf("error failed to allocate random buffer\n");
    return -1;
  }
  int rfd;
  if ((rfd = open("/dev/urandom", O_RDONLY)) == -1)
  {
    printf("error: failed to open /dev/random: %s\n", strerror(errno));
    return -1;
  }
  int rdres = read(rfd, randbuffer, (10 * 1024));
  if (rdres != (10 * 1024))
  {
    printf("warning: reading from /dev/random did not return expected value: %d\n", rdres);
  }
  if (close(rfd))
  {
    printf("error: failed to close /dev/random: %s\n", strerror(errno));
    return -1;
  }

  // Open, write random data to, and set meta info for a specific block
  block = dal->open(dal->ctxt, DAL_WRITE, maxloc, "obj");
  if (block == NULL)
  {
    printf("error: failed to open block context for random data write: %s\n", strerror(errno));
    return -1;
  }
  if (dal->put(block, randbuffer, (10 * 1024)))
  {
    printf("warning: put did not return expected value\n");
  }
  meta_info meta_val_2 = { .N = 3, .E = 1, .O = 4, .partsz = 14096, .versz = 10576, .blocksz = 1048570, .crcsum = 1234567, .totsz = 7654321 };
  if (dal->set_meta(block, &meta_val_2))
  {
    printf("warning: set_meta did not return expected value\n");
  }
  if (dal->abort(block))
  {
    printf("error: failed to abort block write context: %s\n", strerror(errno));
    return -1;
  }

  // Open the same block for read and verify all values
  // We should receive all the same information from the same set of operations as before, since all new data was aborted
  if (readbuffer == NULL)
  {
    printf("error: failed to allocate read buffer\n");
    return -1;
  }
  block = dal->open(dal->ctxt, DAL_READ, maxloc, "obj");
  if (block == NULL)
  {
    printf("error: failed to open block context for read: %s\n", strerror(errno));
    return -1;
  }
  int res;
  if ((res = dal->get(block, readbuffer, (10 * 1024), 0) != (10 * 1024)))
  {
    printf("error: get did not return expected value %d\n", res);
    return -1;
  }
  if (memcmp(writebuffer, readbuffer, (10 * 1024)))
  {
    printf("warning: retrieved data does not match written!\n");
  }
  int gmres = dal->get_meta(block, &readmeta);
  if (gmres)
  {
    printf("error: get_meta returned an unexpected value: %d, %s\n", gmres, (char *)readbuffer);
    return -1;
    ;
  }
  if (cmp_minfo(&meta_val, &readmeta))
  {
    printf("warning: retrieved meta value does not match written! \n");
  }
  if (dal->close(block))
  {
    printf("error: failed to close block read context: %s\n", strerror(errno));
    return -1;
  }

  // Delete the block we created
  if (dal->del(dal->ctxt, maxloc, "obj"))
  {
    printf("warning: del failed!\n");
  }

  // Free the DAL
  if (dal->cleanup(dal))
  {
    printf("error: failed to cleanup DAL\n");
    return -1;
  }

  /*free the document */
  free(writebuffer);
  free(readbuffer);
  free(randbuffer);

  return 0;
}
