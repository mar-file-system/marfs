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
#include <stdlib.h>

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
   doc = xmlReadFile("./testing/config.xml", NULL, XML_PARSE_NOBLANKS);

   if (doc == NULL)
   {
      printf("error: could not parse file %s\n", "./dal/testing/config.xml");
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
   BLOCK_CTXT block = dal->open(dal->ctxt, DAL_WRITE, maxloc, "");
   if (block == NULL)
   {
      printf("error: failed to open block context for write: %s\n", strerror(errno));
      return -1;
   }
   if (dal->put(block, writebuffer, (10 * 1024)))
   {
      printf("error: put did not return expected value\n");
      return -1;
   }
   meta_info meta_val = { .N = 1, .E = 5, .O = 0, .partsz = 4023, .versz = 108576, .blocksz = 1, .crcsum = 42, .totsz = 7654321 };
   if (dal->set_meta(block, &meta_val))
   {
      printf("error: set_meta did not return expected value\n");
      return -1;
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
   block = dal->open(dal->ctxt, DAL_READ, maxloc, "");
   if (block == NULL)
   {
      printf("error: failed to open block context for read: %s\n", strerror(errno));
      return -1;
   }
   if (dal->get(block, readbuffer, (10 * 1024), 0) != (10 * 1024))
   {
      printf("error: get did not return expected value\n");
      return -1;
   }
   if (memcmp(writebuffer, readbuffer, (10 * 1024)))
   {
      printf("error: retrieved data does not match written!\n");
      return -1;
   }
   meta_info readmeta;
   if (dal->get_meta(block, &readmeta))
   {
      printf("error: get_meta returned an unexpected value\n");
      return -1;
   }
   if (cmp_minfo(&meta_val, &readmeta))
   {
      printf("error: retrieved meta value does not match written!\n");
      return -1;
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
      printf("error: reading from /dev/random did not return expected value: %d\n", rdres);
      return -1;
   }
   if (close(rfd))
   {
      printf("error: failed to close /dev/random: %s\n", strerror(errno));
      return -1;
   }

   // Open, write random data to, and set meta info for a specific block
   block = dal->open(dal->ctxt, DAL_WRITE, maxloc, "");
   if (block == NULL)
   {
      printf("error: failed to open block context for random data write: %s\n", strerror(errno));
      return -1;
   }
   if (dal->put(block, randbuffer, (10 * 1024)))
   {
      printf("error: put did not return expected value\n");
      return -1;
   }
   meta_info meta_val_2;
   cpy_minfo( &meta_val_2, &readmeta );
   meta_val_2.E = 13;
   if (dal->set_meta(block, &meta_val_2))
   {
      printf("error: set_meta did not return expected value\n");
      return -1;
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
   block = dal->open(dal->ctxt, DAL_READ, maxloc, "");
   if (block == NULL)
   {
      printf("error: failed to open block context for read: %s\n", strerror(errno));
      return -1;
   }
   if (dal->get(block, readbuffer, (10 * 1024), 0) != (10 * 1024))
   {
      printf("error: get did not return expected value\n");
      return -1;
   }
   if (memcmp(writebuffer, readbuffer, (10 * 1024)))
   {
      printf("error: retrieved data does not match written!\n");
      return -1;
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
      printf("error: retrieved meta value does not match written! \n");
      return -1;
   }
   if (dal->close(block))
   {
      printf("error: failed to close block read context: %s\n", strerror(errno));
      return -1;
   }

   // Delete the block we created
   if (dal->del(dal->ctxt, maxloc, ""))
   {
      printf("error: del failed!\n");
      return -1;
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
