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
#include <ftw.h>
#include <stdlib.h>

// WARNING: error-prone and ugly method of deleting files, written for simplicity only
//          don't replicate this junk into ANY production code paths!
size_t dirlistpos = 0;
char** dirlist = NULL;
int ftwnotedir( const char* fpath, const struct stat* sb, int typeflag ) {
   if ( typeflag != FTW_D ) {
      if ( unlink( fpath ) ) {
         printf( "ERROR: Failed to unlink non-directory path: \"%s\"\n", fpath );
         return -1;
      }
      printf( "Removed non-directory during tree deletion: \"%s\"\n", fpath );
      return 0;
   }
   dirlist[dirlistpos] = strdup( fpath );
   if ( dirlist[dirlistpos] == NULL ) {
      printf( "Failed to duplicate dir name: \"%s\"\n", fpath );
      return -1;
   }
   dirlistpos++;
   if ( dirlistpos >= 1024 ) { printf( "Dirlist has insufficient lenght!\n" ); return -1; }
   return 0;
}
int deletefstree( const char* basepath ) {
   dirlist = malloc( sizeof(char*) * 1024 );
   if ( dirlist == NULL ) {
      printf( "Failed to allocate dirlist\n" );
      return -1;
   }
   if ( ftw( basepath, ftwnotedir, 100 ) ) {
      printf( "Failed to identify reference dirs of \"%s\"\n", basepath );
      return -1;
   }
   int retval = 0;
   while ( dirlistpos ) {
      dirlistpos--;
      if ( strcmp( dirlist[dirlistpos], basepath ) ) {
         printf( "Deleting: \"%s\"\n", dirlist[dirlistpos] );
         if ( rmdir( dirlist[dirlistpos] ) ) {
            printf( "ERROR -- failed to delete \"%s\"\n", dirlist[dirlistpos] );
            retval = -1;
         }
      }
      free( dirlist[dirlistpos] );
   }
   free( dirlist );
   return retval;
}


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
  doc = xmlReadFile("./testing/timer_config.xml", NULL, XML_PARSE_NOBLANKS);

  if (doc == NULL)
  {
    printf("error: could not parse file %s\n", "./dal/testing/timer_config.xml");
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
  int i;
  for (i = 0; i < 1024; i++)
  {
    if (dal->put(block, writebuffer, (10 * 1024)))
    {
      printf("error: put did not return expected value\n");
      return -1;
    }
  }
  meta_info meta_val = { .N = 3, .E = 1, .O = 3, .partsz = 4096, .versz = 1048576, .blocksz = 10485760, .crcsum = 1234567, .totsz = 7654321 };
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

  // Delete the timing data output
  if ( deletefstree( "./timing_test_data_TMP" ) ) {
    printf( "Failed to delete timing output data: \"./timing_test_data_TMP\"\n" );
    return -1;
  }

  // Delete timing data root
  if ( rmdir( "./timing_test_data_TMP" ) ) {
    printf( "Failed to delete timing output data root dir: \"./timing_test_data_TMP\"\n" );
    return -1;
  }

  return 0;
}
