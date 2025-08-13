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
#include <errno.h>
#include <libs3.h>

#define DATASIZE 50000

// #define DATASIZE 1048580

static S3Status statusG;

static S3Status propertiesCallback(const S3ResponseProperties *properties, void *callbackData)
{
  return S3StatusOK;
}

static void completeCallback(S3Status status, const S3ErrorDetails *error, void *callbackData)
{
  statusG = status;
}

static S3ResponseHandler delHandler = {
    &propertiesCallback,
    &completeCallback,

};

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
  doc = xmlReadFile("./testing/s3_config.xml", NULL, XML_PARSE_NOBLANKS);

  if (doc == NULL)
  {
    printf("error: could not parse file %s\n", "./dal/testing/s3_config.xml");
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
    if (errno = ENONET)
    {
      return 0;
    }
    return -1;
  }

  int ret = 0;
  if ((ret = dal->verify(dal->ctxt, 1)))
  {
    printf("error: failed to initially verify DAL: %d issues detected\n", ret);
    return -1;
  }

  printf("deleting bucket \"b1.1.1\"\n");
  int i = 5;
  do
  {
    S3_delete_bucket(S3ProtocolHTTP, S3UriStylePath, "test", "test", NULL, NULL, "b1.1.1", "us-east-1", NULL, 0, &delHandler, NULL);
    i--;
  } while (S3_status_is_retryable(statusG) && i >= 0);

  if (statusG != S3StatusOK)
  {
    printf("failed to delete bucket \"b1.1.1\" (%s)\n", S3_get_status_name(statusG));
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

  return 0;
}
