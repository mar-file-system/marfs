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

#include "ne/ne.h"
#include <unistd.h>
#include <stdio.h>
#include <string.h>

// sentinel values to ensure good data transfer
char tail_sent = 'T';
unsigned int crc_sent = UINT_MAX;

size_t fill_buffer(size_t prev_data, size_t iosz, size_t partsz, void *buffer)
{
  size_t to_write = iosz;
  unsigned int parts = (prev_data / partsz);
  printf("Writing %zu bytes of part %u to buffer...\n", to_write, parts);
  // loop over parts, filling in data
  while (to_write > 0)
  {
    size_t prev_pfill = prev_data % partsz;
    // check if we need to write out a head sentinel
    if (prev_pfill < sizeof(unsigned int))
    {
      size_t fill_size = sizeof(unsigned int) - prev_pfill;
      if (fill_size > to_write)
      {
        fill_size = to_write;
      }
      //printf( "   %zu bytes of header value %u at offset %zu\n", fill_size, parts, prev_pfill );
      memcpy(buffer, ((void *)(&parts)) + prev_pfill, fill_size);
      buffer += fill_size;
      prev_data += fill_size;
      prev_pfill += fill_size;
      to_write -= fill_size;
    }
    // check if we need to write out any filler data
    if (to_write > 0)
    {
      // check if our data to be written is less than a complete filler
      size_t fill_size = (to_write < ((partsz - prev_pfill) - sizeof(char))) ? to_write : ((partsz - prev_pfill) - sizeof(char));
      //printf( "   %zu bytes of zero-fill\n", fill_size );
      bzero(buffer, fill_size);
      buffer += fill_size;
      prev_data += fill_size;
      prev_pfill += fill_size;
      to_write -= fill_size;
    }
    // check if we need to write out a tail sentinel
    if (to_write > 0)
    {
      memcpy(buffer, &tail_sent, sizeof(char));
      //printf( "   1 byte tail\n" );
      buffer += sizeof(char);
      prev_data += sizeof(char);
      prev_pfill += sizeof(char);
      to_write -= sizeof(char);
    }
    printf("Filled part %d, up to total data size %zu\n", parts, prev_data);
    // sanity check that we have properly filled a part
    if (prev_pfill == partsz)
    {
      parts++;
    }
    else if (to_write != 0)
    {
      printf("ERROR: data remains to write, but we haven't filled a part!\n");
      return 0;
    }
  }

  return iosz;
}

size_t verify_data(size_t prev_ver, size_t partsz, size_t buffsz, void *buffer)
{
  // create dummy zero buffer for comparisons
  char *zerobuff = calloc(1, partsz);
  if (zerobuff == NULL)
  {
    printf("ERROR: failed to allocate space for dummy zero buffer!\n");
    return 0;
  }
  // determine how much to write ( read == filling buffers from IO, write == filling from erasure/data parts )
  size_t data_to_chk = buffsz;
  unsigned int parts = (prev_ver / partsz);
  printf("Verifying %zu bytes starting at part %u in buffer...\n", buffsz, parts);
  // loop over parts, filling in data
  while (data_to_chk > 0)
  {
    size_t prev_pfill = prev_ver % partsz;
    // check if we need to write out a head sentinel
    if (prev_pfill < sizeof(unsigned int))
    {
      size_t fill_size = sizeof(unsigned int) - prev_pfill;
      if (fill_size > data_to_chk)
      {
        fill_size = data_to_chk;
      }
      if (memcmp(buffer, ((void *)(&parts)) + prev_pfill, fill_size))
      {
        printf("ERROR: Failed to verify %zu bytes of header for part %u (offset=%zu)!\n", fill_size, parts, prev_pfill);
        free(zerobuff);
        return 0;
      }
      buffer += fill_size;
      prev_ver += fill_size;
      prev_pfill += fill_size;
      data_to_chk -= fill_size;
    }
    // check if we need to write out any filler data
    if (data_to_chk > 0)
    {
      // check if our data to be written is less than a complete filler
      size_t fill_size = (data_to_chk < ((partsz - prev_pfill) - sizeof(char))) ? data_to_chk : ((partsz - prev_pfill) - sizeof(char));
      if (memcmp(buffer, zerobuff, fill_size))
      {
        // find the explicit offset of the error
        char *bref = (char *)buffer;
        while (*bref == 0)
        {
          prev_pfill++;
          bref++;
        }
        printf("ERROR: Failed to verify filler of part %d (offset=%zu)! ( %d != 0 )\n", parts, prev_pfill, *bref);
        free(zerobuff);
        return 0;
      }
      buffer += fill_size;
      prev_ver += fill_size;
      prev_pfill += fill_size;
      data_to_chk -= fill_size;
    }
    // check if we need to write out a tail sentinel
    if (data_to_chk > 0)
    {
      if (memcmp(buffer, &tail_sent, sizeof(char)))
      {
        printf("ERROR: failed to verify tail of part %d (offset=%zu)!\n", parts, prev_pfill);
        free(zerobuff);
        return 0;
      }
      buffer += sizeof(char);
      prev_ver += sizeof(char);
      prev_pfill += sizeof(char);
      data_to_chk -= sizeof(char);
    }
    // sanity check that we have properly filled a part
    if (prev_pfill == partsz)
    {
      parts++;
    }
    else if (data_to_chk != 0)
    {
      printf("ERROR: data remains to verify, but we haven't completed a part!\n");
      free(zerobuff);
      return 0;
    }
  }

  printf("checked up to part %u in buffer\n", parts);

  free(zerobuff);
  return buffsz;
}

int test_values(xmlNode *root_element, ne_erasure *epat, size_t iosz, size_t partsz)
{
  printf("\nTesting basic libne capabilities with iosz=%zu / partsz=%zu\n", iosz, partsz);

  void *iobuff = calloc(1, iosz);
  if (iobuff == NULL)
  {
    printf("ERROR: Failed to allocate space for an iobuffer!\n");
    return -1;
  }

  ne_location max_loc = {.pod = 1, .cap = 2, .scatter = 3};
  ne_location cur_loc = {.pod = 0, .cap = 1, .scatter = 2};

  // create a new libne ctxt
  ne_ctxt ctxt = ne_init(root_element, max_loc, epat->N + epat->E, NULL);
  if (ctxt == NULL)
  {
    printf("ERROR: Failed to initialize ne_ctxt 2!\n");
    return -1;
  }

  // open a write handle
  printf("Writing out data stripe...\n");
  ne_handle write_handle = ne_open(ctxt, "", cur_loc, *epat, NE_WRALL);
  if (write_handle == NULL)
  {
    printf("ERROR: Failed to open a write handle!\n");
    return -1;
  }
  // write out data
  int i;
  int iocnt = 10;
  for (i = 0; i < iocnt; i++)
  {
    // populate our data buffer
    if (iosz != fill_buffer(iosz * i, iosz, partsz, iobuff))
    {
      printf("ERROR: Failed to populate data buffer 2 %d!\n", i);
      return -1;
    }
    // write our data buffer
    if (iosz != ne_write(write_handle, iobuff, iosz))
    {
      printf("ERROR: Unexpected return value from ne_write 2 %d!\n", i);
      return -1;
    }
  }
  // close our handle
  if (ne_close(write_handle, NULL, NULL) < 0)
  {
    printf("ERROR: Failure of ne_close 2!\n");
    return -1;
  }
  printf("...write handle 2 closed...\n");

  // open a read handle to verify our data
  printf("...Verifying written data (RDONLY)...\n");
  ne_handle read_handle = ne_open(ctxt, "", cur_loc, *epat, NE_RDONLY);
  if (read_handle == NULL)
  {
    printf("ERROR: Failed to open a read handle!\n");
    return -1;
  }
  // read out data
  for (i = 0; i < iocnt; i++)
  {
    // read our into our data buffer
    ssize_t readsz = 0;
    if ((readsz = ne_read(read_handle, iobuff, iosz)) != iosz)
    {
      printf("ERROR: Unexpected return value from ne_read: %zd\n", readsz);
      return -1;
    }
  }
  // seek back to the beginning and re-read
  printf("...seeking to offset zero to re-read...\n");
  if (ne_seek(read_handle, 0) != 0)
  {
    printf("ERROR: Failed to seek to zero!\n");
    return -1;
  }
  // read all data, again
  for (i = 0; i < iocnt; i++)
  {
    // read our into our data buffer
    ssize_t readsz = 0;
    if ((readsz = ne_read(read_handle, iobuff, iosz)) != iosz)
    {
      printf("ERROR: Unexpected return value from ne_read: %zd\n", readsz);
      return -1;
    }
  }
  // close our handle
  if (ne_close(read_handle, NULL, NULL) < 0)
  {
    printf("ERROR: Failure of ne_close!\n");
    return -1;
  }

  // open a read handle to verify our data
  printf("...Verifying written data (RDALL)...\n");
  read_handle = ne_open(ctxt, "", cur_loc, *epat, NE_RDALL);
  if (read_handle == NULL)
  {
    printf("ERROR: Failed to open a read handle!\n");
    return -1;
  }
  // read out data
  for (i = 0; i < iocnt; i++)
  {
    // read our into our data buffer
    if (iosz != ne_read(read_handle, iobuff, iosz))
    {
      printf("ERROR: Unexpected return value from ne_read!\n");
      return -1;
    }
  }
  // close our handle
  if (ne_close(read_handle, NULL, NULL) < 0)
  {
    printf("ERROR: Failure of ne_close!\n");
    return -1;
  }

  // open a handle by stating (no epat struct)
  printf("...Verifying written data (NE_STAT/RD_ONLY)...\n");
  ne_handle stat_handle = ne_stat(ctxt, "", cur_loc);
  if (stat_handle == NULL)
  {
    printf("ERROR: Failed to open a ne_stat handle!\n");
    return -1;
  }
  // convert to a RD_ONLY handle
  read_handle = ne_convert_handle(stat_handle, NE_RDONLY);
  if (read_handle == NULL)
  {
    printf("ERROR: Failed to convert to a read handle!\n");
    return -1;
  }
  // read out data
  for (i = 0; i < iocnt; i++)
  {
    // read our into our data buffer
    if (iosz != ne_read(read_handle, iobuff, iosz))
    {
      printf("ERROR: Unexpected return value from ne_read!\n");
      return -1;
    }
  }
  // close our handle
  if (ne_close(read_handle, NULL, NULL) < 0)
  {
    printf("ERROR: Failure of ne_close!\n");
    return -1;
  }

  // delete our test object
  if (ne_delete(ctxt, "", cur_loc))
  {
    printf("ERROR: Failed to delete written object!\n");
    return -1;
  }

  // close our ne_ctxt
  if (ne_term(ctxt))
  {
    printf("ERROR: Failure of ne_term!\n");
    return -1;
  }
  free(iobuff);

  return 0;
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
  doc = xmlReadFile("./testing/noop_config.xml", NULL, XML_PARSE_NOBLANKS);

  if (doc == NULL)
  {
    printf("error: could not parse file %s\n", "./dal/testing/noop_config.xml");
    return -1;
  }

  /*Get the root element node */
  root_element = xmlDocGetRootElement(doc);

  // Test with a small partsz and larger, aligned iosz
  size_t iosz = 8196;
  size_t partsz = 4096;
  ne_erasure epat = {.N = 10, .E = 2, .O = 2, .partsz = 1048572};
  if (test_values(root_element, &epat, iosz, partsz))
  {
    return -1;
  }
  // Test with a larger partsz and much larger iosz
  iosz = 262143; // note that this value divides cleanly into 1048572 ( the io_size of the posix DAL - 4byte CRC )
  if (test_values(root_element, &epat, iosz, partsz))
  {
    return -1;
  }

  /* Free the xml Doc */
  xmlFreeDoc(doc);
  /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
  xmlCleanupParser();

  return 0;
}
