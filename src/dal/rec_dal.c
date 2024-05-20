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

#include "erasureUtils_auto_config.h"
#ifdef DEBUG_DAL
#define DEBUG DEBUG_DAL
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "rec_dal"
#include "logging/logging.h"

#include "dal.h"
#include "metainfo.h"
#include "ne/ne.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <pwd.h>
#include <unistd.h>

//   -------------    REC DEFINITIONS    -------------

#define IO_SIZE 1048576 // Preferred I/O Size

//   -------------    REC CONTEXT    -------------

typedef struct rec_dal_context_struct
{
  ne_ctxt nctxt;        // Context for underlying ne instance
  char *lmap;           // String mapping dal locations to ne locations
  char *meta_sfx;       // Suffix to add for meta files
  DAL_location max_loc; // Maximum pod/cap/block/scatter values
  ne_erasure epat;      // Erasure configuration of underlying ne instance
  int rdonly;           // whether we should open read handles as NE_RDALL or NE_RDONLY handles
} * REC_DAL_CTXT;

typedef struct rec_block_context_struct
{
  ne_handle d_handle; // Data object handle (if open)
  ne_handle m_handle; // Meta object handle (if open)
  REC_DAL_CTXT dctxt; // Context for dal
  char *objID;        // Object identifier
  ne_location loc;    // Object location in underlying ne instance
  ne_mode nmode;      // Mode in which this block was opened in underlying ne instance
  DAL_MODE mode;      // Mode in which this block was opened
  int m_acc;          // If the meta handle has been accessed (read or write)
  int d_acc;          // If the data handle has been accessed (read or write)
} * REC_BLOCK_CTXT;

//   -------------    REC INTERNAL FUNCTIONS    -------------

/** (INTERNAL HELPER FUNCTION)
 * Simple check of limits to ensure we don't overrun allocated strings
 * @param DAL_location loc : Location to be checked
 * @param DAL_location* max_loc : Reference to the maximum acceptable location value
 * @return int : Zero if the location is acceptable, -1 otherwise
 */
static int check_loc_limits(DAL_location loc, const DAL_location *max_loc)
{
  //
  if (loc.pod > max_loc->pod)
  {
    LOG(LOG_ERR, "pod value of %d exceeds limit of %d\n", loc.pod, max_loc->pod);
    return -1;
  }
  if (loc.cap > max_loc->cap)
  {
    LOG(LOG_ERR, "cap value of %d exceeds limit of %d\n", loc.cap, max_loc->cap);
    return -1;
  }
  if (loc.block > max_loc->block)
  {
    LOG(LOG_ERR, "block value of %d exceeds limit of %d\n", loc.block, max_loc->block);
    return -1;
  }
  if (loc.scatter > max_loc->scatter)
  {
    LOG(LOG_ERR, "scatter value of %d exceeds limit of %d\n", loc.scatter, max_loc->scatter);
    return -1;
  }
  return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Calculate the number of decimal digits required to represent a given value
 * @param int value : Integer value to be represented in decimal
 * @return int : Number of decimal digits required, or -1 on a bounds error
 */
static int num_digits(int value)
{
  if (value < 0)
  {
    return -1;
  } // negative values not permitted
  if (value < 10)
  {
    return 1;
  }
  if (value < 100)
  {
    return 2;
  }
  if (value < 1000)
  {
    return 3;
  }
  if (value < 10000)
  {
    return 4;
  }
  if (value < 100000)
  {
    return 5;
  }
  // only support values up to 5 digits long
  return -1;
}

/** (INTERNAL HELPER FUNCTION)
 * Return the value within a DAL_location specified by a character
 * @param char m : First letter of the DAL_location field to be returned
 * @param DAL_location old_loc : DAL_location to extract data from
 * @return int : Value of requested field on success, -1 otherwise
 */
static int conv_one(char m, DAL_location old_loc)
{
  switch (m)
  {
  case 'p':
    return old_loc.pod;
    break;
  case 'b':
    return old_loc.block;
    break;
  case 'c':
    return old_loc.cap;
    break;
  case 's':
    return old_loc.scatter;
    break;
  }
  return -1;
}

/** (INTERNAL HELPER FUNCTION)
 * Map a DAL_location to a ne_location using the given mapping
 * @param char *map : String mapping from DAL_location to ne_location
 * @param DAL_location old_loc : DAL_location to extract data from
 * @return int : ne_location that corresponds to old_loc using the given mapping
 * (NOTE: fields within the return value may be -1 if an invalid mapping was
 * given.)
 */
static ne_location conv_loc(char *map, DAL_location old_loc)
{
  ne_location loc = {.pod = conv_one(map[0], old_loc), .cap = conv_one(map[1], old_loc), .scatter = conv_one(map[2], old_loc)};
  return loc;
}

/** (INTERNAL HELPER FUNCTION)
 * Close a given ne_handle and attempt to rebuild if any blocks are detected to
 * have errors.
 * @param ne_handle handle : Handle to close
 * @param REC_BLOCK_CTXT bctxt : Context used to reopen and rebuild the handle
 * (NOTE: handle must be one of the handles within bctxt)
 * @param char *objID : Object identifier for reopening the handle
 * (NOTE: objID should be the object identifier used to initially open handle)
 * @return int : Number of blocks with errors that could not be rebuilt on
 * success, and -1 on a failure.
 */
static int close_handle(ne_handle handle, REC_BLOCK_CTXT bctxt, char *objID)
{
  // close the handle
  LOG(LOG_INFO, "closing %s\n", objID);
  ne_state *sref = calloc(1, sizeof(ne_state));
  int ret = ne_close(handle, NULL, sref);
  // attempt to rebuild blocks with errors
  if (ret > 0)
  {
    LOG(LOG_ERR, "errors detected\n");
    // reopen handle (should be with all the same arguments as before)
    handle = ne_open(bctxt->dctxt->nctxt, objID, bctxt->loc, bctxt->dctxt->epat, bctxt->nmode);
    if (handle != NULL)
    {
      if (!ne_seed_status(handle, sref))
      {
        ne_rebuild(handle, NULL, NULL);
      }
      // determine how many block errors could not be fixed
      ret = ne_close(handle, NULL, NULL);
    }
  }
  free(sref);
  return ret;
}


int rec_set_meta_internal(BLOCK_CTXT ctxt, const char *meta_buf, size_t size)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }
  REC_BLOCK_CTXT bctxt = (REC_BLOCK_CTXT)ctxt; // should have been passed a rec context

  // abort, unless we're writing
  if (bctxt->mode != DAL_WRITE && bctxt->mode != DAL_REBUILD)
  {
    LOG(LOG_ERR, "Can only perform get ops on a DAL_READ or DAL_REBUILD block handle!\n");
    return -1;
  }

  // write the provided buffer out to the handle
  if (ne_write(bctxt->m_handle, meta_buf, size) != size)
  {
    LOG(LOG_ERR, "failed to write buffer to meta object: \"%s%s\" (%s)\n", bctxt->objID, bctxt->dctxt->meta_sfx, strerror(errno));
    return -1;
  }
  bctxt->m_acc = 1;

  return 0;
}

ssize_t rec_get_meta_internal(BLOCK_CTXT ctxt, char *meta_buf, size_t size)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }
  REC_BLOCK_CTXT bctxt = (REC_BLOCK_CTXT)ctxt; // should have been passed a rec context

  // abort, unless we're reading
  if (bctxt->mode != DAL_READ && bctxt->mode != DAL_METAREAD)
  {
    LOG(LOG_ERR, "Can only perform get ops on a DAL_READ or DAL_METAREAD block handle!\n");
    return -1;
  }

  // seek to given offset
  if (ne_seek(bctxt->m_handle, 0) != 0)
  {
    LOG(LOG_ERR, "failed to seek to offset %d in \"%s%s\" (%s)\n", 0, bctxt->objID, bctxt->dctxt->meta_sfx, strerror(errno));
    return -1;
  }

  // read data from the handle to the provided buffer
  ssize_t result = ne_read(bctxt->m_handle, meta_buf, size);

  bctxt->m_acc = 1;

  return result;
}



//   -------------    REC IMPLEMENTATION    -------------

int rec_verify(DAL_CTXT ctxt, int flags)
{
  errno = ENOSYS;
  return -1;
}

int rec_migrate(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline)
{
  errno = ENOSYS;
  return -1;
}

int rec_del(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
  LOG(LOG_INFO, "rec_del\n");
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return -1;
  }
  REC_DAL_CTXT dctxt = (REC_DAL_CTXT)ctxt; // should have been passed a rec context

  // ensure location is valid
  if (check_loc_limits(location, &(dctxt->max_loc)))
  {
    errno = EDOM;
    return -1;
  }

  // map DAL_location to corresponding ne_location
  ne_location loc = conv_loc(dctxt->lmap, location);

  // append the meta suffix and check for success
  char *metaID = malloc(sizeof(objID) + sizeof(dctxt->meta_sfx));
  if (metaID == NULL)
  {
    return -1;
  }
  if (sprintf(metaID, "%s%s", objID, dctxt->meta_sfx) < 0)
  {
    LOG(LOG_ERR, "failed to append meta suffix \"%s\" to objID \"%s\"!\n", dctxt->meta_sfx, objID);
    errno = EBADF;
    free(metaID);
    return -1;
  }

  // delete the meta object and check for success
  if (ne_delete(dctxt->nctxt, metaID, loc))
  {
    LOG(LOG_ERR, "deleting meta object \"%s\" failed (%s)\n", metaID, strerror(errno));
    free(metaID);
    return -1;
  }

  // delete the data object and check for success
  if (ne_delete(dctxt->nctxt, objID, loc))
  {
    LOG(LOG_ERR, "deleting data object \"%s\" failed (%s)\n", objID, strerror(errno));
    free(metaID);
    return -1;
  }

  free(metaID);
  return 0;
}

int rec_stat(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
  LOG(LOG_INFO, "rec_stat\n");
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return -1;
  }
  REC_DAL_CTXT dctxt = (REC_DAL_CTXT)ctxt; // should have been passed a rec context

  // ensure location is valid
  if (check_loc_limits(location, &(dctxt->max_loc)))
  {
    errno = EDOM;
    return -1;
  }

  // map DAL_location to corresponding ne_location
  ne_location loc = conv_loc(dctxt->lmap, location);

  // stat the object and check for success
  ne_handle handle = ne_stat(dctxt->nctxt, objID, loc);
  if (handle == NULL)
  {
    LOG(LOG_ERR, "stat failed (%s)\n", strerror(errno));
    return -1;
  }
  ne_abort(handle);
  return 0;
}

int rec_cleanup(DAL dal)
{
  LOG(LOG_INFO, "rec_cleanup\n");
  if (dal == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal!\n");
    return -1;
  }
  REC_DAL_CTXT dctxt = (REC_DAL_CTXT)dal->ctxt; // should have been passed a rec context

  // terminate libne instance
  if (ne_term(dctxt->nctxt))
  {
    return -1;
  }
  // free DAL context state
  free(dctxt->lmap);
  free(dctxt->meta_sfx);
  free(dctxt);
  // free DAL struct and its associated state
  free(dal);
  return 0;
}

BLOCK_CTXT rec_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID)
{
  LOG(LOG_INFO, "rec_open %d\n", mode);
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return NULL;
  }
  REC_DAL_CTXT dctxt = (REC_DAL_CTXT)ctxt; // should have been passed a rec context

  // ensure location is valid
  if (check_loc_limits(location, &(dctxt->max_loc)))
  {
    errno = EDOM;
    return NULL;
  }

  // allocate space for a new BLOCK context
  REC_BLOCK_CTXT bctxt = malloc(sizeof(struct rec_block_context_struct));
  if (bctxt == NULL)
  {
    return NULL;
  } // malloc will set errno

  // populate BLOCK context fields
  bctxt->dctxt = dctxt;
  bctxt->loc = conv_loc(dctxt->lmap, location);
  bctxt->mode = mode;
  bctxt->objID = malloc(sizeof(objID) + sizeof(dctxt->meta_sfx));
  strcpy(bctxt->objID, objID);
  bctxt->m_acc = 0;
  bctxt->d_acc = 0;

  // append the meta suffix and check for success
  char *res = strcat(bctxt->objID, dctxt->meta_sfx);
  if (res != bctxt->objID)
  {
    LOG(LOG_ERR, "failed to append meta suffix \"%s\" to objID \"%s\"!\n", dctxt->meta_sfx, objID);
    errno = EBADF;
    free(bctxt->objID);
    free(bctxt);
    return NULL;
  }

  if (mode == DAL_READ || mode == DAL_METAREAD)
  {
    if (dctxt->rdonly)
    {
      bctxt->nmode = NE_RDONLY;
    }
    else
    {
      bctxt->nmode = NE_RDALL;
    }
  }
  else
  {
    bctxt->nmode = NE_WRALL;
  }

  // open the meta handle and check for success
  bctxt->m_handle = ne_open(dctxt->nctxt, bctxt->objID, bctxt->loc, dctxt->epat, bctxt->nmode);
  if (bctxt->m_handle == NULL)
  {
    LOG(LOG_ERR, "failed to open meta object: \"%s\" (%s)\n", bctxt->objID, strerror(errno));
    if (mode == DAL_METAREAD)
    {
      free(bctxt->objID);
      free(bctxt);
      return NULL;
    }
  }
  // remove any suffix in the simplest possible manner
  *(bctxt->objID + strlen(objID)) = '\0';

  if (mode != DAL_METAREAD)
  {
    // open the data handle and check for success
    bctxt->d_handle = ne_open(dctxt->nctxt, bctxt->objID, bctxt->loc, dctxt->epat, bctxt->nmode);
    if (bctxt->d_handle == NULL)
    {
      LOG(LOG_ERR, "failed to open object: \"%s\" (%s)\n", bctxt->objID, strerror(errno));
      free(bctxt->objID);
      free(bctxt);
      return NULL;
    }
  }

  // finally, return a reference to our BLOCK context
  return bctxt;
}

int rec_set_meta(BLOCK_CTXT ctxt, const meta_info* source)
{
  LOG(LOG_INFO, "rec_set_meta\n");
  return dal_set_meta_helper( rec_set_meta_internal, ctxt, source );
}

int rec_get_meta(BLOCK_CTXT ctxt, meta_info* target)
{
  LOG(LOG_INFO, "rec_get_meta\n");
  return dal_get_meta_helper( rec_get_meta_internal, ctxt, target );
}

int rec_put(BLOCK_CTXT ctxt, const void *buf, size_t size)
{
  LOG(LOG_INFO, "rec_put\n");
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }
  REC_BLOCK_CTXT bctxt = (REC_BLOCK_CTXT)ctxt; // should have been passed a rec context

  // abort, unless we're writing
  if (bctxt->mode != DAL_WRITE && bctxt->mode != DAL_REBUILD)
  {
    LOG(LOG_ERR, "Can only perform get ops on a DAL_READ or DAL_REBUILD block handle!\n");
    return -1;
  }

  // just a write to our pre-opened FD
  if (ne_write(bctxt->d_handle, buf, size) != size)
  {
    LOG(LOG_ERR, "write to \"%s\" failed (%s)\n", bctxt->objID, strerror(errno));
    return -1;
  }

  bctxt->d_acc = 1;

  return 0;
}

ssize_t rec_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset)
{
  LOG(LOG_INFO, "rec_get\n");
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }
  REC_BLOCK_CTXT bctxt = (REC_BLOCK_CTXT)ctxt; // should have been passed a rec context

  // abort, unless we're reading
  if (bctxt->mode != DAL_READ)
  {
    LOG(LOG_ERR, "Can only perform get ops on a DAL_READ block handle!\n");
    return -1;
  }

  // seek to given offset
  if (ne_seek(bctxt->d_handle, offset) != offset)
  {
    LOG(LOG_ERR, "failed to seek to offset %d in \"%s\" (%s)\n", offset, bctxt->objID, strerror(errno));
    return -1;
  }

  // read from our open handle
  ssize_t result = ne_read(bctxt->d_handle, buf, size);

  bctxt->d_acc = 1;

  return result;
}

int rec_abort(BLOCK_CTXT ctxt)
{
  LOG(LOG_INFO, "rec_abort\n");
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }
  REC_BLOCK_CTXT bctxt = (REC_BLOCK_CTXT)ctxt; // should have been passed a posix context

  // abort meta and data handle, note but bypass failure
  int ret = 0;
  if (ne_abort(bctxt->m_handle))
  {
    LOG(LOG_ERR, "failed to abort meta handle \"%s%s\" (%s)\n", bctxt->objID, bctxt->dctxt->meta_sfx, strerror(errno));
    ret--;
  }
  if (bctxt->mode != DAL_METAREAD && ne_abort(bctxt->d_handle))
  {
    LOG(LOG_ERR, "failed to abort data handle \"%s\" (%s)\n", bctxt->objID, strerror(errno));
    ret--;
  }

  // free state
  free(bctxt->objID);
  free(bctxt);
  return ret;
}

int rec_close(BLOCK_CTXT ctxt)
{
  LOG(LOG_INFO, "rec_close\n");
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }
  REC_BLOCK_CTXT bctxt = (REC_BLOCK_CTXT)ctxt; // should have been passed a posix context

  // append meta suffix and check for success (in case of meta rebuild)
  int len = strlen(bctxt->objID);
  if (strcat(bctxt->objID, bctxt->dctxt->meta_sfx) != bctxt->objID)
  {
    LOG(LOG_ERR, "failed to append meta suffix \"%s\" to objID \"%s\"!\n", bctxt->dctxt->meta_sfx, bctxt->objID);
    errno = EBADF;
    return -1;
  }

  // attempt to close our meta handle and check for success. Abort if not accessed
  int ret = 0;
  if (bctxt->m_acc)
  {
    if (close_handle(bctxt->m_handle, bctxt, bctxt->objID))
    {
      LOG(LOG_ERR, "failed to close meta handle \"%s%s\" (%s)\n", bctxt->objID, bctxt->dctxt->meta_sfx, strerror(errno));
      *(bctxt->objID + len) = '\0';
      return -1;
    }
  }
  else if (ne_abort(bctxt->m_handle))
  {
    LOG(LOG_ERR, "failed to abort meta handle \"%s\" (%s)\n", bctxt->objID, strerror(errno));
    ret--;
  }

  // make sure no suffix remains
  *(bctxt->objID + len) = '\0';

  // attempt to close our data handle and check for success. Abort if not accessed
  if (bctxt->mode != DAL_METAREAD)
  {
    if (bctxt->d_acc)
    {
      if (close_handle(bctxt->d_handle, bctxt, bctxt->objID))
      {
        LOG(LOG_ERR, "failed to close data handle \"%s\" (%s)\n", bctxt->objID, strerror(errno));
        return -1;
      }
    }
    else if (ne_abort(bctxt->d_handle))
    {
      LOG(LOG_ERR, "failed to abort data handle \"%s\" (%s)\n", bctxt->objID, strerror(errno));
      ret--;
    }
  }

  // free state
  free(bctxt->objID);
  free(bctxt);
  return ret;
}

//   -------------    REC INITIALIZATION    -------------

DAL rec_dal_init(xmlNode *root, DAL_location max_loc)
{
  LOG(LOG_INFO, "rec_dal_init\n");
  // first, calculate the number of digits required for pod/cap/block/scatter
  int d_pod = num_digits(max_loc.pod);
  if (d_pod < 1)
  {
    errno = EDOM;
    LOG(LOG_ERR, "detected an inappropriate value for maximum pod: %d\n", max_loc.pod);
    return NULL;
  }
  int d_cap = num_digits(max_loc.cap);
  if (d_cap < 1)
  {
    errno = EDOM;
    LOG(LOG_ERR, "detected an inappropriate value for maximum cap: %d\n", max_loc.cap);
    return NULL;
  }
  int d_block = num_digits(max_loc.block);
  if (d_block < 1)
  {
    errno = EDOM;
    LOG(LOG_ERR, "detected an inappropriate value for maximum block: %d\n", max_loc.block);
    return NULL;
  }
  int d_scatter = num_digits(max_loc.scatter);
  if (d_scatter < 1)
  {
    errno = EDOM;
    LOG(LOG_ERR, "detected an inappropriate value for maximum scatter: %d\n", max_loc.scatter);
    return NULL;
  }

  // make sure we start on a 'hostname' node
  if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "DAL", 4) == 0)
  {
    // allocate space for our context struct
    REC_DAL_CTXT dctxt = malloc(sizeof(struct rec_dal_context_struct));
    if (dctxt == NULL)
    {
      return NULL;
    } // malloc will set errno

    //initialize mapping string before populating it with data from config
    dctxt->lmap = calloc(4, sizeof(char));
    dctxt->lmap[3] = '\0';

    dctxt->rdonly = 0;
    size_t io_size = IO_SIZE;
    int N = -1;
    int E = -1;
    xmlNode *node = root;
    // find the io size, meta suffix, and location mapping data from config
    while (node != NULL)
    {
      if (node->type == XML_ELEMENT_NODE && strncmp((char *)node->name, "io_size", 8) == 0)
      {
        io_size = atol((char *)node->children->content);
      }
      else if (node->type == XML_ELEMENT_NODE && strncmp((char *)node->name, "N", 2) == 0)
      {
        N = atol((char *)node->children->content);
      }
      else if (node->type == XML_ELEMENT_NODE && strncmp((char *)node->name, "E", 2) == 0)
      {
        E = atol((char *)node->children->content);
      }
      else if (node->type == XML_ELEMENT_NODE && strncmp((char *)node->name, "meta_sfx", 9) == 0)
      {
        dctxt->meta_sfx = strdup((char *)node->children->content);
      }
      else if (node->type == XML_ELEMENT_NODE && strncmp((char *)node->name, "pod", 4) == 0)
      {
        dctxt->lmap[0] = ((char *)node->children->content)[0];
      }
      else if (node->type == XML_ELEMENT_NODE && strncmp((char *)node->name, "cap", 4) == 0)
      {
        dctxt->lmap[1] = ((char *)node->children->content)[0];
      }
      else if (node->type == XML_ELEMENT_NODE && strncmp((char *)node->name, "scatter", 8) == 0)
      {
        dctxt->lmap[2] = ((char *)node->children->content)[0];
      }
      else if (strncmp((char *)node->name, "ne_rdonly", 10) == 0)
      {
        dctxt->rdonly = 1;
      }

      node = node->next;
    }

    // fail if any location mapping data is missing
    int i;
    for (i = 0; i < 3; i++)
    {
      if (dctxt->lmap[i] != 'p' && dctxt->lmap[i] != 'b' && dctxt->lmap[i] != 'c' && dctxt->lmap[i] != 's')
      {
        LOG(LOG_ERR, "Invalid location mapping given: pod - %c, cap - %c, scatter - %c\n", dctxt->lmap[0], dctxt->lmap[1], dctxt->lmap[2]);
        if (dctxt->meta_sfx != NULL)
        {
          free(dctxt->meta_sfx);
        }
        free(dctxt->lmap);
        free(dctxt);
        return NULL;
      }
    }

    // fail if invalid striping pattern given
    if (N < 1 || E < 1)
    {
      LOG(LOG_ERR, "Invalid striping pattern given: N - %d, E - %d\n", N, E);
      if (dctxt->meta_sfx != NULL)
      {
        free(dctxt->meta_sfx);
      }
      free(dctxt->lmap);
      free(dctxt);
      return NULL;
    }

    // set default meta suffix if none included in config
    if (dctxt->meta_sfx == NULL)
    {
      dctxt->meta_sfx = strdup("_meta");
    }

    // populate other DAL context fields
    dctxt->max_loc = max_loc;
    ne_erasure epat = {.N = N, .E = E, .O = 1, .partsz = 1024};
    dctxt->epat = epat;

    // initialize underlying libne instance
    dctxt->nctxt = ne_init(root, conv_loc(dctxt->lmap, max_loc), 6, NULL);
    if (dctxt->nctxt == NULL)
    {
      free(dctxt->meta_sfx);
      free(dctxt->lmap);
      free(dctxt);
    }

    // allocate and populate a new DAL structure
    DAL rdal = malloc(sizeof(struct DAL_struct));
    if (rdal == NULL)
    {
      LOG(LOG_ERR, "failed to allocate space for a DAL_struct\n");
      free(dctxt->nctxt);
      free(dctxt->meta_sfx);
      free(dctxt->lmap);
      free(dctxt);
      return NULL;
    } // malloc will set errno
    rdal->name = "s3";
    rdal->ctxt = (DAL_CTXT)dctxt;
    rdal->io_size = io_size;
    rdal->verify = rec_verify;
    rdal->migrate = rec_migrate;
    rdal->open = rec_open;
    rdal->set_meta = rec_set_meta;
    rdal->get_meta = rec_get_meta;
    rdal->put = rec_put;
    rdal->get = rec_get;
    rdal->abort = rec_abort;
    rdal->close = rec_close;
    rdal->del = rec_del;
    rdal->stat = rec_stat;
    rdal->cleanup = rec_cleanup;
    return rdal;
  }
  else
  {
    LOG(LOG_ERR, "root node of config is expected to be \"DAL\"\n");
  }
  errno = EINVAL;
  return NULL; // failure of any condition check fails the function
}
