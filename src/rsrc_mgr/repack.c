/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <errno.h>
#include <stdlib.h>

#include "datastream/datastream.h"
#include "rsrc_mgr/common.h"
#include "rsrc_mgr/repack.h"

/**
 * Initialize a new repackstreamer
 * @return REPACKSTREAMER : New repackstreamer, or NULL on failure
 */
REPACKSTREAMER repackstreamer_init(void) {
   // allocate a new struct
   REPACKSTREAMER repackst = malloc(sizeof(*repackst));
   // populate all struct elements
   pthread_mutex_init(&repackst->lock, NULL);
   repackst->streamcount = 10;
   repackst->streamlist = calloc(repackst->streamcount, sizeof(DATASTREAM));
   repackst->streamstatus = calloc(10, sizeof(char));
   return repackst;
}

static void repackstreamer_destroy(REPACKSTREAMER repackst) {
   free(repackst->streamstatus);
   free(repackst->streamlist);
   pthread_mutex_destroy(&repackst->lock);
   free(repackst);
}

/**
 * Checkout a repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to checkout from
 * @return DATASTREAM* : Checked out datastream, or NULL on failure
 */
DATASTREAM* repackstreamer_getstream(REPACKSTREAMER repackst) {
   // check for NULL arg
   if (repackst == NULL) {
      LOG(LOG_ERR, "Received a NULL repackstreamer ref\n");
      errno = EINVAL;
      return NULL;
   }

   // acquire struct lock
   pthread_mutex_lock(&repackst->lock);

   // check for available datastreams
   size_t index = 0;
   for (; index < repackst->streamcount; index++) {
      if (repackst->streamstatus[index] == 0) {
         repackst->streamstatus[index] = 1;
         pthread_mutex_unlock(&repackst->lock);
         LOG(LOG_INFO, "Handing out available stream at position %zu\n", index);
         return repackst->streamlist + index;
      }
   }

   // no avaialable datastreams, so we must expand our allocation (double the current count)
   LOG(LOG_INFO, "Expanding allocation to %zu streams\n", repackst->streamcount * 2);
   DATASTREAM* newstlist = realloc(repackst->streamlist, sizeof(DATASTREAM) * (repackst->streamcount * 2));
   if (newstlist == NULL) {
      LOG(LOG_ERR, "Failed to reallocate streamlist to a length of %zu entries\n", repackst->streamcount * 2);
      pthread_mutex_unlock(&repackst->lock);
      return NULL;
   }

   repackst->streamlist = newstlist; // might end up leaving this expanded, but that's fine
   char* newststatus = realloc(repackst->streamstatus, sizeof(char) * (repackst->streamcount * 2));
   if (newststatus == NULL) {
      LOG(LOG_ERR, "Failed to reallocate streamstatus to a length of %zu entries\n", repackst->streamcount * 2);
      pthread_mutex_unlock(&repackst->lock);
      return NULL;
   }

   repackst->streamstatus = newststatus;
   repackst->streamcount *= 2;

   // zero out new allocation
   size_t newpos = index; // cache the lowest, newly-allocated index
   for (; index < repackst->streamcount; index++) {
      repackst->streamlist[index] = NULL;
      repackst->streamstatus[index] = 0;
   }

   // hand out a newly-allocated stream
   repackst->streamstatus[newpos] = 1;
   pthread_mutex_unlock(&repackst->lock);
   LOG(LOG_INFO, "Handing out newly-allocated position %zu\n", newpos);
   return repackst->streamlist + newpos;
}

/**
 * Return a previously checked out repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to return to
 * @param DATASTREAM* stream : Repack datastream to return
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_returnstream(REPACKSTREAMER repackst, DATASTREAM* stream) {
   // check for NULL args
   if (repackst == NULL) {
      LOG(LOG_ERR, "Received a NULL repackstreamer ref\n");
      errno = EINVAL;
      return -1;
   }

   if (stream == NULL) {
      LOG(LOG_ERR, "Received a NULL stream ref\n");
      errno = EINVAL;
      return -1;
   }

   // acquire struct lock
   pthread_mutex_lock(&repackst->lock);

   // calculate the corresponding index of this stream
   size_t index = (size_t)(stream - repackst->streamlist);
   // sanity check the result
   if (index >= repackst->streamcount  ||  stream < repackst->streamlist) {
      LOG(LOG_ERR, "Returned stream is not a member of allocated list\n");
      pthread_mutex_unlock(&repackst->lock);
      return -1;
   }

   // ensure the stream was indeed passed out
   if (repackst->streamstatus[index] != 1) {
      LOG(LOG_ERR, "Returned stream %zu was not currently active\n", index);
      pthread_mutex_unlock(&repackst->lock);
      return -1;
   }

   // update status and return
   repackst->streamstatus[index] = 0;
   pthread_mutex_unlock(&repackst->lock);
   LOG(LOG_INFO, "Stream %zu has been returned\n", index);

   return 0;
}

/**
 * Terminate the given repackstreamer and close all associated datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to close
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_complete(REPACKSTREAMER repackst) {
   // check for NULL arg
   if (repackst == NULL) {
      LOG(LOG_ERR, "Received a NULL repackstreamer ref\n");
      errno = EINVAL;
      return -1;
   }

   // acquire struct lock
   pthread_mutex_lock(&repackst->lock);

   // iterate over all streams
   int retval = 0;
   char prevactive = 1;
   size_t index = 0;
   for (; index < repackst->streamcount; index++) {
      if (repackst->streamlist[index] != NULL) {
         // minor sanity check, not even certain that this is a true failure
         if (prevactive == 0) {
            LOG(LOG_WARNING, "Encountered active stream at index %zu with previous stream being NULL\n");
         }
         // close the active stream
         int closeres = datastream_close(repackst->streamlist + index);
         if (closeres) {
            LOG(LOG_ERR, "Failed to close repack stream %zu\n", index);
            if (retval == 0) { retval = closeres; }
         }
      }
      else { prevactive = 0; }
   }

   repackstreamer_destroy(repackst);
   return retval;
}

/**
 * Abort the given repackstreamer, bypassing all locks and releasing all datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to abort
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_abort(REPACKSTREAMER repackst) {
   // check for NULL arg
   if (repackst == NULL) {
      LOG(LOG_ERR, "Received a NULL repackstreamer ref\n");
      errno = EINVAL;
      return -1;
   }

   // don't bother acquiring the lock
   // iterate over all streams
   int retval = 0;
   char prevactive = 1;
   for (size_t index = 0; index < repackst->streamcount; index++) {
      if (repackst->streamlist[index] != NULL) {
         // minor sanity check, not even certain that this is a true failure
         if (prevactive == 0) {
            LOG(LOG_WARNING, "Encountered active stream at index %zu with previous stream being NULL\n");
         }
         // release the active stream
         int closeres = datastream_release(repackst->streamlist + index);
         if (closeres) {
            LOG(LOG_ERR, "Failed to release repack stream %zu\n", index);
            if (retval == 0) { retval = closeres; }
         }
      }
      else { prevactive = 0; }
   }

   repackstreamer_destroy(repackst);
   return retval;
}
