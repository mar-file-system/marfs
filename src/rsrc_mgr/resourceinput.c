/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "rsrc_mgr/consts.h"
#include "rsrc_mgr/resourceinput.h"

/**
 * Initialize a given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be initialized
 * @param marfs_ns* ns : MarFS NS associated with the new resourceinput
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the previous NS
 *                         NOTE -- caller should never modify this again
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_init(RESOURCEINPUT* resourceinput, marfs_position* pos, size_t clientcount) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput != NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   if (pos == NULL || pos->ns == NULL || pos->ctxt == NULL) {
      LOG(LOG_ERR, "Received NULL position/NS/CTXT ref\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = malloc(sizeof(*rin));
   rin->ns = pos->ns;
   rin->ctxt = pos->ctxt;
   rin->rlog = NULL;
   rin->refmax = 0;
   rin->refindex = 0;
   rin->prepterm = 0;
   rin->clientcount = clientcount;
   pthread_cond_init(&rin->complete, NULL);
   pthread_cond_init(&rin->updated, NULL);
   pthread_mutex_init(&rin->lock, NULL);

   *resourceinput = rin;
   return 0;
}

/**
 * Update the given RESOURCEINPUT structure to use the given logpath as a new input source
 * @param RESOURCEINPUT* resourceinput : Resourceinput to update
 * @param const char* logpath : Path of the new (RECORD) resourcelog
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_setlogpath(RESOURCEINPUT* resourceinput, const char* logpath) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   // verify that we don't have an active resourcelog
   if (rin->rlog) {
      LOG(LOG_ERR, "Already have an active resourcelog value\n");
      pthread_mutex_unlock(&rin->lock);
      errno = EINVAL;
      return -1;
   }

   // don't allow inputs to be updated if threads are already preping for termination
   if (rin->prepterm) {
      LOG(LOG_ERR, "Resourceinput cannot be updated while preparing for termination\n");
      pthread_mutex_unlock(&rin->lock);
      errno = EINVAL;
      return -1;
   }

   // initialize the new resourcelog
   if (resourcelog_init(&rin->rlog, logpath, RESOURCE_READ_LOG, rin->ns)) {
      LOG(LOG_ERR, "Failed to initialize resourcelog input: \"%s\"\n", logpath);
      pthread_mutex_unlock(&rin->lock);
      return -1;
   }

   pthread_cond_broadcast(&rin->updated); // notify all waiting threads
   pthread_mutex_unlock(&rin->lock);

   LOG(LOG_INFO, "Successfully initialized input resourcelog \"%s\"\n", logpath);
   return 0;
}

/**
 * Set the active reference dir range of the given resourceinput
 * NOTE -- this will fail if the range has not been fully traversed
 * @param RESOURCEINPUT* resourceinput : Resourceinput to have its range set
 * @param ssize_t start : Starting index of the reference dir range
 * @param ssize_t end : Ending index of the reference dir range (non-inclusive)
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_setrange(RESOURCEINPUT* resourceinput, size_t start, size_t end) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   // verify that the ref range has already been traversed
   if (rin->refindex != rin->refmax) {
      LOG(LOG_ERR, "Ref range of tracker has not been fully traversed\n");
      goto error;
   }

   // don't allow inputs to be updated if threads are already preping for termination
   if (rin->prepterm) {
      LOG(LOG_ERR, "Resourceinput cannot be updated while preparing for termination\n");
      goto error;
   }

   // check that range values are valid
   if (start >= end || end > rin->ns->prepo->metascheme.refnodecount) {
      LOG(LOG_ERR, "Invalid reference range values: (start = %zu / end = %zu / max = %zu)\n", start, end, rin->ns->prepo->metascheme.refnodecount);
      goto error;
   }

   // set range values
   rin->refindex = start;
   rin->refmax = end;
   pthread_cond_broadcast(&rin->updated); // notify all waiting threads
   pthread_mutex_unlock(&rin->lock);

   return 0;

  error:
   pthread_mutex_unlock(&rin->lock);
   errno = EINVAL;
   return -1;
}

/**
 * Get the next ref index to be processed from the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to get the next ref index from
 * @param opinfo** nextop : Reference to be populated with a new op from an input logfile
 * @param MDAL_SCANNER* scanner : Reference to be populated with a new reference scanner
 * @param char** rdirpath : Reference to be populated with the path of a newly opened reference dir
 * @return int : Zero, if no inputs are currently available;
 *               One, if an input was produced;
 *               Ten, if the caller should prepare for termination (resourceinput is preparing to be closed)
 */
int resourceinput_getnext(RESOURCEINPUT* resourceinput, opinfo** nextop, MDAL_SCANNER* scanner, char** rdirpath) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   // check for active resourcelog
   if (rin->rlog) {
      if (resourcelog_readop(&rin->rlog, nextop)) {
         LOG(LOG_ERR, "Failed to read operation from active resourcelog\n");
         resourcelog_abort(&rin->rlog);
         pthread_mutex_unlock(&rin->lock);
         return -1;
      }

      // check for completion of log
      if (*nextop == NULL) {
         LOG(LOG_INFO, "Statelog has been completely read\n");
         if (resourcelog_term(&rin->rlog, NULL, 1)) {
            // nothing to do but complain
            LOG(LOG_WARNING, "Failed to properly terminate input resourcelog\n");
         }
         rin->rlog = NULL; // be certain this is NULLed out
         pthread_cond_signal(&rin->complete);
      }
      else {
         // provide the read value
         pthread_mutex_unlock(&rin->lock);
         LOG(LOG_INFO, "Produced a new operation from logfile input\n");
         return 1;
      }
   }

   MDAL_SCANNER scanres = NULL;
   HASH_NODE* node = NULL;
   while (scanres == NULL) {
      // check if the ref range has already been traversed
      if (rin->refindex == rin->refmax) {
         LOG(LOG_INFO, "Resource inputs have been fully traversed\n");
         int retval = 0;
         if (rin->prepterm) {
            LOG(LOG_INFO, "Caller should prepare for termination\n");
            retval = 10;
         }

         pthread_mutex_unlock(&rin->lock);
         return retval;
      }

      // retrieve the next reference index value
      ssize_t res = rin->refindex;
      rin->refindex++;
      LOG(LOG_INFO, "Passing out reference index: %zd\n", res);

      // open the corresponding reference scanner
      MDAL nsmdal = rin->ns->prepo->metascheme.mdal;
      node = rin->ns->prepo->metascheme.refnodes + res;
      scanres = nsmdal->openscanner(rin->ctxt, node->name);
      if (scanres == NULL && errno != ENOENT) { // only missing dir is acceptable
         // complain if we failed to open the dir for any other reason
         LOG(LOG_ERR, "Failed to open scanner for refdir: \"%s\" (index %zd)\n", node->name, res);
         rin->refindex--;
         pthread_mutex_unlock(&rin->lock);
         return -1;
      }

      // check for ref range completion
      if (rin->refindex == rin->refmax) {
         LOG(LOG_INFO, "Ref range has been completed\n");
         pthread_cond_signal(&rin->complete);
      }
   }

   pthread_mutex_unlock(&rin->lock);
   *scanner = scanres;
   *rdirpath = node->name;

   return 1;
}

/**
 * Destroy all available inputs and signal threads to prepare or for imminent termination
 * NOTE -- this is useful for aborting, if a thread has hit a fatal error
 * @param RESOURCEINPUT* resourceinput : Resourceinput to purge
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_purge(RESOURCEINPUT* resourceinput) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   LOG(LOG_INFO, "Purging resource inputs\n");

   // potentially terminate our resourcelog
   if (rin->rlog && resourcelog_abort(&rin->rlog)) {
      // nothing to do but complain
      LOG(LOG_WARNING, "Failed to abort input resourcelog\n");
   }

   rin->rlog = NULL; // be certain this is NULLed out

   // set ref range values to indicate completion
   rin->refindex = rin->refmax;

   // set prepterm to cause a bypass of the synchronized termination logic
   rin->prepterm = 3;

   // make sure all threads wake up
   pthread_cond_broadcast(&rin->updated);
   pthread_cond_broadcast(&rin->complete);
   pthread_mutex_unlock(&rin->lock);

   return 0;
}

/**
 * Wait for the given resourceinput to have available inputs, or for immenent termination
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforupdate(RESOURCEINPUT* resourceinput) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   // wait for the ref range to be traversed
   while (rin->rlog == NULL && rin->refindex == rin->refmax && rin->prepterm == 0) {
      pthread_cond_wait(&rin->updated, &rin->lock);
   }

   pthread_mutex_unlock(&rin->lock);

   LOG(LOG_INFO, "Detected available inputs\n");
   return 0;
}

/**
 * Wait for the given resourceinput to be terminated (synchronizing like this ensures ALL work gets enqueued)
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforterm(RESOURCEINPUT* resourceinput) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   // wait for the master proc to signal us
   char diddec = 0;
   while (rin->prepterm < 2) {
      // check if we should decrement active client count
      if (!diddec && rin->prepterm == 1) {
         if (rin->clientcount) {
            LOG(LOG_INFO, "Decrementing active client count from %zu to %zu\n", rin->clientcount, rin->clientcount - 1);
            rin->clientcount--; // show that we are waiting
            diddec = 1;
         }
         else if (rin->clientcount == 0) {
            LOG(LOG_ERR, "ClientCount is already zeroed out, but this client is only now waiting!\n");
         }

         pthread_cond_signal(&rin->complete); // signal the master proc to check status
      }

      pthread_cond_wait(&rin->updated, &rin->lock);
   }

   if (rin->prepterm == 2) {
      LOG(LOG_INFO, "Incrementing active client count from %zu to %zu\n", rin->clientcount, rin->clientcount + 1);
      rin->clientcount++; // show that we are exiting
   }

   pthread_cond_signal(&rin->complete);
   pthread_mutex_unlock(&rin->lock);

   LOG(LOG_INFO, "Ready for input termination\n");
   return 0;
}

/**
 * Wait for all inputs in the given resourceinput to be consumed
 * @param RESOURCEINPUT* resourceinput : Resourceinput to wait on
 * @param int : Zero on success, or -1 on failure
 */
int resourceinput_waitforcomp(RESOURCEINPUT* resourceinput) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   // wait for the ref range to be traversed
   while (rin->rlog || rin->refindex != rin->refmax) {
      pthread_cond_wait(&rin->complete, &rin->lock);
   }

   pthread_mutex_unlock(&rin->lock);

   LOG(LOG_INFO, "Detected input completion\n");
   return 0;
}

/**
 * Terminate the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_term(RESOURCEINPUT* resourceinput) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   // acquire the structure lock
   pthread_mutex_lock(&rin->lock);

   // verify that the resourcelog has been completed
   if (rin->rlog) {
      LOG(LOG_ERR, "Statelog of inputs has not yet been fully traversed\n");
      goto error;
   }

   // verify that the ref range has been traversed
   if (rin->refindex != rin->refmax) {
      LOG(LOG_ERR, "Ref range of inputs has not yet been fully traversed\n");
      goto error;
   }

   // if the resourceinput has been purged, just indicate a failure without freeing anything
   if (rin->prepterm > 2) {
      LOG(LOG_ERR, "Cannot terminate a purged resourceinput\n");
      goto error;
   }

   const size_t origclientcount = rin->clientcount; // remember the total number of clients
   LOG(LOG_INFO, "Synchronizing with %zu clients for termination\n", origclientcount);

   // signal clients to prepare for termination
   if (rin->prepterm < 1) { rin->prepterm = 1; }
   pthread_cond_broadcast(&rin->updated);
   while (rin->prepterm < 2 && rin->clientcount) {
      pthread_cond_wait(&rin->complete, &rin->lock);
   }

   LOG(LOG_INFO, "All %zu clients are ready for termination\n", origclientcount);

   // signal clients to exit
   if (rin->prepterm < 2) { rin->prepterm = 2; }
   pthread_cond_broadcast(&rin->updated);
   while (rin->prepterm < 3 && rin->clientcount < origclientcount) {
      pthread_cond_wait(&rin->complete, &rin->lock);
   }

   rin->prepterm = 3; // just in case
   LOG(LOG_INFO, "All %zu clients have terminated\n", origclientcount);
   pthread_mutex_unlock(&rin->lock);

   return 0;

  error:
   pthread_mutex_unlock(&rin->lock);
   errno = EINVAL;
   return -1;
}

/**
 * Destroy the given resourceinput
 * @param RESOURCEINPUT* resourceinput : Resourceinput to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int resourceinput_destroy(RESOURCEINPUT* resourceinput) {
   // check for valid ref
   if (resourceinput == NULL || *resourceinput == NULL) {
      LOG(LOG_ERR, "Received an invalid resourceinput arg\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCEINPUT rin = *resourceinput;

   char havelock = 1;

   // attempt to acquire the mutex lock, but don't let it stop us from killing the struct
   struct timespec waittime = {
      .tv_sec = 5, // maximum wait of 5sec to acquire the lock
      .tv_nsec = 0
   };
   if (pthread_mutex_timedlock(&rin->lock, &waittime)) {
      LOG(LOG_WARNING, "Failed to acquire lock, but pressing on regardless\n");
      havelock = 0;
   }

   // verify that clients have been properly signaled to term
   if (rin->prepterm < 3) {
      LOG(LOG_ERR, "Received resourceinput arg has not yet been terminated or purged\n");
      errno = EINVAL;
      return -1;
   }

   // begin destroying the structure
   *resourceinput = NULL;

   // abort the resourcelog, if present
   if (rin->rlog && resourcelog_abort(&rin->rlog)) {
      LOG(LOG_WARNING, "Failed to abort input resourcelog\n");
   }

   pthread_cond_destroy(&rin->complete);
   pthread_cond_destroy(&rin->updated);
   if (havelock) { pthread_mutex_unlock(&rin->lock); }
   pthread_mutex_destroy(&rin->lock);
   // DO NOT free ctxt or NS
   free(rin);

   return 0;
}
