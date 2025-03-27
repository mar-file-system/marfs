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

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "rsrc_mgr/consts.h"
#include "rsrc_mgr/resourcethreads.h"

//   -------------   THREAD BEHAVIOR FUNCTIONS    -------------

/**
 * Resource thread initialization (producers and consumers)
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_init_func(unsigned int tID, void* global_state, void** state) {
   // cast values to appropriate types
   rthread_global_state* gstate = (rthread_global_state*)global_state;

   rthread_state* tstate = malloc(sizeof(*tstate));
   memset(tstate, 0, sizeof(*tstate));
   tstate->tID = tID;
   tstate->gstate = gstate;
   *state = tstate;

   LOG(LOG_INFO, "Thread %u has initialized\n", tstate->tID);
   return 0;
}

/**
 * Resource thread consumer behavior
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_consumer_func(void** state, void** work_todo) {
   // cast values to appropriate types
   rthread_state* tstate = (rthread_state*)(*state);
   opinfo* op = (opinfo*)(*work_todo);

   // execute operation
   if (op) {
      if (tstate->gstate->dryrun) {
         LOG(LOG_INFO, "Thread %u is discarding (DRY-RUN) a %s operation on StreamID \"%s\"\n", tstate->tID,
              (op->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
              (op->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
              (op->type == MARFS_REBUILD_OP)    ? "REBUILD" :
              (op->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN", op->ftag.streamid);
         resourcelog_freeopinfo(op);
      }
      else {
         LOG(LOG_INFO, "Thread %u is executing a %s operation on StreamID \"%s\"\n", tstate->tID,
              (op->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
              (op->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
              (op->type == MARFS_REBUILD_OP)    ? "REBUILD" :
              (op->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN", op->ftag.streamid);
         if (process_executeoperation(&tstate->gstate->pos, op, &tstate->gstate->rlog, tstate->gstate->rpst, NULL)) {
            LOG(LOG_ERR, "Thread %u has encountered critical error during operation execution\n", tstate->tID);
            *work_todo = NULL;
            tstate->fatalerror = 1;
            // ensure termination of all other threads (avoids possible deadlock)
            if (resourceinput_purge(&tstate->gstate->rinput)) {
               LOG(LOG_WARNING, "Failed to purge resource input following fatal error\n");
            }
            return -1;
         }
      }

      *work_todo = NULL;
   }

   return 0;
}

/**
 * Generate a rebuild opinfo element corresponding to the given marker and object
 * @param char* markerpath : Reference path of the rebuild marker (will be bundled into the opinfo ref)
 * @param time_t rebuildthresh : Rebuild threshold value (files more recent than this will be ignored)
 * @param size_t objno : Index of the object corresponding to the marker
 * @return opinfo* : Reference to the newly generated op, or NULL on failure
 *                   NOTE -- errno will be set to ETIME, specifically in the case of the marker being too recent
 *                           (ctime >= rebuildthresh)
 */
static opinfo* process_rebuildmarker(marfs_position* pos, char* markerpath, time_t rebuildthresh, size_t objno) {
   // check args
   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL marfs_position reference\n");
      errno = EINVAL;
      return NULL;
   }

   if (markerpath == NULL) {
      LOG(LOG_ERR, "Received a NULL markerpath reference\n");
      errno = EINVAL;
      return NULL;
   }

   // convenience refs
   marfs_ms* ms = &pos->ns->prepo->metascheme;

   // open a file handle for the marker
   MDAL_FHANDLE mhandle = ms->mdal->openref(pos->ctxt, markerpath, O_RDONLY, 0);
   if (mhandle == NULL) {
      LOG(LOG_ERR, "Failed to open handle for marker path \"%s\"\n", markerpath);
      return NULL;
   }

   // allocate rebuild op extended info
   rebuild_info* rinfo = calloc(1, sizeof(*rinfo));

   // generate the RTAG name
   char* rtagname = rtag_getname(objno);
   if (rtagname == NULL) {
      LOG(LOG_ERR, "Failed to identify the name of object %zu RTAG name\n", objno);
      free(rinfo);
      ms->mdal->close(mhandle);
      return NULL;
   }

   // retrieve the RTAG from the marker file
   // NOTE -- RTAG is just a rebuild speedup.  A missing value doesn't prevent rebuild completion.
   //         Therefore, ENOATTR is acceptable.  Anything else, however, is unusual enough to abort for.
   ssize_t rtaglen = ms->mdal->fgetxattr(mhandle, 1, rtagname, NULL, 0);
   if (rtaglen < 1 && errno != ENOATTR) {
      LOG(LOG_ERR, "Failed to retrieve \"%s\" value from marker file \"%s\" (%s)\n", rtagname, markerpath, strerror(errno));
      free(rtagname);
      free(rinfo);
      ms->mdal->close(mhandle);
      return NULL;
   }

   if (rtaglen > 0) {
      // allocate RTAG string
      char* rtagstr = malloc(sizeof(char) * (1 + rtaglen));

      // retrieve the RTAG value, for real
      if (ms->mdal->fgetxattr(mhandle, 1, rtagname, rtagstr, rtaglen + 1) != rtaglen) {
         LOG(LOG_ERR, "\"%s\" value of \"%s\" marker file has an inconsistent length\n", rtagname, markerpath);
         free(rtagstr);
         free(rtagname);
         free(rinfo);
         ms->mdal->close(mhandle);
         return NULL;
      }

      *(rtagstr + rtaglen) = '\0'; // ensure a NULL-terminated value

      // allocate an RTAG entry
      rinfo->rtag = calloc(1, sizeof(RTAG));

      // populate RTAG entry
      if (rtag_initstr(rinfo->rtag, rtagstr)) {
         LOG(LOG_ERR, "Failed to parse \"%s\" value of marker file \"%s\"\n", rtagname, markerpath);
         free(rinfo->rtag);
         free(rtagstr);
         free(rtagname);
         free(rinfo);
         ms->mdal->close(mhandle);
         return NULL;
      }

      free(rtagstr);

      // verify RTAG is sufficiently old to process
      if (rinfo->rtag->createtime >= rebuildthresh) {
         LOG(LOG_INFO, "Marker path \"%s\" RTAG create time is too recent to rebuild\n", markerpath);
         rtag_free(rinfo->rtag);
         free(rinfo->rtag);
         free(rtagname);
         free(rinfo);
         ms->mdal->close(mhandle);
         errno = ETIME;
         return NULL;
      }
   }
   else {
      // no RTAG timestamp, so rely on stat ctime as a backup
      struct stat stval;
      if (ms->mdal->fstat(mhandle, &stval)) {
         LOG(LOG_ERR, "Failed to stat via handle for marker path \"%s\"\n", markerpath);
         free(rtagname);
         free(rinfo);
         ms->mdal->close(mhandle);
         return NULL;
      }

      if (stval.st_ctime >= rebuildthresh) {
         LOG(LOG_INFO, "Marker path \"%s\" ctime is too recent to rebuild\n", markerpath);
         free(rtagname);
         free(rinfo);
         ms->mdal->close(mhandle);
         errno = ETIME;
         return NULL;
      }
   }

   free(rtagname);

   // duplicate marker path
   rinfo->markerpath = strdup(markerpath);
   if (rinfo->markerpath == NULL) {
      LOG(LOG_ERR, "Failed to duplicate rebuild marker path \"%s\"\n", markerpath);
      if (rinfo->rtag) { rtag_free(rinfo->rtag); free(rinfo->rtag); }
      free(rinfo);
      ms->mdal->close(mhandle);
      return NULL;
   }

   // retrieve the FTAG value
   ssize_t ftagstrlen = ms->mdal->fgetxattr(mhandle, 1, FTAG_NAME, NULL, 0);
   if (ftagstrlen < 2) {
      LOG(LOG_ERR, "Failed to retrieve FTAG from marker file \"%s\"\n", markerpath);
      free(rinfo->markerpath);
      if (rinfo->rtag) { rtag_free(rinfo->rtag); free(rinfo->rtag); }
      free(rinfo);
      ms->mdal->close(mhandle);
      return NULL;
   }

   char* ftagstr = malloc(sizeof(char) * (ftagstrlen + 1));
   if (ms->mdal->fgetxattr(mhandle, 1, FTAG_NAME, ftagstr, ftagstrlen) != ftagstrlen) {
      LOG(LOG_ERR, "FTAG of marker file \"%s\" has an inconsistent length\n", markerpath);
      free(ftagstr);
      free(rinfo->markerpath);
      if (rinfo->rtag) { rtag_free(rinfo->rtag); free(rinfo->rtag); }
      free(rinfo);
      ms->mdal->close(mhandle);
      return NULL;
   }

   ftagstr[ftagstrlen] = '\0'; // ensure a NULL-terminated string

   // allocate a new operation struct
   opinfo* op = calloc(1, sizeof(*op));
   op->type = MARFS_REBUILD_OP;
   op->extendedinfo = (void*)rinfo;
   op->start = 1;

   op->count = 1;
   // parse in the FTAG
   if (ftag_initstr(&op->ftag, ftagstr)) {
      LOG(LOG_ERR, "Failed to parse FTAG value from marker file \"%s\"\n", markerpath);
      free(op);
      free(ftagstr);
      free(rinfo->markerpath);
      if (rinfo->rtag) { rtag_free(rinfo->rtag); free(rinfo->rtag); }
      free(rinfo);
      ms->mdal->close(mhandle);
      return NULL;
   }

   op->ftag.objno = objno; // overwrite object number with the one we are actually targeting
   free(ftagstr);

   // close our handle
   if (ms->mdal->close(mhandle)) {
      LOG(LOG_ERR, "Failed to close handle for marker file \"%s\"\n", markerpath);
      if (op->ftag.ctag) { free(op->ftag.ctag); }
      if (op->ftag.streamid) { free(op->ftag.streamid); }
      free(op);
      free(rinfo->markerpath);
      if (rinfo->rtag) { rtag_free(rinfo->rtag); free(rinfo->rtag); }
      free(rinfo);
      return NULL;
   }

   return op;
}

// enqueue previously produced GC op(s)
static void process_gc(rthread_state* tstate, opinfo** newop) {
    // first, we must identify where the op chain transitions from obj to ref deletions
    opinfo* refdel = NULL;
    opinfo* gcparse = tstate->gcops;
    while (gcparse) {
        if (gcparse->type == MARFS_DELETE_REF_OP) {
            refdel = gcparse;
            break;
        }

        gcparse = gcparse->next;
    }

    // NOTE -- object deletions always preceed reference deletions, so it should be safe to just check the first
    if (tstate->gcops->type == MARFS_DELETE_OBJ_OP) {
        if (tstate->gcops->count > 1) {
            // temporarily strip our op chain down to a single DEL-OBJ op, followed by all reference deletions
            opinfo* orignext = tstate->gcops->next;
            tstate->gcops->next = refdel;

            // split the object deletion op apart into multiple work packages
            *newop = resourcelog_dupopinfo(tstate->gcops);

            // restore original op chain structure
            tstate->gcops->next = orignext;

            // check for duplicated op chain
            if (*newop) {
                (*newop)->count = 1; // set to a single object deletion
                tstate->gcops->count--; // note one less op to distribute
                delobj_info* delobjinf = (delobj_info*) tstate->gcops->extendedinfo;
                delobjinf->offset++; // note to skip over one additional leading object
            }
            else {
                LOG(LOG_WARNING, "Failed to duplicate GC op prior to distribution!\n");
            }
        }
        else {
            // check if we have additional ops between the lead op and the first ref del op
            if (tstate->gcops->next != refdel) {
                // duplicate the REF-DEL portion of the op chain
                opinfo* refdup = resourcelog_dupopinfo(refdel);
                if (refdup) {
                    // need to strip off our leading op, and attach it to a new chain
                    opinfo* orignext = tstate->gcops->next;
                    tstate->gcops->next = refdup;
                    *newop = tstate->gcops;
                    tstate->gcops = orignext;
                }
                else {
                    LOG(LOG_WARNING, "Failed to duplicate GC REF-DEL op prior to distribution!\n");
                }
            }
        }
    }

    if (*newop == NULL) {
        // just pass out whatever ops remain
        *newop = tstate->gcops;
        tstate->gcops = NULL; // remove state reference, so we don't repeat
    }
}

// walk our current datastream
static int process_walker(rthread_state* tstate, opinfo **newop) {
   (void) newop;

   int walkres = streamwalker_iterate(&tstate->walker, &tstate->gcops, &tstate->repackops, &tstate->rebuildops);
   if (walkres < 0) { // check for failure
       LOG(LOG_ERR, "Thread %u failed to walk a stream beginning in refdir \"%s\" of NS \"%s\"\n",
          tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr);
       snprintf(tstate->errorstr, MAX_STR_BUFFER,
                "Thread %u failed to walk a stream beginning in refdir \"%s\" of NS \"%s\"\n",
                tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr);

       goto error;
   }
   else if (walkres > 0) {
      // log every operation prior to distributing them
      if (tstate->rebuildops) {
         if (resourcelog_processop(&tstate->gstate->rlog, tstate->rebuildops, NULL)) {
            LOG(LOG_ERR, "Thread %u failed to log start of a REBUILD operation\n", tstate->tID);
            snprintf(tstate->errorstr, MAX_STR_BUFFER,
                     "Thread %u failed to log start of a REBUILD operation\n", tstate->tID);

            goto error;
         }
      }

      if (tstate->repackops) {
         if (resourcelog_processop(&tstate->gstate->rlog, tstate->repackops, NULL)) {
            LOG(LOG_ERR, "Thread %u failed to log start of a REPACK operation\n", tstate->tID);
            snprintf(tstate->errorstr, MAX_STR_BUFFER,
                     "Thread %u failed to log start of a REPACK operation\n", tstate->tID);

            goto error;
         }
      }

      if (tstate->gcops) {
         if (resourcelog_processop(&tstate->gstate->rlog, tstate->gcops, NULL)) {
            LOG(LOG_ERR, "Thread %u failed to log start of a GC operation\n", tstate->tID);
            snprintf(tstate->errorstr, MAX_STR_BUFFER,
                     "Thread %u failed to log start of a GC operation\n", tstate->tID);

            goto error;
         }
      }
   }
   else if (walkres == 0) { // check for end of stream
      LOG(LOG_INFO, "Thread %u has reached the end of a datastream\n", tstate->tID);
      streamwalker_report tmpreport = {0};
      if (streamwalker_close(&tstate->walker, &tmpreport)) {
         LOG(LOG_ERR, "Thread %u failed to close a streamwalker\n", tstate->tID);
         snprintf(tstate->errorstr, MAX_STR_BUFFER, "Thread %u failed to close a streamwalker\n", tstate->tID);

         tstate->walker = NULL; // don't repeat a close attempt

         goto error;
      }

      tstate->report.fileusage   += tmpreport.fileusage;
      tstate->report.byteusage   += tmpreport.byteusage;
      tstate->report.filecount   += tmpreport.filecount;
      tstate->report.objcount    += tmpreport.objcount;
      tstate->report.bytecount   += tmpreport.bytecount;
      tstate->report.streamcount += tmpreport.streamcount;
      tstate->report.delobjs     += tmpreport.delobjs;
      tstate->report.delfiles    += tmpreport.delfiles;
      tstate->report.delstreams  += tmpreport.delstreams;
      tstate->report.volfiles    += tmpreport.volfiles;
      tstate->report.rpckfiles   += tmpreport.rpckfiles;
      tstate->report.rpckbytes   += tmpreport.rpckbytes;
      tstate->report.rbldobjs    += tmpreport.rbldobjs;
      tstate->report.rbldbytes   += tmpreport.rbldbytes;
   }

   return 0;

  error:
   tstate->fatalerror = 1;

   // ensure termination of all other threads (avoids possible deadlock)
   if (resourceinput_purge(&tstate->gstate->rinput)) {
       LOG(LOG_WARNING, "Failed to purge resource input following fatal error\n");
   }

   return -1;
}

// iterate through the scanner, looking for new operations to dispatch
static int process_scanner(rthread_state* tstate, opinfo** newop) {
   char* reftgt = NULL;
   ssize_t tgtval = 0;
   int scanres = process_refdir(tstate->gstate->pos.ns, tstate->scanner, tstate->rdirpath, &reftgt, &tgtval);
   if (scanres == 0) {
      LOG(LOG_INFO, "Thread %u has finished scan of reference dir \"%s\"\n", tstate->tID, tstate->rdirpath);
      if (cleanup_refdir(&tstate->gstate->pos, tstate->rdirpath, tstate->gstate->thresh.gcthreshold)) {
         LOG(LOG_ERR, "Thread %u failed to cleanup reference dir \"%s\"\n", tstate->tID, tstate->rdirpath);
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                  "Thread %u failed to cleanup reference dir \"%s\"\n", tstate->tID, tstate->rdirpath);

         tstate->scanner = NULL;
         tstate->rdirpath = NULL;

         goto error;
      }

      // NULL out our dir references, just in case
      tstate->scanner = NULL;
      tstate->rdirpath = NULL;
   }
   else if (scanres == 1) { // start of a new datastream to be walked
      // only copy relevant threshold values for this walk
      thresholds tmpthresh = tstate->gstate->thresh;
      if (!tstate->gstate->lbrebuild) {
          tmpthresh.rebuildthreshold = 0;
      }

      LOG(LOG_INFO, "Thread %u beginning streamwalk from reference file \"%s\"\n", tstate->tID, reftgt);

      if (streamwalker_open(&tstate->walker, &tstate->gstate->pos, reftgt, tmpthresh, &tstate->gstate->rebuildloc)) {
         LOG(LOG_ERR, "Thread %u failed to open streamwalker for \"%s\" of NS \"%s\"\n",
             tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr);
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                  "Thread %u failed to open streamwalker for \"%s\" of NS \"%s\"\n",
                  tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr);

         goto error;
      }

      tstate->streamcount++;
   }
   else if (scanres == 2) { // rebuild marker file
       if (tstate->gstate->lbrebuild) { //skip marker files, if we're rebuilding based on object location
          LOG(LOG_INFO, "Skipping rebuild marker file, as we are doing location-based rebuild: \"%s\"\n", reftgt);
       }
       else {
          // note the rebuild candidate regardless, to give an indication of remaining count
          errno = 0;
          tstate->report.rbldobjs++;
          *newop = process_rebuildmarker(&tstate->gstate->pos, reftgt, tstate->gstate->thresh.rebuildthreshold, tgtval);
          if (*newop == NULL && errno != ETIME) { // only ignore failure due to recently created marker file
              LOG(LOG_ERR, "Thread %u failed to process rebuild marker \"%s\" of NS \"%s\"\n",
                 tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr);
              snprintf(tstate->errorstr, MAX_STR_BUFFER,
                      "Thread %u failed to process rebuild marker \"%s\" of NS \"%s\"\n",
                      tstate->tID, (reftgt) ? reftgt : "NULL-REFERENCE!", tstate->gstate->pos.ns->idstr);

              goto error;
          }
          else if (*newop) {
             // log the new operation, before we distribute it
             if (resourcelog_processop(&tstate->gstate->rlog, *newop, NULL)) {
                 LOG(LOG_ERR, "Thread %u failed to log start of a marker REBUILD operation\n", tstate->tID);
                 snprintf(tstate->errorstr, MAX_STR_BUFFER,
                          "Thread %u failed to log start of a marker REBUILD operation\n", tstate->tID);

                 resourcelog_freeopinfo(*newop);

                 goto error;
             }
          }
       }
   }
   else if (scanres == 3) { // repack marker file
      // TODO
   }
   else if (scanres == 10) {
      // ignore unknown entry type
      LOG(LOG_WARNING, "Thread %u ignoring unknown reference entry: \"%s/%s\"\n",
          tstate->tID, tstate->rdirpath, reftgt);
   }
   else {
      // an error occurred
      LOG(LOG_ERR, "Thread %u failed to process reference dir \"%s\" of NS \"%s\"\n",
          tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr);
      snprintf(tstate->errorstr, MAX_STR_BUFFER,
               "Thread %u failed to process reference dir \"%s\" of NS \"%s\"\n",
               tstate->tID, tstate->rdirpath, tstate->gstate->pos.ns->idstr);

      tstate->fatalerror = 1;

      goto error;
   }

   free(reftgt);

   return 0;

  error:
   free(reftgt);

   tstate->fatalerror = 1;

   // ensure termination of all other threads (avoids possible deadlock)
   if (resourceinput_purge(&tstate->gstate->rinput)) {
       LOG(LOG_WARNING, "Failed to purge resource input following fatal error\n");
   }

   return -1;
}

// pull from our resource input reference
static int process_rinput_ref(rthread_state* tstate, opinfo **newop) {
   int inputres = 0;
   while ((inputres = resourceinput_getnext(&tstate->gstate->rinput, newop, &tstate->scanner, &tstate->rdirpath)) == 0) {
      // wait until inputs are available
      LOG(LOG_INFO, "Thread %u is waiting for inputs\n", tstate->tID);
      if (resourceinput_waitforupdate(&tstate->gstate->rinput)) {
         LOG(LOG_ERR, "Thread %u failed to wait for resourceinput update while scanning NS \"%s\"\n",
             tstate->tID, tstate->gstate->pos.ns->idstr);
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                  "Thread %u failed to wait for resourceinput update while scanning NS \"%s\"\n",
                  tstate->tID, tstate->gstate->pos.ns->idstr);

         goto error;
      }
    }

   // check for termination condition
   if (inputres == 10) {
      LOG(LOG_INFO, "Thread %u is waiting for termination\n", tstate->tID);
      if (resourceinput_waitforterm(&tstate->gstate->rinput)) {
         LOG(LOG_ERR, "Thread %u failed to wait for input termination\n", tstate->tID);
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                  "Thread %u failed to wait for input termination\n", tstate->tID);

         goto error;
      }

      LOG(LOG_INFO, "Thread %u is signaling FINISHED state\n", tstate->tID);
      return 1;
   }

   // check for failure
   if (inputres < 0) {
      LOG(LOG_INFO, "Thread %u failed to retrieve next input while scanning NS \"%s\"\n",
          tstate->tID, tstate->gstate->pos.ns->idstr);
      snprintf(tstate->errorstr, MAX_STR_BUFFER,
               "Thread %u failed to retrieve next input while scanning NS \"%s\"\n",
               tstate->tID, tstate->gstate->pos.ns->idstr);

      goto error;
   }

   // if we got an op directly, we'll need to process it
   if (*newop) {
      // log the operation
      if (resourcelog_processop(&tstate->gstate->rlog, *newop, NULL)) {
         LOG(LOG_ERR, "Thread %u failed to log start of a resourceinput provided %s operation\n", tstate->tID,
             ((*newop)->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
             ((*newop)->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
             ((*newop)->type == MARFS_REBUILD_OP)    ? "REBUILD" :
             ((*newop)->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN");
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                  "Thread %u failed to log start of a resourceinput provided %s operation\n", tstate->tID,
                  ((*newop)->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
                  ((*newop)->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
                  ((*newop)->type == MARFS_REBUILD_OP)    ? "REBUILD" :
                  ((*newop)->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN");

         resourcelog_freeopinfo(*newop);

         goto error;
      }

      // filter the op to our appropriate 'queue'
      switch ((*newop)->type) {
         case MARFS_DELETE_OBJ_OP:
         case MARFS_DELETE_REF_OP:
             tstate->gcops = *newop;
             *newop = NULL;
             break;
         case MARFS_REPACK_OP:
             tstate->repackops = *newop;
             *newop = NULL;
             break;
          case MARFS_REBUILD_OP:
             tstate->rebuildops = *newop;
             *newop = NULL;
             break;
             // unrecognized ops will just be passed out immediately
      }
   }

   return 0;

  error:
   tstate->fatalerror = 1;

   // ensure termination of all other threads (avoids possible deadlock)
   if (resourceinput_purge(&tstate->gstate->rinput)) {
       LOG(LOG_WARNING, "Failed to purge resource input following fatal error\n");
   }

   return -1;
}

/**
 * Resource thread producer behavior
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
int rthread_producer_func(void** state, void** work_tofill) {
   // cast values to appropriate types
   rthread_state* tstate = (rthread_state*)(*state);

   // loop until we have an op to enqueue
   opinfo* newop = NULL;
   while (newop == NULL) {
      if (tstate->rebuildops) {
         // enqueue previously produced rebuild op(s)
         newop = tstate->rebuildops; // hand out the next rebuild op
         tstate->rebuildops = tstate->rebuildops->next; // progress to the subsequent op
         newop->next = NULL; // break the op chain, handing out all ops independently
      }
      else if (tstate->repackops) {
         // enqueue previously produced repack op(s)
         newop = tstate->repackops;
         tstate->repackops = NULL; // remove state reference, so we don't repeat
      }
      else if (tstate->gcops) {
         process_gc(tstate, &newop);
      }
      else if (tstate->walker) {
         if (process_walker(tstate, &newop) != 0) {
            return -1;
         }
      }
      else if (tstate->scanner) {
         if (process_scanner(tstate, &newop) != 0) {
            return -1;
         }
      }
      else {
         const int rc = process_rinput_ref(tstate, &newop);
         if (rc != 0) {
            return rc;
         }
      }
   }

   LOG(LOG_INFO, "Thread %u dispatching a %s%s operation on StreamID \"%s\"\n", tstate->tID,
        (newop->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
        (newop->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
        (newop->type == MARFS_REBUILD_OP)    ? "REBUILD" :
        (newop->type == MARFS_REPACK_OP)     ? "REPACK"  : "UNKNOWN",
        (newop->next == NULL) ? "" :
        (newop->next->type == MARFS_DELETE_OBJ_OP) ? " + DEL-OBJ" :
        (newop->next->type == MARFS_DELETE_REF_OP) ? " + DEL-REF" :
        (newop->next->type == MARFS_REBUILD_OP)    ? " + REBUILD" :
        (newop->next->type == MARFS_REPACK_OP)     ? " + REPACK"  : " + UNKNOWN", newop->ftag.streamid);

   // actually populate our work package
   *work_tofill = (void*)newop;

   return 0;
}

/**
 * Resource thread termination (producers and consumers)
 * NOTE -- see thread_queue.h in the erasureUtils repo for arg / return descriptions
 */
void rthread_term_func(void** state, void** prev_work, TQ_Control_Flags flg) {
   // cast values to appropriate types
   rthread_state* tstate = (rthread_state*)(*state);

   // producers may need to cleanup remaining state
   if (tstate->rebuildops) {
      LOG(LOG_INFO, "Thread %u is destroying non-issued REBUILD ops\n", tstate->tID);

      resourcelog_freeopinfo(tstate->rebuildops);
      tstate->rebuildops = NULL;

      // this is non-standard, so ensure we note an error
      if (!tstate->fatalerror) {
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                   "Thread %u held non-issued REBUILD ops at termination\n", tstate->tID);
         tstate->fatalerror = 1;
      }
   }

   if (tstate->repackops) {
      LOG(LOG_INFO, "Thread %u is destroying non-issued REPACK ops\n", tstate->tID);
      resourcelog_freeopinfo(tstate->repackops);
      tstate->repackops = NULL;
   }

   if (tstate->gcops) {
      LOG(LOG_ERR, "Thread %u is destroying remaining GC ops\n", tstate->tID);
      resourcelog_freeopinfo(tstate->gcops);
      tstate->gcops = NULL;

      // this is non-standard, so ensure we note an error
      if (!tstate->fatalerror) {
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                   "Thread %u held non-issued GC ops at termination\n", tstate->tID);
         tstate->fatalerror = 1;
      }
   }

   if (tstate->scanner) {
      LOG(LOG_ERR, "Thread %u is destroying remaining scanner handle\n", tstate->tID);
      tstate->gstate->pos.ns->prepo->metascheme.mdal->closescanner(tstate->scanner);

      // this is non-standard, so ensure we note an error
      if (!tstate->fatalerror) {
         snprintf(tstate->errorstr, MAX_STR_BUFFER,
                   "Thread %u held an open MDAL_SCANNER at termination\n", tstate->tID);
         tstate->fatalerror = 1;
      }
   }

   // merely note termination (state struct itself will be freed by master proc)
   LOG(LOG_INFO, "Thread %u is terminating\n", tstate->tID);
}
