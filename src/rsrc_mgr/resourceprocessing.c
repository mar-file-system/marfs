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

#include <dirent.h>
#include <string.h>

#include "datastream/datastream.h"
#include "resourceprocessing.h"
#include "rsrc_mgr/consts.h"
#include "rsrc_mgr/streamwalker.h"

// ENOATTR is not always defined, so define a convenience val
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

static void process_deleteobj(marfs_position* pos, opinfo* op) {
   marfs_ds* ds = &pos->ns->prepo->datascheme;
   while (op) {
      op->start = 0;

      size_t countval = 0;

      // check for extendedinfo

      delobj_info* delobjinf = (delobj_info*)op->extendedinfo;
      if (delobjinf != NULL) {
         countval = delobjinf->offset; // skip ahead by some offset, if specified
      }

      while (countval < op->count + delobjinf->offset) {
         // identify the object target of the op
         FTAG tmptag = op->ftag;
         tmptag.objno += countval;

         char* objname = NULL;
         ne_erasure erasure;
         ne_location location;
         if (datastream_objtarget(&tmptag, ds, &objname, &erasure, &location)) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to identify object target %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
            break;
         }

         // delete the object
         LOG(LOG_INFO, "Deleting object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);

         int olderrno = errno;
         if (ne_delete(ds->nectxt, objname, location)) {
            if (errno == ENOENT) {
               LOG(LOG_INFO, "Object %zu of stream \"%s\" was already deleted\n", tmptag.objno, tmptag.streamid);
            }
            else {
               op->errval = (errno) ? errno : ENOTRECOVERABLE;
               LOG(LOG_ERR, "Failed to delete object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
               free(objname);
               break;
            }
         }
         errno = olderrno;

         errno = olderrno;
         free(objname);
         countval++;
      }

      op = op->next;
   }

   return;
}

static void process_deleteref(const marfs_position* pos, opinfo* op) {
   // identify the appropriate reference table to be used for path determination
   HASH_TABLE reftable = NULL;
   if (op->ftag.refbreadth == pos->ns->prepo->metascheme.refbreadth  &&
        op->ftag.refdepth == pos->ns->prepo->metascheme.refdepth  &&
        op->ftag.refdigits == pos->ns->prepo->metascheme.refdigits) {
      // we can safely use the default reference table
      reftable = pos->ns->prepo->metascheme.reftable;
   }
   else {
      // we must generate a fresh ref table, based on FTAG values
      reftable = config_genreftable(NULL, NULL, op->ftag.refbreadth, op->ftag.refdepth, op->ftag.refdigits);
      if (reftable == NULL) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to generate reference table with values (breadth=%d, depth=%d, digits=%d)\n",
              op->ftag.refbreadth, op->ftag.refdepth, op->ftag.refdigits);
         return;
      }
   }

   for (; op; op = op->next) {
      op->start = 0;

      // verify we have extendedinfo
      delref_info* delrefinf = (delref_info*)op->extendedinfo;
      if (delrefinf == NULL) {
         op->errval = EINVAL;
         LOG(LOG_ERR, "DEL-REF op is missing extendedinfo\n");
         continue;
      }

      // convenience refs
      MDAL mdal = pos->ns->prepo->metascheme.mdal;

      // first, need to attach a GCTAG to the previous active file
      GCTAG gctag = {
         .refcnt = op->count,
         .delzero = delrefinf->delzero,
         .eos = delrefinf->eos,
         .inprog = 0
      };

      // note any previously deleted refs between us and prev_active_index
      if (op->ftag.fileno > delrefinf->prev_active_index + 1) {
         gctag.refcnt += (op->ftag.fileno - 1) - delrefinf->prev_active_index;
         LOG(LOG_INFO, "Including previous gap of %zu files (%zu resultant gap)\n",
                        (op->ftag.fileno - 1) - delrefinf->prev_active_index, gctag.refcnt);
      }

      if (op->count) { gctag.inprog = 1; } // set inprog, if we're actually doing any reference deletions

      size_t gctaglen = gctag_tostr(&gctag, NULL, 0);
      if (gctaglen < 1) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to identify length of GCTAG for stream \"%s\"\n", op->ftag.streamid);
         continue;
      }

      char* gctagstr = malloc(sizeof(char) * (gctaglen + 1));
      if (gctag_tostr(&gctag, gctagstr, gctaglen+1) != gctaglen) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "GCTAG has an inconsistent length for stream \"%s\"\n", op->ftag.streamid);
         free(gctagstr);
         continue;
      }

      // identify the reference path of our initial target
      FTAG tmptag = op->ftag;
      tmptag.fileno = delrefinf->prev_active_index;

      char* reftgt = datastream_genrpath(&tmptag, reftable, NULL, NULL);
      if (reftgt == NULL) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to identify reference path of active fileno %zu of stream \"%s\"\n", tmptag.fileno, op->ftag.streamid);
         free(gctagstr);
         continue;
      }

      MDAL_FHANDLE activefile = mdal->openref(pos->ctxt, reftgt, O_RDWR, 0);
      if (activefile == NULL) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to open handle for active fileno %zu of stream \"%s\"\n", tmptag.fileno, op->ftag.streamid);
         free(reftgt);
         free(gctagstr);
         continue;
      }

      LOG(LOG_INFO, "Attaching GCTAG \"%s\" to reference file \"%s\"\n", gctagstr, reftgt);

      if (mdal->fsetxattr(activefile, 1, GCTAG_NAME, gctagstr, gctaglen, 0)) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to attach GCTAG \"%s\" to reference file \"%s\"\n", gctagstr, reftgt);
         free(reftgt);
         free(gctagstr);
         continue;
      }

      free(gctagstr);

      // for a zero-count refdel op, we're done here
      if (op->count == 0) {
         if (mdal->close(activefile)) {
            LOG(LOG_WARNING, "Failed to close handle for active file \"%s\"\n", reftgt);
         }
         free(reftgt);
         continue;
      }

      // iterate over reference targets
      size_t countval = 0;
      while (countval < op->count) {
         // identify the reference path
         tmptag = op->ftag;
         tmptag.fileno += countval;

         char* rpath = datastream_genrpath(&tmptag, reftable, NULL, NULL);
         if (rpath == NULL) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to identify reference path of fileno %zu of stream \"%s\"\n", tmptag.fileno, op->ftag.streamid);
            mdal->close(activefile);
            free(reftgt);
            break;
         }

         // perform the deletion
         int olderrno = errno;
         errno = 0;
         if (mdal->unlinkref(pos->ctxt, rpath) && errno != ENOENT) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to unlink reference path \"%s\"\n", rpath);
            free(rpath);
            mdal->close(activefile);
            free(reftgt);
            break;
         }

         errno = olderrno;
         LOG(LOG_INFO, "Deleted reference path \"%s\"\n", rpath);
         free(rpath);
         countval++;
      }

      if (countval < op->count) { continue; } // skip over remaining, if we hit an error

      // update GCTAG to reflect completion
      gctag.inprog = 0;
      gctaglen = gctag_tostr(&gctag, NULL, 0);
      if (gctaglen < 1) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to identify length of GCTAG for stream \"%s\"\n", op->ftag.streamid);
         mdal->close(activefile);
         free(reftgt);
         continue;
      }

      gctagstr = malloc(sizeof(char) * (gctaglen + 1));
      if (gctag_tostr(&gctag, gctagstr, gctaglen+1) != gctaglen) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "GCTAG has an inconsistent length for stream \"%s\"\n", op->ftag.streamid);
         free(gctagstr);
         mdal->close(activefile);
         free(reftgt);
         continue;
      }

      LOG(LOG_INFO, "Updating GCTAG to \"%s\" for reference file \"%s\"\n", gctagstr, reftgt);

      if (mdal->fsetxattr(activefile, 1, GCTAG_NAME, gctagstr, gctaglen, 0)) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to update GCTAG to \"%s\" for reference file \"%s\"\n", gctagstr, reftgt);
         free(gctagstr);
         mdal->close(activefile);
         free(reftgt);
         continue;
      }

      free(gctagstr);

      // close our active file, and terminate
      if (mdal->close(activefile)) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to close handle for active file \"%s\"\n", reftgt);
         free(reftgt);
         continue;
      }

      free(reftgt);
   }

   // potentially destroy our custom hash table
   if (reftable != pos->ns->prepo->metascheme.reftable) {
      HASH_NODE* nodelist = NULL;
      size_t count = 0;
      if (hash_term(reftable, &nodelist, &count)) {
         LOG(LOG_WARNING, "Failed to delete non-NS reference table\n");
      }
      else {
         while (count) {
            count--;
            free(nodelist[count].name);
         }
         free(nodelist);
      }
   }

   return;
}

static void process_rebuild(const marfs_position* pos, opinfo* op) {
   // quick refs
   marfs_ds* ds = &pos->ns->prepo->datascheme;
   marfs_ms* ms = &pos->ns->prepo->metascheme;
   for (; op; op = op->next) {
      op->start = 0;
      rebuild_info* rebinf = (rebuild_info*)op->extendedinfo;
      size_t countval = 0;
      while (countval < op->count) {
         // identify the object target of the op
         FTAG tmptag = op->ftag;
         tmptag.objno += countval;

         char* objname = NULL;
         ne_erasure erasure;
         ne_location location;
         if (datastream_objtarget(&tmptag, ds, &objname, &erasure, &location)) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to identify object target %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
            break;
         }

         // open an object handle
         ne_handle obj = ne_open(ds->nectxt, objname, location, erasure, NE_REBUILD);
         if (obj == NULL) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to open rebuild handle for object \"%s\"\n", objname);
            free(objname);
            break;
         }

         free(objname);

         // if we have an rtag value, seed it in prior to rebuilding
         if (rebinf && rebinf->rtag && rebinf->rtag->stripestate.meta_status && rebinf->rtag->stripestate.data_status) {
            if (ne_seed_status(obj, &rebinf->rtag->stripestate)) {
               LOG(LOG_WARNING, "Failed to seed rtag status into handle for object %zu of stream \"%s\"\n",
                                 tmptag.objno, tmptag.streamid);
            }
         }

         // rebuild the object, performing up to 2 attempts
         int iteration = 0;
         while (iteration < 2) {
            LOG(LOG_INFO, "Rebuilding object %zu of stream \"%s\" (attempt %d)\n",
                           tmptag.objno, tmptag.streamid, (int)iteration + 1);
            int rebuildres = ne_rebuild(obj, NULL, NULL);
            if (rebuildres < 0) { // unmitigated failure
               iteration = -1;
               break;
            }
            else if (rebuildres == 0) { // rebuild success
               LOG(LOG_INFO, "Successfully rebuilt object %zu of stream \"%s\"\n",
                              tmptag.objno, tmptag.streamid);
               break;
            }
            else {
               LOG(LOG_WARNING, "Object %zu of stream \"%s\" still contains %d errors after iteration %d\n",
                                 tmptag.objno, tmptag.streamid, rebuildres, iteration);
            }
            // incomplete rebuild (reattempt may succeed)
            iteration++;
         }

         // check for excessive rebuild reattempts
         if (iteration >= 2 || iteration < 0) {
            if (iteration < 0) {
               LOG(LOG_ERR, "Rebuild failure for object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
            }
            else {
               LOG(LOG_ERR, "Excessive rebuild reattempts for object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
            }
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            if (ne_abort(obj)) {
               LOG(LOG_ERR, "Failed to properly abort rebuild handle for object %zu of stream \"%s\"\n",
                             tmptag.objno, tmptag.streamid);
            }
            obj = NULL;
            break;
         }

         // close the object reference
         if (obj && ne_close(obj, NULL, NULL)) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to finalize rebuild of object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
            break;
         }

         countval++;
      }

      if (countval < op->count) { continue; } // skip over remaining, if we hit an error

      // potentially cleanup the rtag
      if (rebinf && rebinf->rtag) {
         // generate the RTAG name
         char* rtagstr = rtag_getname(op->ftag.objno);
         if (rtagstr == NULL) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to identify the name of object %zu RTAG in stream \"%s\"\n", op->ftag.objno, op->ftag.streamid);
            continue;
         }

         // open a file handle for the marker
         if (rebinf->markerpath == NULL) {
            op->errval = EINVAL;
            LOG(LOG_ERR, "No marker path available by which to remove RTAG of object %zu in stream \"%s\"\n",
                          op->ftag.objno, op->ftag.streamid);
            free(rtagstr);
            continue;
         }

         MDAL_FHANDLE mhandle = ms->mdal->openref(pos->ctxt, rebinf->markerpath, O_RDONLY, 0);
         if (mhandle == NULL) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to open handle for marker path \"%s\"\n", rebinf->markerpath);
            free(rtagstr);
            continue;
         }

         // remove the RTAG
         if (ms->mdal->fremovexattr(mhandle, 1, rtagstr)) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to remove \"%s\" xattr from marker file \"%s\"\n", rtagstr, rebinf->markerpath);
            ms->mdal->close(mhandle);
            free(rtagstr);
            continue;
         }

         free(rtagstr);

         // close our handle
         if (ms->mdal->close(mhandle)) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to close handle for marker file \"%s\"\n", rebinf->markerpath);
            continue;
         }
      }

      // potentially cleanup the rebuild marker
      if (rebinf && rebinf->markerpath) {
         // unlink the rebuild marker
         if (ms->mdal->unlinkref(pos->ctxt, rebinf->markerpath)) {
            op->errval = (errno) ? errno : ENOTRECOVERABLE;
            LOG(LOG_ERR, "Failed to unlink marker file \"%s\"\n", rebinf->markerpath);
            continue;
         }
      }
   }

   // rebuild complete
   return;
}

static void process_repack(marfs_position* pos, opinfo* op, REPACKSTREAMER rpckstr, const char* ctagsuf) {
   // check out a repack stream reference
   DATASTREAM* rpckstream = repackstreamer_getstream(rpckstr);
   char rpckerror = 0;
   if (rpckstream == NULL) {
      LOG(LOG_ERR, "Failed to retrieve a repack datastream from our repackstreamer\n");
      rpckerror = 1;
   }

   // identify the appropriate reference table to be used for path determination
   HASH_TABLE reftable = NULL;
   if (op->ftag.refbreadth == pos->ns->prepo->metascheme.refbreadth  &&
        op->ftag.refdepth == pos->ns->prepo->metascheme.refdepth  &&
        op->ftag.refdigits == pos->ns->prepo->metascheme.refdigits) {
      // we can safely use the default reference table
      reftable = pos->ns->prepo->metascheme.reftable;
   }
   else {
      // we must generate a fresh ref table, based on FTAG values
      reftable = config_genreftable(NULL, NULL, op->ftag.refbreadth, op->ftag.refdepth, op->ftag.refdigits);
      if (reftable == NULL) {
         LOG(LOG_ERR, "Failed to generate reference table with values (breadth=%d, depth=%d, digits=%d)\n",
              op->ftag.refbreadth, op->ftag.refdepth, op->ftag.refdigits);
         rpckerror = 1;
      }
   }

   // allocate a 1MiB buffer to use for data migration
   void* iobuf = malloc(1024 * 1024);
   DATASTREAM readstream = NULL;
   opinfo* prevop = op;

   for (; op; op = op->next) {
      op->start = 0;
      if (rpckerror) { op->errval = (errno) ? errno : ENOTRECOVERABLE; continue; }

      // identify the reference path of the target file
      char* reftgt = datastream_genrpath(&op->ftag, reftable, NULL, NULL);
      if (reftgt == NULL) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to identify reference path of active fileno %zu of stream \"%s\"\n", op->ftag.fileno, op->ftag.streamid);
         rpckerror = 1;
         continue;
      }

      // establish our client tag
      ssize_t ctaglen;
      if (ctagsuf) {
         ctaglen = snprintf(NULL, 0, "RMAN-%s-Repack\n", ctagsuf);
      }
      else {
         ctaglen = snprintf(NULL, 0, "RMAN-Repack\n");
      }

      char* ctag = calloc(sizeof(char), ctaglen + 1);

      if (ctagsuf) {
         snprintf(ctag, ctaglen + 1, "RMAN-%s-Repack\n", ctagsuf);
      }
      else {
         snprintf(ctag, ctaglen + 1, "RMAN-Repack\n");
      }

      // open a repack datastream for the target file
      if (datastream_repack(rpckstream, reftgt, pos, ctag)) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to open repack stream for reference target: \"%s\"\n", reftgt);
         free(ctag);
         free(reftgt);
         rpckerror = 1;
         continue;
      }

      free(ctag);

      // open a read datastream for the target file
      if (datastream_open(&readstream, READ_STREAM, reftgt, pos, NULL)) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;
         LOG(LOG_ERR, "Failed to open read stream for reference target: \"%s\"\n", reftgt);
         if (datastream_release(rpckstream)) {
            LOG(LOG_WARNING, "Failed to abort repack stream after previous failure\n");
         }
         free(reftgt);
         rpckerror = 1;
         continue;
      }

      // read all data from the file, writing it out to the repack stream
      ssize_t iores = 1024 * 1024;
      while (iores) {
         iores = datastream_read(&readstream, iobuf, 1024 * 1024);

         if (iores < 0) {
            LOG(LOG_ERR, "Failed to read from reference target: \"%s\"\n", reftgt);
            break;
         }

         if (iores != datastream_write(rpckstream, iobuf, (size_t)iores)) {
            LOG(LOG_ERR, "Failed to write to repack stream of reference target: \"%s\"\n", reftgt);
            break;
         }
      }

      if (iores) {
         op->errval = (errno) ? errno : ENOTRECOVERABLE;

         // cleanup from previous errors
         if (datastream_release(rpckstream)) {
            LOG(LOG_WARNING, "Failed to abort repack stream after previous failure\n");
         }
         if (datastream_release(&readstream)) {
            LOG(LOG_WARNING, "Failed to abort read stream after previous failure\n");
         }

         free(reftgt);
         rpckerror = 1;
         continue;
      }

      free(reftgt);
      prevop = op;
   }

   // cleanup our iobuffer
   if (iobuf) { free(iobuf); }

   // potentially destroy our custom hash table
   if (reftable != pos->ns->prepo->metascheme.reftable) {
      HASH_NODE* nodelist = NULL;
      size_t count = 0;
      if (hash_term(reftable, &nodelist, &count)) {
         // just complain
         LOG(LOG_WARNING, "Failed to delete non-NS reference table\n");
      }
      else {
         while (count) {
            count--;
            free(nodelist[count].name);
         }
         free(nodelist);
      }
   }

   // close our read stream
   if (readstream && datastream_close(&readstream)) {
      // this isn't worth aborting over, it's just... odd
      LOG(LOG_WARNING, "Failed to close read stream\n");
   }

   // check in our repack stream reference (will only be closed when the repackstreamer is terminated)
   if (rpckstr && repackstreamer_returnstream(rpckstr, rpckstream)) {
      // this is a problem, so at least mark our final op as being in error
      LOG(LOG_ERR, "Failed to return our repack stream\n");
      if (prevop) { prevop->errval = ENOTRECOVERABLE; }
   }

   // repack complete
   return;
}

/**
 * Attempts deletion of the specified reference dir 'branch' (directory and all parent dirs) based on the given gcthreshold
 * @param marfs_position* pos : Current MarFS position
 * @param const char* refdirpath : Path of the reference dir to be cleaned up
 * @param time_t gcthresh : GC threshold value
 * @return int : Zero on success, or -1 on failure
 */
int cleanup_refdir(marfs_position* pos, const char* refdirpath, time_t gcthresh) {
   // check args
   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL marfs_position reference\n");
      errno = EINVAL;
      return -1;
   }

   if (pos->ns == NULL || pos->ctxt == NULL) {
      LOG(LOG_ERR, "Received an invalid, non-established marfs_position\n");
      errno = EINVAL;
      return -1;
   }

   if (refdirpath == NULL) {
      LOG(LOG_ERR, "Received a NULL refdirpath reference\n");
      errno = EINVAL;
      return -1;
   }

   // nothing to do, if this isn't a GC run
   if (gcthresh == 0) {
      LOG(LOG_INFO, "Skipping cleanup reference dir \"%s\" (non-GC run)\n", refdirpath);
      return 0;
   }

   // convenience refs
   marfs_ms* ms = &pos->ns->prepo->metascheme;

   // stat the reference dir
   struct stat stval;
   if (ms->mdal->statref(pos->ctxt, refdirpath, &stval)) {
      LOG(LOG_ERR, "Failed to stat reference dir: \"%s\"\n", refdirpath);
      return -1;
   }

   // check if this refdir is both empty (at least in terms of subdirs) and sufficiently old
   if (stval.st_nlink > 2 || stval.st_ctime >= gcthresh) {
      LOG(LOG_INFO, "Skipping cleanup reference dir \"%s\" (inelligible)\n", refdirpath);
      return 0;
   }

   // duplicate our refdir string, as we'll need to make modifications
   char* rpath = strdup(refdirpath);

   // attempt the removal of the target dir and its parents
   int rmdirres = 0;
   while (rmdirres == 0) {
      // attempt to destory this reference dir
      LOG(LOG_INFO, "Attempting cleanup of reference dir \"%s\"\n", rpath);
      rmdirres = ms->mdal->destroyrefdir(pos->ctxt, rpath);
      if (rmdirres) {
         if (errno == ENOTEMPTY) {
            LOG(LOG_INFO, "Skipping cleanup reference dir \"%s\" (not empty)\n", refdirpath);
            free(rpath);
            return 0;
         }
         else if (errno == ENOENT) {
            // someone beat us to it, which we don't have to worry about
            rmdirres = 0;
         }
         else {
            LOG(LOG_ERR, "Failed to cleanup reference dir \"%s\" (%s)\n", rpath, strerror(errno));
         }
      }

      // truncate off the last path element of this reference dir string
      char* rparse = rpath;
      char* finelem = rpath;
      while (*rparse != '\0') {
         if (*rparse == '/') {
            // note start of this separator
            finelem = rparse;
            // skip all duplicates
            while (*rparse == '/') { rparse++; }
         }
         else { rparse++; }
      }

      if (finelem == rpath) { break; } // indicates we've already hit the final path element
      *finelem = '\0';
   }

   free(rpath);

   return rmdirres;
}

/**
 * Process the next entry from the given refdir scanner
 * @param marfs_ns* ns : Reference to the current NS
 * @param MDAL_SCANNER refdir : Scanner reference to iterate through
 * @param char** reftgt : Reference to be populated with the next reference path tgt
 *                        Left NULL if the ref dir has been completely traversed
 * @param ssize_t* tgtval : Reference to be populated with the tgt's file/objno value
 *                          (see ftag_metainfo() return value)
 * @return int : Value of zero -> the reference dir has been completed and closed,
 *               Value of one -> entry is populated with the start of a datastream,
 *               Value of two -> entry is populated with a rebuild marker file,
 *               Value of three -> entry is populated with a repack marker file,
 *               Value of ten -> entry is of an unknown type
 *               Value of negative one -> an error occurred
 */
int process_refdir(marfs_ns* ns, MDAL_SCANNER refdir, const char* refdirpath, char** reftgt, ssize_t* tgtval) {
	// validate args
	if (ns == NULL) {
      LOG(LOG_ERR, "Received a NULL NS ref\n");
      errno = EINVAL;
      return -1;
   }

   if (refdir == NULL) {
      LOG(LOG_ERR, "Received a NULL scanner\n");
      errno = EINVAL;
      return -1;
   }

   if (refdirpath == NULL) {
      LOG(LOG_ERR, "Received a NULL reference dir path\n");
      errno = EINVAL;
      return -1;
   }

   if (reftgt == NULL) {
      LOG(LOG_ERR, "Received a NULL reftgt value\n");
      errno = EINVAL;
      return -1;
   }

   if (tgtval == NULL) {
      LOG(LOG_ERR, "Received a NULL tgtval value\n");
      errno = EINVAL;
      return -1;
   }

   // scan through the dir until we find something of interest
   MDAL mdal = ns->prepo->metascheme.mdal;
   struct dirent* dent = NULL;
   int olderrno = errno;
   errno = 0;
   char type = 9;
   while ((dent = mdal->scan(refdir)) != NULL) {
      // skip any 'hidden' files or default ('.'/'..') entries
      if (*(dent->d_name) == '.') {
         continue;
      }

      // identify the entry type
      const ssize_t parseval = ftag_metainfo(dent->d_name, &type);
      if (parseval < 0) {
         LOG(LOG_WARNING, "Failed to identify entry type: \"%s\"\n", dent->d_name);
         // unknown type (set to 9, since we'll return type + 1)
         type = 9;
         // rezero errno, so it doesn't mess up our scan() error check
         errno = 0;
      }
      else if (type == 0 && parseval != 0) {
         // skip any non-zero reference targets
         continue;
      }

      *tgtval = parseval;
      break; // exit the loop by default
   }

   // check for scan failure
   if (errno) {
      LOG(LOG_ERR, "Detected failure of scan() for refdir \"%s\"\n", refdirpath);
      return -1;
   }
   else if (dent == NULL) { // check for EOF
      if (mdal->closescanner(refdir)) {
         // just complain
         LOG(LOG_WARNING, "Failed to close scanner for ref dir \"%s\"\n", refdirpath);
      }
      return 0;
   }

   // populate the reftgt string
   int rpathlen = snprintf(NULL, 0, "%s/%s", refdirpath, dent->d_name);
   if (rpathlen < 1) {
      LOG(LOG_ERR, "Failed to identify length of ref path for \"%s\"\n", dent->d_name);
      return -1;
   }

   *reftgt = malloc(sizeof(char) * (rpathlen + 1));
   if (snprintf(*reftgt, rpathlen + 1, "%s/%s", refdirpath, dent->d_name) != rpathlen) {
      LOG(LOG_ERR, "Inconsistent length for ref path of \"%s\"\n", dent->d_name);
      free(*reftgt);
      *reftgt = NULL;
      errno = EDOM;
      return -1;
   }

   errno = olderrno; // restore old errno

   return ((int)type + 1);
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
opinfo* process_rebuildmarker(marfs_position* pos, char* markerpath, time_t rebuildthresh, size_t objno) {
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

/**
 * Perform the given operation
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the current NS
 * @param opinfo* op : Reference to the operation to be performed
 *                     NOTE -- this will be updated to reflect operation completion / error
 * @param RESOURCELOG* log : Resource log to be updated with op completion / error
 * @param REPACKSTREAMER rpckstr : Repack streamer to be used for repack operations
 * @param const char* ctag : Optional client tag for repacking
 * @return int : Zero on success, or -1 on failure
 *               NOTE -- This func will not return 'failure' unless a critical internal error occurs.
 *                       'Standard' operation errors will simply be reflected in the op struct itself.
 */
int process_executeoperation(marfs_position* pos, opinfo* op, RESOURCELOG* rlog, REPACKSTREAMER rpkstr, const char* ctag) {
   // check arguments
   if (op == NULL) {
      LOG(LOG_ERR, "Received a NULL operation value\n");
      errno = EINVAL;
      return -1;
   }

   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL marfs_position value\n");
      resourcelog_freeopinfo(op);
      errno = EINVAL;
      return -1;
   }

   if (pos->ctxt == NULL) {
      LOG(LOG_ERR, "Received a marfs_position value with no MDAL_CTXT\n");
      resourcelog_freeopinfo(op);
      errno = EINVAL;
      return -1;
   }

   if (!op->start) {
      LOG(LOG_ERR, "Received a non-start operation value\n");
      resourcelog_freeopinfo(op);
      errno = EINVAL;
      return -1;
   }

   char abortops = 0;
   while (op) {
      opinfo* nextop = op->next;
      if (abortops) {
         LOG(LOG_ERR, "Skipping %s operation due to previous op errors\n",
                       (op->type == MARFS_DELETE_OBJ_OP) ? "DEL-OBJ" :
                       (op->type == MARFS_DELETE_REF_OP) ? "DEL-REF" :
                       (op->type == MARFS_REBUILD_OP)    ? "REBUILD" :
                       (op->type == MARFS_REPACK_OP)     ? "REPACK"  :
                       "UNKNOWN");
         op->errval = EAGAIN;
      }
      else {
         // execute the operation
         switch (op->type) {
            case MARFS_DELETE_OBJ_OP:
               LOG(LOG_INFO, "Performing object deletion op on stream \"%s\"\n", op->ftag.streamid);
               op->next = NULL; // break off the first op of the chain
               process_deleteobj(pos, op);
               break;
            case MARFS_DELETE_REF_OP:
               LOG(LOG_INFO, "Performing reference deletion op on stream \"%s\"\n", op->ftag.streamid);
               nextop = NULL; // executing entire chain, so no subsequent op to execute
               process_deleteref(pos, op);
               break;
            case MARFS_REBUILD_OP:
               LOG(LOG_INFO, "Performing rebuild op on stream \"%s\"\n", op->ftag.streamid);
               nextop = NULL; // executing entire chain, so no subsequent op to execute
               process_rebuild(pos, op);
               break;
            case MARFS_REPACK_OP:
               LOG(LOG_INFO, "Performing repack op on stream \"%s\"\n", op->ftag.streamid);
               nextop = NULL; // executing entire chain, so no subsequent op to execute
               process_repack(pos, op, rpkstr, ctag);
               break;
            default:
               LOG(LOG_ERR, "Unrecognized operation type value\n");
               resourcelog_freeopinfo(op);
               return -1;
         }
      }

      // log operation end, and check if we can progress
      char progress = 0;
      LOG(LOG_INFO, "Logging end of operation stream \"%s\"\n", op->ftag.streamid);
      if (resourcelog_processop(rlog, op, &progress)) {
         LOG(LOG_ERR, "Failed to log end of operation on stream \"%s\"\n", op->ftag.streamid);
         resourcelog_freeopinfo(op);
         if (nextop) { resourcelog_freeopinfo(nextop); }
         return -1;
      }

      resourcelog_freeopinfo(op);

      if (progress == 0) {
         LOG(LOG_INFO, "Terminating execution, as portions of this op have not yet been completed\n");
         if (nextop) { resourcelog_freeopinfo(nextop); }
         return 0;
      }

      if (progress < 0) {
         LOG(LOG_ERR, "Previous operation failures will prevent execution of remaining op chain\n");
         abortops = 1;
      }

      op = nextop;
   }

   LOG(LOG_INFO, "Terminating execution, as all operations were completed\n");
   return 0;
}
