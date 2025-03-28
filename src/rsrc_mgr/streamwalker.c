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

#include "rsrc_mgr/common.h"
#include "rsrc_mgr/streamwalker.h"

static void destroystreamwalker(streamwalker walker) {
   if (walker) {
      marfs_ms* ms = &walker->pos.ns->prepo->metascheme;
      if (walker->reftable && walker->reftable != ms->reftable) {
         // destroy the custom hash table
         HASH_NODE* nodelist = NULL;
         size_t count = 0;
         if (hash_term(walker->reftable, &nodelist, &count)) {
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

      if (walker->ftagstr) { free(walker->ftagstr); }
      if (walker->gcops) { resourcelog_freeopinfo(walker->gcops); }
      if (walker->rpckops) { resourcelog_freeopinfo(walker->rpckops); }
      if (walker->rbldops) { resourcelog_freeopinfo(walker->rbldops); }
      if (walker->ftag.ctag) { free(walker->ftag.ctag); }
      if (walker->ftag.streamid) { free(walker->ftag.streamid); }
      free(walker);
   }
}

static int process_getfileinfo(const char* reftgt, char getxattrs, streamwalker walker, char* filestate) {
   MDAL mdal = walker->pos.ns->prepo->metascheme.mdal;
   if (getxattrs) {
      // open the target file
      MDAL_FHANDLE handle = mdal->openref(walker->pos.ctxt, reftgt, O_RDONLY, 0);
      if (handle == NULL) {
         if (errno == ENOENT) {
            LOG(LOG_INFO, "Reference file does not exist: \"%s\"\n", reftgt);
            *filestate = 0;
            return 0;
         }
         LOG(LOG_ERR, "Failed to open current reference file target: \"%s\"\n", reftgt);
         return -1;
      }

      // attempt to retrieve the GC tag
      // NOTE -- it is ESSENTIAL to do this prior to the FTAG, so that ftagstr always contains the actual FTAG string
      ssize_t getres = mdal->fgetxattr(handle, 1, GCTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1);

      // check for overflow
      if (getres > 0 && getres >= walker->ftagstralloc) {
         // increase our allocated string length
         char* newstr = malloc(sizeof(char) * (getres + 1));

         // swap the new reference in
         free(walker->ftagstr);
         walker->ftagstr = newstr;
         walker->ftagstralloc = getres + 1;

         // pull the xattr again
         if (mdal->fgetxattr(handle, 1, GCTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1) != getres) {
            LOG(LOG_ERR, "Inconsistent length for gctag of reference file target: \"%s\"\n", reftgt);
            mdal->close(handle);
            return -1;
         }
      }

      // check for error (missing xattr is acceptable here though)
      if (getres <= 0 && errno != ENODATA) {
         LOG(LOG_ERR, "Failed to retrieve gctag of reference file target: \"%s\"\n", reftgt);
         mdal->close(handle);
         return -1;
      }
      else if (getres > 0) {
         // we must parse the GC tag value
         *(walker->ftagstr + getres) = '\0'; // ensure our string is NULL terminated
         if (gctag_initstr(&walker->gctag, walker->ftagstr)) {
            LOG(LOG_ERR, "Failed to parse GCTAG for reference file target: \"%s\"\n", reftgt);
            mdal->close(handle);
            return -1;
         }
      }
      else {
         // no GCTAG, so zero out values
         walker->gctag.refcnt = 0;
         walker->gctag.eos = 0;
         walker->gctag.inprog = 0;
         walker->gctag.delzero = 0;
      }

      // retrieve FTAG of the current file
      getres = mdal->fgetxattr(handle, 1, FTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1);

      // check for overflow
      if (getres > 0 && getres >= walker->ftagstralloc) {
         // double our allocated string length
         char* newstr = malloc(sizeof(char) * (getres + 1));

         // swap the new reference in
         free(walker->ftagstr);
         walker->ftagstr = newstr;
         walker->ftagstralloc = getres + 1;

         // pull the xattr again
         if (mdal->fgetxattr(handle, 1, FTAG_NAME, walker->ftagstr, walker->ftagstralloc - 1) != getres) {
            LOG(LOG_ERR, "Inconsistent length for ftag of reference file target: \"%s\"\n", reftgt);
            mdal->close(handle);
            return -1;
         }
      }

      // check for error (missing xattr is acceptable here though)
      if (getres <= 0 && errno != ENODATA) {
         LOG(LOG_ERR, "Failed to retrieve ftag of reference file target: \"%s\"\n", reftgt);
         mdal->close(handle);
         return -1;
      }

      // potentially parse the ftag
      int retval = 1; // assume missing ftag
      if (getres > 0) {
         // potentially clear old ftag values
         if (walker->ftag.ctag) { free(walker->ftag.ctag); walker->ftag.ctag = NULL; }
         if (walker->ftag.streamid) { free(walker->ftag.streamid); walker->ftag.streamid = NULL; }
         *(walker->ftagstr + getres) = '\0'; // ensure our string is NULL terminated
         if (ftag_initstr(&walker->ftag, walker->ftagstr)) {
            LOG(LOG_ERR, "Failed to parse ftag value of reference file target: \"%s\"\n", reftgt);
            mdal->close(handle);
            return -1;
         }
         retval = 0; // indicate unmitigated success
      }

      // stat the file
      if (mdal->fstat(handle, &walker->stval)) {
         LOG(LOG_ERR, "Failed to stat reference file target via handle: \"%s\"\n", reftgt);
         mdal->close(handle);
         return -1;
      }

      // finally, close the file
      if (mdal->close(handle)) {
         // just complain
         LOG(LOG_WARNING, "Failed to close handle for reference target: \"%s\"\n", reftgt);
      }

      // NOTE -- The resource manager will skip pulling RTAG xattrs, in this specific case.
      //         The 'location-based' rebuild, performed by this code, is intended for worst-case data damage situations.
      //         It is intended to rebuild all objects tied to a specific location, without the need for a client to read
      //         and tag those objects in advance.  As such, the expectation is that no RTAG values will exist.
      //         If they do, they will be rebuilt seperately, via their rebuild marker file.
      // populate state value based on link count
      *filestate = (walker->stval.st_nlink > 1) ? 2 : 1;

      return retval;
   }

   // stat the file by path
   if (mdal->statref(walker->pos.ctxt, reftgt, &walker->stval)) {
      if (errno == ENOENT) {
         LOG(LOG_INFO, "Reference file does not exist: \"%s\"\n", reftgt);
         *filestate = 0;
         return 0;
      }

      LOG(LOG_ERR, "Failed to stat reference file target via handle: \"%s\"\n", reftgt);
      return -1;
   }

   // zero out some xattr values, so we don't get confused
   walker->gctag.refcnt = 0;
   walker->gctag.eos = 0;
   walker->gctag.delzero = 0;
   walker->gctag.inprog = 0;

   // populate state value based on link count
   *filestate = (walker->stval.st_nlink > 1) ? 2 : 1;

   return 0;
}

static int searchoperations(opinfo** opchain, operation_type type, FTAG* ftag, opinfo** optgt) {
   opinfo* prevop = NULL;
   if (opchain && *opchain) {
      // check for any existing ops of this type in the chain
      opinfo* parseop = *opchain;
      while (parseop) {
         // for most ops, matching on type is sufficent to reuse the same op tgt
         // For object deletions, repacks, and rebuilds, we need to check that the new tgt is in the same 'chain'
         if (parseop->type == type  &&
              (type != MARFS_DELETE_OBJ_OP || ftag->objno == (parseop->ftag.objno + parseop->count))  &&
              (type != MARFS_REPACK_OP || ftag->fileno == (parseop->ftag.fileno + parseop->count))  &&
              (type != MARFS_REBUILD_OP || ftag->objno == (parseop->ftag.objno + parseop->count))
           ) {
            *optgt = parseop;
            return 0;
         }
         prevop = parseop;
         parseop = parseop->next;
      }
   }

   *optgt = prevop; // return a reference to the tail of the operation chain
   return -1;
}

static int process_identifyoperation(opinfo** opchain, operation_type type, FTAG* ftag, opinfo** optgt) {
   // check for any existing ops of this type in the chain
   opinfo* prevop = NULL;
   if (searchoperations(opchain, type, ftag, &prevop) == 0) {
      *optgt = prevop;
      return 0;
   }

   // allocate a new operation struct
   opinfo* newop = malloc(sizeof(*newop));
   newop->type = type;
   newop->extendedinfo = NULL;
   newop->start = 1;
   newop->count = 0;
   newop->errval = 0;
   newop->ftag = *ftag;
//      if (prevop && strcmp(prevop->ftag.ctag, ftag->ctag) == 0) {
//         newop->ftag.ctag = prevop->ftag.ctag;
//      }
   // create new strings, so we don't have a potential double-free in the future
   newop->ftag.ctag = strdup(ftag->ctag);
   if (newop->ftag.ctag == NULL) {
      LOG(LOG_ERR, "Failed to duplicate FTAG ctag string: \"%s\"\n", ftag->ctag);
      free(newop);
      return -1;
   }

   newop->ftag.streamid = strdup(ftag->streamid);

   // allocate extended info
   switch(type) {
      case MARFS_DELETE_OBJ_OP:
         newop->extendedinfo = calloc(1, sizeof(delobj_info));
         break;
      case MARFS_DELETE_REF_OP:
         newop->extendedinfo = calloc(1, sizeof(delref_info));
         break;
      case MARFS_REBUILD_OP:
         newop->extendedinfo = NULL; // do NOT allocate extended info for location-based rebuilds calloc(1, sizeof(struct rebuild_info_struct));
         break;
      case MARFS_REPACK_OP:
         newop->extendedinfo = calloc(1, sizeof(repack_info));
         break;
   }

   // check for allocation failure
   if (newop->extendedinfo == NULL && type != MARFS_REBUILD_OP) {
      LOG(LOG_ERR, "Failed to allocate operation extended info\n");
      free(newop->ftag.streamid);
      free(newop->ftag.ctag);
      free(newop);
      return -1;
   }

   newop->next = NULL;

   // insert the new op into the chain and return its reference
   if (opchain) {
      if (type == MARFS_DELETE_REF_OP && prevop) {
         // special case, reference deletion should always be inserted at the tail
         prevop->next = newop;
      }
      else {
         // default to inserting at the head
         opinfo* prevhead = *opchain;
         *opchain = newop;
         newop->next = prevhead;
      }
   }

   *optgt = newop;

   return 0;
}

/**
 * Open a streamwalker based on the given fileno zero reference target
 * @param streamwalker* swalker : Reference to be populated with the produced streamwalker
 * @param marfs_position* pos : MarFS position to be used by this walker
 * @param const char* reftgt : Reference path of the first (fileno zero) file of the datastream
 * @param thresholds thresh : Threshold values to be used for determining operation targets
 * @param ne_location* rebuildloc : Location-based rebuild target
 * @return int : Zero on success, -1 on failure, or 1 if the initial reference target should be unlinked (cleanup stream)
 *               NOTE -- It is possible for success to be indicated without any streamwalker being produced,
 *                       such as, if the stream is incomplete (missing FTAG values).
 *                       Failure will only be indicated if an unexpected condition occurred, such as, if the
 *                       datastream is improperly formatted.
 */
int streamwalker_open(streamwalker* swalker, marfs_position* pos, const char* reftgt, thresholds thresh, ne_location* rebuildloc) {
   // validate args
   if (swalker == NULL) {
      LOG(LOG_ERR, "Received a NULL streamwalker* arg\n");
      errno = EINVAL;
      return -1;
   }

   if (*swalker != NULL) {
      LOG(LOG_ERR, "Received a reference to an active streamwalker\n");
      errno = EINVAL;
      return -1;
   }

   if (pos == NULL) {
      LOG(LOG_ERR, "Received a NULL position ref\n");
      errno = EINVAL;
      return -1;
   }

   if (pos->ns == NULL) {
      LOG(LOG_ERR, "Received a position ref with no defined NS\n");
      errno = EINVAL;
      return -1;
   }

   if (reftgt == NULL) {
      LOG(LOG_ERR, "Received a NULL reference tgt path\n");
      errno = EINVAL;
      return -1;
   }

   if (thresh.rebuildthreshold && rebuildloc == NULL) {
      LOG(LOG_ERR, "Rebuild threshold is set, but no rebuild location was specified\n");
      errno = EINVAL;
      return -1;
   }

   // allocate a new streamwalker struct
   streamwalker walker = malloc(sizeof(*walker));

   // establish position
   walker->pos = *pos;
   if (walker->pos.ctxt == NULL) {
      LOG(LOG_ERR, "Received a marfs position for NS \"%s\" with no associated CTXT\n", pos->ns->idstr);
      free(walker);
      errno = EINVAL;
      return -1;
   }

   // keep a datascheme quick reference var
   marfs_ds* ds = &walker->pos.ns->prepo->datascheme;
   // populate initialization elements
   walker->gcthresh = thresh.gcthreshold;
   walker->repackthresh = thresh.repackthreshold;
   walker->rebuildthresh = thresh.rebuildthreshold;
   walker->cleanupthresh = thresh.cleanupthreshold;

   if (rebuildloc) {
      walker->rebuildloc = *rebuildloc;
   }
   else {
      memset(&walker->rebuildloc, 0, sizeof(ne_location));
   }

   // zero out report info
   memset(&walker->report, 0, sizeof(streamwalker_report));

   // initialize iteration info
   walker->fileno = 0;
   walker->objno = 0;
   // populate a bunch of placeholder info for the remainder
   walker->reftable = NULL;
   memset(&walker->stval, 0, sizeof(struct stat));
   memset(&walker->ftag, 0, sizeof(FTAG));
   memset(&walker->gctag, 0, sizeof(GCTAG));
   walker->headerlen = 0;
   walker->ftagstr = malloc(sizeof(char) * 1024);
   walker->ftagstralloc = 1024;
   walker->gcops = NULL;
   walker->activefiles = 0;
   walker->activeindex = 0;
   walker->rpckops = NULL;
   walker->activebytes = 0;
   walker->rbldops = NULL;

   // retrieve xattrs from the inital stream file
   char filestate = 0;
   int getres = process_getfileinfo(reftgt, 1, walker, &filestate);
   if (getres < 0 || !(filestate)) {
      LOG(LOG_ERR, "Failed to get info from initial reference target: \"%s\"\n", reftgt);
      free(walker->ftagstr);
      free(walker);
      return -1;
   }

   // missing FTAG value means we can't walk the stream
   if (getres > 0 || walker->ftag.streamid == NULL) {
      LOG(LOG_INFO, "Initial reference target lacks an FTAG: \"%s\"\n", reftgt);
      int retval = 0;
      // check for possible gc of this incomplete stream
      walker->report.filecount++;
      walker->report.bytecount += walker->stval.st_size;
      walker->report.streamcount++;

      if (filestate == 1 && walker->gcthresh && walker->stval.st_ctime < walker->gcthresh) {
         // tell the caller to unlink this file + stream
         walker->report.delfiles++;
         walker->report.delstreams++;
         retval = 1;
      }

      free(walker->ftagstr);
      free(walker);

      // this is not a fatal condition
      return retval;
   }

   // calculate our header length
   RECOVERY_HEADER header = {
      .majorversion = walker->ftag.majorversion,
      .minorversion = walker->ftag.minorversion,
      .ctag = walker->ftag.ctag,
      .streamid = walker->ftag.streamid
   };

   walker->headerlen = recovery_headertostr(&header, NULL, 0);

   // calculate the ending position of this file
   size_t endobj = datastream_filebounds(&walker->ftag);

   // TODO sanity checks
   // perform state checking for this first file
   char assumeactive = 0;
   char eos = walker->ftag.endofstream;
   if ((walker->ftag.state & FTAG_DATASTATE) < FTAG_FIN) { eos = 1; }
   if (walker->gctag.eos) { eos = 1; }
   if (filestate == 1) {
      // file is inactive
      if (walker->gcthresh && walker->stval.st_ctime < walker->gcthresh) {
         // this file is elligible for GC
         if (eos) {
            // only GC this initial ref if it is the last one remaining
            opinfo* optgt = NULL;
            if (process_identifyoperation(&walker->gcops, MARFS_DELETE_REF_OP, &walker->ftag, &optgt)) {
               LOG(LOG_ERR, "Failed to identify operation target for deletion of file %zu\n", walker->ftag.fileno);
               destroystreamwalker(walker);
               return -1;
            }
            // impossible to have an existing op; populate new op
            optgt->count = 1; // omitting gctag.refcnt is fine here, as we are about to delete the entire stream
            delref_info* delrefinf = optgt->extendedinfo;
            delrefinf->prev_active_index = walker->activeindex;
            delrefinf->eos = 1;
            walker->report.delfiles++;
            walker->report.delstreams++;
         }

         // potentially generate object GC ops, only if we haven't already
         if ((endobj != walker->objno || eos) && !walker->gctag.delzero) {
            // potentially generate GC ops for objects spanned by this file
            FTAG tmptag = walker->ftag;
            opinfo* optgt;
            size_t finobj = endobj;
            if (eos) { finobj++; } // include the final obj referenced by this file, specifically if no files follow
            while (tmptag.objno < finobj) {
               // generate ops for all but the last referenced object
               optgt = NULL;
               if (process_identifyoperation(&walker->gcops, MARFS_DELETE_OBJ_OP, &tmptag, &optgt)) {
                  LOG(LOG_ERR, "Failed to identify operation target for deletion of spanned obj %zu\n", tmptag.objno);
                  destroystreamwalker(walker);
                  return -1;
               }

               // sanity check
               if (optgt->count + optgt->ftag.objno != tmptag.objno) {
                  LOG(LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n",
                                optgt->count + optgt->ftag.objno, tmptag.objno);
                  destroystreamwalker(walker);
                  return -1;
               }

               // update operation
               optgt->count++;

               // update our record
               walker->report.delobjs++;

               // iterate to the next obj
               tmptag.objno++;
            }

            // need to generate a 'dummy' refdel op, specifically to drop a GCTAG on file zero
            optgt = NULL;
            if (process_identifyoperation(&walker->gcops, MARFS_DELETE_REF_OP, &walker->ftag, &optgt)) {
               LOG(LOG_ERR, "Failed to identify operation target for attachment of delzero tag\n");
               destroystreamwalker(walker);
               return -1;
            }

            if (optgt->count == 0 && walker->gctag.refcnt) {
               optgt->count = walker->gctag.refcnt; // don't obliterate our existing GC tag count
               optgt->ftag.fileno++; // target this 'real' refdel op at the subsequent file, not fileno zero
            }

            delref_info* delrefinf = (delref_info*)optgt->extendedinfo;
            delrefinf->delzero = 1;
            delrefinf->eos = eos;
         }
      }
      else if (walker->stval.st_ctime >= walker->gcthresh) {
         // this file was too recently deactivated to gc
         walker->report.volfiles++;
         assumeactive = 1;
      }
   }

   walker->report.filecount++;
   walker->report.bytecount += walker->ftag.bytes;
   walker->report.streamcount++;
   // only update object count if we are doing a 'full' walk
   if (walker->gcthresh || walker->repackthresh || walker->rebuildthresh) {
      // NOTE -- technically, objcount will run one object 'ahead' until iteration completion (we don't count final obj)
      if (!(walker->gctag.delzero)) { walker->report.objcount += endobj + 1; } // note first obj set, if not already deleted
      else if (!(eos)) { walker->report.objcount += 1; } // only count ahead if this is the sole file remaining
      LOG(LOG_INFO, "Noting %zu active objects (one ahead) from file zero\n", walker->report.objcount);
   }

   if (filestate > 1) {
      // file is active
      walker->report.fileusage++;
      walker->report.byteusage += walker->ftag.bytes;

      // TODO potentially generate repack op
      // potentially generate rebuild ops
      if (walker->rebuildthresh && walker->stval.st_ctime < walker->rebuildthresh) {
         // iterate over all objects spanned by this file
         FTAG tmptag = walker->ftag;
         size_t finobj = endobj;
         if (eos) { finobj++; } // include the final obj referenced by this file, specifically if no files follow
         while (tmptag.objno < finobj) {
            // check if object targets our rebuild location
            char* objname = NULL;
            ne_erasure erasure;
            ne_location location;
            if (datastream_objtarget(&tmptag, ds, &objname, &erasure, &location)) {
               LOG(LOG_ERR, "Failed to populate object target info for object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
               destroystreamwalker(walker);
               return -1;
            }

            free(objname); // don't actually need object name, so just destroy it immediately

            // check for location match
            if ((walker->rebuildloc.pod < 0 || walker->rebuildloc.pod == location.pod)  &&
                 (walker->rebuildloc.cap < 0 || walker->rebuildloc.cap == location.cap)  &&
                 (walker->rebuildloc.scatter < 0 || walker->rebuildloc.scatter == location.scatter)) {
               // generate a rebuild op for this object
               opinfo* optgt = NULL;
               if (process_identifyoperation(&walker->rbldops, MARFS_REBUILD_OP, &tmptag, &optgt)) {
                  LOG(LOG_ERR, "Failed to identify operation target for rebuild of spanned obj %zu\n", tmptag.objno);
                  destroystreamwalker(walker);
                  return -1;
               }

               // sanity check
               if (optgt->count + optgt->ftag.objno != tmptag.objno) {
                  LOG(LOG_ERR, "Existing obj rebuild count (%zu) does not match current obj (%zu)\n",
                                optgt->count + optgt->ftag.objno, tmptag.objno);
                  destroystreamwalker(walker);
                  return -1;
               }

               // update operation
               optgt->count++;

               // update our record
               walker->report.rbldobjs++;
            }

            // iterate to the next obj
            tmptag.objno++;
         }
      }
   }

   if (filestate > 1 || assumeactive) {
      // update state to reflect active initial file
      walker->activefiles++;
      walker->activebytes += walker->ftag.bytes;
   }

   // update walker state to reflect new target
   walker->objno = endobj;

   // identify the appropriate reference table for stream iteration
   if (walker->ftag.refbreadth == walker->pos.ns->prepo->metascheme.refbreadth  &&
        walker->ftag.refdepth == walker->pos.ns->prepo->metascheme.refdepth  &&
        walker->ftag.refdigits == walker->pos.ns->prepo->metascheme.refdigits) {
      // we can safely use the default reference table
      walker->reftable = walker->pos.ns->prepo->metascheme.reftable;
   }
   else {
      // we must generate a fresh ref table, based on FTAG values
      walker->reftable = config_genreftable(NULL, NULL, walker->ftag.refbreadth, walker->ftag.refdepth, walker->ftag.refdigits);
      if (walker->reftable == NULL) {
         LOG(LOG_ERR, "Failed to generate reference table with values (breadth=%d, depth=%d, digits=%d)\n",
              walker->ftag.refbreadth, walker->ftag.refdepth, walker->ftag.refdigits);
         destroystreamwalker(walker);
         return -1;
      }
   }

   // return the initialized walker
   *swalker = walker;

   return 0;
}

/**
 * Iterate over a datastream, accumulating quota values and identifying operation targets
 * NOTE -- This func will return all possible operations, given walker settings.  It is up to the caller whether those ops
 *         will actually be executed via process_operation().
 * @param streamwalker* swalker : Reference to the streamwalker to be iterated
 * @param opinfo** gcops : Reference to be populated with generated GC operations
 * @param opinfo** repackops : Reference to be populated with generated repack operations
 * @param opinfo** rebuildops : Reference to be populated with generated rebuild operations
 * @return int : 0, if the end of the datastream was reached and no new operations were generated;
 *               1, if new operations were generated by this iteration;
 *               -1, if a failure occurred
 */
int streamwalker_iterate(streamwalker* swalker, opinfo** gcops, opinfo** repackops, opinfo** rebuildops) {
   // validate args
   if (swalker == NULL) {
      LOG(LOG_ERR, "Received NULL streamwalker reference\n");
      errno = EINVAL;
      return -1;
   }

   if (*swalker == NULL) {
      LOG(LOG_ERR, "Received NULL streamwalker\n");
      errno = EINVAL;
      return -1;
   }

   streamwalker walker = *swalker;
   if (walker->gcthresh != 0 && gcops == NULL) {
      LOG(LOG_ERR, "Received NULL gcops reference when the walker is set to produce those operations\n");
      errno = EINVAL;
      return -1;
   }

   if (walker->repackthresh != 0 && repackops == NULL) {
      LOG(LOG_ERR, "Received NULL repackops reference when the walker is set to produce those operations\n");
      errno = EINVAL;
      return -1;
   }

   if (walker->rebuildthresh != 0 && rebuildops == NULL) {
      LOG(LOG_ERR, "Received NULL rebuildops reference when the walker is set to produce those operations\n");
      errno = EINVAL;
      return -1;
   }

   // set up some initial values
   marfs_ds* ds = &walker->pos.ns->prepo->datascheme;
   size_t repackbytethresh = (walker->pos.ns->prepo->datascheme.objsize) ?  // base repack on NS defined target obj size
      (walker->pos.ns->prepo->datascheme.objsize - walker->headerlen) / 2 : 0;
   char pullxattrs = (walker->gcthresh == 0 && walker->repackthresh == 0 && walker->rebuildthresh == 0) ? 0 : 1;
   char dispatchedops = 0;
   // iterate over all reference targets
   while (walker->ftag.endofstream == 0 && (walker->ftag.state & FTAG_DATASTATE) >= FTAG_FIN) {
      // calculate offset of next file tgt
      size_t tgtoffset = 1;
      if (walker->gctag.refcnt) {
         // handle any existing gctag value
         tgtoffset += walker->gctag.refcnt; // skip over count indicated by the tag
         if (walker->gctag.inprog && walker->gcthresh) {
            LOG(LOG_INFO, "Cleaning up in-progress deletion of %zu reference files from previous instance\n", walker->gctag.refcnt);
            opinfo* optgt = NULL;
            walker->ftag.fileno++; // temporarily increase fileno, to match that of the subsequent ref deletion tgt
            if (process_identifyoperation(&walker->gcops, MARFS_DELETE_REF_OP, &walker->ftag, &optgt)) {
               LOG(LOG_ERR, "Failed to identify operation target for GCTAG recovery\n");
               walker->ftag.fileno--;
               return -1;
            }

            walker->ftag.fileno--;

            if (optgt->count) {
               // sanity check
               if (optgt->ftag.fileno + optgt->count - 1 != walker->fileno) {
                  LOG(LOG_ERR, "Failed sanity check: Active ref del op (%zu) does not reach walker fileno (%zu)\n",
                                optgt->ftag.fileno + optgt->count - 1, walker->fileno);
                  return -1;
               }

               // add the reference deletions to our existing operation
               optgt->count += walker->gctag.refcnt;
               delref_info* extinf = optgt->extendedinfo;
               if (walker->gctag.eos) { extinf->eos = 1; }
            }
            else {
               // populate newly created op
               optgt->count = walker->gctag.refcnt;
               optgt->ftag.fileno = walker->ftag.fileno; // be SURE that this is not a dummy op, targeting fileno zero
               delref_info* extinf = optgt->extendedinfo;
               extinf->prev_active_index = walker->activeindex;
               extinf->eos = walker->gctag.eos;
            }
            walker->report.delfiles += walker->gctag.refcnt;
            // clear the 'inprogress' state, just to make sure we never repeat this process
            walker->gctag.inprog = 0;
         }

         // this could potentially indicate a premature EOS
         if (walker->gctag.eos) {
            LOG(LOG_INFO, "GC tag indicates EOS at fileno %zu\n", walker->fileno);
            break;
         }
      }

      // generate next target info
      FTAG tmptag = walker->ftag;
      tmptag.fileno = walker->fileno;
      tmptag.fileno += tgtoffset;

      char* reftgt = datastream_genrpath(&tmptag, walker->reftable, NULL, NULL);
      if (reftgt == NULL) {
         LOG(LOG_ERR, "Failed to generate reference path for corrected tgt (%zu)\n", walker->fileno);
         return -1;
      }

      // pull info for the next reference target
      char filestate = -1;
      char prevdelzero = walker->gctag.delzero;
      char haveftag = pullxattrs;
      int getres = process_getfileinfo(reftgt, pullxattrs, walker, &filestate);
      if (getres < 0) {
         LOG(LOG_ERR, "Failed to get info for reference target: \"%s\"\n", reftgt);
         free(reftgt);
         return -1;
      }

      if (getres > 0) {
         // missing FTAG value means we can't continue to walk the stream
         LOG(LOG_INFO, "Failed to get FTAG of reference target: \"%s\"\n", reftgt);
         walker->report.filecount++;
         walker->report.bytecount += walker->stval.st_size;

         // check for possible gc of this incomplete stream
         if (filestate == 1 && walker->gcthresh && walker->stval.st_ctime < walker->gcthresh) {
            // tell the caller to unlink this reference file, and that the stream ends here
            opinfo* optgt = NULL;
            if (process_identifyoperation(&walker->gcops, MARFS_DELETE_REF_OP, &tmptag, &optgt)) {
               LOG(LOG_ERR, "Failed to identify operation target for deletion of file %zu\n", tmptag.fileno);
               free(reftgt);
               return -1;
            }

            delref_info* delrefinf = optgt->extendedinfo;
            delrefinf->eos = 1; // ALWAYS set this as EndOfStream
            if (optgt->count) {
               // update existing op
               optgt->count++;
               optgt->count += walker->gctag.refcnt;
               // sanity check
               if (delrefinf->prev_active_index != walker->activeindex) {
                  LOG(LOG_ERR, "Active delref op active index (%zu) does not match current val (%zu)\n",
                                delrefinf->prev_active_index, walker->activeindex);
                  free(reftgt);
                  return -1;
               }
            }
            else {
               // populate new op
               optgt->ftag.fileno = walker->ftag.fileno; // be SURE that this is not a dummy op, targeting fileno zero
               optgt->count = 1;
               optgt->count += walker->gctag.refcnt;
               delrefinf->prev_active_index = walker->activeindex;
            }
            walker->report.delfiles++;
         }
         else {
            walker->report.volfiles++;
         }

         // immediately break out of this loop, dispatching any remaining ops
         free(reftgt);
         break;
      }

      pullxattrs = (walker->gcthresh == 0 && walker->repackthresh == 0 && walker->rebuildthresh == 0) ? 0 : 1; // return to default behavior
      if (filestate == 0) {
         // file doesn't exist (likely that we skipped a GCTAG on the previous file)
         // decrement to the previous index and make sure to check for xattrs
         if (walker->fileno == 0) {
            // can't decrement beyond the beginning of the datastream
            LOG(LOG_ERR, "Initial reference target does not exist: \"%s\"\n", reftgt);
            free(reftgt);
            return -1;
         }

         if (walker->fileno == walker->ftag.fileno) {
            // looks like we already pulled xattrs from the previous file, and must not have found a GCTAG
            if ((walker->ftag.state & FTAG_DATASTATE) == FTAG_FIN) {
               // special case, writer has yet to / failed to create the next reference file
               LOG(LOG_WARNING, "Datastream break (assumed EOS) detected at file number %zu: \"%s\"\n", walker->fileno, reftgt);
               free(reftgt);

               // modify any active GC ref-del op to properly include EOS value
               if (walker->gcthresh) {
                  opinfo* optgt = NULL;
                  if (searchoperations(&walker->gcops, MARFS_DELETE_REF_OP, &tmptag, &optgt) == 0) {
                     LOG(LOG_INFO, "Detected outstanding reference deletion op is being set to assume EOS\n");
                     delref_info* delrefinf = optgt->extendedinfo;
                     delrefinf->eos = 1; // set to assume EndOfStream
                  }
               }

               walker->ftag.endofstream = 1; // set previous ftag value to reflect assumed EOS
               break;
            }

            LOG(LOG_ERR, "Datastream break detected at file number %zu: \"%s\"\n", walker->fileno, reftgt);
            free(reftgt);
            return -1;
         }

         // generate the rpath of the previous file
         free(reftgt);
         tmptag.fileno = walker->fileno;
         reftgt = datastream_genrpath(&tmptag, walker->reftable, NULL, NULL);
         if (reftgt == NULL) {
            LOG(LOG_ERR, "Failed to generate reference path for previous file (%zu)\n", walker->fileno);
            return -1;
         }

         LOG(LOG_INFO, "Pulling xattrs from previous fileno (%zu) due to missing fileno %zu\n",
              walker->fileno, walker->fileno + 1);

         // failure or missing file is unacceptable here
         getres = process_getfileinfo(reftgt, 1, walker, &filestate);
         if (getres < 0 || filestate == 0) {
            LOG(LOG_ERR, "Failed to get info for previous ref tgt: \"%s\"\n", reftgt);
            free(reftgt);
            return -1;
         }

         if (getres > 0) {
            // missing FTAG value means we can't walk the stream
            LOG(LOG_INFO, "Failed to get FTAG of previous reference target: \"%s\"\n", reftgt);
            // check for possible gc of this incomplete stream
            if (filestate == 1 && walker->gcthresh && walker->stval.st_ctime < walker->gcthresh) {
               // tell the caller to unlink this reference file, and that the stream ends here
               opinfo* optgt = NULL;
               if (process_identifyoperation(&walker->gcops, MARFS_DELETE_REF_OP, &tmptag, &optgt)) {
                  LOG(LOG_ERR, "Failed to identify operation target for deletion of file %zu\n", walker->ftag.fileno);
                  free(reftgt);
                  return -1;
               }

               delref_info* delrefinf = optgt->extendedinfo;
               delrefinf->eos = 1; // ALWAYS set this as EndOfStream
               if (optgt->count) {
                  // update existing op
                  optgt->count++;
                  optgt->count += walker->gctag.refcnt;
                  // sanity check
                  if (delrefinf->prev_active_index != walker->activeindex) {
                     LOG(LOG_ERR, "Active delref op active index (%zu) does not match current val (%zu)\n",
                                   delrefinf->prev_active_index, walker->activeindex);
                     free(reftgt);
                     return -1;
                  }
               }
               else {
                  // populate new op
                  optgt->ftag.fileno = walker->ftag.fileno; // be SURE that this is not a dummy op, targeting fileno zero
                  optgt->count = 1;
                  optgt->count += walker->gctag.refcnt;
                  delrefinf->prev_active_index = walker->activeindex;
               }

               walker->report.delfiles++;
            }
            else {
               walker->report.volfiles++;
            }

            // immediately break out of this loop, dispatching any remaining ops
            free(reftgt);
            walker->ftag.state = FTAG_INIT; // force an 'init' condition into our ftag state, so closestreamwalker() knows this wasn't called early
            break;
         }

         free(reftgt);
         continue; // restart this iteration, now with all info available
      }

      free(reftgt);
      // many checks are only appropriate if we're pulling xattrs
      size_t endobj = walker->objno;
      char eos = 0;
      if (haveftag) {
         // check for innapropriate FTAG value
         if (walker->ftag.fileno != walker->fileno + tgtoffset) {
            LOG(LOG_ERR, "Invalid FTAG filenumber (%zu) on file %zu\n", walker->ftag.fileno, walker->fileno + tgtoffset);
            return -1;
         }

         endobj = datastream_filebounds(&walker->ftag); // update ending object index
         eos = walker->ftag.endofstream;
         if ((walker->ftag.state & FTAG_DATASTATE) < FTAG_FIN) { eos = 1; }
         if (walker->gctag.refcnt && walker->gctag.eos) { eos = 1; }

         // check for object transition
         if (walker->ftag.objno != walker->objno) {
            // only update object count if we are doing a 'full' walk
            if (walker->gcthresh || walker->repackthresh || walker->rebuildthresh) {
               // note the previous obj in counts
               walker->report.objcount++;
            }

            // we may need to delete the previous object IF we are GCing AND no active refs existed for that obj
            //    AND it is not an already a deleted object0
            if (walker->gcthresh && walker->activefiles == 0  &&
                 (walker->objno != 0 || prevdelzero == 0)) {
               // need to prepend an object deletion operation for the previous objno
               LOG(LOG_INFO, "Adding deletion op for object %zu\n", walker->objno);
               opinfo* optgt = NULL;
               FTAG tmptag = walker->ftag;
               tmptag.objno = walker->objno;
               if (process_identifyoperation(&walker->gcops, MARFS_DELETE_OBJ_OP, &tmptag, &optgt)) {
                  LOG(LOG_ERR, "Failed to identify operation target for deletion of object %zu\n", walker->objno);
                  return -1;
               }

               // sanity check
               if (optgt->count + optgt->ftag.objno != walker->objno) {
                  LOG(LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n",
                                optgt->count + optgt->ftag.objno, walker->objno);
                  return -1;
               }

               // update operation
               optgt->count++;

               // update our record
               walker->report.delobjs++;
               if (walker->objno == 0) {
                  LOG(LOG_INFO, "Updating DEL-REF op to note deletion of object zero\n");
                  // need to ensure we specifically note deletion of initial object
                  optgt = NULL;
                  if (process_identifyoperation(&walker->gcops, MARFS_DELETE_REF_OP, &walker->ftag, &optgt)) {
                     LOG(LOG_ERR, "Failed to identify operation target for noting deletion of obj zero\n");
                     return -1;
                  }
                  // NOTE -- don't increment count (as we normally would), as we aren't actually deleting another ref
                  delref_info* delrefinf = (delref_info*)optgt->extendedinfo;
                  delrefinf->delzero = 1;
               }
            }

            // need to handle repack ops
            if (walker->rpckops) {
               if (walker->activebytes >= repackbytethresh) {
                  // discard all ops
                  // NOTE -- a zero value for repackbytethresh (no object chunking) will always cause this discard
                  LOG(LOG_INFO, "Discarding repack ops due to excessive active bytes: %zu\n", walker->activebytes);
                  resourcelog_freeopinfo(walker->rpckops);
               }
               else {
                  // dispatch all ops
                  walker->report.rpckfiles += walker->rpckops->count;
                  walker->report.rpckbytes += ((repack_info*) walker->rpckops->extendedinfo)->totalbytes;
                  *repackops = walker->rpckops;
                  dispatchedops = 1; // note to exit after this file
               }

               walker->rpckops = NULL;
            }

            // possibly update rebuild ops
            if (walker->rebuildthresh && walker->activefiles && walker->stval.st_ctime < walker->rebuildthresh) {
               // check if previous object targets our rebuild location
               char* objname = NULL;
               ne_erasure erasure;
               ne_location location;
               FTAG tmptag = walker->ftag;
               tmptag.objno = walker->objno;
               if (datastream_objtarget(&tmptag, ds, &objname, &erasure, &location)) {
                  LOG(LOG_ERR, "Failed to populate object target info for object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
                  return -1;
               }

               free(objname); // don't actually need object name, so just destroy it immediately
               // check for location match
               if ((walker->rebuildloc.pod < 0 || walker->rebuildloc.pod == location.pod)  &&
                    (walker->rebuildloc.cap < 0 || walker->rebuildloc.cap == location.cap)  &&
                    (walker->rebuildloc.scatter < 0 || walker->rebuildloc.scatter == location.scatter)) {
                  // generate a rebuild op for this object
                  opinfo* optgt = NULL;
                  if (process_identifyoperation(&walker->rbldops, MARFS_REBUILD_OP, &tmptag, &optgt)) {
                     LOG(LOG_ERR, "Failed to identify operation target for rebuild of spanned obj %zu\n", tmptag.objno);
                     return -1;
                  }
                  // sanity check
                  if (optgt->count + optgt->ftag.objno != tmptag.objno) {
                     LOG(LOG_ERR, "Existing obj rebuild count (%zu) does not match current obj (%zu)\n",
                                   optgt->count + optgt->ftag.objno, tmptag.objno);
                     return -1;
                  }
                  // update operation
                  optgt->count++;
                  // update our record
                  walker->report.rbldobjs++;
               }
            }

            // update state
            walker->activefiles = 0; // update active file count for new obj
            walker->activebytes = 0; // update active byte count for new obj
            walker->objno = walker->ftag.objno; // progress to the new obj
         }
      }

      char assumeactive = 0;
      if (filestate == 1) {
         // file is inactive
         if (walker->gcthresh && walker->stval.st_ctime < walker->gcthresh && haveftag) {
            // this file is elligible for GC; create/update a reference deletion op
            opinfo* optgt = NULL;
            if (process_identifyoperation(&walker->gcops, MARFS_DELETE_REF_OP, &walker->ftag, &optgt)) {
               LOG(LOG_ERR, "Failed to identify operation target for deletion of file %zu\n", walker->ftag.fileno);
               return -1;
            }

            delref_info* delrefinf = NULL;
            if (optgt->count) {
               // update existing op
               optgt->count++;
               optgt->count += walker->gctag.refcnt;
               delrefinf = optgt->extendedinfo;
               // sanity check
               if (delrefinf->prev_active_index != walker->activeindex) {
                  LOG(LOG_ERR, "Active delref op active index (%zu) does not match current val (%zu)\n",
                                delrefinf->prev_active_index, walker->activeindex);
                  return -1;
               }

               if (delrefinf->eos == 0) { delrefinf->eos = eos; }
            }
            else {
               // populate new op
               optgt->ftag.fileno = walker->ftag.fileno; // be SURE that this is not a dummy op, targeting fileno zero
               optgt->count = 1;
               optgt->count += walker->gctag.refcnt;
               delrefinf = optgt->extendedinfo;
               delrefinf->prev_active_index = walker->activeindex;
               delrefinf->eos = eos;
            }

            walker->report.delfiles++;

            if (endobj != walker->objno) {
               // potentially generate a GC op for the first obj referenced by this file
               FTAG tmptag = walker->ftag;
               if (walker->activefiles == 0 && (walker->ftag.objno != 0 || walker->gctag.delzero == 0)) {
                  LOG(LOG_INFO, "Adding deletion op for inactive file initial object %zu\n", walker->ftag.objno);
                  optgt = NULL;
                  if (process_identifyoperation(&walker->gcops, MARFS_DELETE_OBJ_OP, &tmptag, &optgt)) {
                     LOG(LOG_ERR, "Failed to identify operation target for deletion of initial spanned obj %zu\n", tmptag.objno);
                     return -1;
                  }
                  // sanity check
                  if (optgt->count + optgt->ftag.objno != tmptag.objno) {
                     LOG(LOG_ERR, "Existing obj deletion count (%zu = Count%zu+Tag%zu) does not match current obj (%zu)\n",
                                   optgt->count + optgt->ftag.objno, optgt->count, optgt->ftag.objno, tmptag.objno);
                     return -1;
                  }

                  // update operation
                  optgt->count++;

                  // potentially note deletion of object zero
                  if (tmptag.objno == 0) { delrefinf->delzero = 1; }

                  // update our record
                  walker->report.delobjs++;
               }

               // potentially generate GC ops for objects spanned by this file
               tmptag.objno++;
               while (tmptag.objno < endobj) {
                  // generate ops for all but the last referenced object
                  LOG(LOG_INFO, "Adding deletion op for inactive spanned object %zu\n", tmptag.objno);
                  optgt = NULL;
                  if (process_identifyoperation(&walker->gcops, MARFS_DELETE_OBJ_OP, &tmptag, &optgt)) {
                     LOG(LOG_ERR, "Failed to identify operation target for deletion of spanned obj %zu\n", tmptag.objno);
                     return -1;
                  }

                  // sanity check
                  if (optgt->count + optgt->ftag.objno != tmptag.objno) {
                     LOG(LOG_ERR, "Existing obj deletion count (%zu = Count%zu+Tag%zu) does not match current obj (%zu)\n",
                                   optgt->count + optgt->ftag.objno, optgt->count, optgt->ftag.objno, tmptag.objno);
                     return -1;
                  }

                  // update operation
                  optgt->count++;

                  // update our record
                  walker->report.delobjs++;

                  // iterate to the next obj
                  tmptag.objno++;
               }
            }

            // potentially generate a GC op for the final object of the stream
            if (eos && (endobj != walker->objno || walker->activefiles == 0)) {
               FTAG tmptag = walker->ftag;
               tmptag.objno = endobj;

               optgt = NULL;
               if (process_identifyoperation(&walker->gcops, MARFS_DELETE_OBJ_OP, &tmptag, &optgt)) {
                  LOG(LOG_ERR, "Failed to identify operation target for deletion of final stream obj %zu\n", tmptag.objno);
                  return -1;
               }
               // sanity check
               if (optgt->count + optgt->ftag.objno != tmptag.objno) {
                  LOG(LOG_ERR, "Existing obj deletion count (%zu) does not match current obj (%zu)\n",
                                optgt->count + optgt->ftag.objno, walker->objno);
                  return -1;
               }

               // update operation
               optgt->count++;

               // update our record
               walker->report.delobjs++;
            }
         }
         else if (walker->stval.st_ctime >= walker->gcthresh) {
            // this file was too recently deactivated to gc
            walker->report.volfiles++;
            assumeactive = 1;
         }
      }

      // note newly encountered file
      walker->report.filecount++;
      if (haveftag) { walker->report.bytecount += walker->ftag.bytes; }
      else { walker->report.bytecount += walker->stval.st_size; }
      if (filestate > 1) {
         // file is active
         walker->report.fileusage++;
         if (haveftag) { walker->report.byteusage += walker->ftag.bytes; }
         else { walker->report.byteusage += walker->stval.st_size; }
         // TODO manage repack ops
         // potentially generate rebuild ops
         if (walker->rebuildthresh && walker->stval.st_ctime < walker->rebuildthresh) {
            // iterate over all objects spanned by this file
            FTAG tmptag = walker->ftag;
            size_t finobj = endobj;
            if (eos) { finobj++; } // include the final obj referenced by this file, specifically if no files follow
            while (tmptag.objno < finobj) {
               // check if object targets our rebuild location
               char* objname = NULL;
               ne_erasure erasure;
               ne_location location;
               if (datastream_objtarget(&tmptag, ds, &objname, &erasure, &location)) {
                  LOG(LOG_ERR, "Failed to populate object target info for object %zu of stream \"%s\"\n", tmptag.objno, tmptag.streamid);
                  return -1;
               }

               free(objname); // don't actually need object name, so just destroy it immediately

               // check for location match
               if ((walker->rebuildloc.pod < 0 || walker->rebuildloc.pod == location.pod)  &&
                    (walker->rebuildloc.cap < 0 || walker->rebuildloc.cap == location.cap)  &&
                    (walker->rebuildloc.scatter < 0 || walker->rebuildloc.scatter == location.scatter)) {
                  // generate a rebuild op for this object
                  opinfo* optgt = NULL;
                  if (process_identifyoperation(&walker->rbldops, MARFS_REBUILD_OP, &tmptag, &optgt)) {
                     LOG(LOG_ERR, "Failed to identify operation target for rebuild of spanned obj %zu\n", tmptag.objno);
                     return -1;
                  }

                  // sanity check
                  if (optgt->count + optgt->ftag.objno != tmptag.objno) {
                     LOG(LOG_ERR, "Existing obj rebuild count (%zu) does not match current obj (%zu)\n",
                                   optgt->count + optgt->ftag.objno, tmptag.objno);
                     return -1;
                  }

                  // update operation
                  optgt->count++;

                  // update our record
                  walker->report.rbldobjs++;
               }

               // iterate to the next obj
               tmptag.objno++;
            }
         }
      }

      // potentially update values based on spanned objects
      if (walker->objno != endobj) {
         walker->activefiles = 0; // update active file count for new obj
         walker->activebytes = 0; // update active byte count for new obj
         // only update object count if we are doing a 'full' walk
         if (walker->gcthresh || walker->repackthresh || walker->rebuildthresh) {
            walker->report.objcount += endobj - walker->objno;
         }
         // need to handle existing rebuild ops
         if (walker->rbldops) {
            // dispatch all ops
            *rebuildops = walker->rbldops;
            walker->rbldops = NULL;
            dispatchedops = 1; // note to exit after this file
         }
      }

      if (filestate > 1 || assumeactive) {
         // handle GC state
         walker->activeindex = walker->fileno + tgtoffset;
         walker->activefiles++;

         // handle repack state
         if (haveftag) { walker->activebytes += walker->ftag.bytes; }
         else { walker->activebytes += walker->stval.st_size; }

         // dispatch any GC ops
         if (walker->gcops) {
            *gcops = walker->gcops;
            dispatchedops = 1; // note to exit after this file
            walker->gcops = NULL;
         }
      }

      // update walker state to reflect new target
      walker->fileno += tgtoffset;
      walker->objno = endobj;

      // check for termination due to operation dispatch
      if (dispatchedops) {
         LOG(LOG_INFO, "Exiting due to operation dispatch\n");
         return 1;
      }
   }

   // dispatch any remianing ops
   if (walker->gcops) {
      *gcops = walker->gcops;
      dispatchedops = 1;
      walker->gcops = NULL;
   }

   if (walker->rpckops) {
      if (walker->activebytes >= repackbytethresh) {
         // discard all ops
         LOG(LOG_INFO, "Discarding repack ops due to excessive active bytes: %zu\n", walker->activebytes);
         resourcelog_freeopinfo(walker->rpckops);
      }
      else {
         // record repack counts
         opinfo* repackparse = walker->rpckops;
         while (repackparse) {
            walker->report.rpckfiles += repackparse->count;
            walker->report.rpckbytes += ((repack_info*) repackparse->extendedinfo)->totalbytes;
            repackparse = repackparse->next;
         }
         // dispatch all ops
         *repackops = walker->rpckops;
         dispatchedops = 1;
      }
      walker->rpckops = NULL;
   }

   if (walker->rbldops) {
      *rebuildops = walker->rbldops;
      dispatchedops = 1;
      walker->rbldops = NULL;
   }

   if (dispatchedops) {
      LOG(LOG_INFO, "Returning note of operation dispatch\n");
      return 1;
   }

   LOG(LOG_INFO, "Stream traversal complete: \"%s\"\n", walker->ftag.streamid);
   return 0;
}

/**
 * Close the given streamwalker
 * @param streamwalker* swalker : Reference to the streamwalker to be closed
 * @param streamwalker_report* report : Reference to a report to be populated with final counts
 * @return int : Zero on success, 1 if the walker was closed prior to iteration completion, or -1 on failure
 */
int streamwalker_close(streamwalker* swalker, streamwalker_report* report) {
   // check args
   if (swalker == NULL) {
      LOG(LOG_ERR, "Received a NULL streamwalker reference\n");
      errno = EINVAL;
      return -1;
   }

   streamwalker walker = *swalker;
   if (walker == NULL) {
      LOG(LOG_ERR, "Received a NULL streamwalker\n");
      errno = EINVAL;
      return -1;
   }

   // populate final report
   if (report)  {
      *report = walker->report;
   }

   // check for incomplete walker
   int retval = 0;
   if (walker->gcops || walker->rpckops || walker->rbldops  ||
        (
            walker->ftag.endofstream == 0  &&
            (walker->gctag.refcnt == 0 || walker->gctag.eos == 0)  &&
            (walker->ftag.state & FTAG_DATASTATE) >= FTAG_FIN
       )) {
      LOG(LOG_WARNING, "Streamwalker closed prior to iteration completion\n");
      retval = 1;
   }

   destroystreamwalker(walker);
   *swalker = NULL;
   return retval;
}
