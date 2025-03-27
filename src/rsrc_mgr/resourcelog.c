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

#include <pthread.h>

#include "rsrc_mgr/consts.h"
#include "rsrc_mgr/resourcelog.h"

//   -------------   INTERNAL DEFINITIONS    -------------

#define RECORD_LOG_PREFIX "RESOURCE-RECORD-LOGFILE\n" // prefix for a 'record'-log
                                                      //    - only op starts, no completions
#define MODIFY_LOG_PREFIX "RESOURCE-MODIFY-LOGFILE\n" // prefix for a 'modify'-log
                                                      //    - mix of op starts and completions

typedef struct opchain {
   struct opchain* next; // subsequent op chains in this list (or NULL, if none remain)
   struct opinfo* chain; // ops in this chain
} opchain;

typedef struct resourcelog {
   // synchronization and access control
   pthread_mutex_t   lock;
   pthread_cond_t    nooutstanding;  // left NULL for a 'record' log
   ssize_t           outstandingcnt; // count of scanners still running (threads that could potentially submit more ops)
                                     //  always zero for a 'record' log
   // state info
   resourcelog_type     type;
   operation_summary summary;
   HASH_TABLE        inprogress;  // left NULL for a 'record' log
   int               logfile;
   char*             logfilepath;
}*RESOURCELOG;

//   -------------   INTERNAL FUNCTIONS    -------------

/**
 * Clean up the provided resourcelog (lock must be held)
 * @param RESOURCELOG rsrclog : Reference to the resourcelog to be cleaned
 * @param char destroy : If non-zero, the entire resourcelog structure will be freed
 *                       If zero, all state will be purged, but the struct can be reinitialized
 */
static void cleanuplog(RESOURCELOG rsrclog, char destroy) {
   HASH_NODE* nodelist = NULL;
   size_t index = 0;
   if (rsrclog->inprogress) {
      hash_term(rsrclog->inprogress, &nodelist, &index);
      while (index) {
         opchain* chain = (opchain*) nodelist[index - 1].content;
         while (chain) {
            resourcelog_freeopinfo(chain->chain);
            opchain* nextchain = chain->next;
            free(chain);
            chain = nextchain;
         }

         free(nodelist[index - 1].name);
         index--;
      }

      free(nodelist);
      rsrclog->inprogress = NULL;
   }

   free(rsrclog->logfilepath);
   close(rsrclog->logfile);

   if (destroy) {
      pthread_cond_destroy(&rsrclog->nooutstanding);
      pthread_mutex_unlock(&rsrclog->lock);
      pthread_mutex_destroy(&rsrclog->lock);
      free(rsrclog);
   }
   else {
      pthread_mutex_unlock(&rsrclog->lock);
   }
}

/**
 * Incorporate the given opinfo string into the given resourcelog
 * @param RESOURCELOG rsrclog : resourcelog to be updated
 * @param opinfo* newop : Operation(s) to be included
 *                        NOTE -- This func will never free opinfo structs
 * @param char* progressop : Flag to be populated with the completion status of an operation set
 *                           If set to zero, the updated operation is still ongoing
 *                           If set to one, the updated operation has sucessfully completed
 *                           If set to negative one, the updated operation completed with errors
 * @return int : Zero on success, or -1 on failure
 */
int processopinfo(RESOURCELOG rsrclog, opinfo* newop, char* progressop, char* dofree) {
   // identify the length of the given chain and validate a consistent 'start' value
   LOG(LOG_INFO, "Processing operation chain on Stream \"%s\" w/ CTag \"%s\"\n", newop->ftag.streamid, newop->ftag.ctag);

   opinfo* finop = newop;
   size_t oplength = 0;
   while (finop) {
      oplength++;
      LOG(LOG_INFO, "   (\"%s\", \"%s\") -- %s OP of Count %zd (%s)\n", finop->ftag.ctag, finop->ftag.streamid,
                     (finop->type == MARFS_DELETE_OBJ_OP) ? "MARFS_DELETE_OBJ" :
                     (finop->type == MARFS_DELETE_REF_OP) ? "MARFS_DELETE_REF" :
                     (finop->type == MARFS_REBUILD_OP) ? "MARFS_REBUILD" :
                     (finop->type == MARFS_REPACK_OP) ? "MARFS_REPACK" :
                     "UNKNOWN", finop->count, (finop->start) ? "INITIATION" : "COMPLETION");
      if (finop->start != newop->start) {
         LOG(LOG_ERR, "Operation chain has inconsistent START value\n");
         return -1;
      }

      finop = finop->next;
   }

   LOG(LOG_INFO, "   (\"%s\", \"%s\") -- End of OP chain of length %zd\n", newop->ftag.ctag, newop->ftag.streamid, oplength);

   // map this operation into our inprogress hash table
   HASH_NODE* node = NULL;
   if (hash_lookup(rsrclog->inprogress, newop->ftag.streamid, &node) < 0) {
      LOG(LOG_ERR, "Failed to map operation on \"%s\" into inprogress HASH_TABLE\n", newop->ftag.streamid);
      return -1;
   }

   // traverse the attached operations, looking for a match
   char activechain = 0;
   char activealike = 0; // track if we have active ops of the same type in the same 'segment'
   opinfo* opindex = NULL;
   opchain* chainindex = node->content;
   opchain** prevchainref = (opchain**)&node->content;
   while (chainindex) {
      opindex = chainindex->chain;
      // check for an opchain on the same stream (ctag, streamid)
      if (opindex && strcmp(opindex->ftag.streamid, newop->ftag.streamid) == 0  &&
            ((opindex->ftag.ctag == NULL && newop->ftag.ctag == NULL)  ||
                   (opindex->ftag.ctag && newop->ftag.ctag && strcmp(opindex->ftag.ctag, newop->ftag.ctag) == 0))
        ) {
         while (opindex != NULL) {
            // check for exact match (type, fileno, objno)
            if ((opindex->ftag.objno == newop->ftag.objno)  &&
                 (opindex->ftag.fileno == newop->ftag.fileno)  &&
                 (opindex->type == newop->type)) {
               break;
            }
            else if (opindex->type != newop->type) {
               activealike = 0; // op 'segement' is broken, reset
            }
            else if (opindex->count) {
               activealike = 1; // found an active op of the same type
            }

            if (opindex->count) {
                activechain = 1;
            }

            opindex = opindex->next;
         }

         if (opindex) {
            break; // break from the parent loop, if we've found a match
         }
      }

      opindex = NULL; // moving to a new chain, so NULL out our potential op match
      activealike = 0; // moving to a new chain, so the op 'segment' is broken
      activechain = 0; // moving to a new chain, so reset active state
      prevchainref = &chainindex->next;
      chainindex = chainindex->next;
   }

   if (opindex != NULL) {
      // otherwise, for op completion, we'll need to process each operation in the chain...
      opinfo* parseop = newop; // new op being added to totals
      opinfo* parseindex = opindex; // old op being compared against
      opinfo* previndex = opindex;  // end of the chain of old ops
      while (parseop) {
         if (newop->start == 0) {
            // check for excessive count
            if (parseop->count > parseindex->count) {
               LOG(LOG_ERR, "Processed %s op count (%zu) exceeds expected count (%zu)\n",
                             (parseop->type == MARFS_DELETE_OBJ_OP) ? "MARFS_DELETE_OBJ" :
                             (parseop->type == MARFS_DELETE_REF_OP) ? "MARFS_DELETE_REF" :
                             (parseop->type == MARFS_REBUILD_OP) ? "MARFS_REBUILD" :
                             (parseop->type == MARFS_REPACK_OP) ? "MARFS_REPACK" :
                             "UNKNOWN",
                             parseop->count, parseindex->count);
               return -1;
            }

            // ...note each in our totals...
            switch(parseop->type) {
               case MARFS_DELETE_OBJ_OP:
                  LOG(LOG_INFO, "Noted completion of a MARFS_DELETE_OBJ operation\n");
                  rsrclog->summary.deletion_object_count += parseop->count;
                  if (parseop->errval) { rsrclog->summary.deletion_object_failures += parseop->count; }
                  break;
               case MARFS_DELETE_REF_OP:
                  LOG(LOG_INFO, "Noted completion of a MARFS_DELETE_REF operation\n");
                  rsrclog->summary.deletion_reference_count += parseop->count;
                  if (parseop->errval) { rsrclog->summary.deletion_reference_failures += parseop->count; }
                  break;
               case MARFS_REBUILD_OP:
                  LOG(LOG_INFO, "Noted completion of a MARFS_REBUILD operation\n");
                  rsrclog->summary.rebuild_count += parseop->count;
                  if (parseop->errval) { rsrclog->summary.rebuild_failures += parseop->count; }
                  break;
               case MARFS_REPACK_OP:
                  LOG(LOG_INFO, "Noted completion of a MARFS_REPACK operation\n");
                  rsrclog->summary.repack_count += parseop->count;
                  if (parseop->errval) { rsrclog->summary.repack_failures += parseop->count; }
                  break;
               default:
                  LOG(LOG_ERR, "Unrecognized operation type value\n");
                  return -1;
            }

            if (parseop->errval) {
               parseindex->errval = parseop->errval; // potentially note an error
            }

            // decrement remaining op count
            LOG(LOG_INFO, "Decrementing remaining op count (%zd) by %zd\n", parseindex->count, parseop->count);
            parseindex->count -= parseop->count; // decrement count

            // check for completion of the specified op
            if (parseindex->count) {
               // first operation is not complete, so cannot progress op chain
               if (parseop->next) {
                  LOG(LOG_ERR, "Operation is incomplete, but input operation chain continues\n");
                  return -1;
               }

               if (progressop) {
                  *progressop = 0;
               }

               *dofree = 1; // not incorporating these ops, so the caller should free
               return 0;
            }

            if (parseop->next == NULL && parseindex->next) {
               // check for subsequent active op in the same segment
               opinfo* tmpparse = parseindex;
               while (tmpparse->next && parseop->type == tmpparse->next->type) {
                  if (tmpparse->next->count) {
                     activealike = 1;
                     break;
                  }
                  tmpparse = tmpparse->next;
               }
            }

            // progress to the next op in the chain, validating that it matches the subsequent in the opindex chain
            parseop = parseop->next;
            previndex = parseindex; // keep track of where the index op chain terminates
            parseindex = parseindex->next;

            if (parseop) {
               // at this point, any variation between operation chains is a fatal error
               if (parseindex == NULL || ftag_cmp(&parseop->ftag, &parseindex->ftag) || parseop->type != parseindex->type) {
                  LOG(LOG_ERR, "Operation completion chain does not match outstainding operation chain\n");
                  return -1;
               }
            }
            else if (progressop) {
               if (activealike) {
                  *progressop = 0; // do not progress if alike ops remain in a single 'segment'
               }
               else if (previndex->errval == 0) {
                  *progressop = 1; // corresponding ops were completed without error
               }
               else {
                  *progressop = -1; // note that the corresponding ops were completed with errors
               }
            }

            // decrement in-progress cnt
            rsrclog->outstandingcnt--;
            LOG(LOG_INFO, "Decrementing Outstanding OP Count to %zd\n", rsrclog->outstandingcnt);
         }
         else {
            if (parseop->count > parseindex->count) {
               LOG(LOG_INFO, "Resetting count of in-progress operation from %zu to %zu\n",
                    parseindex->count, parseop->count);

               // potentially increment our in-progress cnt, if this operation is being re-issued
               if (parseindex->count == 0) {
                  rsrclog->outstandingcnt++;
                  LOG(LOG_INFO, "Incremented Outstanding OP Count to %zd\n", rsrclog->outstandingcnt);
               }

               parseindex->count = parseop->count;
            }

            // just progress to the next op
            parseop = parseop->next;
            parseindex = parseindex->next;
         }
      }
      // check if this op has rendered the entire op chain complete
      // NOTE -- This method of freeing opchains is intended to avoid early freeing in the
      //         case of a trailing op with count == 0.  However, it has a potential for memory
      //         inefficiency.
      //         If the caller completes the chain via anything other than the tail op, the
      //         chain will not be freed until the entire resourcelog is terminated.
      //         As we never expect the caller to behave like this, I am leaving this logic
      //         as is.
      if (newop->start == 0 && activechain == 0 && parseindex == NULL) {
         // destroy the entire operation chain
         resourcelog_freeopinfo(chainindex->chain);

         // ...and remove the matching op chain
         *prevchainref = chainindex->next;
         free(chainindex);
      }

      // a matching op means the parsed operation can be discarded
      // tell the caller to free their own op chain
      *dofree = 1;
   }
   else {
      // should indicate the start of a new operation
      if (newop->start == 0) {
         LOG(LOG_ERR, "Received completion of op with no parsed start of op in logfile \"%s\"\n", rsrclog->logfilepath);
         return -1;
      }
      if (rsrclog->type == RESOURCE_MODIFY_LOG) {
         // stitch the new op chain onto the end of our inprogress list
         *prevchainref = calloc(1, sizeof(**prevchainref));
         (*prevchainref)->chain = newop;
         // note that we have another op chain in flight
         rsrclog->outstandingcnt += oplength;
         LOG(LOG_INFO, "Increased Outstanding OP Count by %zd, to %zd\n", oplength, rsrclog->outstandingcnt);
         // tell the caller NOT to free this chain
         *dofree = 0;
      }
      else { // for RECORD logs, note all ops in our totals
         opinfo* parseop = newop; // new op being added to totals
         while (parseop) {
            // ...note each in our totals...
            switch(parseop->type) {
               case MARFS_DELETE_OBJ_OP:
                  rsrclog->summary.deletion_object_count += parseop->count;
                  LOG(LOG_INFO, "Recorded MARFS_DELETE_OBJ operation\n");
                  break;
               case MARFS_DELETE_REF_OP:
                  rsrclog->summary.deletion_reference_count += parseop->count;
                  LOG(LOG_INFO, "Recorded MARFS_DELETE_REF operation\n");
                  break;
               case MARFS_REBUILD_OP:
                  rsrclog->summary.rebuild_count += parseop->count;
                  LOG(LOG_INFO, "Recorded MARFS_REBUILD operation\n");
                  break;
               case MARFS_REPACK_OP:
                  rsrclog->summary.repack_count += parseop->count;
                  LOG(LOG_INFO, "Recorded MARFS_REPACK operation\n");
                  break;
               default:
                  LOG(LOG_ERR, "Unrecognized operation type value\n");
                  return -1;
            }

            parseop = parseop->next;
         }

         // tell the caller to free their own op chain
         *dofree = 1;
      }
   }

   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Free the given opinfo struct chain
 * @param opinfo* op : Reference to the opinfo chain to be freed
 */
void resourcelog_freeopinfo(opinfo* op) {
   char* prevctag = NULL;
   char* prevstreamid = NULL;
   while (op) {
      if (op->extendedinfo) {
         switch (op->type) { // only REBUILD and DEL-REF ops should have extended info at all
            case MARFS_REBUILD_OP:
               {
                  rebuild_info* extinfo = (rebuild_info*)op->extendedinfo;
                  free(extinfo->markerpath);
                  if (extinfo->rtag) {
                     rtag_free(extinfo->rtag);
                     free(extinfo->rtag);
                  }
                  break;
               }
            default: // nothing extra to be done for other ops
               break;
         }

         free(op->extendedinfo);
      }
      // free ctag+streamid only if they are not simply duplicated references throughout the op chain
      // NOTE -- This won't catch if non-neighbor ops have the same string values, but I am considering that a degenerate case.
      //         Behavior would be a double free by this func.
      if (op->ftag.ctag && op->ftag.ctag != prevctag) {
         prevctag = op->ftag.ctag;
         free(op->ftag.ctag);
      }

      if (op->ftag.streamid && op->ftag.streamid != prevstreamid) {
         prevstreamid = op->ftag.streamid;
         free(op->ftag.streamid);
      }

      opinfo* nextop = op->next;
      free(op);
      op = nextop;
   }
}

/**
 * Duplicate the given opinfo struct chain
 * @param opinfo* op : Reference to the opinfo chain to be duplicated
 * @return opinfo* : Reference to the newly created duplicate chain
 */
opinfo* resourcelog_dupopinfo(opinfo* op) {
   // validate arg
   if (op == NULL) {
      LOG(LOG_ERR, "Received a NULL opinfo reference\n");
      return NULL;
   }

   // parse over the op chain
   opinfo* genchain = NULL;
   opinfo** newop = &genchain;
   while (op) {
      // allocate a new op struct
      *newop = malloc(sizeof(**newop));
      **newop = *op; // just duplicate op values, to start
      (*newop)->ftag.ctag = NULL;
      (*newop)->ftag.streamid = NULL;
      (*newop)->extendedinfo = NULL; // always NULL these out, just in case
      // allocate new ctag/streamid
      (*newop)->ftag.ctag = strdup(op->ftag.ctag);
      (*newop)->ftag.streamid = strdup(op->ftag.streamid);

      // potentially populate extended info
      if (op->extendedinfo) {
         switch (op->type) { // only REBUILD and DEL-REF ops should have extended info at all
            case MARFS_DELETE_OBJ_OP:
               {
                  delobj_info* extinfo = (delobj_info*)op->extendedinfo;
                  delobj_info* newinfo = malloc(sizeof(*newinfo));
                  *newinfo = *extinfo;
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            case MARFS_DELETE_REF_OP:
               {
                  delref_info* extinfo = (delref_info*)op->extendedinfo;
                  delref_info* newinfo = malloc(sizeof(*newinfo));
                  *newinfo = *extinfo;
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            case MARFS_REBUILD_OP:
               {
                  rebuild_info* extinfo = (rebuild_info*)op->extendedinfo;
                  rebuild_info* newinfo = NULL;
                  // must allow for rebuild ops without extended info (rebuild by location case)
                  if (extinfo) {
                     // duplicate extended info
                     newinfo = calloc(1, sizeof(*newinfo));
                     if (extinfo->rtag) {
                        newinfo->rtag = calloc(1, sizeof(RTAG));
                        if (rtag_dup(extinfo->rtag, newinfo->rtag)) {
                           LOG(LOG_ERR, "Failed to duplicate rebuild extended info RTAG structure\n");
                           free(newinfo->rtag);
                           resourcelog_freeopinfo(genchain);
                           return NULL;
                        }
                     }

                     if (extinfo->markerpath) {
                        newinfo->markerpath = strdup(extinfo->markerpath);
                        if (newinfo->markerpath == NULL) {
                           LOG(LOG_ERR, "Failed to duplicate rebuild marker path\n");
                           if (newinfo->rtag) {
                              rtag_free(newinfo->rtag);
                              free(newinfo->rtag);
                           }
                           free(newinfo);
                           resourcelog_freeopinfo(genchain);
                           return NULL;
                        }
                     }
                  }
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            case MARFS_REPACK_OP:
               {
                  repack_info* extinfo = (repack_info*)op->extendedinfo;
                  repack_info* newinfo = malloc(sizeof(*newinfo));
                  *newinfo = *extinfo;
                  (*newop)->extendedinfo = newinfo;
                  break;
               }
            default: // nothing extra to be done for other ops
               LOG(LOG_ERR, "Unrecognized operation type with extended info value\n");
               resourcelog_freeopinfo(genchain);
               return NULL;
         }
      }

      LOG(LOG_INFO, "Successfully duplicated operation\n");
      op = op->next;
      newop = &(*newop)->next;
   }

   return genchain;
}

/**
 * Generates the pathnames of logfiles and parent dirs
 * @param char create : Create flag
 *                      If non-zero, this func will attempt to create all intermediate directory paths (not the final tgt)
 * @param const char* logroot : Root of the logfile tree
 * @param const char* iteration : ID string for this program iteration (can be left NULL to gen parent path)
 * @param marfs_ns* ns : MarFS NS to process (can be left NULL to gen parent path, ignored if prev is NULL)
 * @param ssize_t ranknum : Processing rank (can be < 0 to gen parent path, ignored if prev is NULL)
 * @return char* : Path of the corresponding log location, or NULL if an error occurred
 *                 NOTE -- It is the caller's responsibility to free this string
 */
char* resourcelog_genlogpath(char create, const char* logroot, const char* iteration, marfs_ns* ns, ssize_t ranknum) {
   // check for invalid args
   if (logroot == NULL) {
      LOG(LOG_ERR, "Received a NULL logroot value\n");
      errno = EINVAL;
      return NULL;
   }

   // if we have a NS, identify an appropriate FS path
   char* nspath = NULL;
   if (ns) {
      nspath = strdup(ns->idstr);
      char* tmpparse = nspath;
      while (*tmpparse != '\0') {
         if (*tmpparse == '/') { *tmpparse = '#'; }
         tmpparse++;
      }
   }

   // identify length of the constructed path
   ssize_t pathlen = 0;
   if (iteration && nspath && ranknum >= 0) {
      pathlen = snprintf(NULL, 0, "%s/%s/%s/resourcelog-%zu", logroot, iteration, nspath, ranknum);
   }
   else if (iteration && nspath) {
      pathlen = snprintf(NULL, 0, "%s/%s/%s", logroot, iteration, nspath);
   }
   else if (iteration) {
      pathlen = snprintf(NULL, 0, "%s/%s", logroot, iteration);
   }
   else {
      pathlen = snprintf(NULL, 0, "%s", logroot);
   }

   if (pathlen < 1) {
      LOG(LOG_ERR, "Failed to identify strlen of logfile path\n");
      free(nspath);
      return NULL;
   }

   // allocate the path
   char* path = malloc(sizeof(char) * (pathlen + 1));

   // populate the path root
   const ssize_t lrootlen = snprintf(path, pathlen + 1, "%s", logroot);

   // potentially exit here
   if (iteration == NULL) {
      LOG(LOG_INFO, "Generated root path: \"%s\"\n", path);
      free(nspath);
      return path;
   }

   // create, if necessary
   if (create && mkdir(path, 0700) && errno != EEXIST) {
      LOG(LOG_ERR, "Failed to create log root dir: \"%s\"\n", path);
      free(nspath);
      free(path);
      return NULL;
   }

   // populate the path iteration
   const ssize_t iterlen = snprintf(path + lrootlen, (pathlen - lrootlen) + 1, "/%s", iteration);

   // potentially exit here
   if (nspath == NULL) {
      LOG(LOG_INFO, "Generated iteration path: \"%s\"\n", path);
      return path;
   }

   // create, if necessary
   if (create && mkdir(path, 0700) && errno != EEXIST) {
      LOG(LOG_ERR, "Failed to create logfile iteration dir: \"%s\"\n", path);
      free(nspath);
      free(path);
      return NULL;
   }

   // populate the path ns
   const ssize_t nslen = snprintf(path + lrootlen + iterlen, (pathlen - (lrootlen + iterlen)) + 1, "/%s", nspath);

   // create NS parent paths, if necessary
   if (create) {
      // have to iterate over and create all intermediate dirs
      char* parse = path + lrootlen + iterlen + 1;
      while (*parse != '\0') {
         if (*parse == '/') {
            *parse = '\0';
            if (mkdir(path, 0700) && errno != EEXIST) {
               LOG(LOG_ERR, "Failed to create log NS subdir: \"%s\"\n", path);
               free(nspath);
               free(path);
               return NULL;
            }

            *parse = '/';
         }

         parse++;
      }
   }

   free(nspath); // done with this value

   // potentially exit here
   if (ranknum < 0) {
      LOG(LOG_INFO, "Generated NS log path: \"%s\"\n", path);
      return path;
   }

   // create, if necessary
   if (create && mkdir(path, 0700) && errno != EEXIST) {
      LOG(LOG_ERR, "Failed to create logfile NS dir: \"%s\"\n", path);
      free(path);
      return NULL;
   }

   // populate the final logfile path
   // NOTE -- we never create this file in this func
   snprintf(path + lrootlen + iterlen + nslen, (pathlen - (lrootlen + iterlen + nslen)) + 1, "/resourcelog-%zu", ranknum);

   return path;
}

/**
 * Initialize a resourcelog, associated with the given logging root, namespace, and rank
 * @param RESOURCELOG* resourcelog : Statelog to be initialized
 *                             NOTE -- This can either be a NULL value, or a resourcelog which was
 *                                     previously terminated / finalized
 * @param const char* logpath : Location of the resourcelog file
 * @param resourcelog_type type : Type of resourcelog to open
 * @param marfs_ns* ns : MarFS NS being operated upon
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_init(RESOURCELOG* resourcelog, const char* logpath, resourcelog_type type, marfs_ns* ns) {
   // check for invalid args
   if (resourcelog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   if (type != RESOURCE_RECORD_LOG && type != RESOURCE_MODIFY_LOG && type != RESOURCE_READ_LOG) {
      LOG(LOG_ERR, "Unknown resourcelog type value\n");
      errno = EINVAL;
      return -1;
   }

   if (type != RESOURCE_READ_LOG && ns == NULL) {
      LOG(LOG_ERR, "Recieved a NULL 'ns' arg for a non-read log\n");
      errno = EINVAL;
      return -1;
   }

   if (logpath == NULL) {
      LOG(LOG_ERR, "Received a NULL logpath value\n");
      errno = EINVAL;
      return -1;
   }

   // identify our actual resourcelog
   RESOURCELOG rsrclog = *resourcelog;
   // FIXME
   // allocate a new resourcelog
   rsrclog = malloc(sizeof(*rsrclog));

   pthread_mutex_init(&rsrclog->lock, NULL);
   pthread_cond_init(&rsrclog->nooutstanding, NULL);
   pthread_mutex_lock(&rsrclog->lock);
   rsrclog->outstandingcnt = 0;
   rsrclog->type = type; // may be updated later
   memset(&rsrclog->summary, 0, sizeof(rsrclog->summary));
   rsrclog->inprogress = NULL;
   rsrclog->logfile = -1;
   rsrclog->logfilepath = NULL;
   // initialize our logging path
   rsrclog->logfilepath = strdup(logpath);

   // open our logfile
   const int openmode = (type == RESOURCE_READ_LOG)?O_RDONLY:(O_CREAT | O_EXCL | O_WRONLY);
   rsrclog->logfile = open(rsrclog->logfilepath, openmode, 0600);
   if (rsrclog->logfile < 0) {
      LOG(LOG_ERR, "Failed to open resourcelog: \"%s\"\n", rsrclog->logfilepath);
      cleanuplog(rsrclog, 1);
      return -1;
   }

   // when reading an existing logfile, behavior is significantly different
   if (type == RESOURCE_READ_LOG) {
      // read in the header value of an existing log file
      char buffer[128] = {0};
      char recordshortest;
      ssize_t shortestprefx;
      ssize_t extraread;
      if (strlen(RECORD_LOG_PREFIX) < strlen(MODIFY_LOG_PREFIX)) {
         recordshortest = 1;
         shortestprefx = strlen(RECORD_LOG_PREFIX);
         extraread = strlen(MODIFY_LOG_PREFIX) - shortestprefx;
      }
      else {
         recordshortest = 0;
         shortestprefx = strlen(MODIFY_LOG_PREFIX);
         extraread = strlen(RECORD_LOG_PREFIX) - shortestprefx;
      }

      if (shortestprefx + extraread >= 128) {
         LOG(LOG_ERR, "Logfile header strings exceed memory allocation!\n");
         cleanuplog(rsrclog, 1);
         return -1;
      }

      if (read(rsrclog->logfile, buffer, shortestprefx) != shortestprefx) {
         LOG(LOG_ERR, "Failed to read prefix string of length %zd from logfile: \"%s\"\n",
                       shortestprefx, rsrclog->logfilepath);
         cleanuplog(rsrclog, 1);
         return -1;
      }

      // string comparison, accounting for possible variety of header length, is a bit complex
      if (recordshortest) {
         // check if this is a RECORD log first
         if (strncmp(buffer, RECORD_LOG_PREFIX, shortestprefx)) {
            // not a RECORD log, so read in extra MODIFY prefix chars, if necessary
            if (extraread > 0 && read(rsrclog->logfile, buffer + shortestprefx, extraread) != extraread) {
               LOG(LOG_ERR, "Failed to read in trailing chars of MODIFY prefix\n");
               cleanuplog(rsrclog, 1);
               return -1;
            }

            // check for MODIFY prefix
            if (strncmp(buffer, MODIFY_LOG_PREFIX, shortestprefx + extraread)) {
               LOG(LOG_ERR, "Failed to identify header prefix of logfile: \"%s\"\n", rsrclog->logfilepath);
               cleanuplog(rsrclog, 1);
               return -1;
            }

            // we are reading a MODIFY log
            LOG(LOG_INFO, "Identified as a MODIFY log source: \"%s\"\n", rsrclog->logfilepath);
            rsrclog->type = RESOURCE_MODIFY_LOG | RESOURCE_READ_LOG;
         }
         else {
            // we are reading a RECORD log
            LOG(LOG_INFO, "Identified as a RECORD log source: \"%s\"\n", rsrclog->logfilepath);
            rsrclog->type = RESOURCE_RECORD_LOG | RESOURCE_READ_LOG;
         }
      }
      else {
         // check if this is a MODIFY log first
         if (strncmp(buffer, MODIFY_LOG_PREFIX, shortestprefx)) {
            // not a MODIFY log, so read in extra RECORD prefix chars, if necessary
            if (extraread > 0 && read(rsrclog->logfile, buffer + shortestprefx, extraread) != extraread) {
               LOG(LOG_ERR, "Failed to read in trailing chars of RECORD prefix\n");
               cleanuplog(rsrclog, 1);
               return -1;
            }

            // check for RECORD prefix
            if (strncmp(buffer, RECORD_LOG_PREFIX, shortestprefx + extraread)) {
               LOG(LOG_ERR, "Failed to identify header prefix of logfile: \"%s\"\n", rsrclog->logfilepath);
               cleanuplog(rsrclog, 1);
               return -1;
            }

            // we are reading a RECORD log
            LOG(LOG_INFO, "Identified as a RECORD log source: \"%s\"\n", rsrclog->logfilepath);
            rsrclog->type = RESOURCE_RECORD_LOG | RESOURCE_READ_LOG;
         }
         else {
            // we are reading a MODIFY log
            LOG(LOG_INFO, "Identified as a MODIFY log source: \"%s\"\n", rsrclog->logfilepath);
            rsrclog->type = RESOURCE_MODIFY_LOG | RESOURCE_READ_LOG;
         }
      }

      // when reading a log, we can exit early
      pthread_mutex_unlock(&rsrclog->lock);

      *resourcelog = rsrclog;
      return 0;
   }

   // write out our log prefix
   if (rsrclog->type == RESOURCE_MODIFY_LOG) {
      if (write(rsrclog->logfile, MODIFY_LOG_PREFIX, strlen(MODIFY_LOG_PREFIX)) !=
            strlen(MODIFY_LOG_PREFIX)) {
         LOG(LOG_ERR, "Failed to write out MODIFY log header to new logfile\n");
         cleanuplog(rsrclog, 1);
         return -1;
      }
   }
   else {
      if (write(rsrclog->logfile, RECORD_LOG_PREFIX, strlen(RECORD_LOG_PREFIX)) !=
            strlen(RECORD_LOG_PREFIX)) {
         LOG(LOG_ERR, "Failed to write out RECORD log header to new logfile\n");
         cleanuplog(rsrclog, 1);
         return -1;
      }
   }

   // initialize our HASH_TABLE
   HASH_NODE* nodelist = malloc(sizeof(HASH_NODE) * ns->prepo->metascheme.refnodecount);
   size_t index = 0;
   for (; index < ns->prepo->metascheme.refnodecount; index++) {
      // initialize node list to mirror the reference nodes
      nodelist[index].content = NULL;
      nodelist[index].weight = ns->prepo->metascheme.refnodes[index].weight;
      nodelist[index].name = strdup(ns->prepo->metascheme.refnodes[index].name);
   }

   rsrclog->inprogress = hash_init(nodelist, ns->prepo->metascheme.refnodecount, 0);
   if (rsrclog->inprogress == NULL) {
      LOG(LOG_ERR, "Failed to initialize inprogress hash table\n");
      while (index) {
         free(nodelist[index - 1].name);
         index--;
      }
      free(nodelist);
      cleanuplog(rsrclog, 1);
      return -1;
   }

//
//   // TODO : This code no longer belongs here, but might be useful elsewhere
//   // parse over logfile entries
//   char eof = 0;
//   opinfo* parsedop = NULL;
//   while ((parsedop = parselogline(rsrclog->logfile, &eof)) != NULL) {
//      // map this operation into our inprogress hash table
//      if (processopinfo(rsrclog, parsedop) < 0) {
//         LOG(LOG_ERR, "Failed to process lines from logfile: \"%s\"\n", rsrclog->logfilepath);
//         cleanuplog(rsrclog, 1);
//         return -1;
//      }
//   }
//   if (eof == 0) {
//      LOG(LOG_ERR, "Failed to parse existing logfile: \"%s\"\n", rsrclog->logfilepath);
//      cleanuplog(rsrclog, 1);
//      return -1;
//   }
//   // potentially integrate info from any pre-existing logfiles
//   if (cleanup) {
//      // identify and open our parent dir
//      char* parentparse = rsrclog->logfilepath;
//      char* prevent = NULL;
//      for (; *parentparse != '\0'; parentparse++) {
//         if (*parentparse == '/') { prevent = parentparse; } // update previous dir sep
//      }
//      if (prevent == NULL) {
//         LOG(LOG_ERR, "Failed to identify parent dir of logfile path: \"%s\"\n", rsrclog->logfilepath);
//         cleanuplog(rsrclog, 1);
//         return -1;
//      }
//      *prevent = '\0'; // insert a NULL, to allow us to open the parent dir
//      DIR* parentdir = opendir(rsrclog->logfilepath);
//      *prevent = '/';
//      if (parentdir == NULL) {
//         LOG(LOG_ERR, "Failed to open parent dir of logfile: \"%s\"\n", rsrclog->logfilepath);
//         cleanuplog(rsrclog, 1);
//         return -1;
//      }
//      // readdir to identify all lingering logfiles
//      int olderr = errno;
//      errno = 0;
//      struct dirent* entry = NULL;
//      while ((entry = readdir(parentdir)) != NULL) {
//         if (strcmp(prevent + 1, entry->name)) {
//            // open and parse all old logfiles under the same dir
//            LOG(LOG_INFO, "Attempting cleanup of existing logfile: \"%s\"\n", entry->name);
//            int oldlog = openat(dirfd(parentdir), entry->name, O_RDONLY);
//            if (oldlog < 0) {
//               LOG(LOG_ERR, "Failed to open existing logfile for cleanup: \"%s\"\n", entry->name);
//               closedir(parentdir);
//               cleanuplog(rsrclog, 1);
//               return -1;
//            }
//            while ((parsedop = parselogline(oldlog, &eof)) != NULL) {
//               // duplicate this op into our initial logfile
//               if (printlogline(rsrclog->logfile, parsedop)) {
//                  LOG(LOG_ERR, "Failed to duplicate op from old logfile \"%s\" into active log: \"%s\"\n",
//                       entry->name, rsrclog->logfilepath);
//                  close(oldlog);
//                  closedir(parentdir);
//                  cleanuplog(rsrclog, 1);
//                  return -1;
//               }
//               // map any ops to our hash table
//               if (processopinfo(rsrclog, parsedop) < 0) {
//                  LOG(LOG_ERR, "Failed to process lines from old logfile: \"%s\"\n", entry->name);
//                  close(oldlog);
//                  closedir(parentdir);
//                  cleanuplog(rsrclog, 1);
//                  return -1;
//               }
//            }
//            if (eof == 0) {
//               LOG(LOG_ERR, "Failed to parse old logfile: \"%s\"\n", entry->name);
//               close(oldlog);
//               closedir(parentdir);
//               cleanuplog(rsrclog, 1);
//               return -1;
//            }
//            close(oldlog);
//            // delete the old logfile
//            if (unlinkat(dirfd(parentdir), entry->name)) {
//               LOG(LOG_ERR, "Failed to unlink old logfile: \"%s\"\n", entry->name);
//               closedir(parentdir);
//               cleanuplog(rsrclog, 1);
//               return -1;
//            }
//         }
//      }
//      closedir(parentdir);
//   }


   // finally done
   pthread_mutex_unlock(&rsrclog->lock);

   *resourcelog = rsrclog;
   return 0;
}

/**
 * Replay all operations from a given inputlog (reading from a MODIFY log) into a given
 *  outputlog (writing to a MODIFY log), then delete and terminate the inputlog
 * NOTE -- This function is intended for picking up state from a previously aborted run.
 * @param RESOURCELOG* inputlog : Source inputlog to be read from
 * @param RESOURCELOG* outputlog : Destination outputlog to be written to
 * @param int (*filter)(const opinfo* op) : Function pointer defining an operation filter (ignored if NULL)
 *                                            *param const opinfo* : Reference to the op to potentially include
 *                                            *return int : Zero if the op should be included, non-zero if not
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_replay(RESOURCELOG* inputlog, RESOURCELOG* outputlog, int (*filter)(const opinfo* op)) {
   // check for invalid args
   if (inputlog == NULL || outputlog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   if (*inputlog == NULL || *outputlog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog\n");
      errno = EINVAL;
      return -1;
   }

   if (!((*inputlog)->type & RESOURCE_READ_LOG)) {
      LOG(LOG_ERR, "Invalid input resourcelog type values\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCELOG inrsrclog = *inputlog;
   RESOURCELOG outrsrclog = *outputlog;

   // acquire both resourcelog locks
   pthread_mutex_lock(&inrsrclog->lock);
   pthread_mutex_lock(&outrsrclog->lock);

   // process all entries from the current resourcelog
   size_t opcnt = 0;
   opinfo* parsedop = NULL;
   char eof = 0;
   while ((parsedop = parselogline(inrsrclog->logfile, &eof)) != NULL) {
      // duplicate the parsed op (for printing)
      opinfo* dupop = resourcelog_dupopinfo(parsedop);
      if (dupop == NULL) {
         LOG(LOG_ERR, "Failed to duplicate parsed operation chain\n");
         pthread_mutex_unlock(&outrsrclog->lock);
         pthread_mutex_unlock(&inrsrclog->lock);
         resourcelog_freeopinfo(parsedop);
         return -1;
      }

      char dofree = 1;

      // check our filter
      if (filter == NULL || filter(dupop) == 0) {
         // incorporate the op into our current state
         if ((outrsrclog->type & ~(RESOURCE_READ_LOG)) == RESOURCE_MODIFY_LOG) {
            dofree = 0;
            if (processopinfo(outrsrclog, parsedop, NULL, &dofree) < 0) {
               LOG(LOG_ERR, "Failed to process lines from old logfile: \"%s\"\n", inrsrclog->logfilepath);
               pthread_mutex_unlock(&outrsrclog->lock);
               pthread_mutex_unlock(&inrsrclog->lock);
               resourcelog_freeopinfo(dupop);
               resourcelog_freeopinfo(parsedop);
               return -1;
            }
         }

         // duplicate this op into our output logfile (must use duplicate, as parsedop->next may be modified)
         if (printlogline(outrsrclog->logfile, dupop)) {
            LOG(LOG_ERR, "Failed to duplicate op from input logfile \"%s\" into active log: \"%s\"\n",
                 inrsrclog->logfilepath, outrsrclog->logfilepath);
            pthread_mutex_unlock(&outrsrclog->lock);
            pthread_mutex_unlock(&inrsrclog->lock);
            resourcelog_freeopinfo(dupop);
            if (dofree)
               resourcelog_freeopinfo(parsedop);
            return -1;
         }
      }

      resourcelog_freeopinfo(dupop);

      if (dofree)
         resourcelog_freeopinfo(parsedop);

      opcnt++;
   }

   if (eof != 1) {
      LOG(LOG_ERR, "Failed to parse input logfile: \"%s\"\n", inrsrclog->logfilepath);
      pthread_mutex_unlock(&outrsrclog->lock);
      pthread_mutex_unlock(&inrsrclog->lock);
      return -1;
   }

   LOG(LOG_INFO, "Replayed %zu ops from input log (\"%s\") into output log (\"%s\")\n",
                  opcnt, inrsrclog->logfilepath, outrsrclog->logfilepath);

   // cleanup the inputlog
   *inputlog = NULL;

   // FIXME: unlock order is incorrect, but not sure if can flip
   pthread_mutex_unlock(&inrsrclog->lock);

   if (resourcelog_term(&inrsrclog, NULL, 1)) {
      LOG(LOG_ERR, "Failed to cleanup input logfile\n");
      pthread_mutex_unlock(&outrsrclog->lock);
      return -1;
   }

   if (outrsrclog->outstandingcnt == 0) {
      // potentially signal that the output log has quiesced
      pthread_cond_signal(&outrsrclog->nooutstanding);
   }

   pthread_mutex_unlock(&outrsrclog->lock);

   return 0;
}

/**
 * Record that a certain number of threads are currently processing
 * @param RESOURCELOG* resourcelog : Statelog to be updated
 * @param size_t numops : Number of additional processors (can be negative to reduce cnt)
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_update_inflight(RESOURCELOG* resourcelog, ssize_t numops) {
   // check for invalid args
   if (resourcelog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCELOG rsrclog = *resourcelog;
   if (rsrclog == NULL || rsrclog->logfile < 1) {
      LOG(LOG_ERR, "Received an uninitialized resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   // acquire resourcelog lock
   pthread_mutex_lock(&rsrclog->lock);

   // check for excessive reduction
   if (rsrclog->outstandingcnt < -numops) {
      LOG(LOG_WARNING, "Value of %zd would result in negative thread count\n", numops);
      numops = -rsrclog->outstandingcnt;
   }

   // modify count
   rsrclog->outstandingcnt += numops;

   LOG(LOG_INFO, "Modified Outstanding OP Count by %zd, to %zd\n", numops, rsrclog->outstandingcnt);

   // check for quiesced state
   if (rsrclog->outstandingcnt == 0 && pthread_cond_signal(&rsrclog->nooutstanding)) {
      LOG(LOG_ERR, "Failed to signal 'no outstanding ops' condition\n");
      pthread_mutex_unlock(&rsrclog->lock);
      return -1;
   }

   pthread_mutex_unlock(&rsrclog->lock);

   return 0;
}

/**
 * Process the given operation
 * @param RESOURCELOG* resourcelog : Statelog to update (must be writing to this resourcelog)
 * @param opinfo* op : Operation (or op sequence) to process
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_processop(RESOURCELOG* resourcelog, opinfo* op, char* progress) {
   // check for invalid args
   if (resourcelog == NULL || *resourcelog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   if ((*resourcelog)->type & RESOURCE_READ_LOG) {
      LOG(LOG_ERR, "Cannot update a reading resourcelog\n");
      errno = EINVAL;
      return -1;
   }

   if (op == NULL) {
      LOG(LOG_ERR, "Received a NULL op reference\n");
      errno = EINVAL;
      return -1;
   }

   // produce a duplicate of the given op chain
   opinfo* dupop = resourcelog_dupopinfo(op);
   if (dupop == NULL) {
      LOG(LOG_ERR, "Failed to duplicate operation chain\n");
      return -1;
   }

   RESOURCELOG rsrclog = *resourcelog;

   // acquire resourcelog lock
   pthread_mutex_lock(&rsrclog->lock);

   // special check for RECORD log
//   if (rsrclog->type == RESOURCE_RECORD_LOG) {
//      // traverse the op chain to ensure we don't have any completions slipping into this RECORD log
//      opinfo* parseop = dupop;
//      while (parseop) {
//         if (parseop->start == 0) {
//            LOG(LOG_ERR, "Detected op completion struct in chain for RECORD log\n");
//            errno = EINVAL;
//            pthread_mutex_unlock(&rsrclog->lock));
//            resourcelog_freeopinfo(dupop);
//            return -1;
//         }
//         parseop = parseop->next;
//      }
//   }
   // incorporate operation info
   char dofree = 0;
   if (processopinfo(rsrclog, dupop, progress, &dofree)) {
      LOG(LOG_ERR, "Failed to incorportate op info into MODIFY log\n");
      pthread_mutex_unlock(&rsrclog->lock);
      resourcelog_freeopinfo(dupop);
      return -1;
   }

   // output the operation to the actual log file (must use the initial, unmodified op)
   if (printlogline(rsrclog->logfile, op)) {
      LOG(LOG_ERR, "Failed to output operation info to logfile: \"%s\"\n", rsrclog->logfilepath);
      pthread_mutex_unlock(&rsrclog->lock);
      if (dofree)
         resourcelog_freeopinfo(dupop);
      return -1;
   }

   // check for quiesced state
   if (rsrclog->type == RESOURCE_MODIFY_LOG &&
        rsrclog->outstandingcnt == 0 && pthread_cond_signal(&rsrclog->nooutstanding)) {
      LOG(LOG_ERR, "Failed to signal 'no outstanding ops' condition\n");
      pthread_mutex_unlock(&rsrclog->lock);
      if (dofree)
         resourcelog_freeopinfo(dupop);
      return -1;
   }

   pthread_mutex_unlock(&rsrclog->lock);

   if (dofree)
      resourcelog_freeopinfo(dupop);

   return 0;
}

/**
 * Parse the next operation info sequence from the given RECORD resourcelog
 * @param RESOURCELOG* resourcelog : Statelog to read
 * @param opinfo** op : Reference to be populated with the parsed operation info sequence
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_readop(RESOURCELOG* resourcelog, opinfo** op) {
   // check for invalid args
   if (resourcelog == NULL || *resourcelog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   if ((*resourcelog)->type != (RESOURCE_RECORD_LOG | RESOURCE_READ_LOG)) {
      LOG(LOG_ERR, "Statelog is not a RECORD log, open for read\n");
      errno = EINVAL;
      return -1;
   }

   if (op == NULL) {
      LOG(LOG_ERR, "Receieved a NULL value instead of an opinfo* reference\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCELOG rsrclog = *resourcelog;

   // acquire resourcelog lock
   pthread_mutex_lock(&rsrclog->lock);

   // parse a new op sequence from the logfile
   char eof = 0;
   opinfo* parsedop = parselogline(rsrclog->logfile, &eof);
   if (parsedop == NULL) {
      if (eof < 0) {
         LOG(LOG_ERR, "Hit unexpected EOF on logfile: \"%s\"\n", rsrclog->logfilepath);
         pthread_mutex_unlock(&rsrclog->lock);
         return -1;
      }

      if (eof == 0) {
         LOG(LOG_ERR, "Failed to parse operation info from logfile: \"%s\"\n", rsrclog->logfilepath);
         pthread_mutex_unlock(&rsrclog->lock);
         return -1;
      }

      LOG(LOG_INFO, "Hit EOF on logfile: \"%s\"\n", rsrclog->logfilepath);
   }

   // release the log lock
   pthread_mutex_unlock(&rsrclog->lock);

   *op = parsedop;
   return 0;
}

/**
 * Deallocate and finalize a given resourcelog
 * NOTE -- this will fail if there are currently any ops in flight
 * @param RESOURCELOG* resourcelog : Statelog to be terminated
 * @param operation_summary* summary : Reference to be populated with summary values (ignored if NULL)
 * @param char delete : Flag indicating whether the logfile should be deleted on termination
 *                      If non-zero, the file is deleted
 * @return int : Zero on success, 1 if the log was preserved due to errors, or -1 on failure
 */
int resourcelog_term(RESOURCELOG* resourcelog, operation_summary* summary, char delete) {
   // check for invalid args
   if (resourcelog == NULL || *resourcelog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCELOG rsrclog = *resourcelog;

   // acquire the resourcelog lock
   pthread_mutex_lock(&rsrclog->lock);

   // abort if any outstanding ops remain
   if (rsrclog->outstandingcnt != 0) {
      LOG(LOG_ERR, "Resourcelog still has %zd outstanding operations\n", rsrclog->outstandingcnt);
      pthread_mutex_unlock(&rsrclog->lock);
      errno = EAGAIN;
      return -1;
   }

   // check for any recorded errors
   const int errpresent = (
      rsrclog->summary.deletion_object_failures     ||
      rsrclog->summary.deletion_reference_failures  ||
      rsrclog->summary.rebuild_failures             ||
      rsrclog->summary.repack_failures
   );

   // potentially record summary info
   if (summary) {
       *summary = rsrclog->summary;
   }

   // close our logfile prior to (possibly) unlinking it
   if (rsrclog->logfile > 0) {
      int cres = close(rsrclog->logfile);
      rsrclog->logfile = 0; // avoid possible double close
      if (cres) {
         LOG(LOG_ERR, "Failed to close resourcelog\n");
         cleanuplog(rsrclog, 1); // this will release the lock
         *resourcelog = NULL;
         return -1;
      }
   }

   if (!errpresent && delete) {
      // if there were no errors recorded, just delete the logfile
      if (unlink(rsrclog->logfilepath)) {
         LOG(LOG_ERR, "Failed to unlink log file: \"%s\"\n", rsrclog->logfilepath);
         cleanuplog(rsrclog, 1); // this will release the lock
         *resourcelog = NULL;
         return -1;
      }

      // also, attempt to remove the next two parent dirs (NS and iteration) of this logfile, skipping on error
      char pdel = 0;
      while (pdel < 2) {
         // NOTE -- frustrates me greatly to put this sleep in, but a brief pause really seems to help any NFS-hosted
         //         log location to cleanup state, allowing us to delete the parent dir
         usleep(100000);
         // trim logpath at the last '/' char, to get the parent dir path
         char* prevsep = NULL;
         char* pparse = rsrclog->logfilepath;
         while (*pparse != '\0') {
            if (*pparse == '/') {
               prevsep = pparse;
               while (*pparse == '/') { pparse++; } // ignore repeated '/' chars
            }
            else { pparse++; }
         }

         if (prevsep == NULL) {
             LOG(LOG_WARNING, "Skipping deletion of parent path %d, as we failed to find a separator\n", (int)pdel);
             break;
         }

         *prevsep = '\0';

         pdel++;

         if (rmdir(rsrclog->logfilepath)) {
            LOG(LOG_WARNING, "Failed to rmdir parent path %d: \"%s\" (%s)\n", (int)pdel, rsrclog->logfilepath, strerror(errno));
            break;
         }
      }
   }

   cleanuplog(rsrclog, 1); // this will release the lock
   *resourcelog = NULL;

   return (errpresent) ? 1 : 0;
}

/**
 * Deallocate and finalize a given resourcelog without waiting for completion
 * @param RESOURCELOG* resourcelog : Statelog to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_abort(RESOURCELOG* resourcelog) {
   // check for invalid args
   if (resourcelog == NULL || *resourcelog == NULL) {
      LOG(LOG_ERR, "Received a NULL resourcelog reference\n");
      errno = EINVAL;
      return -1;
   }

   RESOURCELOG rsrclog = *resourcelog;
   cleanuplog(rsrclog, 1); // this will release the lock
   *resourcelog = NULL;

   return 0;
}
