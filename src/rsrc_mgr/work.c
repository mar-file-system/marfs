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

#include "rsrc_mgr/loginfo.h"
#include "rsrc_mgr/outputinfo.h"
#include "rsrc_mgr/resourceinput.h"

static int error_only_filter(const opinfo* op) {
    return !op->errval;
}

/**
 * Configure the current rank to target the given NS (configures state and launches worker threads)
 * @param rmanstate* rman : State struct for the current rank
 * @param marfs_ns* ns : New NS to target
 * @param workresponse* response : Response to populate with error description, on failure
 * @return int : Zero on success, -1 on failure
 */
static int setranktgt(rmanstate* rman, marfs_ns* ns, workresponse* response) {
   // update position value
   if (config_establishposition(&rman->gstate.pos, rman->config)) {
      LOG(LOG_ERR, "Failed to establish a root NS position\n");
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to establish a root NS position");
      return -1;
   }

   char* tmpnspath = NULL;
   if (config_nsinfo(ns->idstr, NULL, &tmpnspath)) {
      LOG(LOG_ERR, "Failed to identify NS path of NS \"%s\"\n", ns->idstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
                "Failed to identify NS path of NS \"%s\"", ns->idstr);
      return -1;
   }

   char* nspath = strdup(tmpnspath+1); // strip off leading '/', to get a relative NS path
   free(tmpnspath);
   if (config_traverse(rman->config, &rman->gstate.pos, &nspath, 0)) {
      LOG(LOG_ERR, "Failed to traverse config to new NS path: \"%s\"\n", nspath);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
                "Failed to traverse config to new NS path: \"%s\"", nspath);
      free(nspath);
      return -1;
   }
   free(nspath);

   if (rman->gstate.pos.ctxt == NULL && config_fortifyposition(&rman->gstate.pos)) {
      LOG(LOG_ERR, "Failed to fortify position for new NS: \"%s\"\n", ns->idstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
                "Failed to fortify position for new NS: \"%s\"", ns->idstr);
      goto error;
   }

   // update our resource input struct
   if (resourceinput_init(&rman->gstate.rinput, &rman->gstate.pos, rman->gstate.numprodthreads)) {
      LOG(LOG_ERR, "Failed to initialize resourceinput for new NS target: \"%s\"\n", ns->idstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
                "Failed to initialize resourceinput for new NS target: \"%s\"", ns->idstr);
      goto error;
   }

   // update our output resource log
   char* outlogpath = resourcelog_genlogpath(1, rman->logroot, rman->iteration, ns, rman->ranknum);
   if (outlogpath == NULL) {
      LOG(LOG_ERR, "Failed to identify output logfile path of rank %zu for NS \"%s\"\n",
                    rman->ranknum, ns->idstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
                "Failed to identify output logfile path of rank %zu for NS \"%s\"",
                rman->ranknum, ns->idstr);
      goto rman_error;
   }

   if (resourcelog_init(&rman->gstate.rlog, outlogpath,
                          (rman->gstate.dryrun) ? RESOURCE_RECORD_LOG : RESOURCE_MODIFY_LOG, ns)) {
      LOG(LOG_ERR, "Failed to initialize output logfile: \"%s\"\n", outlogpath);
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to initialize output logfile: \"%s\"", outlogpath);
      free(outlogpath);
      goto rman_error;
   }

   free(outlogpath);

   // update our repack streamer
   if ((rman->gstate.rpst = repackstreamer_init()) == NULL) {
      LOG(LOG_ERR, "Failed to initialize repack streamer\n");
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to initialize repack streamer");
      goto rman_error;
   }

   // kick off our worker threads
   TQ_Init_Opts tqopts = {
      .log_prefix = "RManWorker",
      .init_flags = 0,
      .max_qdepth = rman->gstate.numprodthreads + rman->gstate.numconsthreads,
      .global_state = &rman->gstate,
      .num_threads = rman->gstate.numprodthreads + rman->gstate.numconsthreads,
      .num_prod_threads = rman->gstate.numprodthreads,
      .thread_init_func = rthread_init_func,
      .thread_consumer_func = rthread_consumer_func,
      .thread_producer_func = rthread_producer_func,
      .thread_pause_func = NULL,
      .thread_resume_func = NULL,
      .thread_term_func = rthread_term_func
   };

   if ((rman->tq = tq_init(&tqopts)) == NULL) {
      LOG(LOG_ERR, "Failed to start ThreadQueue for NS \"%s\"\n", ns->idstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to start ThreadQueue for NS \"%s\"", ns->idstr);
      goto rman_error;
   }

   // ensure all threads have initialized
   if (tq_check_init(rman->tq)) {
      LOG(LOG_ERR, "Some threads failed to initialize for NS \"%s\"\n", ns->idstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "Some threads failed to initialize for NS \"%s\"", ns->idstr);
      goto tq_error;
   }

   LOG(LOG_INFO, "Rank %zu is now targetting NS \"%s\"\n", rman->ranknum, ns->idstr);
   return 0;

  tq_error:
   tq_set_flags(rman->tq, TQ_ABORT);

  rman_error:
   if (rman->gstate.rpst) {
       repackstreamer_abort(rman->gstate.rpst);
       rman->gstate.rpst = NULL;
   }
   resourcelog_term(&rman->gstate.rlog, NULL, 1);
   resourceinput_purge(&rman->gstate.rinput);

  error:
   config_abandonposition(&rman->gstate.pos);

   return -1;
}

/**
 * Calculate the min and max values for the given ref distribution of the NS
 * @param marfs_ns* ns : Namespace to split ref ranges across
 * @param size_t workingranks : Total number of operating ranks
 * @param size_t refdist : Reference distribution index
 * @param size_t* refmin : Reference to be populated with the minimum range value
 * @param size_t* refmin : Reference to be populated with the maximum range value (non-inclusive)
 */
static void getNSrange(marfs_ns* ns, size_t workingranks, size_t refdist, size_t* refmin, size_t* refmax) {
   size_t refperrank = ns->prepo->metascheme.refnodecount / workingranks; // 'average' number of reference ranges per rank (truncated to integer)
   size_t remainder = ns->prepo->metascheme.refnodecount % workingranks;  // 'remainder' ref dirs, omitted if every rank merely got the 'average'
   size_t prevextra = (refdist > remainder) ? remainder : refdist; // how many 'remainder' dirs were included in previous distributions
   *refmin = (refdist * refperrank) + prevextra; // include the 'average' per-rank count, plus any 'remainder' dirs already issued
   *refmax = (*refmin + refperrank) + ((refdist < remainder) ? 1 : 0); // add on the 'average' and one extra, if 'remainder' dirs still exist
   LOG(LOG_INFO, "Using Min=%zu / Max=%zu for ref distribution %zu on NS \"%s\"\n", *refmin, *refmax, refdist, ns->idstr);
   return;
}

// potentially update our state to target the NS
static int handle_rlog_request(rmanstate* rman, workrequest* request, workresponse* response) {
   if (rman->gstate.rlog == NULL) {
      if (setranktgt(rman, rman->nslist[request->nsindex], response)) {
         LOG(LOG_ERR, "Failed to update target of rank %zu to NS \"%s\"\n",
             rman->ranknum, rman->nslist[request->nsindex]->idstr);
         // leave the errorstr alone, as the helper func will have left a more descriptive message
         return -1;
      }
   }

   // identify the path of the rlog
   const char* tmplogroot = rman->execprevroot?rman->execprevroot:rman->logroot;
   char* rlogpath = resourcelog_genlogpath(0, tmplogroot, request->iteration,
                                           rman->nslist[request->nsindex], request->ranknum);
   if (rlogpath == NULL) {
      LOG(LOG_ERR, "Failed to generate logpath for NS \"%s\" ranknum \"%s\"\n",
          rman->nslist[request->nsindex]->idstr, request->ranknum);
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to generate logpath for NS \"%s\" ranknum \"%zu\"",
               rman->nslist[request->nsindex]->idstr, request->ranknum);
      return -1;
   }

   // determine what we're actually doing with this logfile
   if (rman->execprevroot) {
      // we are using the rlog as an input
      if (resourceinput_setlogpath(&rman->gstate.rinput, rlogpath)) {
         LOG(LOG_ERR, "Failed to update rank %zu input to logfile \"%s\"\n", rman->ranknum, rlogpath);
         snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to update rank %zu input to logfile \"%s\"",
                  rman->ranknum, rlogpath);
         free(rlogpath);
         return -1;
      }

      // wait for our input to be exhausted (don't respond until we are ready for more work)
      if (resourceinput_waitforcomp(&rman->gstate.rinput)) {
         LOG(LOG_ERR, "Failed to wait for completion of logfile \"%s\"\n", request->refdist);
         snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to wait for completion of logfile \"%s\"", rlogpath);
         free(rlogpath);
         return -1;
      }
   }
   else {
      // we are just cleaning up this old logfile by replaying it into our current one
      // open the specified rlog for read
      RESOURCELOG newlog = NULL;
      if (resourcelog_init(&newlog, rlogpath, RESOURCE_READ_LOG, rman->nslist[request->nsindex])) {
         const int err = errno;
         LOG(LOG_ERR, "Failed to open logfile for read: \"%s\" (%s)\n", rlogpath, strerror(err));
         snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to open logfile for read: \"%s\" (%s)",
                  rlogpath, strerror(err));
         free(rlogpath);
         return -1;
      }

      if (resourcelog_replay(&newlog, &rman->gstate.rlog, NULL)) {
         LOG(LOG_ERR, "Failed to replay logfile \"%s\" into active state\n", rlogpath);
         snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to replay logfile \"%s\"", rlogpath);
         resourcelog_abort(&newlog);
         free(rlogpath);
         return -1;
      }
   }

   free(rlogpath);
   return 0;
}

// potentially update our state to target the NS
static int handle_ns_request(rmanstate* rman, workrequest* request, workresponse* response) {
   if (rman->gstate.rlog == NULL) {
      if (setranktgt(rman, rman->nslist[request->nsindex], response)) {
         LOG(LOG_ERR, "Failed to update target of rank %zu to NS \"%s\"\n",
             rman->ranknum, rman->nslist[request->nsindex]->idstr);
         // leave the errorstr alone, as the helper func will have left a more descriptive message
         return -1;
      }
   }

   // calculate the start and end reference indices for this request
   size_t refmin = -1;
   size_t refmax = -1;
   getNSrange(rman->nslist[request->nsindex], rman->workingranks, request->refdist, &refmin, &refmax);

   // only actually perform the work if it is a valid reference range
   // NOTE -- This is to handle a case where the number of NS ref dirs < the number of resource manager worker ranks
   if (refmax != refmin) {
      // update our input to reference the new target range
      if (resourceinput_setrange(&rman->gstate.rinput, refmin, refmax)) {
         LOG(LOG_ERR, "Failed to set NS \"%s\" reference range values for distribution %zu\n",
             rman->nslist[request->nsindex]->idstr, request->refdist);
         snprintf(response->errorstr, MAX_ERROR_BUFFER,
                  "Failed to set NS \"%s\" reference range values for distribution %zu",
                  rman->nslist[request->nsindex]->idstr, request->refdist);
         return -1;
      }

      // wait for our input to be exhausted (don't respond until we are ready for more work)
      if (resourceinput_waitforcomp(&rman->gstate.rinput)) {
          LOG(LOG_ERR, "Failed to wait for completion of NS \"%s\" reference distribution %zu\n",
              rman->nslist[request->nsindex]->idstr, request->refdist);
          snprintf(response->errorstr, MAX_ERROR_BUFFER,
                   "Failed to wait for completion of NS \"%s\" reference distribution %zu",
                   rman->nslist[request->nsindex]->idstr, request->refdist);
          return -1;
      }
   }
   else {
      // NOTE -- In this case, refmin and refmax should both equal the total refdir count
      LOG(LOG_INFO, "Skipping distribution %zu of NS \"%s\" (%zu of %zu dirs already processed)\n",
          request->refdist, rman->nslist[request->nsindex]->idstr, refmin, refmax);
   }

   return 0;
}

// complete any outstanding work within the current NS
static int handle_complete_request(rmanstate* rman, workrequest* request, workresponse* response) {
   (void) request;

   if (rman->gstate.rlog == NULL) {
      LOG(LOG_ERR, "Rank %zu was asked to complete work, but has none\n", rman->ranknum);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
               "Rank %zu was asked to complete work, but has none", rman->ranknum);
      return -1;
   }

   // terminate the resource input
   if (rman->gstate.rinput && resourceinput_term(&rman->gstate.rinput)) {
      LOG(LOG_ERR, "Failed to terminate resourceinput\n");
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to terminate resourceinput");
      // don't quit out yet, try to collect thread states first
   }

   char threaderror = 0;
   if (rman->tq) {
      // wait for the queue to be marked as FINISHED
      TQ_Control_Flags setflags = 0;
      if (tq_wait_for_flags(rman->tq, 0, &setflags)) {
         LOG(LOG_ERR, "Failed to wait on ThreadQueue state flags\n");
         snprintf(response->errorstr, MAX_ERROR_BUFFER, "Failed to wait on ThreadQueue state flags");
         return -1;
      }

      if (setflags != TQ_FINISHED) {
         LOG(LOG_WARNING, "Unexpected (NON-FINISHED) ThreadQueue state flags: %u\n", setflags);
         threaderror = 1;
      }
      else {
         // wait for TQ completion
         if (tq_wait_for_completion(rman->tq)) {
            LOG(LOG_ERR, "Failed to wait for ThreadQueue completion\n");
            threaderror = 1;
         }
      }

      // gather all thread status values
      rthread_state* tstate = NULL;
      int retval;
      while ((retval = tq_next_thread_status(rman->tq, (void**)&tstate)) > 0) {
         LOG(LOG_INFO, "Got state for Thread %zu\n", tstate->tID);

         // verify thread status
         if (tstate == NULL) {
            LOG(LOG_ERR, "Rank %zu encountered NULL thread state when completing NS \"%s\"\n",
                rman->ranknum, rman->gstate.pos.ns->idstr);
            snprintf(response->errorstr, MAX_ERROR_BUFFER,
                     "Rank %zu encountered NULL thread state when completing NS \"%s\"",
                     rman->ranknum, rman->gstate.pos.ns->idstr);
            free(tstate);
            return -1;
         }

         if (tstate->fatalerror) {
            LOG(LOG_ERR, "Fatal Error in NS \"%s\": \"%s\"\n",
                rman->gstate.pos.ns->idstr, tstate->errorstr);
            snprintf(response->errorstr, MAX_ERROR_BUFFER, "Fatal Error in NS \"%s\": \"%s\"",
                     rman->gstate.pos.ns->idstr, tstate->errorstr);
            free(tstate);
            return -1;
         }

         response->report.fileusage   += tstate->report.fileusage;
         response->report.byteusage   += tstate->report.byteusage;
         response->report.filecount   += tstate->report.filecount;
         response->report.objcount    += tstate->report.objcount;
         response->report.bytecount   += tstate->report.bytecount;
         response->report.streamcount += tstate->report.streamcount;
         response->report.delobjs     += tstate->report.delobjs;
         response->report.delfiles    += tstate->report.delfiles;
         response->report.delstreams  += tstate->report.delstreams;
         response->report.volfiles    += tstate->report.volfiles;
         response->report.rpckfiles   += tstate->report.rpckfiles;
         response->report.rpckbytes   += tstate->report.rpckbytes;
         response->report.rbldobjs    += tstate->report.rbldobjs;
         response->report.rbldbytes   += tstate->report.rbldbytes;
         free(tstate);
      }

      if (retval) {
         LOG(LOG_ERR, "Failed to collect thread states during completion of work on NS \"%s\"\n",
             rman->gstate.pos.ns->idstr);
         snprintf(response->errorstr, MAX_ERROR_BUFFER,
                  "Failed to collect thread status during completion of work on NS \"%s\"",
                  rman->gstate.pos.ns->idstr);
         return -1;
      }

      response->haveinfo = 1; // note that we have info for the manager to process
      // close our the thread queue, repack streamer, and logs
      if (tq_close(rman->tq)) {
         LOG(LOG_ERR, "Failed to close ThreadQueue after completion of work on NS \"%s\"\n",
             rman->gstate.pos.ns->idstr);
         snprintf(response->errorstr, MAX_ERROR_BUFFER,
                  "Failed to close ThreadQueue after completion of work on NS \"%s\"",
                  rman->gstate.pos.ns->idstr);
         return -1;
      }

      rman->tq = NULL;
   }

   // destroy the resource input, if present
   if (rman->gstate.rinput) {
       if (resourceinput_destroy(&rman->gstate.rinput)) {
           // nothing to do but complain
           LOG(LOG_WARNING, "Failed to destroy resourceinput structure\n");
           rman->gstate.rinput = NULL; // NULL it out anyhow
       }
   }

   // need to preserve our logfile, allowing the manager to remove it
   char* outlogpath = resourcelog_genlogpath(0, rman->logroot, rman->iteration, rman->gstate.pos.ns, rman->ranknum);
   int rlogret;
   if ((rlogret = resourcelog_term(&rman->gstate.rlog, &response->summary, 0)) < 0) {
      LOG(LOG_ERR, "Failed to terminate log \"%s\" following completion of work on NS \"%s\"\n",
          outlogpath, rman->gstate.pos.ns->idstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
               "Failed to terminate log \"%s\" following completion of work on NS \"%s\"",
               outlogpath, rman->gstate.pos.ns->idstr);
      free(outlogpath);
      return -1;
   }
   free(outlogpath);

   if (rlogret) { response->errorlog = 1; } // note if our log was preserved due to errors being present
   if (rman->gstate.rpst) {
      if (repackstreamer_complete(rman->gstate.rpst)) {
         LOG(LOG_ERR, "Failed to complete repack streamer during completion of NS \"%s\"\n", rman->gstate.pos.ns->idstr);
         snprintf(response->errorstr, MAX_ERROR_BUFFER,
                  "Failed to complete repack streamer during completion of NS \"%s\"", rman->gstate.pos.ns->idstr);
         return -1;
      }

      rman->gstate.rpst = NULL;
   }

   const char* nsidstr = rman->gstate.pos.ns->idstr;
   if (config_abandonposition(&rman->gstate.pos)) {
      LOG(LOG_ERR, "Failed to abandon marfs position during completion of NS \"%s\"\n", nsidstr);
      snprintf(response->errorstr, MAX_ERROR_BUFFER,
               "Failed to abandon marfs position during completion of NS \"%s\"", nsidstr);
      return -1;
   }

   if (threaderror) {
      snprintf(response->errorstr, MAX_ERROR_BUFFER, "ThreadQueue had unexpected termination condition");
      return -1;
   }

   return 0;
}

/**
 * Process the given request
 * @param rmanstate* rman : Current resource manager rank state
 * @param workrequest* request : Request to process
 * @param workresponse* response : Response to populate
 * @return int : Zero if the rank should send the populated response and terminate
 *               One if the rank should send the populated response and continue processing
 *              -1 if the rank should send the populated response, then abort
 */
int handlerequest(rmanstate* rman, workrequest* request, workresponse* response) {
   // pre-populate response with a 'fatal error' condition, just in case
   response->request = *request;
   response->haveinfo = 0;
   memset(&response->report, 0, sizeof(response->report));
   memset(&response->summary, 0, sizeof(response->summary));
   response->errorlog = 0;
   response->fatalerror = 1;
   snprintf(response->errorstr, MAX_ERROR_BUFFER, "UNKNOWN-ERROR!");

   // identify and process the request
   int rc = 0;
   switch (request->type) {
      case RLOG_WORK:
         rc = handle_rlog_request(rman, request, response);
         break;
      case NS_WORK:
         rc = handle_ns_request(rman, request, response);
         break;
      case COMPLETE_WORK:
         rc = handle_complete_request(rman, request, response);
         break;
      case TERMINATE_WORK:
         // simply note and terminate
         response->fatalerror = 0;
         rc = 0;
         break;
      case ABORT_WORK:
      default:
         // clear our fatal error state, but indicate to abort after sending the response
         response->fatalerror = 0;
         rc = -1;
         break;
   }

   if ((rc == -1) || (request->type == TERMINATE_WORK)) {
       return rc;
   }

   // clear our fatal error state
   response->fatalerror = 0;

   // indicate that we have a response to send
   return 1;
}

static void update_walk_report(rmanstate* rman, workresponse* response) {
    // incorporate walk report
    rman->walkreport[response->request.nsindex].fileusage   += response->report.fileusage;
    rman->walkreport[response->request.nsindex].byteusage   += response->report.byteusage;
    rman->walkreport[response->request.nsindex].filecount   += response->report.filecount;
    rman->walkreport[response->request.nsindex].objcount    += response->report.objcount;
    rman->walkreport[response->request.nsindex].bytecount   += response->report.bytecount;
    rman->walkreport[response->request.nsindex].streamcount += response->report.streamcount;
    rman->walkreport[response->request.nsindex].delobjs     += response->report.delobjs;
    rman->walkreport[response->request.nsindex].delfiles    += response->report.delfiles;
    rman->walkreport[response->request.nsindex].delstreams  += response->report.delstreams;
    rman->walkreport[response->request.nsindex].volfiles    += response->report.volfiles;
    rman->walkreport[response->request.nsindex].rpckfiles   += response->report.rpckfiles;
    rman->walkreport[response->request.nsindex].rpckbytes   += response->report.rpckbytes;
    rman->walkreport[response->request.nsindex].rbldobjs    += response->report.rbldobjs;
    rman->walkreport[response->request.nsindex].rbldbytes   += response->report.rbldbytes;
}

static void update_log_summary(rmanstate* rman, workresponse* response) {
    // incorporate log summary
    rman->logsummary[response->request.nsindex].deletion_object_count       += response->summary.deletion_object_count;
    rman->logsummary[response->request.nsindex].deletion_object_failures    += response->summary.deletion_object_failures;
    rman->logsummary[response->request.nsindex].deletion_reference_count    += response->summary.deletion_reference_count;
    rman->logsummary[response->request.nsindex].deletion_reference_failures += response->summary.deletion_reference_failures;
    rman->logsummary[response->request.nsindex].rebuild_count               += response->summary.rebuild_count;
    rman->logsummary[response->request.nsindex].rebuild_failures            += response->summary.rebuild_failures;
    rman->logsummary[response->request.nsindex].repack_count                += response->summary.repack_count;
    rman->logsummary[response->request.nsindex].repack_failures             += response->summary.repack_failures;
}

static int handle_rlog_ns_response(rmanstate* rman, const size_t ranknum, workresponse* response, workrequest* request) {
   // this rank needs work to process, specifically in the same NS
   if (rman->oldlogs) {
      // start by checking for old resource logs to process
      HASH_NODE* resnode = NULL;
      if (hash_lookup(rman->oldlogs, rman->nslist[response->request.nsindex]->idstr, &resnode)) {
         fprintf(stderr, "ERROR: Failure of hash_lookup() when looking for logfiles in NS \"%s\" for rank %zu\n",
                 rman->nslist[response->request.nsindex]->idstr, ranknum);
         rman->fatalerror = 1;
         return -1;
      }

      // check for any nodes with log processing requests remaining
      loginfo* linfo = (loginfo*)(resnode->content);
      if (linfo->logcount) {
         // use the request at the tail of our list
         LOG(LOG_INFO, "Passing out log %zu for NS \"%s\" from iteration \"%s\"\n",
             linfo->logcount,
             rman->nslist[linfo->nsindex]->idstr,
             linfo->requests[linfo->logcount - 1].iteration);
         *request = linfo->requests[linfo->logcount - 1];
         linfo->logcount--;
         return 1;
      }
   }

   // no resource logs remain to process in the active NS of this rank
   if (rman->execprevroot) {
      // if we are picking up a previous run, this means no more work remains for the active NS at all
      LOG(LOG_INFO, "Signaling Rank %zu to complete and quiesce, as no resource logs remain in NS \"%s\"\n",
          ranknum, rman->nslist[response->request.nsindex]->idstr);
      *request = response->request; // populate request with active NS values
      request->type = COMPLETE_WORK;
      return 1;
   }

   // check for any remaining work in the rank's active NS
   if (rman->distributed[response->request.nsindex] < rman->workingranks) {
      request->type = NS_WORK;
      request->nsindex = response->request.nsindex;
      request->refdist = rman->distributed[response->request.nsindex];
      request->iteration[0] = '\0';
      request->ranknum = ranknum;
      rman->distributed[response->request.nsindex]++; // note newly distributed range

      LOG(LOG_INFO, "Passing out reference range %zu of NS \"%s\" to Rank %zu\n",
          request->refdist, rman->nslist[response->request.nsindex]->idstr, ranknum);

      // check through remaining namespaces for any undistributed work
      size_t nsindex = 0;
      for (; nsindex < rman->nscount; nsindex++) {
          if (rman->distributed[nsindex] < rman->workingranks) {
              break;
          }
      }

      if (nsindex == rman->nscount) {
         // just handed out the last NS ref range for processing
         printf("  -- All NS reference ranges have been handed out for processing --\n");
      }

      return 1;
   }

   // all work in the active NS has been completed
   LOG(LOG_INFO, "Signaling Rank %zu to complete and quiesce, as no reference ranges remain in NS \"%s\"\n",
       ranknum, rman->nslist[response->request.nsindex]->idstr);
   *request = response->request; // populate request with active NS values
   request->type = COMPLETE_WORK;
   return 1;
}

static int handle_complete_response(rmanstate* rman, const size_t ranknum, workresponse* response, workrequest* request) {
   if (response->request.nsindex != rman->nscount) { // only perform processing / cleanup for real completions
      printf("  Rank %zu completed work on NS \"%s\"\n", ranknum, rman->nslist[response->request.nsindex]->idstr);
      // possibly process info from the rank
      if (response->haveinfo) {
         update_walk_report(rman, response);
         update_log_summary(rman, response);
         // output all gathered info, prior to possible log deletion
         outputinfo(rman->summarylog, rman->nslist[response->request.nsindex],
                    &response->report, &response->summary);
      }

      // identify the output log of the transmitting rank
      char* outlogpath = resourcelog_genlogpath(0, rman->logroot, rman->iteration,
                                                rman->nslist[response->request.nsindex], ranknum);
      if (outlogpath == NULL) {
         fprintf(stderr, "ERROR: Failed to identify the output log path of Rank %zu for NS \"%s\"\n",
                 ranknum, rman->nslist[response->request.nsindex]->idstr);
         rman->fatalerror = 1;
         return -1;
      }

      // potentially duplicate the logfile to a final location
      if (rman->preservelogtgt) {
         char* preslogpath = resourcelog_genlogpath(1, rman->preservelogtgt, rman->iteration,
                                                    rman->nslist[response->request.nsindex], ranknum);
         if (preslogpath == NULL) {
             fprintf(stderr, "ERROR: Failed to identify the preserve log path of Rank %zu for NS \"%s\"\n",
                     ranknum, rman->nslist[response->request.nsindex]->idstr);
             free(outlogpath);
             rman->fatalerror = 1;
             return -1;
         }

         // simply use a hardlink for this purpose, no need to make a 'real' duplicate
         if (link(outlogpath, preslogpath)) {
            fprintf(stderr, "ERROR: Failed to link logfile \"%s\" to new target path: \"%s\"\n",
                    outlogpath, preslogpath);
            free(preslogpath);
            free(outlogpath);
            rman->fatalerror = 1;
            return -1;
         }

         free(preslogpath);
      }

      // open the logfile for read
      RESOURCELOG ranklog = NULL;
      if (resourcelog_init(&ranklog, outlogpath, RESOURCE_READ_LOG, rman->nslist[response->request.nsindex])) {
         fprintf(stderr, "ERROR: Failed to open the output log of Rank %zu for NS \"%s\": \"%s\"\n",
                 ranknum, rman->nslist[response->request.nsindex]->idstr, outlogpath);
         free(outlogpath);
         rman->fatalerror = 1;
         return -1;
      }

      // process the work log
      if (response->errorlog) {
         // note a non-fatal error (non-zero exit code for this run)
         rman->nonfatalerror = 1;

         // parse through our output path, looking for the final path element
         char* parselogpath = outlogpath;
         char* finelem = outlogpath;
         while (*parselogpath != '\0') {
            // check for '/' separator and skip beyond it
            if (*parselogpath == '/') {
               while (*parselogpath == '/') {
                  parselogpath++;
                  finelem = parselogpath;
               }
            }
            else {
                parselogpath++;  // only increment here if we aren't already doing so above (avoids possible OOB error with '/' final char)
            }
         }

         // identify the errorlog output location
         int logdirlen = finelem - outlogpath; // pointer arithmetic to get prefix dir string length
         int fnamelen = parselogpath - finelem;
         size_t errloglen = logdirlen + strlen(ERROR_LOG_PREFIX) + fnamelen;
         char* errlogpath = malloc(sizeof(char) * (errloglen + 1));
         snprintf(errlogpath, errloglen + 1, "%.*s%s%s", logdirlen, outlogpath, ERROR_LOG_PREFIX, finelem);

         // open the error log for write
         RESOURCELOG errlog = NULL;
         if (resourcelog_init(&errlog, errlogpath, RESOURCE_RECORD_LOG, rman->nslist[response->request.nsindex])) {
            fprintf(stderr, "ERROR: Failed to open the error log of Rank %zu: \"%s\"\n", ranknum, errlogpath);
            resourcelog_term(&ranklog, NULL, 0);
            free(errlogpath);
            free(outlogpath);
            rman->fatalerror = 1;
            return -1;
         }

         // replay the logfile into the error log location
         if (resourcelog_replay(&ranklog, &errlog, error_only_filter)) {
             fprintf(stderr, "ERROR: Failed to replay errors from logfile \"%s\" into \"%s\"\n", outlogpath, errlogpath);
             resourcelog_term(&errlog, NULL, 1);
             resourcelog_term(&ranklog, NULL, 0);
             free(errlogpath);
             free(outlogpath);
             rman->fatalerror = 1;
             return -1;
         }

         if (resourcelog_term(&errlog, NULL, 0)) {
             fprintf(stderr, "ERROR: Failed to finalize error logfile: \"%s\"\n", errlogpath);
             free(errlogpath);
             free(outlogpath);
             rman->fatalerror = 1;
             return -1;
         }

         free(errlogpath);
      }
      else {
          // simply delete the output logfile of this rank
          if (resourcelog_term(&ranklog, NULL, 1)) {
             fprintf(stderr, "ERROR: Failed to delete the output log of Rank %zu for NS \"%s\": \"%s\"\n",
                     ranknum, rman->nslist[response->request.nsindex]->idstr, outlogpath);
             free(outlogpath);
             rman->fatalerror = 1;
             return -1;
          }
      }

      free(outlogpath);
   } // end of response content processing

   // this rank needs work to process, with no preference for NS
   if (rman->oldlogs) {
      // start by checking for old resource logs to process
      if (hash_reset(rman->oldlogs) < 0) {
         fprintf(stderr, "ERROR: Failure of hash_reset() when trying to identify logfiles for rank %zu to process\n",
                 ranknum);
         rman->fatalerror = 1;
         return -1;
      }

      HASH_NODE* resnode = NULL;
      while (resnode) {
         // check for any nodes with log processing requests remaining
         loginfo* linfo = (loginfo*)(resnode->content);
         if (linfo->logcount) {
            // use the request at the tail of our list
            LOG(LOG_INFO, "Passing out log %zu for NS \"%s\" from iteration \"%s\"\n",
                linfo->logcount,
                rman->nslist[linfo->nsindex]->idstr,
                linfo->requests[linfo->logcount - 1].iteration);
            *request = linfo->requests[linfo->logcount - 1];
            linfo->logcount--;
            return 1;
         }

         if (hash_iterate(rman->oldlogs, &resnode) < 0) {
            fprintf(stderr,
                    "ERROR: Failure of hash_iterate() when trying to identify logfiles for rank %zu to process\n",
                    ranknum);
            rman->fatalerror = 1;
            return -1;
         }
      }

      printf("  -- All old logfiles have been handed out for processing --\n");

      // it seems we've iterated over our entire oldlogs table, with no requests remaining
      // need to free our table
      size_t ncount = 0;
      if (hash_term(rman->oldlogs, &resnode, &ncount)) {
          fprintf(stderr, "ERROR: Failure of hash_term() after passing out all logfiles for processing\n");
          rman->fatalerror = 1;
          return -1;
      }

      rman->oldlogs = NULL;

      // free all subnodes and requests
      for (size_t nindex = 0; nindex < ncount; nindex++) {
          free(resnode[nindex].name);
          loginfo* linfo = (loginfo*)(resnode[nindex].content);
          if (linfo) {
             free(linfo->requests);
             free(linfo);
          }
      }

      free(resnode); // these were allocated in one block, and thus require only one free()
   }

   // no resource logs remain to process
   if (rman->execprevroot) {
      // if we are picking up a previous run, this means no more work remains at all
      LOG(LOG_INFO, "Signaling Rank %zu to terminate, as no resource logs remain\n", ranknum);
      request->type = TERMINATE_WORK;
      return 1;
   }

   // first, check specifically for NSs that have yet to be processed at all
   char anynswork = 0;
   for (size_t nsindex = 0; nsindex < rman->nscount; nsindex++) {
      if (rman->distributed[nsindex] == 0) {
         // this NS still has yet to be worked on at all
         request->type = NS_WORK;
         request->nsindex = nsindex;
         request->refdist = rman->distributed[nsindex];
         request->iteration[0] = '\0';
         request->ranknum = ranknum;
         rman->distributed[nsindex]++; // note newly distributed range

         LOG(LOG_INFO, "Passing out reference range %zu of NS \"%s\" to Rank %zu\n",
             request->refdist, rman->nslist[nsindex]->idstr, ranknum);
         printf("  Rank %zu is beginning work on NS \"%s\" (ref range %zu)\n",
                ranknum, rman->nslist[nsindex]->idstr, request->refdist);

         // check through remaining namespaces for any undistributed work
         for (; anynswork == 0 && nsindex < rman->nscount; nsindex++) {
            if (rman->distributed[nsindex] < rman->workingranks) {
               break;
            }
         }

         if (nsindex == rman->nscount) {
            // just handed out the last NS ref range for processing
            printf("  -- All NS reference ranges have been handed out for processing --\n");
         }

         return 1;
      }
      else if (rman->distributed[nsindex] < rman->workingranks) {
          anynswork = 1;
      }
   }

   // next, check for NSs with ANY remaining work to distribute
   for (size_t nsindex = 0; nsindex < rman->nscount; nsindex++) {
      if (rman->distributed[nsindex] < rman->workingranks) {
          // this NS still has reference ranges to be scanned
          request->type = NS_WORK;
          request->nsindex = nsindex;
          request->refdist = rman->distributed[nsindex];
          request->iteration[0] = '\0';
          request->ranknum = ranknum;
          rman->distributed[nsindex]++; // note newly distributed range

          LOG(LOG_INFO, "Passing out reference range %zu of NS \"%s\" to Rank %zu\n",
              request->refdist, rman->nslist[nsindex]->idstr, ranknum);
          printf("  Rank %zu is picking up work on NS \"%s\" (ref range %zu)\n",
                 ranknum, rman->nslist[nsindex]->idstr, request->refdist);

          // check through remaining namespaces for any undistributed work
          for (; nsindex < rman->nscount; nsindex++) {
             if (rman->distributed[nsindex] < rman->workingranks) {
                 break;
             }
          }

          if (nsindex == rman->nscount) {
             // just handed out the last NS ref range for processing
             printf("  -- All NS reference ranges have been handed out for processing --\n");
          }

          return 1;
      }
   }

   // no NS reference ranges remain to process, so we now need to signal termination
   LOG(LOG_INFO, "Signaling Rank %zu to terminate, as no NS reference ranges remain\n", ranknum);
   request->type = TERMINATE_WORK;
   return 1;
}

/**
 * Process the given response
 * @param rmanstate* rman : Rank state
 * @param size_t ranknum : Responding rank number
 * @param workresponse* response : Response to process
 * @param workrequest* request : New request to populate
 * @return int : A positive value if a new request has been populated;
 *               0 if no request should be sent (rank has exited, or hit a fatal error);
 *               -1 if a fatal error has occurred in this function itself (invalid processing)
 */
int handleresponse(rmanstate* rman, size_t ranknum, workresponse* response, workrequest* request) {
   // check for a fatal error condition, as this overrides all other behaviors
   if (response->fatalerror) {
      // note a fatalerror, and don't generate a request
      fprintf(stderr, "ERROR: Rank %zu%s hit a fatal error condition\n"
                       "       All ranks will now attempt to terminate\n"
                       "       Work Type --\n"
                       "         %s\n"
                       "       Error Description --\n"
                       "         \"%s\"\n",
                       ranknum, (ranknum == rman->ranknum) ? " (this rank)" : "",
                       (response->request.type == RLOG_WORK)      ? "Resource Log Processing" :
                       (response->request.type == NS_WORK)        ? "Namespace Processing"    :
                       (response->request.type == COMPLETE_WORK)  ? "Namespace Finalization"  :
                       (response->request.type == TERMINATE_WORK) ? "Termination"             :
                       (response->request.type == ABORT_WORK)     ? "Abort Condition"         :
                       "UNKNOWN WORK TYPE",
                       response->errorstr);
      rman->fatalerror = 1; // set our fatal error condition, so we signal future workers to terminate
      rman->terminatedworkers[ranknum] = 1;
      return 0;
   }

   if (rman->fatalerror && (response->request.type != ABORT_WORK)) {
       // if we are in an error state, all ranks should be signaled to abort
       LOG(LOG_INFO, "Signaling Rank %zu to Abort\n", ranknum);
       request->type = ABORT_WORK;
       request->nsindex = 0;
       request->refdist = 0;
       request->iteration[0] = '\0';
       request->ranknum = 0;
       return 1;
   }

   int rc = 0;
   switch (response->request.type) {
      case RLOG_WORK:
      case NS_WORK:
         rc = handle_rlog_ns_response(rman, ranknum, response, request);
         break;
      case COMPLETE_WORK:
         rc = handle_complete_response(rman, ranknum, response, request);
         break;
      case TERMINATE_WORK:
      case ABORT_WORK:
         // these simply indicate that the rank has terminated
         rman->terminatedworkers[ranknum] = 1;
         printf("  Rank %zu is terminating\n", ranknum);
         rc = 0;
         break;
      default:
         LOG(LOG_ERR, "Encountered unrecognized response type\n");
         fprintf(stderr, "ERROR: Encountered response to an unknown request type\n");
         rman->fatalerror = 1;
         rc = -1;
         break;
   }

   return rc;
}
