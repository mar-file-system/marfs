/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <ftw.h>
#include <stdio.h>
#include <unistd.h>

#include "resourcelog.h"

// WARNING: error-prone and ugly method of deleting dir trees, written for simplicity only
//          don't replicate this junk into ANY production code paths!
size_t tgtlistpos = 0;
char** tgtlist = NULL;

int ftwnotetgt(const char* fpath, const struct stat* sb, int typeflag) {
   (void) sb; (void) typeflag;

   tgtlist[tgtlistpos] = strdup(fpath);
   tgtlistpos++;

   if (tgtlistpos >= 1048576) {
       printf("Dirlist has insufficient length! (curtgt = %s)\n", fpath);
       return -1;
   }

   return 0;
}

int deletefstree(const char* basepath) {
   tgtlist = malloc(sizeof(char*) * 1048576);

   if (ftw(basepath, ftwnotetgt, 100)) {
      printf("Failed to identify reference tgts of \"%s\"\n", basepath);
      return -1;
   }

   int retval = 0;
   while (tgtlistpos) {
      tgtlistpos--;
      if (strcmp(tgtlist[tgtlistpos], basepath)) {
         //printf("Deleting: \"%s\"\n", tgtlist[tgtlistpos]);
         errno = 0;
         if (rmdir(tgtlist[tgtlistpos])) {
            if (errno != ENOTDIR || unlink(tgtlist[tgtlistpos])) {
               printf("ERROR -- failed to delete \"%s\"\n", tgtlist[tgtlistpos]);
               retval = -1;
            }
         }
      }

      free(tgtlist[tgtlistpos]);
   }

   free(tgtlist);
   return retval;
}

int main(void)
{
   // create required config root dirs
   if (mkdir("./test_rman_topdir", S_IRWXU) && errno != EEXIST) {
      printf("failed to create \"./test_rman_topdir\"\n");
      return -1;
   }

   if (mkdir("./test_rman_topdir/dal_root", S_IRWXU) && errno != EEXIST) {
      printf("failed to create \"./test_rman_topdir/dal_root\"\n");
      return -1;
   }

   if (mkdir("./test_rman_topdir/mdal_root", S_IRWXU) && errno != EEXIST) {
      printf("failed to create \"./test_rman_topdir/mdal_root\"\n");
      return -1;
   }

   // initialize a fresh marfs config
   pthread_mutex_t erasurelock;
   pthread_mutex_init(&erasurelock, NULL);

   marfs_config* config = config_init("./testing/config.xml", &erasurelock);
   if (config == NULL) {
      printf("failed to initalize marfs config\n");
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   int flags = CFG_FIX | CFG_OWNERCHECK | CFG_MDALCHECK | CFG_DALCHECK | CFG_RECURSE;
   if (config_verify(config,"/campaign/",flags)) {
      printf("Config validation failure\n");
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // generate all parent paths of a new logfile
   char* logpath = resourcelog_genlogpath(1, "./test_rman_topdir", "test-resourcelog-iteration123456", config->rootns, 0);
   if (logpath == NULL) {
      printf("failed to generate inital logpath\n");
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   operation_summary summary = {0};

   // initaialize that new logfile
   RESOURCELOG rlog = NULL;
   if (resourcelog_init(&rlog, logpath, RESOURCE_RECORD_LOG, config->rootns)) {
      printf("failed to initialize first logfile: \"%s\"\n", logpath);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // create a set of operations
   opinfo* opset = malloc(sizeof(*opset) * 4);
   opinfo* opparse = opset;

   // start of an object deletion op
   opparse->type = MARFS_DELETE_OBJ_OP;
   opparse->extendedinfo = calloc(1, sizeof(delobj_info));

   delobj_info* delobjinf = (delobj_info*)opparse->extendedinfo;
   delobjinf->offset = 3;
   opparse->start = 1;
   opparse->count = 4;
   opparse->errval = 0;
   opparse->ftag.ctag = strdup("someclient");
   opparse->ftag.streamid = strdup("initstreamID123forme");
   opparse->ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   opparse->ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   opparse->ftag.objfiles = 4;
   opparse->ftag.objsize = 1024;
   opparse->ftag.refbreadth = 10;
   opparse->ftag.refdepth = 9;
   opparse->ftag.refdigits = 32;
   opparse->ftag.fileno = 0;
   opparse->ftag.objno = 0;
   opparse->ftag.offset = 12;
   opparse->ftag.endofstream = 0;
   opparse->ftag.protection.N = 5;
   opparse->ftag.protection.E = 1;
   opparse->ftag.protection.O = 0;
   opparse->ftag.protection.partsz = 123;
   opparse->ftag.bytes = 4096;
   opparse->ftag.availbytes = 4096;
   opparse->ftag.recoverybytes = 23;
   opparse->ftag.state = FTAG_COMP | FTAG_READABLE;
   opparse->next = opparse + 1;
   opparse++;

   // start of a ref deletion op
   opparse->type = MARFS_DELETE_REF_OP;
   opparse->extendedinfo = malloc(sizeof(delref_info));

   delref_info* delrefinf = (delref_info*)(opparse->extendedinfo);
   delrefinf->prev_active_index = 0;
   delrefinf->delzero = 1;
   delrefinf->eos = 0;
   opparse->start = 1;
   opparse->count = 1;
   opparse->errval = 0;
   opparse->ftag = (opparse-1)->ftag; // just inherit the same FTAG
   opparse->next = NULL;
   opparse++;

   // start of a rebuild op
   opparse->type = MARFS_REBUILD_OP;
   opparse->extendedinfo = malloc(sizeof(rebuild_info));

   rebuild_info* rebuildinf = (rebuild_info*)(opparse->extendedinfo);
   rebuildinf->markerpath = strdup("nothinmarker");
   rebuildinf->rtag = calloc(1, sizeof(RTAG));
   rebuildinf->rtag->majorversion = RTAG_CURRENT_MAJORVERSION;
   rebuildinf->rtag->minorversion = RTAG_CURRENT_MINORVERSION;
   rebuildinf->rtag->createtime = (time_t)945873284;
   rebuildinf->rtag->stripewidth = 7;
   rebuildinf->rtag->stripestate.versz = 1048576;
   rebuildinf->rtag->stripestate.blocksz = 104857700;
   rebuildinf->rtag->stripestate.totsz = 1034871239847;
   rebuildinf->rtag->stripestate.meta_status = calloc(sizeof(char), rebuildinf->rtag->stripewidth);
   rebuildinf->rtag->stripestate.meta_status[2] = 1;
   rebuildinf->rtag->stripestate.data_status = calloc(sizeof(char), rebuildinf->rtag->stripewidth);
   rebuildinf->rtag->stripestate.data_status[0] = 1;
   rebuildinf->rtag->stripestate.data_status[5] = 1;
   opparse->start = 1;
   opparse->count = 1;
   opparse->errval = 0;
   opparse->ftag = (opparse-1)->ftag; // just inherit the same FTAG
   opparse->next = NULL;
   opparse++;

   // start of a repack op
   opparse->type = MARFS_REPACK_OP;
   opparse->extendedinfo = malloc(sizeof(repack_info));

   repack_info* repackinf = (repack_info*)(opparse->extendedinfo);
   repackinf->totalbytes = 4096;
   opparse->start = 1;
   opparse->count = 1;
   opparse->errval = 0;
   opparse->ftag = (opparse-1)->ftag; // just inherit the same FTAG
   opparse->next = NULL;

   // insert deletion op chain
   char progress = 0;
   if (resourcelog_processop(&rlog, opset, &progress)) {
      printf("failed to insert initial deletion op chain\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   if (progress) {
      printf("insertion of inital deletion chain incorrectly set 'progress' flag\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // insert rebuild op
   if (resourcelog_processop(&rlog, &opset[2], &progress)) {
      printf("failed to insert initial rebuild op\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   if (progress) {
      printf("insertion of inital rebuild op incorrectly set 'progress' flag\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // insert repack op
   if (resourcelog_processop(&rlog, &opset[3], &progress)) {
      printf("failed to insert initial repack op\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   if (progress) {
      printf("insertion of inital repack op incorrectly set 'progress' flag\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // attempt to insert completion of an op (expected to fail)
   opset->start = 0;
   if (resourcelog_processop(&rlog, opset, &progress) == 0) {
      printf("unexpected success for insert of op completion into a RECORD log\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }
   opset->start = 1; //revert change

   // terminate the log, keeping the copy
   if (resourcelog_term(&rlog, &summary, 0)) {
      printf("failed to terminate initial logfile\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // generate a new logfile path
   char* wlogpath = resourcelog_genlogpath(1, "./test_rman_topdir", "test-resourcelog-iteration654321", config->rootns, 10);
   if (wlogpath == NULL) {
      printf("failed to generate write logpath\n");
      resourcelog_term(&rlog, &summary, 1);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // initaialize that new MODIFY logfile
   RESOURCELOG wlog = NULL;
   if (resourcelog_init(&wlog, wlogpath, RESOURCE_MODIFY_LOG, config->rootns)) {
      printf("failed to initialize write logfile: \"%s\"\n", wlogpath);
      goto error_wlog;
   }

   resourcelog_term(&rlog, &summary, 1);

   // read and validate the content of our original log, adding each op to our MODIFY log
   if (resourcelog_init(&rlog, logpath, RESOURCE_READ_LOG, NULL)) {
      printf("failed to initialize first read log\n");
      goto error_wlog;
   }

   opparse = NULL;
   printf("readingopset1\n");
   if (resourcelog_readop(&rlog, &opparse)) {
      printf("failed to read first operation set from first read log\n");
      goto error_rlog;
   }

   opinfo* readops = opparse;
   printf("valop1\n");
   delobj_info* delobjparse = (delobj_info*)opparse->extendedinfo;
   if (opparse->type != opset->type ||
        delobjparse->offset != delobjinf->offset ||
        opparse->start != opset->start ||
        opparse->count != opset->count ||
        opparse->errval != opset->errval ||
        opparse->ftag.majorversion != opset->ftag.majorversion ||
        opparse->ftag.minorversion != opset->ftag.minorversion ||
        strcmp(opparse->ftag.ctag, opset->ftag.ctag) ||
        strcmp(opparse->ftag.streamid, opset->ftag.streamid) ||
        opparse->ftag.objfiles != opset->ftag.objfiles ||
        opparse->ftag.objsize != opset->ftag.objsize ||
        opparse->ftag.refbreadth != opset->ftag.refbreadth ||
        opparse->ftag.refdepth != opset->ftag.refdepth ||
        opparse->ftag.refdigits != opset->ftag.refdigits ||
        opparse->ftag.fileno != opset->ftag.fileno ||
        opparse->ftag.objno != opset->ftag.objno ||
        opparse->ftag.offset != opset->ftag.offset ||
        opparse->ftag.endofstream != opset->ftag.endofstream ||
        opparse->ftag.protection.N != opset->ftag.protection.N ||
        opparse->ftag.protection.E != opset->ftag.protection.E ||
        opparse->ftag.protection.O != opset->ftag.protection.O ||
        opparse->ftag.protection.partsz != opset->ftag.protection.partsz ||
        opparse->ftag.bytes != opset->ftag.bytes ||
        opparse->ftag.availbytes != opset->ftag.availbytes ||
        opparse->ftag.recoverybytes != opset->ftag.recoverybytes ||
        opparse->ftag.state != opset->ftag.state) {
      printf("read op 1 differs from original\n");
      if (opparse->type != opset->type) { printf("type\n"); }
      if (delobjparse->offset != delobjinf->offset) { printf("extendedinfo\n"); }
      if (opparse->start != opset->start) { printf("start\n"); }
      if (opparse->count != opset->count) { printf("count\n"); }
      if (opparse->errval != opset->errval) { printf("errval\n"); }
      goto error_rlog;
   }

   opparse = opparse->next;
   printf("valop2\n");
   delref_info* delrefparse = (delref_info*)opparse->extendedinfo;
   if (opparse->type != (opset+1)->type ||
        delrefparse->prev_active_index != delrefinf->prev_active_index ||
        delrefparse->delzero != delrefinf->delzero ||
        delrefparse->eos != delrefinf->eos ||
        opparse->start != (opset+1)->start ||
        opparse->count != (opset+1)->count ||
        opparse->errval != (opset+1)->errval ||
        opparse->next   != (opset+1)->next ||
        opparse->ftag.majorversion != (opset+1)->ftag.majorversion ||
        opparse->ftag.minorversion != (opset+1)->ftag.minorversion ||
        strcmp(opparse->ftag.ctag, (opset+1)->ftag.ctag) ||
        strcmp(opparse->ftag.streamid, (opset+1)->ftag.streamid) ||
        opparse->ftag.objfiles != (opset+1)->ftag.objfiles ||
        opparse->ftag.objsize != (opset+1)->ftag.objsize ||
        opparse->ftag.refbreadth != (opset+1)->ftag.refbreadth ||
        opparse->ftag.refdepth != (opset+1)->ftag.refdepth ||
        opparse->ftag.refdigits != (opset+1)->ftag.refdigits ||
        opparse->ftag.fileno != (opset+1)->ftag.fileno ||
        opparse->ftag.objno != (opset+1)->ftag.objno ||
        opparse->ftag.offset != (opset+1)->ftag.offset ||
        opparse->ftag.endofstream != (opset+1)->ftag.endofstream ||
        opparse->ftag.protection.N != (opset+1)->ftag.protection.N ||
        opparse->ftag.protection.E != (opset+1)->ftag.protection.E ||
        opparse->ftag.protection.O != (opset+1)->ftag.protection.O ||
        opparse->ftag.protection.partsz != (opset+1)->ftag.protection.partsz ||
        opparse->ftag.bytes != (opset+1)->ftag.bytes ||
        opparse->ftag.availbytes != (opset+1)->ftag.availbytes ||
        opparse->ftag.recoverybytes != (opset+1)->ftag.recoverybytes ||
        opparse->ftag.state != (opset+1)->ftag.state) {
      printf("read op 2 differs from original\n");
      if (opparse->type != (opset+1)->type) { printf("type\n"); }
      if (delrefparse->prev_active_index != delrefinf->prev_active_index) { printf("prevactive\n"); }
      if (delrefparse->delzero != delrefinf->delzero) { printf("delzero\n"); }
      if (delrefparse->eos != delrefinf->eos) { printf("eos\n"); }
      if (opparse->start != (opset+1)->start) { printf("start\n"); }
      if (opparse->count != (opset+1)->count) { printf("count\n"); }
      if (opparse->errval != (opset+1)->errval) { printf("errval\n"); }
      goto error_rlog;
   }

   // output the same ops to our modify log
   if (resourcelog_processop(&wlog, readops, NULL)) {
      printf("failed to output first read op set to MODIFY log\n");
      goto error_rlog;
   }

   resourcelog_freeopinfo(readops);

   // validate the next operation
   if (resourcelog_readop(&rlog, &opparse)) {
      printf("failed to read second operation set from first read log\n");
      goto error_rlog;
   }

   rebuild_info* rebuildparse = (rebuild_info*)opparse->extendedinfo;
   if (opparse->type != opset[2].type ||
        strcmp(rebuildparse->markerpath, rebuildinf->markerpath) ||
        rebuildparse->rtag->stripestate.versz != rebuildinf->rtag->stripestate.versz ||
        rebuildparse->rtag->stripestate.blocksz != rebuildinf->rtag->stripestate.blocksz ||
        rebuildparse->rtag->stripestate.totsz != rebuildinf->rtag->stripestate.totsz ||
        rebuildparse->rtag->stripestate.meta_status[0] != rebuildinf->rtag->stripestate.meta_status[0] ||
        rebuildparse->rtag->stripestate.meta_status[1] != rebuildinf->rtag->stripestate.meta_status[1] ||
        rebuildparse->rtag->stripestate.meta_status[2] != rebuildinf->rtag->stripestate.meta_status[2] ||
        rebuildparse->rtag->stripestate.meta_status[3] != rebuildinf->rtag->stripestate.meta_status[3] ||
        rebuildparse->rtag->stripestate.meta_status[4] != rebuildinf->rtag->stripestate.meta_status[4] ||
        rebuildparse->rtag->stripestate.meta_status[5] != rebuildinf->rtag->stripestate.meta_status[5] ||
        rebuildparse->rtag->stripestate.data_status[0] != rebuildinf->rtag->stripestate.data_status[0] ||
        rebuildparse->rtag->stripestate.data_status[1] != rebuildinf->rtag->stripestate.data_status[1] ||
        rebuildparse->rtag->stripestate.data_status[2] != rebuildinf->rtag->stripestate.data_status[2] ||
        rebuildparse->rtag->stripestate.data_status[3] != rebuildinf->rtag->stripestate.data_status[3] ||
        rebuildparse->rtag->stripestate.data_status[4] != rebuildinf->rtag->stripestate.data_status[4] ||
        rebuildparse->rtag->stripestate.data_status[5] != rebuildinf->rtag->stripestate.data_status[5] ||
        rebuildparse->rtag->stripestate.csum != rebuildinf->rtag->stripestate.csum ||
        opparse->start != opset[2].start ||
        opparse->count != opset[2].count ||
        opparse->errval != opset[2].errval ||
        opparse->next   != opset[2].next ||
        opparse->ftag.majorversion != opset[2].ftag.majorversion ||
        opparse->ftag.minorversion != opset[2].ftag.minorversion ||
        strcmp(opparse->ftag.ctag, opset[2].ftag.ctag) ||
        strcmp(opparse->ftag.streamid, opset[2].ftag.streamid) ||
        opparse->ftag.objfiles != opset[2].ftag.objfiles ||
        opparse->ftag.objsize != opset[2].ftag.objsize ||
        opparse->ftag.refbreadth != opset[2].ftag.refbreadth ||
        opparse->ftag.refdepth != opset[2].ftag.refdepth ||
        opparse->ftag.refdigits != opset[2].ftag.refdigits ||
        opparse->ftag.fileno != opset[2].ftag.fileno ||
        opparse->ftag.objno != opset[2].ftag.objno ||
        opparse->ftag.offset != opset[2].ftag.offset ||
        opparse->ftag.endofstream != opset[2].ftag.endofstream ||
        opparse->ftag.protection.N != opset[2].ftag.protection.N ||
        opparse->ftag.protection.E != opset[2].ftag.protection.E ||
        opparse->ftag.protection.O != opset[2].ftag.protection.O ||
        opparse->ftag.protection.partsz != opset[2].ftag.protection.partsz ||
        opparse->ftag.bytes != opset[2].ftag.bytes ||
        opparse->ftag.availbytes != opset[2].ftag.availbytes ||
        opparse->ftag.recoverybytes != opset[2].ftag.recoverybytes ||
        opparse->ftag.state != opset[2].ftag.state) {
      printf("read op 3 differs from original\n");
      goto error_rlog;
   }

   // output the same ops to our modify log
   if (resourcelog_processop(&wlog, opparse, NULL)) {
      printf("failed to output first read op set to MODIFY log\n");
      goto error_rlog;
   }

   resourcelog_freeopinfo(opparse);

   // validate the next operation
   if (resourcelog_readop(&rlog, &opparse)) {
      printf("failed to read third operation set from first read log\n");
      goto error_rlog;
   }

   repack_info* repackparse = (repack_info*)opparse->extendedinfo;
   if (opparse->type != (opset+3)->type ||
        repackparse->totalbytes != repackinf->totalbytes ||
        opparse->start != (opset+3)->start ||
        opparse->count != (opset+3)->count ||
        opparse->errval != (opset+3)->errval ||
        opparse->next   != (opset+3)->next ||
        opparse->ftag.majorversion != (opset+3)->ftag.majorversion ||
        opparse->ftag.minorversion != (opset+3)->ftag.minorversion ||
        strcmp(opparse->ftag.ctag, (opset+3)->ftag.ctag) ||
        strcmp(opparse->ftag.streamid, (opset+3)->ftag.streamid) ||
        opparse->ftag.objfiles != (opset+3)->ftag.objfiles ||
        opparse->ftag.objsize != (opset+3)->ftag.objsize ||
        opparse->ftag.refbreadth != (opset+3)->ftag.refbreadth ||
        opparse->ftag.refdepth != (opset+3)->ftag.refdepth ||
        opparse->ftag.refdigits != (opset+3)->ftag.refdigits ||
        opparse->ftag.fileno != (opset+3)->ftag.fileno ||
        opparse->ftag.objno != (opset+3)->ftag.objno ||
        opparse->ftag.offset != (opset+3)->ftag.offset ||
        opparse->ftag.endofstream != (opset+3)->ftag.endofstream ||
        opparse->ftag.protection.N != (opset+3)->ftag.protection.N ||
        opparse->ftag.protection.E != (opset+3)->ftag.protection.E ||
        opparse->ftag.protection.O != (opset+3)->ftag.protection.O ||
        opparse->ftag.protection.partsz != (opset+3)->ftag.protection.partsz ||
        opparse->ftag.bytes != (opset+3)->ftag.bytes ||
        opparse->ftag.availbytes != (opset+3)->ftag.availbytes ||
        opparse->ftag.recoverybytes != (opset+3)->ftag.recoverybytes ||
        opparse->ftag.state != (opset+3)->ftag.state) {
      printf("read op 4 differs from original\n");
      goto error_rlog;
   }

   // output the same ops to our modify log
   if (resourcelog_processop(&wlog, opparse, NULL)) {
      printf("failed to output first read op set to MODIFY log\n");
      goto error_rlog;
   }

   resourcelog_freeopinfo(opparse);

   // terminate our reading log
   if (resourcelog_term(&rlog, &summary, 1)) {
      printf("failed to terminate inital reading resourcelog\n");
      goto error_wlog;
   }

   // and abort our writing log
   if (resourcelog_abort(&wlog)) {
      printf("failed to abort inital modify log\n");
      goto error_wlog;
   }

   // open our writen modify log for read
   if (resourcelog_init(&rlog, wlogpath, RESOURCE_READ_LOG, NULL)) {
      printf("failed to initialize first read log\n");
   }

   free(wlogpath);

   // open a new output modify log
   wlogpath = resourcelog_genlogpath(1, "./test_rman_topdir", "test-resourcelog-iteration654321", config->rootns, 1);
   if (wlogpath == NULL) {
      printf("failed to generate second modify logfile path\n");
      goto error_rlog;
   }

   resourcelog_term(&wlog, &summary, 1);
   wlog = NULL;
   if (resourcelog_init(&wlog, wlogpath, RESOURCE_MODIFY_LOG, config->rootns)) {
      printf("failed to initialize write logfile: \"%s\"\n", wlogpath);
      // can't goto error_rlog here because wlog is NULL
      resourcelog_term(&rlog, &summary, 1);
      free(wlogpath);
      free(logpath);
      config_term(config);
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // replay previous info from previous modify log into the new one
   if (resourcelog_replay(&rlog, &wlog, NULL)) {
      printf("failed to replay old logfile\n");
      goto error_rlog;
   }

   // insert first deletion op completion with decreased count
   progress = 0;
   opset->next = NULL;
   opset->count -= 2;
   delobjinf->offset = 2;
   opset->start = 0;
   if (resourcelog_processop(&wlog, opset, &progress)) {
      printf("failed to insert initial deletion op completion\n");
      goto error_rlog;
   }

   if (progress) {
      printf("insertion of inital deletion completion incorrectly set 'progress' flag\n");
      goto error_rlog;
   }

   // reinsert the same op, indicating completion
   if (resourcelog_processop(&wlog, opset, &progress)) {
      printf("failed to insert full deletion op completion\n");
      goto error_rlog;
   }

   if (!progress) {
      printf("insertion of full deletion completion failed to set 'progress' flag\n");
      goto error_rlog;
   }

   // insert rebuild op completion
   opset[2].start = 0;
   progress = 0;
   if (resourcelog_processop(&wlog, &opset[2], &progress)) {
      printf("failed to insert rebuild op completion\n");
      goto error_rlog;
   }

   if (!progress) {
      printf("insertion of rebuild op completion failed to set 'progress' flag\n");
      goto error_rlog;
   }

   // insert repack op completion
   opset[3].start = 0;
   progress = 0;
   if (resourcelog_processop(&wlog, &opset[3], &progress)) {
      printf("failed to insert repack op completion\n");
      goto error_rlog;
   }

   if (!progress) {
      printf("insertion of repack op completion failed to set 'progress' flag\n");
      goto error_rlog;
   }

   // insert final completion of inital deletion op
   opset[1].start = 0;
   progress = 0;
   if (resourcelog_processop(&wlog, &opset[1], &progress)) {
      printf("failed to insert final deletion op completion\n");
      goto error_rlog;
   }

   if (!progress) {
      printf("insertion of final deletion completion failed to set 'progress' flag\n");
      goto error_rlog;
   }

//   // open another readlog for this same file
//   if (resourcelog_init(&rlog), logpath, RESOURCE_READ_LOG, NULL)) {
//      printf("failed to initialize first read log\n");
//      return -1;
//   }


   // replay our record log into the new modify log
//   if (resourcelog_replay(&rlog), &wlog))) {
//      printf("failed to replay source log \"%s\" into dest log \"%s\"\n", logpath, wlogpath);
//      return -1;
//   }

   free(wlogpath);

   // free all operations
   opparse = opset;
   free(opparse->ftag.ctag);
   free(opparse->ftag.streamid);
   free(opparse->extendedinfo);
   opparse++;
   free(opparse->extendedinfo);
   opparse++;
   rebuildinf = (rebuild_info*)(opparse->extendedinfo);
   free(rebuildinf->markerpath);
   rtag_free(rebuildinf->rtag);
   free(rebuildinf->rtag);
   free(opparse->extendedinfo);
   opparse++;
   free(opparse->extendedinfo);
   free(opset);

   printf("terminating\n");

   // terminate the logfile
   if (resourcelog_term(&wlog, &summary, 1)) {
      printf("failed to terminate resourcelog\n");
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   // free logpath
   free(logpath);

   // terminate our config
   if (config_term(config)) {
      printf("failed to terminate config\n");
      pthread_mutex_destroy(&erasurelock);
      return -1;
   }

   pthread_mutex_destroy(&erasurelock);

   // cleanup test trees
   if (deletefstree("./test_rman_topdir")) {
      printf("Failed to delete contents of test tree\n");
      return -1;
   }

   rmdir("./test_rman_topdir");

   return 0;

  error_rlog:
   resourcelog_term(&rlog, &summary, 1);
  error_wlog:
   resourcelog_term(&wlog, &summary, 1);
   free(wlogpath);
   free(logpath);
   config_term(config);
   pthread_mutex_destroy(&erasurelock);
   return -1;
}
