/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <string.h>
#include <fcntl.h>
#include <stdlib.h>

#include "rsrc_mgr/summary_log_setup.h"

static int output_program_args(rmanstate* rman) {
   // start with marfs config version (config changes could seriously break an attempt to re-execute this later)
   if (fprintf(rman->summarylog, "%s\n", rman->config->version) < 1) {
      fprintf(stderr, "ERROR: Failed to output config version to summary log\n");
      return -1;
   }

   // output operation types and threshold values
   if (rman->gstate.dryrun && fprintf(rman->summarylog, "DRY-RUN\n") < 1) {
      fprintf(stderr, "ERROR: Failed to output dry-run flag to summary log\n");
      return -1;
   }

   if (rman->quotas && fprintf(rman->summarylog, "QUOTAS\n") < 1) {
      fprintf(stderr, "ERROR: Failed to output quota flag to summary log\n");
      return -1;
   }

   if (rman->gstate.thresh.gcthreshold &&
        fprintf(rman->summarylog, "GC=%llu\n", (unsigned long long) rman->gstate.thresh.gcthreshold) < 1) {
      fprintf(stderr, "ERROR: Failed to output GC threshold to summary log\n");
      return -1;
   }

   if (rman->gstate.thresh.repackthreshold &&
        fprintf(rman->summarylog, "REPACK=%llu\n", (unsigned long long) rman->gstate.thresh.repackthreshold) < 1) {
      fprintf(stderr, "ERROR: Failed to output REPACK threshold to summary log\n");
      return -1;
   }

   if (rman->gstate.thresh.rebuildthreshold &&
        fprintf(rman->summarylog, "REBUILD=%llu\n", (unsigned long long) rman->gstate.thresh.rebuildthreshold) < 1) {
      fprintf(stderr, "ERROR: Failed to output REBUILD threshold to summary log\n");
      return -1;
   }

   if (rman->gstate.lbrebuild &&
        (
         fprintf(rman->summarylog, "REBUILD-LOCATION-POD=%d\n", rman->gstate.rebuildloc.pod) < 1  ||
         fprintf(rman->summarylog, "REBUILD-LOCATION-CAP=%d\n", rman->gstate.rebuildloc.cap) < 1  ||
         fprintf(rman->summarylog, "REBUILD-LOCATION-SCATTER=%d\n", rman->gstate.rebuildloc.scatter) < 1
       )) {
      fprintf(stderr, "ERROR: Failed to output REBUILD location to summary log\n");
      return -1;
   }

   if (fprintf(rman->summarylog, "\n") < 1) {
      fprintf(stderr, "ERROR: Failed to output header separator summary log\n");
      return -1;
   }

   return 0;
}

int summary_log_setup(rmanstate *rman, const int recurse) {
   // rank zero needs to output our summary header
   if (rman->ranknum == 0) {
      // open our summary log
      size_t alloclen = strlen(rman->logroot) + 1 + strlen(rman->iteration) + 1 + strlen(SUMMARY_FILENAME) + 1;
      char* sumlogpath = malloc(sizeof(char) * alloclen);
      size_t printres = snprintf(sumlogpath, alloclen, "%s/%s", rman->logroot, rman->iteration);
      if (mkdir(sumlogpath, 0700) && errno != EEXIST) {
         fprintf(stderr, "ERROR: Failed to create summary log parent dir: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      printres += snprintf(sumlogpath + printres, alloclen - printres, "/" SUMMARY_FILENAME);
      int sumlog = open(sumlogpath, O_WRONLY | O_CREAT | O_EXCL, 0700);
      if (sumlog < 0) {
         fprintf(stderr, "ERROR: Failed to open summary logfile: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      rman->summarylog = fdopen(sumlog, "w");
      if (rman->summarylog == NULL) {
         fprintf(stderr, "ERROR: Failed to convert summary logfile to file stream: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      // output our program arguments to the summary file
      if (output_program_args(rman)) {
         fprintf(stderr, "ERROR: Failed to output program arguments to summary log: \"%s\"\n",
                 sumlogpath);
         free(sumlogpath);
         return -1;
      }

      free(sumlogpath);

      // print out run info
      printf("Processing %zu Total Namespaces (%sTarget NS \"%s\")\n",
              rman->nscount, (recurse) ? "Recursing Below " : "", (rman->nslist[0])->idstr);
      printf("   Operation Summary:%s%s%s%s%s\n",
              (rman->gstate.dryrun) ? " DRY-RUN" : "", (rman->quotas) ? " QUOTAS" : "",
              (rman->gstate.thresh.gcthreshold) ? " GC" : "",
              (rman->gstate.thresh.repackthreshold) ? " REPACK" : "",
              (rman->gstate.thresh.rebuildthreshold) ?
               ((rman->gstate.lbrebuild) ? " REBUILD(LOCATION)" : " REBUILD(MARKER)") : "");
   }

   return 0;
}
