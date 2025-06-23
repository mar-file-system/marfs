/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <string.h>

#include "parse_program_args.h"

int parse_program_args(rmanstate* rman, FILE* inputsummary) {
   // parse in a verify the config version
   char* readline = NULL;
   size_t linealloc = 0;
   ssize_t linelen = getline(&readline, &linealloc, inputsummary);
   if (linelen < 2) {
      fprintf(stderr, "WARNING: Failed to read MarFS config version string from previous run's summary log\n");
      return -1;
   }

   linelen--; // decrement length, to exclude newline

   if (strncmp(readline, rman->config->version, linelen)) {
      fprintf(stderr, "WARNING: Previous run is associated with a different MarFS config version: \"%s\"\n",
              readline);
      free(readline);
      return -1;
   }

   // parse in operation threshold values and set our state to match
   while ((linelen = getline(&readline, &linealloc, inputsummary)) > 1) {
      linelen--; // decrement length, to exclude newline
      if (strncmp(readline, "QUOTAS", linelen) == 0) {
         rman->quotas = 1;
      }
      else if (strncmp(readline, "DRY-RUN", linelen) == 0) {
         rman->gstate.dryrun = 1;
      }
      else {
         char *value = NULL;
         char *key = strtok_r(readline, "=", &value);

         if (value == (readline + linelen)) {
            fprintf(stderr, "ERROR: Failed to parse previous run's operation info (\"%s\")\n", readline);
            free(readline);
            return -1;
         }

         unsigned long long parseval = 0;
         if (sscanf(value, "%llu", &parseval) != 1) {
            fprintf(stderr, "ERROR: Failed to parse previous run's \"%s\" operation threshold: \"%s\"\n",
                    readline, value);
            free(readline);
            return -1;
         }

         // populate the appropriate value, based on string header
         if (strncmp(key, "GC", 3) == 0) {
            rman->gstate.thresh.gcthreshold = (time_t)parseval;
         }
         else if (strncmp(key, "REPACK", 7) == 0) {
            rman->gstate.thresh.repackthreshold = (time_t)parseval;
         }
         else if (strncmp(key, "REBUILD", 8) == 0) {
            rman->gstate.thresh.rebuildthreshold = (time_t)parseval;
         }
         else if (strncmp(key, "REBUILD-LOCATION-POD", 21) == 0) {
            rman->gstate.rebuildloc.pod = (int)parseval;
            rman->gstate.lbrebuild = 1;
         }
         else if (strncmp(key, "REBUILD-LOCATION-CAP", 21) == 0) {
            rman->gstate.rebuildloc.cap = (int)parseval;
            rman->gstate.lbrebuild = 1;
         }
         else if (strncmp(key, "REBUILD-LOCATION-SCATTER", 25) == 0) {
            rman->gstate.rebuildloc.scatter = (int)parseval;
            rman->gstate.lbrebuild = 1;
         }
         else {
            fprintf(stderr, "ERROR: Encountered unrecognized operation type in log of previous run: \"%s\"\n",
                    readline);
            free(readline);
            return -1;
         }
      }
   }

   free(readline);

   if (linelen < 1) {
      fprintf(stderr, "ERROR: Failed to read operation info from previous run's logfile\n");
      return -1;
   }

   return 0;
}
