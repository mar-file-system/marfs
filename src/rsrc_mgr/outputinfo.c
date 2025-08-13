/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "outputinfo.h"

void outputinfo(FILE* output, marfs_ns* ns, streamwalker_report* report, operation_summary* summary) {
   char userout = 0;
   if (output == stdout) { userout = 1; } // limit info output directly to the user
   fprintf(output, "Namespace \"%s\"%s --\n", ns->idstr, (userout) ? " Totals" : " Incremental Values");
   fprintf(output, "   Walk Report --\n");
   if (!userout || report->fileusage)   { fprintf(output, "      File Usage = %zu\n", report->fileusage); }
   size_t bytetrans = report->byteusage;
   char remainder[6] = {0};
   char* unit = "B";
   while (bytetrans > 1024) {
      if (*unit == 'B') { unit = "KiB"; }
      else if (*unit == 'K') { unit = "MiB"; }
      else if (*unit == 'M') { unit = "GiB"; }
      else if (*unit == 'G') { unit = "TiB"; }
      else if (*unit == 'T') { unit = "PiB"; }
      else { break; }
      if (bytetrans % 1024) {
         snprintf(remainder, 6, ".%.3zu", (((bytetrans % 1024) * 1000) + 512) / 1024);
      } else { *remainder = '\0'; }
      bytetrans /= 1024;
   }
   if (!userout)   { fprintf(output, "      Byte Usage = %zu\n", report->byteusage); }
   else if (report->byteusage) {
      fprintf(output, "      Byte Usage = %zu%s%s\n", bytetrans, remainder, unit);
   }
   if (!userout || report->filecount)   { fprintf(output, "      File Count = %zu\n", report->filecount); }
   if (!userout || report->objcount)    { fprintf(output, "      Object Count = %zu\n", report->objcount); }
   bytetrans = report->bytecount;
   *remainder = '\0';
   unit = "B";
   while (bytetrans > 1024) {
      if (*unit == 'B') { unit = "KiB"; }
      else if (*unit == 'K') { unit = "MiB"; }
      else if (*unit == 'M') { unit = "GiB"; }
      else if (*unit == 'G') { unit = "TiB"; }
      else if (*unit == 'T') { unit = "PiB"; }
      else { break; }
      if (bytetrans % 1024) {
         snprintf(remainder, 6, ".%.3zu", (((bytetrans % 1024) * 1000) + 512) / 1024);
      } else { *remainder = '\0'; }
      bytetrans /= 1024;
   }
   if (!userout)   { fprintf(output, "      Byte Count = %zu\n", report->bytecount); }
   else if (report->bytecount) {
      fprintf(output, "      Byte Count = %zu%s%s\n", bytetrans, remainder, unit);
   }
   if (!userout || report->streamcount) { fprintf(output, "      Stream Count = %zu\n", report->streamcount); }
   if (!userout || report->delobjs)     { fprintf(output, "      Object Deletion Candidates = %zu\n", report->delobjs); }
   if (!userout || report->delfiles)    { fprintf(output, "      File Deletion Candidates = %zu\n", report->delfiles); }
   if (!userout || report->delstreams)  { fprintf(output, "      Stream Deletion Candidates = %zu\n", report->delstreams); }
   if (!userout || report->volfiles)    { fprintf(output, "      Volatile File Count = %zu\n", report->volfiles); }
   if (!userout || report->rpckfiles)   { fprintf(output, "      File Repack Candidates = %zu\n", report->rpckfiles); }
   if (!userout || report->rpckbytes)   { fprintf(output, "      Repack Candidate Bytes = %zu\n", report->rpckbytes); }
   if (!userout || report->rbldobjs)    { fprintf(output, "      Object Rebuild Candidates = %zu\n", report->rbldobjs); }
   if (!userout || report->rbldbytes)   { fprintf(output, "      Rebuild Candidate Bytes = %zu\n", report->rbldbytes); }
   fprintf(output, "   Operation Summary --\n");
   if (!userout || summary->deletion_object_count) {
      fprintf(output, "      Object Deletion Count = %zu (%zu Failures)\n",
               summary->deletion_object_count, summary->deletion_object_failures);
   }
   if (!userout || summary->deletion_reference_count) {
      fprintf(output, "      Reference Deletion Count = %zu (%zu Failures)\n",
               summary->deletion_reference_count, summary->deletion_reference_failures);
   }
   if (!userout || summary->rebuild_count) {
      fprintf(output, "      Object Rebuild Count = %zu (%zu Failures)\n",
               summary->rebuild_count, summary->rebuild_failures);
   }
   if (!userout || summary->repack_count) {
      fprintf(output, "      File Repack Count = %zu (%zu Failures)\n",
               summary->repack_count, summary->repack_failures);
   }
   fprintf(output, "\n");
   fflush(output);
   return;
}
