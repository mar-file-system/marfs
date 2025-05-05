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
