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

#include <string.h>

#include "rsrc_mgr/consts.h"
#include "rsrc_mgr/logline.h"
#include "rsrc_mgr/resourcelog.h" // need resourcelog_freeopinfo

static int parse_del_obj(char **parseloc, opinfo *op) {
   LOG(LOG_INFO, "Parsing a MARFS_DELETE_OBJ operation\n");

   op->type = MARFS_DELETE_OBJ_OP;

   *parseloc += 8;

   // allocate delobj_info
   delobj_info* extinfo = calloc(1, sizeof(*extinfo));

   // parse in delobj_info
   if (strncmp(*parseloc, "{ ", 2)) {
      LOG(LOG_ERR, "Missing '{ ' header for DEL-OBJ extended info\n");
      free(extinfo);
      return -1;
   }

   *parseloc += 2;

   char* endptr = NULL;
   unsigned long long parseval = strtoull(*parseloc, &endptr, 10);
   if (endptr == NULL || *endptr != ' ') {
      LOG(LOG_ERR, "DEL-OBJ extended info has unexpected char in prev_active_index string: '%c'\n", *endptr);
      free(extinfo);
      return -1;
   }

   extinfo->offset = (size_t)parseval;

   *parseloc = endptr + 1;

   if (strncmp(*parseloc, "} ", 2)) {
      LOG(LOG_ERR, "Missing '} ' tail for DEL-OBJ extended info\n");
      free(extinfo);
      return -1;
   }

   *parseloc += 2;

   // attach delobj_info
   op->extendedinfo = extinfo;

   return 0;
}

static int parse_del_ref(char **parseloc, opinfo *op) {
    LOG(LOG_INFO, "Parsing a MARFS_DELETE_REF operation\n");

    op->type = MARFS_DELETE_REF_OP;

    *parseloc += 8;

    // allocate delref_info
    delref_info* extinfo = calloc(1, sizeof(*extinfo));

    // parse in delref_info
    if (strncmp(*parseloc, "{ ", 2)) {
        LOG(LOG_ERR, "Missing '{ ' header for DEL-REF extended info\n");
        free(extinfo);
        return -1;
    }

    *parseloc += 2;

    char* endptr = NULL;
    unsigned long long parseval = strtoull(*parseloc, &endptr, 10);
    if (endptr == NULL || *endptr != ' ') {
        LOG(LOG_ERR, "DEL-REF extended info has unexpected char in prev_active_index string: '%c'\n", *endptr);
        free(extinfo);
        return -1;
    }

    extinfo->prev_active_index = (size_t)parseval;

    *parseloc = endptr + 1;

    if (strncmp(*parseloc, "DZ ", 3) == 0) {
        extinfo->delzero = 1;
    }
    else if (strncmp(*parseloc, "-- ", 3) == 0) {
        extinfo->delzero = 0;
    }
    else {
        LOG(LOG_ERR, "Encountered unrecognized DEL-ZERO value in DEL-REF extended info\n");
        free(extinfo);
        return -1;
    }

    *parseloc += 3;

    if (strncmp(*parseloc, "EOS", 3) == 0) {
        extinfo->eos = 1;
    }
    else if (strncmp(*parseloc, "CNT", 3) == 0) {
        extinfo->eos = 0;
    }
    else {
        LOG(LOG_ERR, "Encountered unrecognized EOS value in DEL-REF extended info\n");
        free(extinfo);
        return -1;
    }

    *parseloc += 3;

    if (strncmp(*parseloc, " } ", 3)) {
        LOG(LOG_ERR, "Missing ' } ' tail for DEL-REF extended info\n");
        free(extinfo);
        return -1;
    }

    *parseloc += 3;

    // attach delref_info
    op->extendedinfo = extinfo;

    return 0;
}

static int parse_rebuild(char **parseloc, opinfo *op) {
    LOG(LOG_INFO, "Parsing a MARFS_REBUILD operation\n");

    op->type = MARFS_REBUILD_OP;

    *parseloc += 8;

    // parse in rebuild_info
    rebuild_info* extinfo = NULL;
    if (strncmp(*parseloc, "{ ", 2) == 0) {
       // allocate rebuild_info
       extinfo = calloc(1, sizeof(*extinfo));

       *parseloc += 2;

       char* endptr = *parseloc;
       while (*endptr != ' ' && *endptr != '\n') { endptr++; }
       if (*endptr != ' ') {
          LOG(LOG_ERR, "Failed to parse markerpath from REBUILD extended info\n");
          free(extinfo);
          return -1;
       }

       *endptr = '\0'; // temporarily truncate string

       extinfo->markerpath = strdup(*parseloc);

       if (extinfo->markerpath == NULL) {
          LOG(LOG_ERR, "Failed to duplicate markerpath from REBUILD extended info\n");
          free(extinfo);
          return -1;
       }

       *endptr = ' ';

       *parseloc = endptr + 1;

       if (**parseloc != '}' && **parseloc != '\0') {
          // possibly parse rtag value
          endptr = *parseloc;
          while (*endptr != '\0' && *endptr != '\n' && *endptr != ' ') {
              endptr++;
          }

          if (*endptr != ' ') {
             LOG(LOG_ERR, "Failed to identify end of rtag marker in REBUILD extended info string\n");
             free(extinfo->markerpath);
             free(extinfo);
             return -1;
          }

          extinfo->rtag = calloc(1, sizeof(RTAG));

          *endptr = '\0'; // truncate string to make rtag parsing easier

          if (rtag_initstr(extinfo->rtag, *parseloc)) {
             LOG(LOG_ERR, "Failed to parse rtag value of REBUILD extended info: \"%s\"\n", *parseloc);
             free(extinfo->rtag);
             free(extinfo->markerpath);
             free(extinfo);
             return -1;
          }

          *endptr = ' ';
          *parseloc = endptr + 1;
       }

       if (strncmp(*parseloc, "} ", 2)) {
          LOG(LOG_ERR, "Missing '} ' tail for REBUILD extended info\n");
          if (extinfo->rtag) {
             rtag_free(extinfo->rtag);
             free(extinfo->rtag);
          }

          free(extinfo->markerpath);
          free(extinfo);
          return -1;
       }

       *parseloc += 2;
    }
    else {
        LOG(LOG_INFO, "Rebuild op is lacking extended info\n");
    }

    // attach rebuild_info
    op->extendedinfo = extinfo;

    return 0;
}

static int parse_repack(char **parseloc, opinfo *op) {
    LOG(LOG_INFO, "Parsing a MARFS_REPACK operation\n");

    op->type = MARFS_REPACK_OP;

    *parseloc += 7;

    // allocate repack_info
    repack_info* extinfo = calloc(1, sizeof(*extinfo));

    // parse in repack_info
    if (strncmp(*parseloc, "{ ", 2)) {
        LOG(LOG_ERR, "Missing '{ ' header for REPACK extended info\n");
        free(extinfo);
        return -1;
    }

    *parseloc += 2;

    char* endptr = NULL;
    unsigned long long parseval = strtoull(*parseloc, &endptr, 10);
    if (endptr == NULL || *endptr != ' ') {
        LOG(LOG_ERR, "REPACK extended info has unexpected char in totalbytes string: '%c'\n", *endptr);
        free(extinfo);
        return -1;
    }

    extinfo->totalbytes = (size_t)parseval;

    *parseloc = endptr;

    if (strncmp(*parseloc, " } ", 3)) {
        LOG(LOG_ERR, "Missing ' } ' tail for REPACK extended info\n");
        free(extinfo);
        return -1;
    }

    *parseloc += 3;

    // attach repack_info
    op->extendedinfo = extinfo;

    return 0;
}

/**
 * Parse a new operation from the given logfile
 * @param int logfile : Reference to the logfile to parse a line from
 * @param char* eof : Reference to a character to be populated with an exit flag value
 *                    1 if we hit EOF on the file on a line division
 *                    -1 if we hit EOF in the middle of a line
 *                    zero otherwise
 * @param int* nextval: whether or not there is another value after this one
 * @return opinfo* : Reference to a new set of operation info structs (caller must free)
 * NOTE -- Under most failure conditions, the logfile offset will be returned to its original value.
 *         This is not the case if parsing reaches EOF, in which case, offset will be left there.
 */
opinfo* parselogline_one(int logfile, char* eof, int* nextval, int* err) {
   char buffer[MAX_BUFFER] = {0};
   char* tgtchar = buffer;
   off_t origoff = lseek(logfile, 0, SEEK_CUR);
   if (origoff < 0) {
      LOG(LOG_ERR, "Failed to identify current logfile offset\n");
      return NULL;
   }
   // read in an entire line
   // NOTE -- Reading one char at a time isn't very efficient, but we don't expect parsing of
   //         logfiles to be a significant performance factor.  This approach greatly simplifies
   //         char buffer mgmt.
   ssize_t readbytes;
   while ((readbytes = read(logfile, tgtchar, 1)) == 1) {
      // check for end of line
      if (*tgtchar == '\n') {
          break;
      }

      // check for excessive string length
      if (tgtchar - buffer >= MAX_BUFFER - 1) {
         LOG(LOG_ERR, "Parsed line exceeds memory limits\n");
         lseek(logfile, origoff, SEEK_SET);
         *eof = 0;
         return NULL;
      }

      tgtchar++;
   }

   if (*tgtchar != '\n') {
      if (readbytes == 0) {
         if (tgtchar == buffer) {
            LOG(LOG_INFO, "Hit EOF on logfile\n");
            *eof = 1;
         }
         else {
            LOG(LOG_ERR, "Hit mid-line EOF on logfile\n");
            *eof = -1;
         }

         return NULL;
      }

      LOG(LOG_ERR, "Encountered error while reading from logfile\n");
      lseek(logfile, origoff, SEEK_SET);
      *eof = 0;
      return NULL;
   }

   *eof = 0; // preemptively populate with zero

   // allocate our operation node
   opinfo* op = malloc(sizeof(*op));
   op->extendedinfo = NULL;
   op->start = 0;
   op->count = 0;
   op->errval = 0;
   op->next = NULL;
   op->ftag.ctag = NULL;
   op->ftag.streamid = NULL;

   int rc = 0;

   // parse the op type and extended info
   char* parseloc = buffer;
   if (strncmp(buffer, "DEL-OBJ ", 8) == 0) {
      rc = parse_del_obj(&parseloc, op);
   }
   else if (strncmp(buffer, "DEL-REF ", 8) == 0) {
      rc = parse_del_ref(&parseloc, op);
   }
   else if (strncmp(buffer, "REBUILD ", 8) == 0) {
      rc = parse_rebuild(&parseloc, op);
   }
   else if (strncmp(buffer, "REPACK ", 7) == 0) {
      rc = parse_repack(&parseloc, op);
   }
   else {
      LOG(LOG_ERR, "Unrecognized operation type value: \"%s\"\n", buffer);
      rc = -1;
   }

   if (rc != 0) {
      goto error;
   }

   // parse the start value
   if (parseloc[0] == 'S') {
      op->start = 1;
   }
   else if (parseloc[0] != 'E') {
      LOG(LOG_ERR, "Unexpected START string value: '%c'\n", parseloc[0]);
      goto error;
   }

   if (parseloc[1] != ' ') {
      LOG(LOG_ERR, "Unexpected trailing character after START value: '%c'\n", parseloc[1]);
      goto error;
   }

   parseloc += 2;

   // parse the count value
   char* endptr = NULL;
   unsigned long long parseval = strtoull(parseloc, &endptr, 10);
   if (endptr == NULL || *endptr != ' ') {
      LOG(LOG_ERR, "Failed to parse COUNT value with unexpected char: '%c'\n", *endptr);
      goto error;
   }

   op->count = (size_t)parseval;

   parseloc = endptr + 1;

   // parse the errno value
   long sparseval = strtol(parseloc, &endptr, 10);
   if (endptr == NULL || *endptr != ' ') {
      LOG(LOG_ERR, "Failed to parse ERRNO value with unexpected char: '%c'\n", *endptr);
      goto error;
   }

   op->errval = (int)sparseval;

   parseloc = endptr + 1;

   // parse the NEXT value
   *nextval = 0;
   if (tgtchar[-1] == '-') {
      if (tgtchar[-2] != ' ') {
         LOG(LOG_ERR, "Unexpected char preceeds NEXT flag: '%c'\n", tgtchar[-2]);
         goto error;
      }

      *nextval = 1; // note that we need to append another op
      tgtchar -= 2; // pull this back, so we'll trim off the NEXT value
   }

   // parse the FTAG value
   *tgtchar = '\0'; // trim the string, to make FTAG parsing easy
   if (ftag_initstr(&op->ftag, parseloc)) {
      LOG(LOG_ERR, "Failed to parse FTAG value of log line\n");
      goto error;
   }

   return op;

  error:
   *err = 1;
   return NULL;
}

opinfo* parselogline(int logfile, char* eof) {
   off_t origoff = lseek(logfile, 0, SEEK_CUR);
   if (origoff < 0) {
      LOG(LOG_ERR, "Failed to identify current logfile offset\n");
      return NULL;
   }

   *eof = 0; // preemptively populate with zero

   int nextval = 1;
   int err = 0;

   opinfo head;
   opinfo *curr = &head;
   while (curr && nextval && !err) {
      curr->next = parselogline_one(logfile, eof, &nextval, &err);
      curr = curr->next;
   }

   if (err) {
      LOG(LOG_ERR, "Failed to parse linked operation\n");
      resourcelog_freeopinfo(head.next);
      if (*eof == 0) {
         lseek(logfile, origoff, SEEK_SET);
      }
      return NULL;
   }

   return head.next;
}

static int print_del_obj(char* buffer, const size_t buffer_size, opinfo* op, size_t *usedbuff) {
   *usedbuff += snprintf(buffer, buffer_size, "%s ", "DEL-OBJ");

   if (op->extendedinfo) {
       delobj_info* delobj = (delobj_info*)op->extendedinfo;

       *usedbuff += snprintf(buffer + *usedbuff, buffer_size - *usedbuff, "{ %zu } ",
                                       delobj->offset);
   }

   return 0;
}

static int print_del_ref(char *buffer, const size_t buffer_size, opinfo *op, size_t *usedbuff) {
   *usedbuff += snprintf(buffer, buffer_size, "%s ", "DEL-REF");

   if (op->extendedinfo) {
      delref_info* delref = (delref_info*)op->extendedinfo;
      *usedbuff += snprintf(buffer + *usedbuff, buffer_size - *usedbuff, "{ %zu %s %s } ",
                            delref->prev_active_index,
                            (delref->delzero == 0) ? "--" : "DZ",
                            (delref->eos == 0) ? "CNT" : "EOS");
   }

   return 0;
}

static int print_rebuild(char *buffer, const size_t buffer_size, opinfo *op, size_t *usedbuff) {
   *usedbuff += snprintf(buffer, buffer_size, "%s ", "REBUILD");

   if (op->extendedinfo) {
      rebuild_info* rebuild = (rebuild_info*)op->extendedinfo;
      *usedbuff += snprintf(buffer + *usedbuff, buffer_size - *usedbuff, "{ %s",
                            rebuild->markerpath);

      if (*usedbuff >= buffer_size) {
         LOG(LOG_ERR, "REBUILD Operation string exceeds memory allocation limits\n");
         return -1;
      }

      if (rebuild->rtag) {
         // possibly print out rtag values, starting with a single leading space char
         *usedbuff += snprintf(buffer + *usedbuff, buffer_size - *usedbuff, " ");

         if (*usedbuff >= buffer_size) {
            LOG(LOG_ERR, "REBUILD Operation string exceeds memory allocation limits\n");
            return -1;
         }

         *usedbuff += rtag_tostr(rebuild->rtag, buffer + *usedbuff, buffer_size - *usedbuff);

         if (*usedbuff >= buffer_size) {
            LOG(LOG_ERR, "REBUILD Operation string exceeds memory allocation limits\n");
            return -1;
         }
      }

      *usedbuff += snprintf(buffer + *usedbuff, buffer_size - *usedbuff, " } ");
   }

   return 0;
}

static int print_repack(char *buffer, const size_t buffer_size, opinfo *op, size_t *usedbuff) {
   *usedbuff += snprintf(buffer, buffer_size, "%s ", "REPACK");

   if (op->extendedinfo) {
      repack_info* repack = (repack_info*)op->extendedinfo;
      *usedbuff += snprintf(buffer + *usedbuff, buffer_size - *usedbuff, "{ %zu } ",
                            repack->totalbytes);
   }

   return 0;
}

/**
 * Print the specified operation info to the specified logfile
 * @param int logfile : File descriptor for the target logfile
 * @param opinfo* op : Reference to the operation to be printed
 * @return int : Zero on success, or -1 on failure
 */
static int printlogline_one(int logfile, opinfo* op) {
   char buffer[MAX_BUFFER];
   size_t usedbuff = 0;
   off_t origoff = lseek(logfile, 0, SEEK_CUR);
   if (origoff < 0) {
      LOG(LOG_ERR, "Failed to identify current logfile offset\n");
      return -1;
   }

   // populate the type string of the operation
   int rc = 0;
   switch (op->type) {
      case MARFS_DELETE_OBJ_OP:
         rc = print_del_obj(buffer, sizeof(buffer), op, &usedbuff);
         break;
      case MARFS_DELETE_REF_OP:
         rc = print_del_ref(buffer, sizeof(buffer), op, &usedbuff);
         break;
      case MARFS_REBUILD_OP:
         rc = print_rebuild(buffer, sizeof(buffer), op, &usedbuff);
         break;
      case MARFS_REPACK_OP:
         rc = print_repack(buffer, sizeof(buffer), op, &usedbuff);
         break;
      default:
         LOG(LOG_ERR, "Unrecognized TYPE value of operation\n");
         rc = -1;
         break;
   }

   if (rc != 0) {
       // error would have been logged by the function
       return -1;
   }

   if (usedbuff >= MAX_BUFFER) {
      LOG(LOG_ERR, "Operation string exceeds memory allocation limits\n");
      return -1;
   }

   // populate start flag
   usedbuff += snprintf(buffer + usedbuff, MAX_BUFFER - usedbuff, "%c ", op->start?'S':'E');

   if (usedbuff >= MAX_BUFFER) {
      LOG(LOG_ERR, "Operation string exceeds memory allocation limits\n");
      return -1;
   }

   // populate the count string
   usedbuff += snprintf(buffer + usedbuff, MAX_BUFFER - usedbuff, "%zu ", op->count);

   if (usedbuff >= MAX_BUFFER) {
      LOG(LOG_ERR, "Operation string exceeds memory allocation limits\n");
      return -1;
   }

   // populate the errval string
   usedbuff += snprintf(buffer + usedbuff, MAX_BUFFER - usedbuff, "%d ", op->errval);

   if (usedbuff >= MAX_BUFFER) {
      LOG(LOG_ERR, "Operation string exceeds memory allocation limits\n");
      return -1;
   }

   // populate the FTAG string
   usedbuff += ftag_tostr(&op->ftag, buffer + usedbuff, MAX_BUFFER - usedbuff);

   if (usedbuff >= MAX_BUFFER) {
      LOG(LOG_ERR, "Operation string exceeds memory allocation limits\n");
      return -1;
   }

   // populate the NEXT flag
   if (op->next) {
      usedbuff += snprintf(buffer + usedbuff, MAX_BUFFER - usedbuff, " -");

      if (usedbuff >= MAX_BUFFER) {
         LOG(LOG_ERR, "Operation string exceeds memory allocation limits\n");
         return -1;
      }
   }

   // populate EOL
   buffer[usedbuff] = '\n';
   usedbuff++;

   if (usedbuff >= MAX_BUFFER) {
      LOG(LOG_ERR, "Operation string exceeds memory allocation limits\n");
      return -1;
   }

   buffer[usedbuff] = '\0'; // NULL-terminate, just in case

   // finally, output the full op line
   if (write(logfile, buffer, usedbuff) != (ssize_t) usedbuff) {
      LOG(LOG_ERR, "Failed to write operation string of length %zd to logfile\n", usedbuff);
      return -1;
   }

   return 0;
}

/**
 * Print the specified operation info (or chain of them) to the specified logfile
 * @param int logfile : File descriptor for the target logfile
 * @param opinfo* op : Reference to the operation to be printed
 * @return int : Zero on success, or -1 on failure
 */
int printlogline(int logfile, opinfo* op) {
    int rc = 0;
    while (op != NULL && rc == 0) {
        rc = printlogline_one(logfile, op);
        op = op->next;
    }
    return rc;
}
