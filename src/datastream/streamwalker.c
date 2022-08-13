#ifndef __MARFS_COPYRIGHT_H__
#define __MARFS_COPYRIGHT_H__

/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#endif

#include "marfs_auto_config.h"
#include "datastream.c"

#include <stdio.h>
#include <string.h>
#include <errno.h>


#define PROGNAME "marfs-streamwalker"
#define OUTPREFX PROGNAME ": "


// Show all the usage options in one place, for easy reference
// An arrow appears next to the one you tried to use.
//
void usage(const char* op) {

   printf("Usage: <op> [<args> ...]\n");
   printf("  Where <op> and <args> are like one of the following:\n");
   printf("\n");

   const char* cmd = op;
   if (!strncmp(op, "help ", 5)) {
      // check if the 'help' command is targetting a specific op
      while (*cmd != '\0' && *cmd != ' ') {
         cmd++;
      }
      if (*cmd == ' ' && *(cmd + 1) != '\0') {
         cmd++;
      }
      else {
         cmd = op;
      } // no specific command specified
   }

#define USAGE(CMD, ARGS, DESC, FULLDESC) \
if ( cmd == op ) { \
   printf("  %2s %-11s %s\n%s", \
            (!strncmp(op, CMD, 11) ? "->" : ""), \
            (CMD), (ARGS), (!strncmp(op, "help", 5) ? "      " DESC "\n" : "") ); \
} \
else if ( !strncmp(cmd, CMD, 11) ) { \
   printf("     %-11s %s\n%s%s", \
            (CMD), (ARGS), "      " DESC "\n", FULLDESC ); \
}

   USAGE("open",
      "( -p userpath | -r refpath )",
      "Begin traversing a new datastream, starting at the given file",
      "       -p userpath  : Specifies a user path to retrieve stream info from\n"
      "                       OR the path of the target NS, if '-r' is used\n"
      "       -r refpath   : Specifies a reference path to retrieve stream info from\n")

      USAGE("shift",
         "( -@ offset | -n filenum )",
         "Move to a new file in the current datastream",
         "       -@ offset  : Specifies a number of files to move forward of backward\n"
         "       -n filenum : Specifies a specific file number to move to\n")

      USAGE("ftag",
         "",
         "Print out the FTAG info of the current file", "")

      USAGE("ref",
         "",
         "Identify the metadata reference path of the current file", "")

      USAGE("obj",
         "[-n chunknum]",
         "Print out the location of the specified object",
         "       -n chunknum : Specifies a specific data chunk\n")

      USAGE("bounds",
         "[-f]",
         "Identify the boundaries of the current stream",
         "       -f : Specifies to identify the bounds of just the current file\n")

      USAGE("ns",
         "[-p nspath]",
         "Print / change the current MarFS namespace target of this program",
         "       -p nspath : Specifies the path of a new NS target")

      USAGE("recovery",
         "[-@ offset -f seekfrom]",
         "Print the recovery information of the specified object",
         "       -@ offset   : Specifies a file offset to seek to\n"
         "       -f seekfrom : Specifies a start location for the seek\n"
         "                    ( either 'set', 'cur', or 'end' )\n")

      USAGE("( exit | quit )",
         "",
         "Terminate", "")

      USAGE("help", "[CMD]", "Print this usage info",
         "       CMD : A specific command, for which to print detailed info\n")

      printf("\n");

#undef USAGE
}


int populate_ftag(marfs_config* config, marfs_position* pathpos, marfs_position* destpos, FTAG* ftag, const char* path, const char* rpath, char prout) {
   char* modpath = NULL;
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   if ( config_duplicateposition( pathpos, &oppos ) ) {
      printf( OUTPREFX "ERROR: Failed to duplicate active position for config traversal\n" );
      return -1;
   }
   if (path != NULL) {
      // perform path traversal to identify marfs position
      modpath = strdup(path);
      if (modpath == NULL) {
         printf(OUTPREFX "ERROR: Failed to create duplicate \"%s\" path for config traversal\n", path);
         config_abandonposition( &oppos );
         return -1;
      }
      if (config_traverse(config, &oppos, &(modpath), 1) < 0) {
         printf(OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
            path);
         free(modpath);
         config_abandonposition( &oppos );
         return -1;
      }
   }

   // attempt to open the target file
   MDAL mdal = oppos.ns->prepo->metascheme.mdal;
   MDAL_FHANDLE handle = NULL;
   if (rpath == NULL) {
      // only actually reference the user path if no reference path was provided
      handle = mdal->open(oppos.ctxt, modpath, O_RDONLY);
   }
   else {
      handle = mdal->openref(oppos.ctxt, rpath, O_RDONLY, 0);
   }
   if (modpath) {
      free(modpath);
   }
   if (handle == NULL) {
      printf(OUTPREFX "ERROR: Failed to open target %s file: \"%s\" (%s)\n",
         (rpath) ? "ref" : "user", (rpath) ? rpath : path, strerror(errno));
      config_abandonposition( &oppos );
      return -1;
   }

   // retrieve the FTAG value from the target file
   char* ftagstr = NULL;
   size_t getres = mdal->fgetxattr(handle, 1, FTAG_NAME, ftagstr, 0);
   if (getres <= 0) {
      printf(OUTPREFX "ERROR: Failed to retrieve FTAG value from target %s file: \"%s\" (%s)\n", (rpath) ? "ref" : "user", (rpath) ? rpath : path, strerror(errno));
      mdal->close(handle);
      config_abandonposition( &oppos );
      return -1;
   }
   ftagstr = calloc(1, getres + 1);
   if (ftagstr == NULL) {
      printf(OUTPREFX "ERROR: Failed to allocate space for an FTAG string value\n");
      mdal->close(handle);
      config_abandonposition( &oppos );
      return -1;
   }
   if (mdal->fgetxattr(handle, 1, FTAG_NAME, ftagstr, getres) != getres) {
      printf(OUTPREFX "ERROR: FTAG value changed while we were reading it\n");
      free(ftagstr);
      mdal->close(handle);
      config_abandonposition( &oppos );
      return -1;
   }
   if (mdal->close(handle)) {
      printf(OUTPREFX "WARNING: Failed to close handle for target file (%s)\n", strerror(errno));
   }
   // cleanup the previous FTAG, if necessary
   if (ftag->ctag) {
      free(ftag->ctag); ftag->ctag = NULL;
   }
   if (ftag->streamid) {
      free(ftag->streamid); ftag->streamid = NULL;
   }
   // populate FTAG values based on xattr content
   if (ftag_initstr(ftag, ftagstr)) {
      printf(OUTPREFX "ERROR: Failed to parse FTAG string: \"%s\" (%s)\n",
         ftagstr, strerror(errno));
      free(ftagstr);
      config_abandonposition( &oppos );
      return -1;
   }
   else {
      // actually update the passed position
      if ( destpos->ns ) { config_abandonposition( destpos ); }
      // do a semi-sketchy direct copy of postion values
      *destpos = oppos; // no need to abandon oppos now
      if (prout) {
         printf(OUTPREFX "Successfully populated FTAG values for target %s file: \"%s\"\n",
            (rpath) ? "ref" : "user", (rpath) ? rpath : path);
      }
   }
   free(ftagstr);
   return 0;
}


int open_command(marfs_config* config, marfs_position* pathpos, marfs_position* tgtpos, FTAG* ftag, char* args) {
   printf("\n");
   // parse args
   char curarg = '\0';
   char* userpath = NULL;
   char* refpath = NULL;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-p") == 0) {
            curarg = 'p';
         }
         else if (strcmp(parse, "-r") == 0) {
            curarg = 'r';
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'open' command: '%s'\n", parse);
            if (userpath) {
               free(userpath);
            }
            if (refpath) {
               free(refpath);
            }
            return -1;
         }
      }
      else {
         char** tgtstr = NULL;
         if (curarg == 'p') {
            if (userpath != NULL) {
               printf(OUTPREFX "ERROR: Detected duplicate '-p' arg: \"%s\"\n", parse);
               free(userpath);
               if (refpath) {
                  free(refpath);
               }
               return -1;
            }
            tgtstr = &(userpath);
         }
         else { // == r
            if (refpath != NULL) {
               printf(OUTPREFX "ERROR: Detected duplicate '-r' arg: \"%s\"\n", parse);
               free(refpath);
               if (userpath) {
                  free(userpath);
               }
               return -1;
            }
            tgtstr = &(refpath);
         }
         *tgtstr = strdup(parse);
         if (*tgtstr == NULL) {
            printf(OUTPREFX "ERROR: Failed to duplicate string argument: \"%s\"\n",
               parse);
            if (userpath) {
               free(userpath);
            }
            if (refpath) {
               free(refpath);
            }
            return -1;
         }
         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }
   // check that we have at least one arg
   if (!(userpath) && !(refpath)) {
      printf(OUTPREFX "ERROR: 'open' command requires at least one '-p' or '-r' arg\n");
      return -1;
   }

   // populate our FTAG and cleanup strings
   int retval = populate_ftag(config, pathpos, tgtpos, ftag, userpath, refpath, 1);
   printf("\n");
   if (userpath) {
      free(userpath);
   }
   if (refpath) {
      free(refpath);
   }
   return retval;
}

int shift_command(marfs_config* config, marfs_position* pos, FTAG* ftag, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (ftag->streamid == NULL) {
      printf(OUTPREFX "ERROR: No FTAG target to shift from\n");
      return -1;
   }

   // parse args
   char curarg = '\0';
   ssize_t offset = 0;
   ssize_t filenum = -1;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-@") == 0) {
            curarg = '@';
         }
         else if (strcmp(parse, "-n") == 0) {
            curarg = 'n';
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'shift' command: '%s'\n", parse);
            return -1;
         }
      }
      else {
         // parse the numeric arg
         char* endptr = NULL;
         long long parseval = strtoll(parse, &(endptr), 0);
         if (*endptr != '\0') {
            printf(OUTPREFX "ERROR: Expected pure numeric argument for '-%c': \"%s\"\n",
               curarg, parse);
            return -1;
         }

         if (curarg == '@') {
            if (offset != 0) {
               printf(OUTPREFX "ERROR: Detected duplicate '-@' arg: \"%s\"\n", parse);
               return -1;
            }
            if (parseval == 0) {
               printf(OUTPREFX "WARNING: Offset value of zero is a no-op\n");
               return -1;
            }
            if (parseval < 0 && llabs(parseval) > ftag->fileno) {
               printf(OUTPREFX "ERROR: Offset value extends beyond beginning of stream\n");
               return -1;
            }
            offset = parseval;
         }
         else { // == n
            if (filenum != -1) {
               printf(OUTPREFX "ERROR: Detected duplicate '-n' arg: \"%s\"\n", parse);
               return -1;
            }
            if (parseval < 0) {
               printf(OUTPREFX "ERROR: Negative filenum value: \"%lld\"\n", parseval);
               return -1;
            }
            filenum = parseval;
         }
         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }
   // check that we have at least one arg
   if (offset == 0 && filenum == -1) {
      printf(OUTPREFX "ERROR: 'shift' command requires either '-@' or '-n' arg\n");
      return -1;
   }

   // process our arg
   size_t origfileno = ftag->fileno;
   if (offset) {
      if (filenum != -1) {
         printf(OUTPREFX "ERROR: 'shift' command cannot support both '-@' and '-n' args\n");
         return -1;
      }
      ftag->fileno += offset;
   }
   else {
      ftag->fileno = filenum;
   }

   // generate a ref path for the new target file
   char* newrpath = datastream_genrpath(ftag, &(pos->ns->prepo->metascheme));
   if (newrpath == NULL) {
      printf(OUTPREFX "ERROR: Failed to identify new ref path\n");
      ftag->fileno = origfileno;
      return -1;
   }

   int retval = populate_ftag(config, pos, pos, ftag, NULL, newrpath, 1);
   printf("\n");
   if (retval) {
      ftag->fileno = origfileno;
   }
   free(newrpath);

   return retval;
}

int ftag_command(marfs_config* config, marfs_position* pos, FTAG* ftag, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (ftag->streamid == NULL) {
      printf(OUTPREFX "ERROR: No current FTAG target\n");
      return -1;
   }
   const char* datastatestr = "INIT";
   if ((ftag->state & FTAG_DATASTATE) == FTAG_SIZED) {
      datastatestr = "SIZED";
   }
   else if ((ftag->state & FTAG_DATASTATE) == FTAG_FIN) {
      datastatestr = "FINALIZED";
   }
   else if ((ftag->state & FTAG_DATASTATE) == FTAG_COMP) {
      datastatestr = "COMPLETE";
   }
   const char* dataaccessstr = "NO-ACCESS";
   if ((ftag->state & FTAG_WRITEABLE)) {
      if ((ftag->state & FTAG_READABLE)) {
         dataaccessstr = "READ-WRITE";
      }
      else {
         dataaccessstr = "WRITE-ONLY";
      }
   }
   else if ((ftag->state & FTAG_READABLE)) {
      dataaccessstr = "READ-ONLY";
   }
   // print out all FTAG values
   printf("Stream Info --\n");
   printf(" Client Tag : %s\n", ftag->ctag);
   printf(" Stream ID  : %s\n", ftag->streamid);
   printf(" Max Files  : %zu\n", ftag->objfiles);
   printf(" Max Size   : %zu\n", ftag->objsize);
   printf("File Position --\n");
   printf(" File Number   : %zu\n", ftag->fileno);
   printf(" Object Number : %zu\n", ftag->objno);
   printf(" Object Offset : %zu\n", ftag->offset);
   printf(" End of Stream : %d\n", ftag->endofstream);
   printf("Data Structure --\n");
   printf(" Bytes       : %zu\n", ftag->bytes);
   printf(" Avail Bytes : %zu\n", ftag->availbytes);
   printf(" Recov Bytes : %zu\n", ftag->recoverybytes);
   printf(" Data State  : %s\n", datastatestr);
   printf(" Data Access : %s\n", dataaccessstr);
   printf(" Protection --\n");
   printf("  N   : %d\n", ftag->protection.N);
   printf("  E   : %d\n", ftag->protection.E);
   printf("  O   : %d\n", ftag->protection.O);
   printf("  psz : %ld\n", ftag->protection.partsz);
   printf("\n");
   return 0;
}

int ref_command(marfs_config* config, marfs_position* pos, FTAG* ftag, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (ftag->streamid == NULL) {
      printf(OUTPREFX "ERROR: No FTAG target to shift from\n");
      return -1;
   }

   // generate a ref path for the new target file
   char* curpath = datastream_genrpath(ftag, &(pos->ns->prepo->metascheme));
   if (curpath == NULL) {
      printf(OUTPREFX "ERROR: Failed to identify current ref path\n");
      return -1;
   }

   // print out the generated path
   printf("Metadata Ref Path : %s\n", curpath);
   printf("Sanitized Path    :  ");
   char* parsepath = curpath;
   while (*parsepath != '\0') {
      if (*parsepath == '*' || *parsepath == '|' || *parsepath == '&') {
         // escape all problem chars
         printf("\\");
      }
      printf("%c", *parsepath);
      parsepath++;
   }
   printf("\n\n");

   free(curpath);

   return 0;
}

int obj_command(marfs_config* config, char* config_path, marfs_position* pos, FTAG* ftag, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (ftag->streamid == NULL) {
      printf(OUTPREFX "ERROR: No current FTAG target\n");
      return -1;
   }

   // parse args
   char curarg = '\0';
   ssize_t chunknum = -1;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-n") == 0) {
            curarg = 'n';
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'obj' command: '%s'\n", parse);
            return -1;
         }
      }
      else { // == 'n'
         // parse the numeric arg
         char* endptr = NULL;
         long long parseval = strtoll(parse, &(endptr), 0);
         if (*endptr != '\0') {
            printf(OUTPREFX "ERROR: Expected pure numeric argument for '-%c': \"%s\"\n",
               curarg, parse);
            return -1;
         }

         if (chunknum != -1) {
            printf(OUTPREFX "ERROR: Detected duplicate '-n' arg: \"%s\"\n", parse);
            return -1;
         }
         if (parseval < 0) {
            printf(OUTPREFX "ERROR: Negative chunknum value: \"%lld\"\n", parseval);
            return -1;
         }
         chunknum = parseval;
         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }


   // identify object bounds of the current file
   RECOVERY_HEADER header = {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = ftag->ctag,
      .streamid = ftag->streamid
   };
   size_t headerlen = recovery_headertostr(&(header), NULL, 0);
   if (headerlen < 1) {
      printf(OUTPREFX "ERROR: Failed to identify recovery header length for file\n");
      headerlen = 0;
   }
   size_t dataperobj = ftag->objsize - (headerlen + ftag->recoverybytes);
   ssize_t fileobjbounds = (ftag->bytes + ftag->offset - headerlen) / dataperobj;
   // special case check
   if ((ftag->state & FTAG_DATASTATE) >= FTAG_FIN && fileobjbounds &&
      (ftag->bytes + ftag->offset - headerlen) % dataperobj == 0) {
      // if we exactly align to object bounds AND the file is FINALIZED,
      //   we've overestimated by one object
      fileobjbounds--;
   }

   if (chunknum > fileobjbounds) {
      printf(OUTPREFX "WARNING: Specified object number exceeds file limits ( selecting object %zd instead )\n", fileobjbounds);
      chunknum = fileobjbounds;
   }

   // iterate over appropriate objects
   size_t curobj = 0;
   if (chunknum >= 0) {
      curobj = chunknum;
   }
   else {
      chunknum = fileobjbounds;
   }
   for (; curobj <= chunknum; curobj++) {
      // identify the object target
      char* objname = NULL;
      ne_erasure erasure;
      ne_location location;
      FTAG curtag = *ftag;
      curtag.objno += curobj;
      if (datastream_objtarget(&(curtag), &(pos->ns->prepo->datascheme), &(objname), &(erasure), &(location))) {
         printf(OUTPREFX "ERROR: Failed to identify data info for chunk %zu\n", curobj);
         continue;
      }
      // print object info
      printf("Obj#%-5zu\n   Pod: %d\n   Cap: %d\n   Scatter: %d\n   ObjName: %s\n   Erasure Information: N %d, E %d, O %d, partsz %lu\n   neutil Args: -c \"%s:/marfs_config/repo name=%s/data/DAL\" -P %d -C %d -S %d -O \"", curobj, location.pod, location.cap, location.scatter, objname, erasure.N, erasure.E, erasure.O, erasure.partsz, config_path, pos->ns->prepo->name, location.pod, location.cap, location.scatter);
      // print sanitized object name
      char* parsepath = objname;
      while (*parsepath != '\0') {
         if (*parsepath == '*' || *parsepath == '|' || *parsepath == '&') {
            // escape all problem chars
            //printf("\\");
         }
         printf("%c", *parsepath);
         parsepath++;
      }
      printf("\"\n");
      free(objname);
   }
   printf("\n");

   return 0;
}

int bounds_command(marfs_config* config, marfs_position* pos, FTAG* ftag, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (ftag->streamid == NULL) {
      printf(OUTPREFX "ERROR: No current FTAG target\n");
      return -1;
   }

   // parse args
   int f_flag = 0;
   char* parse = strtok(args, " ");
   while (parse) {
      if (strcmp(parse, "-f") == 0) {
         f_flag = 1;
      }
      else {
         printf(OUTPREFX "ERROR: Unrecognized argument for 'bound' command: '%s'\n", parse);
         return -1;
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }

   int retval = 0;
   size_t origfileno = ftag->fileno;
   if (!f_flag) {
      // iterate over files until we find EOS
      while (ftag->endofstream == 0 && retval == 0 &&
         (ftag->state & FTAG_DATASTATE) >= FTAG_FIN) {
         // progress to the next file
         ftag->fileno++;
         // generate a ref path for the new target file
         char* newrpath = datastream_genrpath(ftag, &(pos->ns->prepo->metascheme));
         if (newrpath == NULL) {
            printf(OUTPREFX "ERROR: Failed to identify new ref path\n");
            ftag->fileno = origfileno;
            return -1;
         }
         // retrieve the FTAG of the new target
         retval = populate_ftag(config, pos, pos, ftag, NULL, newrpath, 0);
         if (retval) {
            ftag->fileno--;
         } // if we couldn't retrieve this, go to previous
         free(newrpath);
      }
   }

   // identify object bounds of final file
   RECOVERY_HEADER header = {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = ftag->ctag,
      .streamid = ftag->streamid
   };
   size_t headerlen = recovery_headertostr(&(header), NULL, 0);
   if (headerlen < 1) {
      printf(OUTPREFX "ERROR: Failed to identify recovery header length for final file\n");
      headerlen = 0;
   }
   size_t dataperobj = ftag->objsize - (headerlen + ftag->recoverybytes);
   size_t finobjbounds = (ftag->bytes + ftag->offset - headerlen) / dataperobj;
   size_t finobjoff = ((ftag->bytes + ftag->offset - headerlen) % dataperobj) + headerlen;
   // special case check
   if ((ftag->state & FTAG_DATASTATE) >= FTAG_FIN && finobjbounds &&
      (ftag->bytes + ftag->offset - headerlen) % dataperobj == 0) {
      // if we exactly align to object bounds AND the file is FINALIZED,
      //   we've overestimated by one object
      finobjbounds--;
   }


   // print out stream boundaries
   if (!f_flag) {
      char* eosreason = "End of Stream";
      if (retval) {
         eosreason = "Failed to Identify Subsequent File";
      }
      else if ((ftag->state & FTAG_DATASTATE) < FTAG_FIN) {
         eosreason = "Unfinished File";
      }
      printf("File Bounds:\n   0 -- Initial File\n     to\n   %zu -- %s\n",
         ftag->fileno, eosreason);
      printf("Object Bounds:\n   0 -- Initial Object\n     to\n   %zu -- End of Final File\n",
         ftag->objno + finobjbounds);
      // restore the original value
      ftag->fileno = origfileno;
      char* newrpath = datastream_genrpath(ftag, &(pos->ns->prepo->metascheme));
      if (newrpath == NULL) {
         printf(OUTPREFX "ERROR: Failed to identify original ref path\n");
         return -1;
      }
      // retrieve the FTAG of the new target
      retval = populate_ftag(config, pos, pos, ftag, NULL, newrpath, 0);
      free(newrpath);
   }
   else {
      printf("File %zu Bounds:\n   Object %zu Offset %zu\n     to\n   Object %zu Offset %zu\n",
         ftag->fileno, ftag->objno, ftag->offset, ftag->objno + finobjbounds, finobjoff);
      printf("Each data object includes a %zu byte recovery header and a %zu byte recovery tail for this file.\n",
         headerlen, ftag->recoverybytes);
   }

   printf("\n");

   return 0;
}

int refresh_command(marfs_config* config, marfs_position* pos, FTAG* ftag, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (ftag->streamid == NULL) {
      printf(OUTPREFX "ERROR: No FTAG target to shift from\n");
      return -1;
   }

   // check for any args
   if (*args != '\0') {
      printf(OUTPREFX "ERROR: The 'refresh' command does not support arguments\n");
      return -1;
   }

   // generate a ref path for the current target file
   char* newrpath = datastream_genrpath(ftag, &(pos->ns->prepo->metascheme));
   if (newrpath == NULL) {
      printf(OUTPREFX "ERROR: Failed to identify current ref path\n");
      return -1;
   }

   int retval = populate_ftag(config, pos, pos, ftag, NULL, newrpath, 1);
   printf("\n");
   free(newrpath);

   return retval;
}

int recovery_command(marfs_config* config, marfs_position* pos, FTAG* ftag, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (ftag->streamid == NULL) {
      printf(OUTPREFX "ERROR: No current FTAG target\n");
      return -1;
   }

   // parse args
   char curarg = '\0';
   ssize_t offset = -1;
   int seekfrom = -1;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-@") == 0) {
            curarg = '@';
         }
         else if (strcmp(parse, "-f") == 0) {
            curarg = 'f';
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'recovery' command: '%s'\n", parse);
            return -1;
         }
      }
      else if (curarg == '@') {
         // parse the numeric arg
         char* endptr = NULL;
         long long parseval = strtoull(parse, &(endptr), 0);
         if (*endptr != '\0') {
            printf(OUTPREFX "ERROR: Expected pure numeric argument for '-%c': \"%s\"\n",
               curarg, parse);
            return -1;
         }

         if (offset != -1) {
            printf(OUTPREFX "ERROR: Detected duplicate '-@' arg: \"%s\"\n", parse);
            return -1;
         }

         if (parseval >= INT_MAX) {
            printf(OUTPREFX "ERROR: Negative offset value: \"%lld\"\n", parseval);
            return -1;
         }

         offset = parseval;
         curarg = '\0';
      }
      else { // == 'f'
         if (seekfrom != -1) {
            printf(OUTPREFX "ERROR: Detected duplicate '-f' arg: \"%s\"\n", parse);
            return -1;
         }

         if (strcasecmp(parse, "set") == 0) {
            seekfrom = SEEK_SET;
         }
         else if (strcasecmp(parse, "cur") == 0) {
            seekfrom = SEEK_CUR;
         }
         else if (strcasecmp(parse, "end") == 0) {
            seekfrom = SEEK_END;
         }
         else {
            printf(OUTPREFX "ERROR: '-f' argument is unrecognized: \"%s\"\n", parse);
            printf(OUTPREFX "ERROR: Acceptable values are 'set'/'cur'/'end'\n");
            return -1;
         }

         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }

   if ((offset < 0) != (seekfrom < 0)) {
      printf(OUTPREFX "ERROR: Only one of '-@' and '-f is defined\n");
      return -1;
   }

   char* rpath = datastream_genrpath(ftag, &(pos->ns->prepo->metascheme));
   if (rpath == NULL) {
      printf(OUTPREFX "ERROR: Failed to generate reference path\n");
      return -1;
   }

   DATASTREAM stream = genstream(READ_STREAM, rpath, 1, pos, 0, NULL, NULL);
   if (stream == NULL) {
      printf(OUTPREFX "ERROR: Failed to generate stream\n");
      return -1;
   }

   if (offset >= 0 && datastream_seek(&stream, offset, seekfrom) < 0) {
      printf(OUTPREFX "ERROR: Failed to seek\n");
      datastream_close(&stream);
      return -1;
   }

   // read recovery info
   RECOVERY_FINFO info;
   if (datastream_recoveryinfo(&stream, &(info))) {
      printf(OUTPREFX "ERROR: Failed to fetch recovery info\n");
      datastream_close(&stream);
      return -1;
   }

   datastream_close(&stream);

   // parse/print recovery info
   struct tm* time = localtime(&(info.mtime.tv_sec));
   char timestr[20];
   strftime(timestr, 20, "%F %T", time);
   printf("Recovery Info:\n    Inode: %zu\n    Mode: %o\n    Owner: %d\n    Group: %d\n    Size: %zu\n    Mtime: %s.%.9ld\n    EOF: %d\n    Path: %s\n",
      info.inode, info.mode, info.owner, info.group, info.size, timestr, info.mtime.tv_nsec, info.eof, info.path);

   return 0;

}

int ns_command(marfs_config* config, marfs_position* pos, char* args) {
   printf("\n");

   // parse args
   char curarg = '\0';
   char* path = NULL;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-p") == 0) {
            curarg = 'p';
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'ns' command: '%s'\n", parse);
            return -1;
         }
      }
      else {
         // duplicate the path arg
         path = strdup( parse );
         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }

   if (path != NULL) {
      // Need to traverse this path and update the position
      marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
      if ( config_duplicateposition( pos, &oppos ) ) {
         printf( OUTPREFX "ERROR: Failed to duplicate active position for config traversal\n" );
         return -1;
      }
      // perform path traversal to identify marfs position
      int depth;
      if ( (depth = config_traverse(config, &oppos, &(path), 1)) < 0) {
         printf(OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
            path);
         free(path);
         config_abandonposition( &oppos );
         return -1;
      }
      free(path);
      if ( depth ) {
         printf(OUTPREFX "WARNING: Path tgt is not a MarFS NS ( depth = %d )\n", depth );
      }
      else if ( oppos.ctxt == NULL  &&  config_fortifyposition( &oppos ) ) {
         printf(OUTPREFX "ERROR: Failed to fortify new NS position\n" );
         config_abandonposition( &oppos );
         return -1;
      }
      // update our pos arg
      config_abandonposition( pos );
      *pos = oppos; // just direct copy, and don't abandon this position
   }
   printf("\n%s Namespace Target : \"%s\"\n\n", (path == NULL) ? "Current" : "New", pos->ns->idstr);
   return 0;
}


int command_loop(marfs_config* config, char* config_path) {
   // initialize an FTAG struct
   FTAG ftag = {
      .ctag = NULL,
      .streamid = NULL
   };
   // initialize a marfs position
   marfs_position pos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if ( config_establishposition( &pos, config ) ) {
      printf(OUTPREFX "ERROR: Failed to establish a position for the MarFS root\n");
      return -1;
   }
   marfs_position tgtpos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   printf("Initial Namespace Target : \"%s\"\n", pos.ns->idstr);

   // infinite loop, processing user commands
   printf(OUTPREFX "Ready for user commands\n");
   int retval = 0;
   while (1) {
      printf("> ");
      fflush(stdout);
      // read in a new line from stdin ( 4096 char limit )
      char inputline[4097] = { 0 }; // init to NULL bytes
      if (scanf("%4096[^\n]", inputline) < 0) {
         printf(OUTPREFX "ERROR: Failed to read user input\n");
         retval = -1;
         break;
      }
      fgetc(stdin); // to clear newline char
      if (inputline[4095] != '\0') {
         printf(OUTPREFX "ERROR: Input command exceeds parsing limit of 4096 chars\n");
         retval = -1;
         break;
      }

      // parse the input command
      char* parse = inputline;
      char repchar = 0;
      while (*parse != '\0') {
         parse++;
         if (*parse == ' ') {
            *parse = '\0'; repchar = 1;
         }
      }
      // check for program exit right away
      if (strcmp(inputline, "exit") == 0 || strcmp(inputline, "quit") == 0) {
         printf(OUTPREFX "Terminating...\n");
         break;
      }
      // check for 'help' command right away
      if (strcmp(inputline, "help") == 0) {
         if (repchar) {
            *parse = ' ';
         } // undo input line edit
         usage(inputline);
         continue;
      }
      // check for empty command right away
      if (strcmp(inputline, "") == 0) {
         // no-op
         continue;
      }
      if (repchar) {
         parse++; // proceed to the char following ' '
         while (*parse == ' ') {
            parse++;
         } // skip over all ' ' chars
      }

      // command execution
      if (strcmp(inputline, "open") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (open_command(config, &(pos), &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "shift") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (shift_command(config, &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "ftag") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (ftag_command(config, &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "ref") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (ref_command(config, &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "obj") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (obj_command(config, config_path, &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "bounds") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (bounds_command(config, &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "refresh") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (refresh_command(config, &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "recovery") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (recovery_command(config, &(tgtpos), &(ftag), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "ns") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (ns_command(config, &(pos), parse) == 0) {
            retval = 0; // note success
         }
      }
      else {
         printf(OUTPREFX "ERROR: Unrecognized command: \"%s\"\n", inputline);
      }

   }

   // cleanup
   if (ftag.ctag) {
      free(ftag.ctag);
   }
   if (ftag.streamid) {
      free(ftag.streamid);
   }
   if (config_abandonposition(&tgtpos)) {
      printf(OUTPREFX "WARNING: Failed to properly destroy tgt marfs position\n");
   }
   if (config_abandonposition(&pos)) {
      printf(OUTPREFX "WARNING: Failed to properly destroy active marfs position\n");
   }
   return retval;
}


int main(int argc, const char** argv) {
   errno = 0; // init to zero (apparently not guaranteed)
   char* config_path = NULL;

   char pr_usage = 0;
   int c;
   // parse all position-independent arguments
   while ((c = getopt(argc, (char* const*)argv, "c:h")) != -1) {
      switch (c) {
      case 'c':
         config_path = optarg;
         break;
      case 'h':
      case '?':
         pr_usage = 1;
         break;
      default:
         printf("Failed to parse command line options\n");
         return -1;
      }
   }

   // check if we need to print usage info
   if (pr_usage) {
      printf(OUTPREFX "Usage info --\n");
      printf(OUTPREFX "%s -c configpath [-h]\n", PROGNAME);
      printf(OUTPREFX "   -c : Path of the MarFS config file\n");
      printf(OUTPREFX "   -h : Print this usage info\n");
      return -1;
   }

   // verify that a config was defined
   if (config_path == NULL) {
      printf(OUTPREFX "no config path defined ( '-c' arg )\n");
      return -1;
   }

   // read in the marfs config
   marfs_config* config = config_init(config_path);
   if (config == NULL) {
      printf(OUTPREFX "ERROR: Failed to initialize config: \"%s\" ( %s )\n",
         config_path, strerror(errno));
      return -1;
   }
   printf(OUTPREFX "marfs config loaded...\n");

   // enter the main command loop
   int retval = 0;
   if (command_loop(config, config_path)) {
      retval = -1;
   }

   // terminate the marfs config
   if (config_term(config)) {
      printf(OUTPREFX "WARNING: Failed to properly terminate MarFS config ( %s )\n",
         strerror(errno));
      return -1;
   }

   return retval;
}

