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
#include <dirent.h>
#include <readline/readline.h>
#include <readline/history.h>

// ENOATTR is not always defined, so define a convenience val
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

#define PROGNAME "marfs-streamwalker"
#define OUTPREFX PROGNAME ": "

// global vars defining MarFS position / cwd path / tab-completion dir
// NOTE - these are necessary to support readline() behaviors
static marfs_position globalpos = {0};
static char* globalcwdpath = NULL;
static char globalusepathcomp = 1;

// this is used to store the state of a target datastream
typedef struct walkerstate_struct {
   marfs_position pos;
   HASH_TABLE     reftable;
   FTAG           ftag;
   GCTAG          gctag;
   char*          oftagstr;
} walkerstate;

// this is used as a temporary holding structure for new datastream info
typedef struct walkerinfo_struct {
   marfs_position pos;
   char*          ftagstr;
   GCTAG          gctag;
   char*          oftagstr;
} walkerinfo;

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
      "( -p path | -r refpath [-p NSpath] | -t ftagval )",
      "Begin traversing a new datastream, starting at the given file",
      "       -p path      : Specifies a user-visible path to pull stream info from\n"
      "                       OR the path of the target NS, if '-r' is used\n"
      "       -r refpath   : Specifies a reference path to retrieve stream info from\n"
      "       -t ftagval   : Directly specifies the tgt stream info, via FTAG value\n"
      "                       NOTE - When used with this option, file existence is\n"
      "                              not verified and any additional file information\n"
      "                              ( GCTAG, OFTAG, etc. ) WILL be omitted.\n"
      "                              Use the 'refresh' command to force a retrieval\n"
      "                              of this info from the corresponding file.\n")

   USAGE("shift",
      "( -@ offset | -n filenum )",
      "Move to a new file in the current datastream",
      "       -@ offset  : Specifies a number of files to move forward of backward\n"
      "       -n filenum : Specifies a specific file number to move to\n")

   USAGE("tags",
      "",
      "Print out the FTAG info of the current file", "")

   USAGE("ref",
      "",
      "Identify the metadata reference path of the current file", "")

   USAGE("obj",
      "[-@ offset | -n chunknum]",
      "Print out the location of the current / specified object",
      "       -@ chunknum : Specifies a specific object number in this *file*\n"
      "       -n chunknum : Specifies a specific object number in this *stream*\n")

   USAGE("bounds",
      "[-s]",
      "Identify the boundaries of the current file / stream",
      "       -s : Specifies to identify the bounds of the entire stream\n")

   USAGE("cd",
      "[-p dirpath]",
      "Print / change the current working directory of this program",
      "       -p dirpath : Specifies the path of a new directory target")

   USAGE("ls",
      "[-p dirpath]",
      "Print the content of the current / given directory",
      "       -p dirpath : Specifies the path of a target directory")

   USAGE("recovery",
      "[-@ offset [-f seekfrom]]",
      "Print the recovery information of the specified object",
      "       -@ offset   : Specifies a file offset to seek to\n"
      "       -f seekfrom : Specifies a start location for the seek\n"
      "                    ( either 'set', 'cur', or 'end' )\n")

   USAGE("refresh",
      "",
      "Retrieve updated information from the current file target (via refpath)", "")

   USAGE("( exit | quit )",
      "",
      "Terminate", "")

   USAGE("help", "[CMD]", "Print this usage info",
      "       CMD : A specific command, for which to print detailed info\n")

   printf("\n");

#undef USAGE
}


char* command_completion_matches( const char* text, int state ) {

   // get the string length of the term we are substituting
   size_t textlen = strlen(text);

   // first, check if a comand has been entered at all
   char* cmdstart = rl_line_buffer;
   char* cmdend = NULL;
   if ( cmdstart ) {
      // progress this pointer to the first non-whitespace char
      while ( *cmdstart != '\0' ) {
         if ( *cmdstart != ' ' ) { break; }
         cmdstart++;
      }
      if ( *cmdstart == '\0' ) { cmdstart = NULL; } // no cmd string found, so just drop it
      else {
         // find the end of the command string
         cmdend = cmdstart;
         while ( *cmdend != '\0' ) {
            if ( *cmdend == ' ' ) { break; }
            cmdend++;
         }
      }
   }

   // check if we're trying to fill in a command
   if ( cmdstart == NULL  ||  // no command yet started
        ( rl_point >= (cmdstart - rl_line_buffer)  &&  rl_point <= (cmdend - rl_line_buffer) )  ||  // user is typing within the command string
        ( rl_point > (cmdend - rl_line_buffer)  &&  strncmp( cmdstart, "help", cmdend - cmdstart ) == 0 )  // user is typing beyond the 'help' cmd
       ) {
      // iterate over all commands, returning the appropriate match index
      int matchcount = 0;
      if ( strncmp( "bounds", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "bounds" ); }
         matchcount++;
      }
      if ( strncmp( "exit", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "exit" ); }
         matchcount++;
      }
      if ( strncmp( "help", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "help" ); }
         matchcount++;
      }
      if ( strncmp( "ls", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "ls" ); }
         matchcount++;
      }
      if ( strncmp( "cd", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "cd" ); }
         matchcount++;
      }
      if ( strncmp( "obj", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "obj" ); }
         matchcount++;
      }
      if ( strncmp( "open", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "open" ); }
         matchcount++;
      }
      if ( strncmp( "quit", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "quit" ); }
         matchcount++;
      }
      if ( strncmp( "recovery", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "recovery" ); }
         matchcount++;
      }
      if ( strncmp( "ref", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "ref" ); }
         matchcount++;
      }
      if ( strncmp( "refresh", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "refresh" ); }
         matchcount++;
      }
      if ( strncmp( "shift", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "shift" ); }
         matchcount++;
      }
      if ( strncmp( "tags", text, textlen ) == 0 ) {
         if ( matchcount >= state ) { return strdup( "tags" ); }
         matchcount++;
      }
      return NULL; // no command matches remain
   }

   // we have a command established and are filling out some kind of arg

   // iterate forward through the string, trying to establish what arg we are populating
   char* argparse = cmdend;
   char* prevarg = NULL;
   char argstate = 0; // indicate that we are looking for a new arg
   while ( *argparse != '\0'  &&  rl_point > (argparse - rl_line_buffer) ) {
      if ( !argstate  &&  *argparse == '-' ) { argstate = 1; }
      else if ( argstate == 1  &&  prevarg == NULL ) { prevarg = argparse; } // save a pointer to the argument flag
      // modify argstate if necessary
      switch ( argstate ) {
         case 1:
            if ( *argparse == ' ' ) { argstate++; } // note that we are beyond the argument itself, and now looking for a value
            break;
         case 2:
            if ( *argparse != ' ' ) { argstate++; } // note that we are within the value for the previous arg
         case 3:
            if ( *argparse == ' ' ) { argstate = 0; prevarg = NULL; } // note that we are completely done with the previous arg
      }
      argparse++;
   }

   // populate args based on command string
   // NOTE : omitting all commands which don't have arguments to be populated
   if ( strncmp( cmdstart, "bounds", cmdend - cmdstart ) == 0 ) {
      if ( !state ) { return strdup( "-s" ); }
   }
   else if ( strncmp( cmdstart, "ls", cmdend - cmdstart ) == 0 ) {
      if ( argstate <= 1 ) { // we are populating an argument to the previous command
         if ( !state ) { return strdup( "-p" ); }
      }
      else { // we are populating the value of a previous argument flag
         if ( prevarg ) {
            // use built-in path completion, via FUSE, for the '-p' arg
            if ( *prevarg == 'p'  &&  globalusepathcomp ) { return rl_filename_completion_function( text, state ); }
         }
      }
   }
   else if ( strncmp( cmdstart, "cd", cmdend - cmdstart ) == 0 ) {
      if ( argstate <= 1 ) { // we are populating an argument to the previous command
         if ( !state ) { return strdup( "-p" ); }
      }
      else { // we are populating the value of a previous argument flag
         if ( prevarg ) {
            // use built-in path completion, via FUSE, for the '-p' arg
            if ( *prevarg == 'p'  &&  globalusepathcomp ) { return rl_filename_completion_function( text, state ); }
         }
      }
   }
   else if ( strncmp( cmdstart, "obj", cmdend - cmdstart ) == 0 ) {
      if ( argstate <= 1 ) { // we are populating an argument to the previous command
         if ( prevarg ) {
            if ( !state ) {
               switch( *prevarg ) {
                  case '@':
                     return strdup( "-@" );
                  case 'n':
                     return strdup( "-n" );
               }
            }
         }
         else {
            switch ( state ) {
               case 0:
                  return strdup( "-@" );
               case 1:
                  return strdup( "-n" );
            }
         }
      }
   }
   else if ( strncmp( cmdstart, "open", cmdend - cmdstart ) == 0 ) {
      if ( argstate <= 1 ) { // we are populating an argument to the previous command
         if ( prevarg ) {
            if ( !state ) {
               switch( *prevarg ) {
                  case 'p':
                     return strdup( "-p" );
                  case 'r':
                     return strdup( "-r" );
                  case 't':
                     return strdup( "-t" );
               }
            }
         }
         else {
            switch ( state ) {
               case 0:
                  return strdup( "-p" );
               case 1:
                  return strdup( "-r" );
               case 2:
                  return strdup( "-t" );
            }
         }
      }
      else { // we are populating the value of a previous argument flag
         if ( prevarg ) {
            // use built-in path completion, via FUSE, for the '-p' arg
            if ( *prevarg == 'p'  &&  globalusepathcomp ) { return rl_filename_completion_function( text, state ); }
         }
      }
   }
   else if ( strncmp( cmdstart, "recovery", cmdend - cmdstart ) == 0 ) {
      if ( argstate <= 1 ) { // we are populating an argument to the previous command
         if ( prevarg ) {
            if ( !state ) {
               switch( *prevarg ) {
                  case '@':
                     return strdup( "-@" );
                  case 'f':
                     return strdup( "-f" );
               }
            }
         }
         else {
            switch ( state ) {
               case 0:
                  return strdup( "-@" );
               case 1:
                  return strdup( "-f" );
            }
         }
      }
      else { // we are populating the value of a previous argument flag
         if ( prevarg ) {
            // '-f' arg has a very limited set of possible strings
            if ( *prevarg == 'f' ) {
               // iterate over all commands, returning the appropriate match index
               int matchcount = 0;
               if ( strncmp( "set", text, textlen ) == 0 ) {
                  if ( matchcount >= state ) { return strdup( "set" ); }
                  matchcount++;
               }
               if ( strncmp( "cur", text, textlen ) == 0 ) {
                  if ( matchcount >= state ) { return strdup( "cur" ); }
                  matchcount++;
               }
               if ( strncmp( "end", text, textlen ) == 0 ) {
                  if ( matchcount >= state ) { return strdup( "end" ); }
               } // fall through to NULL case
            }
         }
      }
   }
   else if ( strncmp( cmdstart, "shift", cmdend - cmdstart ) == 0 ) {
      if ( argstate <= 1 ) { // we are populating an argument to the previous command
         if ( prevarg ) {
            if ( !state ) {
               switch( *prevarg ) {
                  case '@':
                     return strdup( "-@" );
                  case 'n':
                     return strdup( "-n" );
               }
            }
         }
         else {
            switch ( state ) {
               case 0:
                  return strdup( "-@" );
               case 1:
                  return strdup( "-n" );
            }
         }
      }
   }

   return NULL;
}


int update_state(walkerinfo* sourceinfo, walkerstate* tgtstate, char prout) {

   // populate FTAG values based on xattr content
   FTAG tmpftag = {0};
   if (ftag_initstr(&(tmpftag), sourceinfo->ftagstr)) {
      printf(OUTPREFX "ERROR: Failed to parse FTAG string: \"%s\" (%s)\n",
         sourceinfo->ftagstr, strerror(errno));
      if ( sourceinfo->oftagstr ) { free(sourceinfo->oftagstr); }
      free(sourceinfo->ftagstr);
      config_abandonposition( &(sourceinfo->pos) );
      return -1;
   }
   // check if we need a custom reference table
   HASH_TABLE tmpreftable = sourceinfo->pos.ns->prepo->metascheme.reftable;
   if ( tmpftag.refbreadth != sourceinfo->pos.ns->prepo->metascheme.refbreadth  ||
        tmpftag.refdepth != sourceinfo->pos.ns->prepo->metascheme.refdepth  ||
        tmpftag.refdigits != sourceinfo->pos.ns->prepo->metascheme.refdigits ) {
      tmpreftable = config_genreftable( NULL, NULL, tmpftag.refbreadth,
                                            tmpftag.refdepth, tmpftag.refdigits );
      if ( tmpreftable == NULL ) {
         printf(OUTPREFX "ERROR: Failed to instantiate a custom reference table ( %s )\n", strerror(errno));
         if ( tmpftag.ctag ) { free( tmpftag.ctag ); }
         if ( tmpftag.streamid ) { free( tmpftag.streamid ); }
         if ( sourceinfo->oftagstr ) { free(sourceinfo->oftagstr); }
         free(sourceinfo->ftagstr);
         config_abandonposition( &(sourceinfo->pos) );
         return -1;
      }
   }
   // cleanup previous state, if necessary
   if (tgtstate->ftag.ctag) {
      free(tgtstate->ftag.ctag);
      tgtstate->ftag.ctag = NULL;
   }
   if (tgtstate->ftag.streamid) {
      free(tgtstate->ftag.streamid);
      tgtstate->ftag.streamid = NULL;
   }
   if ( tgtstate->reftable  &&  tgtstate->reftable != tgtstate->pos.ns->prepo->metascheme.reftable ) {
      HASH_NODE* nodelist = NULL;
      size_t nodecount = 0;
      if ( hash_term( tgtstate->reftable, &(nodelist), &(nodecount) ) ){
         printf(OUTPREFX "WARNING: Failed to properly destroy custom reference HASH_TABLE\n");
      }
      else {
         size_t nodeindex = 0;
         for ( ; nodeindex < nodecount; nodeindex++ ) {
            if ( (nodelist + nodeindex)->name ) { free( (nodelist + nodeindex)->name ); }
         }
         free( nodelist );
      }
      tgtstate->reftable = NULL;
   }
   if ( tgtstate->oftagstr ) { free( tgtstate->oftagstr ); tgtstate->oftagstr = NULL; }
   if ( tgtstate->pos.ns  &&  config_abandonposition( &(tgtstate->pos) )) {
      printf(OUTPREFX "WARNING: Failed to properly destroy tgt marfs position\n");
   }
   // update the passed state
   tgtstate->ftag = tmpftag;
   tgtstate->reftable = tmpreftable;
   tgtstate->gctag = sourceinfo->gctag;
   tgtstate->oftagstr = sourceinfo->oftagstr;
   // do a semi-sketchy direct copy of postion values
   tgtstate->pos = sourceinfo->pos;
   if (prout) {
      printf(OUTPREFX "Values Updated\n" );
      if ( tgtstate->gctag.refcnt ) {
         printf(OUTPREFX "   NOTE -- This file has a GCTAG attached\n" );
      }
      if ( tgtstate->oftagstr ) {
         printf(OUTPREFX "   NOTE -- This file has previously been repacked\n" );
      }
      if ( tgtstate->reftable != sourceinfo->pos.ns->prepo->metascheme.reftable ) {
         printf(OUTPREFX "   NOTE -- File has a non-standard reference structure\n" );
      }
      printf("\n");
   }
   // cleanup any unused sourceinfo elements
   free(sourceinfo->ftagstr);
   return 0;

}


int populate_tags(marfs_config* config, marfs_position* pathpos, const char* path, const char* rpath, char prout, walkerinfo* resultinfo) {
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
      if ( errno == ENOENT ) {
         // handle ENOENT slightly differently, as this is a semi-expected case
         printf(OUTPREFX "WARNING: Failed to open target %s file: \"%s\" (%s)\n",
            (rpath) ? "ref" : "user", (rpath) ? rpath : path, strerror(errno));
         config_abandonposition( &oppos );
         return 1;
      }
      printf(OUTPREFX "ERROR: Failed to open target %s file: \"%s\" (%s)\n",
         (rpath) ? "ref" : "user", (rpath) ? rpath : path, strerror(errno));
      config_abandonposition( &oppos );
      return -1;
   }

   // retrieve the FTAG value from the target file
   char* ftagstr = NULL;
   ssize_t getres = mdal->fgetxattr(handle, 1, FTAG_NAME, ftagstr, 0);
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
   // retrieve the GCTAG value from the target file, if present
   GCTAG tmpgctag = {0};
   getres = mdal->fgetxattr(handle, 1, GCTAG_NAME, NULL, 0);
   if ( getres <= 0 ) {
      if ( errno != ENOATTR ) {
         printf(OUTPREFX "ERROR: Failed to retrieve GCTAG value\n");
         free(ftagstr);
         mdal->close(handle);
         config_abandonposition( &oppos );
         return -1;
      }
   }
   else {
      char* gctagstr = calloc(1, getres + 1);
      if ( gctagstr == NULL ) {
         printf(OUTPREFX "ERROR: Failed to allocate space for a GCTAG string value\n");
         free( ftagstr );
         mdal->close(handle);
         config_abandonposition( &oppos );
         return -1;
      }
      if ( mdal->fgetxattr(handle, 1, GCTAG_NAME, gctagstr, getres) != getres ) {
         printf(OUTPREFX "ERROR: GCTAG value changed while we were reading it\n");
         free(gctagstr);
         free(ftagstr);
         mdal->close(handle);
         config_abandonposition( &oppos );
         return -1;
      }
      if ( gctag_initstr( &(tmpgctag), gctagstr ) ) {
         printf(OUTPREFX "ERROR: Failed to parse GCTAG value: \"%s\" (%zd)\n", gctagstr, getres);
         free(gctagstr);
         free(ftagstr);
         mdal->close(handle);
         config_abandonposition( &oppos );
         return -1;
      }
      if ( prout ) {
         printf(OUTPREFX "Found GCTAG value for target %s file: \"%s\"\n",
            (rpath) ? "ref" : "user", (rpath) ? rpath : path);
      }
      free( gctagstr );
   }
   // retrieve the ORIG FTAG value from the target file, if present
   char* tmpoftagstr = NULL;
   getres = mdal->fgetxattr(handle, 1, OREPACK_TAG_NAME, NULL, 0 );
   if ( getres <= 0 ) {
      if ( errno != ENOATTR ) {
         printf(OUTPREFX "ERROR: Failed to retrieve OREPACK value\n");
         free(ftagstr);
         mdal->close(handle);
         config_abandonposition( &oppos );
         return -1;
      }
   }
   else {
      tmpoftagstr = calloc( 1, getres + 1 );
      if ( tmpoftagstr == NULL ) {
         printf(OUTPREFX "ERROR: Failed to allocate space for a OREPACK string value\n");
         free( ftagstr );
         mdal->close(handle);
         config_abandonposition( &oppos );
         return -1;
      }
      if ( mdal->fgetxattr(handle, 1, OREPACK_TAG_NAME, tmpoftagstr, getres) != getres ) {
         printf(OUTPREFX "ERROR: OREPACK value changed while we were reading it\n");
         free(tmpoftagstr);
         free(ftagstr);
         mdal->close(handle);
         config_abandonposition( &oppos );
         return -1;
      }
   }
   if (mdal->close(handle)) {
      printf(OUTPREFX "WARNING: Failed to close handle for target file (%s)\n", strerror(errno));
   }

   if (prout) {
      printf(OUTPREFX "Successfully retrieved values from target %s file: \"%s\"\n",
         (rpath) ? "ref" : "user", (rpath) ? rpath : path);
   }

   // update the passed info struct
   resultinfo->ftagstr = ftagstr;
   resultinfo->gctag = tmpgctag;
   resultinfo->oftagstr = tmpoftagstr;
   // do a semi-sketchy direct copy of postion values
   resultinfo->pos = oppos;

   return 0;
}


int open_command(marfs_config* config, marfs_position* pathpos, walkerstate* state, char* args) {
   printf("\n");
   // parse args
   char curarg = '\0';
   char* userpath = NULL;
   char* refpath = NULL;
   char* ftagval = NULL;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-p") == 0) {
            curarg = 'p';
         }
         else if (strcmp(parse, "-r") == 0) {
            curarg = 'r';
         }
         else if (strcmp(parse, "-t") == 0) {
            curarg = 't';
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'open' command: '%s'\n", parse);
            if (userpath) {
               free(userpath);
            }
            if (refpath) {
               free(refpath);
            }
            if (ftagval) {
               free(ftagval);
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
               if (ftagval) {
                  free(ftagval);
               }
               return -1;
            }
            tgtstr = &(userpath);
         }
         else if (curarg == 'r') {
            if (refpath != NULL) {
               printf(OUTPREFX "ERROR: Detected duplicate '-r' arg: \"%s\"\n", parse);
               free(refpath);
               if (userpath) {
                  free(userpath);
               }
               if (ftagval) {
                  free(ftagval);
               }
               return -1;
            }
            tgtstr = &(refpath);
         }
         else { // == t
            if (ftagval != NULL) {
               printf(OUTPREFX "ERROR: Detected duplicate '-t' arg: \"%s\"\n", parse);
               free(ftagval);
               if (userpath) {
                  free(userpath);
               }
               if (refpath) {
                  free(refpath);
               }
               return -1;
            }
            tgtstr = &(ftagval);
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
            if (ftagval) {
               free(ftagval);
            }
            return -1;
         }
         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }
   // check that we have at least one arg
   if (!(userpath) && !(refpath) && !(ftagval)) {
      printf(OUTPREFX "ERROR: 'open' command requires at least one '-p', '-r', or '-t' arg\n");
      return -1;
   }
   // check for incompatible args
   if ( ftagval  &&  (userpath  ||  refpath) ) {
      free(ftagval);
      if (userpath) {
         free(userpath);
      }
      if (refpath) {
         free(refpath);
      }
      printf(OUTPREFX "ERROR: the 'open' command '-t' arg is incompatible with both '-p' and '-r'\n");
      return -1;
   }

   // populate our FTAG and cleanup strings
   walkerinfo info = {0};
   int retval = 0;
   if ( ftagval ) {
      info.ftagstr = ftagval;
      if ( (retval = config_duplicateposition( pathpos, &(info.pos) )) ) {
         printf(OUTPREFX "ERROR: failed to duplicate current streamwalker position\n");
         free( ftagval );
      }
   }
   else { retval = populate_tags(config, pathpos, userpath, refpath, 1, &info); }
   if ( !retval ) {
      retval = update_state(&info, state, 1);
   }
   if (userpath) {
      free(userpath);
   }
   if (refpath) {
      free(refpath);
   }
   // NOTE -- we explicitly don't free ftagval, as update_state() should have done so already
   return retval;
}

int shift_command(marfs_config* config, walkerstate* state, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (state->ftag.streamid == NULL) {
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
            if (parseval < 0 && llabs(parseval) > state->ftag.fileno) {
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
   size_t origfileno = state->ftag.fileno;
   if (offset) {
      if (filenum != -1) {
         printf(OUTPREFX "ERROR: 'shift' command cannot support both '-@' and '-n' args\n");
         return -1;
      }
      state->ftag.fileno += offset;
   }
   else {
      state->ftag.fileno = filenum;
   }

   // generate a ref path for the new target file
   char* newrpath = datastream_genrpath(&(state->ftag), state->reftable, NULL, NULL);
   if (newrpath == NULL) {
      printf(OUTPREFX "ERROR: Failed to identify new ref path\n");
      state->ftag.fileno = origfileno;
      return -1;
   }

   walkerinfo info = {0};
   int retval = populate_tags(config, &(state->pos), NULL, newrpath, 1, &info);
   if ( !retval ) {
      retval = update_state(&info, state, 1);
   }
   if (retval) {
      state->ftag.fileno = origfileno;
   }
   free(newrpath);

   return retval;
}

int tags_command(marfs_config* config, walkerstate* state, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (state->ftag.streamid == NULL) {
      printf(OUTPREFX "ERROR: No current FTAG target\n");
      return -1;
   }
   const char* datastatestr = "INIT";
   if ((state->ftag.state & FTAG_DATASTATE) == FTAG_SIZED) {
      datastatestr = "SIZED";
   }
   else if ((state->ftag.state & FTAG_DATASTATE) == FTAG_FIN) {
      datastatestr = "FINALIZED";
   }
   else if ((state->ftag.state & FTAG_DATASTATE) == FTAG_COMP) {
      datastatestr = "COMPLETE";
   }
   const char* dataaccessstr = "NO-ACCESS";
   if ((state->ftag.state & FTAG_WRITEABLE)) {
      if ((state->ftag.state & FTAG_READABLE)) {
         dataaccessstr = "READ-WRITE";
      }
      else {
         dataaccessstr = "WRITE-ONLY";
      }
   }
   else if ((state->ftag.state & FTAG_READABLE)) {
      dataaccessstr = "READ-ONLY";
   }
   // print out all FTAG values
   printf("Stream Info --\n");
   printf(" Client Tag : %s\n", state->ftag.ctag);
   printf(" Stream ID  : %s\n", state->ftag.streamid);
   printf(" Max Files  : %zu\n", state->ftag.objfiles);
   printf(" Max Size   : %zu\n", state->ftag.objsize);
   printf("File Position --\n");
   printf(" File Number   : %zu\n", state->ftag.fileno);
   printf(" Object Number : %zu\n", state->ftag.objno);
   printf(" Object Offset : %zu\n", state->ftag.offset);
   printf(" End of Stream : %d\n", state->ftag.endofstream);
   printf("Data Structure --\n");
   printf(" Bytes       : %zu\n", state->ftag.bytes);
   printf(" Avail Bytes : %zu\n", state->ftag.availbytes);
   printf(" Recov Bytes : %zu\n", state->ftag.recoverybytes);
   printf(" Data State  : %s\n", datastatestr);
   printf(" Data Access : %s\n", dataaccessstr);
   printf(" Protection --\n");
   printf("  N   : %d\n", state->ftag.protection.N);
   printf("  E   : %d\n", state->ftag.protection.E);
   printf("  O   : %d\n", state->ftag.protection.O);
   printf("  psz : %ld\n", state->ftag.protection.partsz);
   if ( state->gctag.refcnt  ||  state->gctag.eos  ||  state->gctag.delzero ) {
      printf("GC Info --\n");
      printf(" Reference Count : %zu\n", state->gctag.refcnt );
      printf(" End Of Stream : %d\n", (int)state->gctag.eos );
      printf(" Deleted Zero : %d\n", (int)state->gctag.delzero );
      printf(" In Progress : %d\n", (int)state->gctag.inprog );
   }
   if ( state->oftagstr ) {
      printf("Repacked From -- \"%s\"\n", state->oftagstr);
   }
   printf("\n");
   return 0;
}

int ref_command(marfs_config* config, walkerstate* state, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (state->ftag.streamid == NULL) {
      printf(OUTPREFX "ERROR: No FTAG target to shift from\n");
      return -1;
   }

   // generate a ref path for the new target file
   char* curpath = datastream_genrpath(&(state->ftag), state->reftable, NULL, NULL);
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

int obj_command(marfs_config* config, char* config_path, walkerstate* state, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (state->ftag.streamid == NULL) {
      printf(OUTPREFX "ERROR: No current FTAG target\n");
      return -1;
   }

   // parse args
   char curarg = '\0';
   ssize_t chunknum = -1;
   char haveoffset = 0;
   ssize_t chunkoff = 0;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-n") == 0) {
            if (chunknum != -1) {
               printf(OUTPREFX "ERROR: Detected duplicate '-n' arg\n");
               return -1;
            }
            curarg = 'n';
         }
         else if (strcmp(parse, "-@") == 0) {
            if (haveoffset) {
               printf(OUTPREFX "ERROR: Detected duplicate '-@' arg\n");
               return -1;
            }
            curarg = '@';
            haveoffset = 1;
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'obj' command: '%s'\n", parse);
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

         if ( curarg == 'n' ) {
            if (parseval < 0) {
               printf(OUTPREFX "ERROR: Negative chunknum value: \"%lld\"\n", parseval);
               return -1;
            }
            chunknum = parseval;
         }
         else { // == '@'
            chunkoff = parseval;
         }
         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }
   // check for argument conflict
   if ( haveoffset  &&  chunknum != -1 ) {
      printf(OUTPREFX "ERROR: The '-n' and '-@' arguments are mutually exclusive\n");
      return -1;
   }
   // establish an actual chunk number target
   if ( chunknum == -1 ) { chunknum = 0; }
   if ( haveoffset ) { chunknum += chunkoff + state->ftag.objno; }
   if ( chunknum < 0 ) {
      printf(OUTPREFX "WARNING: Specified offset results in a negative object value: %zd ( referencing object zero instead )\n", chunknum);
      chunknum = 0;
   }
   chunkoff = chunknum - state->ftag.objno;

   // identify object bounds of the current file
   size_t endobj = datastream_filebounds( &(state->ftag) );
   if ( chunknum < state->ftag.objno ) {
      printf(OUTPREFX "WARNING: Specified object number preceeds file limits ( min referenced objno == %zd )\n", state->ftag.objno);
   }
   if (chunknum > endobj) {
      printf(OUTPREFX "WARNING: Specified object number exceeds file limits ( max referenced objno == %zd )\n", endobj);
   }

   // identify the object target
   char* objname = NULL;
   ne_erasure erasure;
   ne_location location;
   FTAG curtag = state->ftag;
   curtag.objno = chunknum;
   if (datastream_objtarget(&(curtag), &(state->pos.ns->prepo->datascheme), &(objname), &(erasure), &(location))) {
      printf(OUTPREFX "ERROR: Failed to identify data info for chunk %zu\n", chunknum);
      return -1;
   }
   // print object info
   printf("Obj#%-5zu [ FileObjNo %zd/%zd ]\n   Pod: %d\n   Cap: %d\n   Scatter: %d\n   ObjName: %s\n   Erasure Information: N %d, E %d, O %d, partsz %lu\n   neutil Args: -c \"%s:/marfs_config/repo name=%s/data/DAL\" -P %d -C %d -S %d -O \"%s\"\n\n", chunknum, chunkoff, endobj - state->ftag.objno, location.pod, location.cap, location.scatter, objname, erasure.N, erasure.E, erasure.O, erasure.partsz, config_path, state->pos.ns->prepo->name, location.pod, location.cap, location.scatter, objname);
   free(objname);

   return 0;
}

int bounds_command(marfs_config* config, walkerstate* state, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (state->ftag.streamid == NULL) {
      printf(OUTPREFX "ERROR: No current FTAG target\n");
      return -1;
   }

   // parse args
   int s_flag = 0;
   char* parse = strtok(args, " ");
   while (parse) {
      if (strcmp(parse, "-s") == 0) {
         s_flag = 1;
      }
      else {
         printf(OUTPREFX "ERROR: Unrecognized argument for 'bound' command: '%s'\n", parse);
         return -1;
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }

   char gcgaps = 0;
   int retval = 0;
   size_t origfileno = state->ftag.fileno;
   state->ftag.fileno = 0; // start with file zero
   FTAG finftag = state->ftag;
   // clear any 'EOS' relevant state
   state->ftag.endofstream = 0;
   state->gctag.eos = 0;
   state->ftag.state = FTAG_FIN;
   finftag.ctag = NULL; // unsafe to reference values we intend to free at any time
   finftag.streamid = NULL;
   char fineos = 0;
   if (s_flag) {
      // iterate over files until we find EOS
      char errorflag = 0;
      while (state->ftag.endofstream == 0 && retval == 0 && state->gctag.eos == 0  &&
         (state->ftag.state & FTAG_DATASTATE) >= FTAG_FIN) {
         // generate a ref path for the new target file
         char* newrpath = datastream_genrpath(&(state->ftag), state->reftable, NULL, NULL);
         if (newrpath == NULL) {
            printf(OUTPREFX "ERROR: Failed to identify new ref path\n");
            state->ftag.fileno = origfileno;
            errorflag = 1;
            break;
         }
         // retrieve the FTAG of the new target
         walkerinfo info = {0};
         retval = populate_tags(config, &(state->pos), NULL, newrpath, 0, &info);
         if ( retval == 1  &&  (state->ftag.state & FTAG_DATASTATE) == FTAG_FIN ) { // ENOENT after a FINALIZED file is a special case
            break;
         }
         if ( !retval ) {
            retval = update_state(&info, state, 0);
         }
         if (retval) {
            printf(OUTPREFX "ERROR: Failed to retrieve FTAG value from fileno %zu\n", state->ftag.fileno);
            state->ftag.fileno = origfileno;
            errorflag = 1;
            break;
         } // if we couldn't retrieve this, go to previous
         else if ( state->gctag.refcnt ) {
            gcgaps = 1;
            printf( "GC Gap: %zu Files Starting at File %zu\n", state->gctag.refcnt, state->ftag.fileno );
         }
         free(newrpath);
         // progress to the next file
         state->ftag.fileno += 1;
         if ( state->gctag.refcnt ) { state->ftag.fileno += state->gctag.refcnt; }
      }
      finftag = state->ftag;
      finftag.fileno--; // subtract one, as we adjusted this before hitting the end of loop check
      finftag.ctag = NULL; // unsafe to reference values we intend to free at any time
      finftag.streamid = NULL;
      fineos = state->gctag.eos;
      // restore the original value
      state->ftag.fileno = origfileno;
      char* newrpath = datastream_genrpath(&(state->ftag), state->reftable, NULL, NULL);
      if (newrpath == NULL) {
         printf(OUTPREFX "ERROR: Failed to identify original ref path\n");
         return -1;
      }
      // retrieve the FTAG of the new target
      walkerinfo info = {0};
      int tmpretval = populate_tags(config, &(state->pos), NULL, newrpath, 0, &info);
      if ( !tmpretval ) {
         tmpretval = update_state(&info, state, 0);
      }
      free(newrpath);
      if ( errorflag ) { return -1; }
   }
   if ( gcgaps ) { printf( "\n" ); }

   // identify object bounds of final file
   RECOVERY_HEADER header = {
      .majorversion = RECOVERY_CURRENT_MAJORVERSION,
      .minorversion = RECOVERY_CURRENT_MINORVERSION,
      .ctag = state->ftag.ctag,
      .streamid = state->ftag.streamid
   };
   size_t headerlen = recovery_headertostr(&(header), NULL, 0);
   if (headerlen < 1) {
      printf(OUTPREFX "ERROR: Failed to identify recovery header length for final file\n");
      headerlen = 0;
   }
   size_t dataperobj = finftag.objsize - (headerlen + finftag.recoverybytes);
   size_t finobjbounds = (finftag.bytes + finftag.offset - headerlen) / dataperobj;
   size_t finobjoff = ((finftag.bytes + finftag.offset - headerlen) % dataperobj) + headerlen;
   // special case check
   if ((finftag.state & FTAG_DATASTATE) >= FTAG_FIN && finobjbounds &&
      (finftag.bytes + finftag.offset - headerlen) % dataperobj == 0) {
      // if we exactly align to object bounds AND the file is FINALIZED,
      //   we've overestimated by one object
      finobjbounds--;
   }


   // print out stream boundaries
   if (s_flag) {
      char* eosreason = "End of Stream";
      if (retval) {
         eosreason = "Failed to Identify Subsequent File";
      }
      else if ((finftag.state & FTAG_DATASTATE) < FTAG_FIN) {
         eosreason = "Unfinished File";
      }
      else if ( fineos ) {
         eosreason = "End of Stream ( GCTAG )";
      }
      printf("File Bounds:\n   0 -- Initial File\n     to\n   %zu -- %s\n",
         finftag.fileno, eosreason);
      printf("Object Bounds:\n   0 -- Initial Object\n     to\n   %zu -- End of Final File\n",
         finftag.objno + finobjbounds);
   }
   else {
      printf("File %zu Bounds:\n   Object %zu Offset %zu\n     to\n   Object %zu Offset %zu\n",
         finftag.fileno, finftag.objno, finftag.offset, finftag.objno + finobjbounds, finobjoff);
      printf("Each data object includes a %zu byte recovery header and a %zu byte recovery tail for this file.\n",
         headerlen, finftag.recoverybytes);
   }

   printf("\n");

   return 0;
}

int refresh_command(marfs_config* config, walkerstate* state, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (state->ftag.streamid == NULL) {
      printf(OUTPREFX "ERROR: No FTAG target to shift from\n");
      return -1;
   }

   // check for any args
   if (*args != '\0') {
      printf(OUTPREFX "ERROR: The 'refresh' command does not support arguments\n");
      return -1;
   }

   // generate a ref path for the current target file
   char* newrpath = datastream_genrpath(&(state->ftag), state->reftable, NULL, NULL);
   if (newrpath == NULL) {
      printf(OUTPREFX "ERROR: Failed to identify current ref path\n");
      return -1;
   }

   walkerinfo info = {0};
   int retval = populate_tags(config, &(state->pos), NULL, newrpath, 1, &info);
   if ( !retval ) {
      retval = update_state(&info, state, 1);
   }
   printf("\n");
   free(newrpath);

   return retval;
}

int recovery_command(marfs_config* config, walkerstate* state, char* args) {
   printf("\n");
   // verify that we have an FTAG value
   if (state->ftag.streamid == NULL) {
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

   char* rpath = datastream_genrpath(&(state->ftag), state->reftable, NULL, NULL);
   if (rpath == NULL) {
      printf(OUTPREFX "ERROR: Failed to generate reference path\n");
      return -1;
   }

   DATASTREAM stream = genstream(READ_STREAM, rpath, 1, &(state->pos), 0, NULL, NULL);
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
   printf("Recovery Info:\n    Inode: %zu\n    Mode: %o\n    Owner: %d\n    Group: %d\n    Size: %zu\n    Mtime: %s.%.9ld\n    EOF: %d\n    Path: %s\n\n",
      info.inode, info.mode, info.owner, info.group, info.size, timestr, info.mtime.tv_nsec, info.eof, info.path);

   return 0;

}

int cd_command(marfs_config* config, char** cwdpath, marfs_position* pos, char* args) {
   printf("\n");

   // parse args
   char curarg = '\0';
   const char* origpath = NULL; // for potential FUSE chdir() use
   char* path = NULL;
   char* parse = strtok(args, " ");
   while (parse) {
      if (curarg == '\0') {
         if (strcmp(parse, "-p") == 0) {
            curarg = 'p';
         }
         else {
            printf(OUTPREFX "ERROR: Unrecognized argument for 'cd' command: '%s'\n", parse);
            return -1;
         }
      }
      else {
         // duplicate the path arg
         path = strdup( parse );
         if ( path == NULL ) {
            printf(OUTPREFX "ERROR: Failed to allocate intermediate string\n" );
            return -1;
         }
         origpath = parse;
         curarg = '\0';
      }

      // progress to the next arg
      parse = strtok(NULL, " ");
   }

   if (path != NULL) {
      // Need to traverse this path relative to our current position
      marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
      if ( config_duplicateposition( pos, &oppos ) ) {
         printf( OUTPREFX "ERROR: Failed to duplicate position for config traversal\n" );
         free( path );
         return -1;
      }
      // perform path traversal to identify marfs position
      int depth;
      if ( (depth = config_traverse(config, &oppos, &(path), 1)) < 0) {
         printf(OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
            path);
         free( path );
         config_abandonposition( &oppos );
         return -1;
      }
      if ( oppos.ctxt == NULL  &&  config_fortifyposition( &oppos ) ) {
         printf(OUTPREFX "ERROR: Failed to fortify new NS position\n" );
         free( path );
         config_abandonposition( &oppos );
         return -1;
      }
      // possibly build up the path, relative to the current NS root
      char* abspath = NULL;
      if ( oppos.depth  &&  strlen( *cwdpath ) ) {
         size_t abspathlen = strlen( *cwdpath ) + 1 + strlen( path ) + 1;
         abspath = calloc( 1, sizeof(char) * (abspathlen + 1) );
         if ( abspath == NULL ) {
            printf( OUTPREFX "ERROR: Failed to allocate absolute path of length %zu\n", abspathlen );
            free( path );
            config_abandonposition( &oppos );
            return -1;
         }
         if ( snprintf( abspath, abspathlen, "%s/%s", *cwdpath, path ) != abspathlen - 1 ) {
            printf( OUTPREFX "ERROR: Failed to construct absolute path of length %zu\n", abspathlen );
            free( abspath );
            free( path );
            config_abandonposition( &oppos );
            return -1;
         }
         // collapse redundant path elements
//printf( "COLLAPSING \"%s\"\n", abspath );
         char* parsepos = abspath;
         char* filltgt = abspath;
         while ( filltgt <= parsepos  &&  *parsepos != '\0' ) {
            char* curelem = parsepos;
            size_t elemlen = 0;
            while ( *parsepos != '\0' ) {
               // check for an end to this path element
               if ( *parsepos == '/' ) {
                  // progress to the start of the next path element
                  while ( *parsepos == '/' ) { parsepos++; }
                  break;
               }
               elemlen++;
               parsepos++;
            }
            if ( elemlen == 2  &&  strncmp( curelem, "..", 2 ) == 0 ) {
               // reverse our filltgt to the start of the previous path element
               filltgt -= 2; // skip over the previous '/' char
               while ( filltgt > abspath  &&  *filltgt != '/' ) { filltgt--; }
               if ( filltgt < abspath ) {
                  // sanity check
                  printf( OUTPREFX "ERROR: Failed to collapse NS-relative CWD path: \"%s\"\n", abspath );
                  free( abspath );
                  free( path );
                  config_abandonposition( &oppos );
                  return -1;
               }
               if ( *filltgt == '/' ) { filltgt++; }
//printf( "trimming elem\n" );
            }
            else if ( elemlen != 1  ||  *curelem != '.' ) {
//printf( "adding elem %.*s\n", (int)elemlen, curelem );
               // fill in our string with this path element
               if ( filltgt != curelem ) {
                  snprintf( filltgt, elemlen + 1, "%s", curelem );
               }
               filltgt += elemlen;
               *filltgt = '/';
               filltgt++;
            }
         }
         *filltgt = '\0';
//printf( "RESULT \"%s\"\n", abspath );
      }
      if ( depth != oppos.depth ) {
         // attempt to open the directory target
         MDAL_DHANDLE tgtdir = oppos.ns->prepo->metascheme.mdal->opendir( oppos.ctxt, path );
         if ( tgtdir == NULL ) {
            printf(OUTPREFX "ERROR: Failed to open target subdir of \"%s\" NS: \"%s\" ( %s )\n",
                   oppos.ns->idstr, path, strerror( errno ) );
            if ( abspath ) { free( abspath ); }
            free( path );
            config_abandonposition( &oppos );
            return -1;
         }
         // update our ctxt to reference the target dir
         if ( oppos.ns->prepo->metascheme.mdal->chdir( oppos.ctxt, tgtdir ) ) {
            printf(OUTPREFX "ERROR: Failed to chdir into target subdir of \"%s\" NS: \"%s\" ( %s )\n",
                   oppos.ns->idstr, path, strerror( errno ) );
            oppos.ns->prepo->metascheme.mdal->closedir( tgtdir );
            if ( abspath ) { free( abspath ); }
            free( path );
            config_abandonposition( &oppos );
            return -1;
         }
         oppos.depth = depth; // we are actually targeting any subpath, if specified
      }
      // if we are using FUSE for path completion, attempt to chdir as well
      if ( globalusepathcomp ) {
         if ( chdir( origpath ) ) {
            printf(OUTPREFX "WARNING: Failed to chdir() into MarFS FUSE position \"%s\" ( %s ) : Path autocompletion is now disabled\n",
                   origpath, strerror(errno) );
            globalusepathcomp = 0;
         }
      }
      // update our pos arg
      config_abandonposition( pos );
      *pos = oppos; // just direct copy, and don't abandon this position
      if ( *cwdpath ) { free( *cwdpath ); }
      if ( abspath ) {
         *cwdpath = abspath; // update our cwd path
         free( path );
      }
      else { *cwdpath = path; }
   }
   printf("%s Namespace Target : \"%s\" Subpath Target : \"%s\" ( Depth = %u )\n\n",
           (path == NULL) ? "Current" : "New", pos->ns->idstr, *cwdpath, pos->depth);
   return 0;
}

int ls_command(marfs_config* config, marfs_position* pos, char* args) {

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
            printf(OUTPREFX "ERROR: Unrecognized argument for 'ls' command: '%s'\n", parse);
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

   // identify a target path
   char* lstgt = (path) ? path : strdup(".");
   if ( lstgt == NULL ) {
      printf( OUTPREFX "ERROR: Failed to duplicate path value\n" );
      return -1;
   }
   marfs_position tmppos = {0};
   if ( config_duplicateposition( pos, &(tmppos) ) ) {
      printf( OUTPREFX "ERROR: Failed to duplicate MarFS position prior to 'ls' path traversal\n" );
      free( lstgt );
      return -1;
   }
   int targetdepth;
   if ( (targetdepth = config_traverse( config, &(tmppos), &(lstgt), 1 )) < 0 ) {
      printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n", lstgt );
      config_abandonposition( &(tmppos) );
      free( lstgt );
      return -1;
   }
   // attempt to open a dir handle for the target path
   MDAL curmdal = tmppos.ns->prepo->metascheme.mdal;
   MDAL_DHANDLE lsdir = NULL;
   if ( targetdepth == 0 ) {
      // ignore our current path
      if ( lstgt ) { free( lstgt ); }
      // identify the full path of the NS target
      if ( config_nsinfo( tmppos.ns->idstr, NULL, &(lstgt) ) ) {
         printf( OUTPREFX "ERROR: Failed to identify path of NS target: \"%s\" ( %s )\n",
                 tmppos.ns->idstr, strerror( errno ) );
         config_abandonposition( &(tmppos) );
         return -1;
      }
      lsdir = curmdal->opendirnamespace( curmdal->ctxt, lstgt );
   }
   else { lsdir = curmdal->opendir( tmppos.ctxt, lstgt ); }
   if ( lsdir == NULL ) {
      printf( OUTPREFX "ERROR: Failed to open a dir handle for tgt path: \"%s\" ( %s )\n",
              lstgt, strerror( errno ) );
      config_abandonposition( &(tmppos) );
      free( lstgt );
      return -1;
   }
   // dump subspaces first, if appropriate
   if ( targetdepth == 0 ) {
      size_t index = 0;
      for ( ; index < tmppos.ns->subnodecount; index++ ) {
         printf( "<NS> %s/\n", tmppos.ns->subnodes[index].name );
      }
   }
   // readdir contents
   errno = 0;
   struct dirent* retval = NULL;
   while ( errno == 0 ) {
      retval = curmdal->readdir( lsdir );
      if ( retval  &&  ( targetdepth  ||  curmdal->pathfilter( retval->d_name ) == 0 ) ) {
         printf( "     %s\n", retval->d_name );
      }
      else if ( retval == NULL ) { break; }
   }
   if ( errno ) {
      printf( OUTPREFX "ERROR: Readdir failure on tgt path: \"%s\" ( %s )\n", lstgt, strerror( errno ) );
   }
   if ( curmdal->closedir( lsdir ) ) {
      printf( OUTPREFX "ERROR: Closedir failure on tgt path: \"%s\" ( %s )\n", lstgt, strerror( errno ) );
   }
   if ( config_abandonposition( &(tmppos) ) ) {
      printf( OUTPREFX "ERROR: Failed to abandon temporary MarFS position ( %s )\n", strerror( errno ) );
   }
   free( lstgt );
   printf("\n");

   return 0;
}

int command_loop(marfs_config* config, char* config_path) {
   // initialize a marfs position
   globalpos.ns = NULL;
   globalpos.depth = 0;
   globalpos.ctxt = NULL;
   if ( config_establishposition( &globalpos, config ) ) {
      printf(OUTPREFX "ERROR: Failed to establish a position for the MarFS root\n");
      return -1;
   }
   // initialize an empty string for our cwd path
   globalcwdpath = strdup( "" );
   if ( globalcwdpath == NULL ) {
      printf(OUTPREFX "ERROR: Failed to allocate a current working directory string\n");
      config_abandonposition( &globalpos );
      return -1;
   }
   // initialize walk state
   walkerstate state;
   bzero( &(state), sizeof( struct walkerstate_struct ) );
   printf("Initial Namespace Target : \"%s\"\n", globalpos.ns->idstr);

   // initialize readline values
   rl_basic_word_break_characters = " \t\n\"\\'`$><=;|&{("; // omit '@' from word break chars ( used in cmd args )
   rl_completion_entry_function = command_completion_matches;

   // infinite loop, processing user commands
   printf(OUTPREFX "Ready for user commands\n");
   int retval = 0;
   while (1) {

      size_t promptlen = strlen(globalpos.ns->idstr) + 1 + strlen( globalcwdpath ) + 4;
      char* promptstr = calloc( 1, promptlen * sizeof(char) );
      if ( promptstr == NULL ) {
         printf(OUTPREFX "ERROR: Failed to allocate prompt string\n" );
         break;
      }
      snprintf( promptstr, promptlen, "%s/%s > ", globalpos.ns->idstr, globalcwdpath );

      char* inputline = readline( promptstr );
      free( promptstr );

      if ( inputline == NULL ) {
         printf(OUTPREFX "Hit EOF on input\n");
         printf(OUTPREFX "Terminating...\n");
         break;
      }

      // parse the input command
      char* parse = inputline;
      char repchar = 0;
      char anycontent = 0;
      while (*parse != '\0') {
         parse++;
         if (anycontent  &&  *parse == ' ') {
            repchar = 1;
            break;
         }
         else { anycontent = 1; }
      }
      // check for empty command right away
      if (!anycontent) {
         // no-op
         continue;
      }
      // add this command line to our history
      add_history(inputline);
      // insert a NULL char, to allow for easy command comparison
      if ( repchar ) { *parse = '\0'; }
      // check for program exit
      if (strcmp(inputline, "exit") == 0 || strcmp(inputline, "quit") == 0) {
         printf(OUTPREFX "Terminating...\n");
         free( inputline );
         break;
      }
      // check for 'help' command
      if (strcmp(inputline, "help") == 0) {
         if (repchar) {
            *parse = ' ';
         } // undo input line edit
         usage(inputline);
         continue;
      }

      // skip to the next comand arg
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
         if (open_command(config, &(globalpos), &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "shift") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (shift_command(config, &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "tags") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (tags_command(config, &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "ref") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (ref_command(config, &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "obj") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (obj_command(config, config_path, &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "bounds") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (bounds_command(config, &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "refresh") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (refresh_command(config, &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "recovery") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (recovery_command(config, &(state), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if (strcmp(inputline, "cd") == 0) {
         errno = 0;
         retval = -1; // assume failure
         if (cd_command(config, &(globalcwdpath), &(globalpos), parse) == 0) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "ls" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         if (ls_command(config, &(globalpos), parse) == 0) {
            retval = 0; // note success
         }
      }
      else {
         printf(OUTPREFX "ERROR: Unrecognized command: \"%s\"\n", inputline);
      }

      free(inputline);

   }

   // cleanup
   if (state.ftag.ctag) {
      free(state.ftag.ctag);
   }
   if (state.ftag.streamid) {
      free(state.ftag.streamid);
   }
   if ( state.reftable  &&  state.reftable != state.pos.ns->prepo->metascheme.reftable ) {
      HASH_NODE* nodelist = NULL;
      size_t nodecount = 0;
      if ( hash_term( state.reftable, &(nodelist), &(nodecount) ) ){
         printf(OUTPREFX "WARNING: Failed to properly destroy custom reference HASH_TABLE\n");
      }
      else {
         size_t nodeindex = 0;
         for ( ; nodeindex < nodecount; nodeindex++ ) {
            if ( (nodelist + nodeindex)->name ) { free( (nodelist + nodeindex)->name ); }
         }
         free( nodelist );
      }
   }
   if ( state.oftagstr ) { free( state.oftagstr ); }
   if ( state.pos.ns  &&  config_abandonposition(&state.pos)) {
      printf(OUTPREFX "WARNING: Failed to properly destroy tgt marfs position\n");
   }
   if ( globalcwdpath ) { free( globalcwdpath ); }
   if (config_abandonposition(&globalpos)) {
      printf(OUTPREFX "WARNING: Failed to properly destroy active marfs position\n");
   }
   return retval;
}


int main(int argc, const char** argv) {
   errno = 0; // init to zero (apparently not guaranteed)
   char* config_path = getenv( "MARFS_CONFIG_PATH" ); // check for config env var

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
      printf(OUTPREFX "   -c : Path of the MarFS config file ( overrides env value )\n");
      printf(OUTPREFX "   -h : Print this usage info\n");
      return -1;
   }

   // verify that a config was defined
   if (config_path == NULL) {
      printf(OUTPREFX "no config path defined ( '-c' arg or 'MARFS_CONFIG_PATH' env var )\n");
      return -1;
   }

   // read in the marfs config
   pthread_mutex_t erasurelock;
   if ( pthread_mutex_init( &erasurelock, NULL ) ) {
      printf( "failed to initialize erasure lock\n" );
      return -1;
   }
   marfs_config* config = config_init(config_path,&erasurelock);
   if (config == NULL) {
      printf(OUTPREFX "ERROR: Failed to initialize config: \"%s\" ( %s )\n",
         config_path, strerror(errno));
      pthread_mutex_destroy( &erasurelock );
      return -1;
   }
   printf(OUTPREFX "marfs config loaded...\n");

   // verify that FUSE appears to be mounted
   struct stat fstatval;
   if ( stat( config->mountpoint, &(fstatval) ) == 0 ) {
      // duplicate FUSE mount path ( for modification )
      char* fmount = strdup( config->mountpoint );
      if ( fmount ) {
         // iterate through the path, looking for the final '/' char
         char* lastsep = fmount;
         char* parse = fmount;
         while ( *parse != '\0' ) {
            if ( *parse == '/' ) { lastsep = parse; }
            parse++;
         }
         *lastsep = '\0'; // truncate off the final path element
         // stat the parent path
         struct stat pstatval;
         int statres;
         if ( lastsep == fmount ) { // FUSE is mounted at the global FS root
            statres = stat( "/", &(pstatval) );
         }
         else {
            statres = stat( fmount, &(pstatval) );
         }
         // check for error / device match between FUSE and parent paths
         if ( statres ) {
            printf(OUTPREFX "WARNING: Failed to stat parent path of MarFS FUSE mount \"%s\" ( %s ) : Path autocompletion is disabled\n",
                   config->mountpoint, strerror(errno) );
            globalusepathcomp = 0;
         }
         else if ( fstatval.st_dev == pstatval.st_dev ) {
            printf(OUTPREFX "WARNING: MarFS FUSE does not appear to be mounted at \"%s\" : Path autocompletion is disabled\n",
                   config->mountpoint );
            globalusepathcomp = 0;
         }
         free( fmount );
      }
      else {
         printf(OUTPREFX "WARNING: Failed to duplicate path of MarFS FUSE mount \"%s\" ( %s ) : Path autocompletion is disabled\n",
                config->mountpoint, strerror(errno) );
         globalusepathcomp = 0;
      }
   }
   else {
      printf(OUTPREFX "WARNING: Failed to stat MarFS FUSE mount \"%s\" ( %s ) : Path autocompletion is disabled\n",
             config->mountpoint, strerror(errno) );
      globalusepathcomp = 0;
   }

   // attempt to chdir into FUSE instance
   if ( globalusepathcomp  &&  chdir( config->mountpoint ) ) {
      printf(OUTPREFX "WARNING: Failed to chdir() into MarFS FUSE at \"%s\" ( %s ) : Path autocompletion is disabled\n",
             config->mountpoint, strerror(errno) );
      globalusepathcomp = 0;
   }

   // enter the main command loop
   int retval = 0;
   if (command_loop(config, config_path)) {
      retval = -1;
   }

   // terminate the marfs config
   if (config_term(config)) {
      printf(OUTPREFX "WARNING: Failed to properly terminate MarFS config ( %s )\n",
         strerror(errno));
      pthread_mutex_destroy( &erasurelock );
      return -1;
   }

   pthread_mutex_destroy( &erasurelock );
   return retval;
}

