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
#include "datastream.h"
#include "general_include/numdigits.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>

#define PROGNAME "marfs-streamutil"
#define OUTPREFX PROGNAME ": "


typedef struct argopts_struct {
   char streamnum;
   int streamnumval;
   char path;
   char* pathval;
   char mode;
   mode_t modeval;
   char ctag;
   char* ctagval;
   char type;
   STREAM_TYPE typeval;
   char bytes;
   size_t bytesval;
   char ifile;
   char ofile;
   char* iofileval;
   char offset;
   off_t offsetval;
   char seekfrom;
   int seekfromval;
   char chunknum;
   size_t chunknumval;
   char length;
   off_t lengthval;
   char abort;
} argopts; 


void free_argopts( argopts* opts ) {
   if ( opts->pathval ) { free( opts->pathval ); }
   if ( opts->ctagval ) { free( opts->ctagval ); }
   if ( opts->iofileval ) { free( opts->iofileval ); }
}


// Show all the usage options in one place, for easy reference
// An arrow appears next to the one you tried to use.
//
void usage(const char *op)
{

   printf("Usage: <op> [<args> ...]\n");
   printf("  Where <op> and <args> are like one of the following:\n");
   printf("\n");

   const char* cmd = op;
   if ( !strncmp(op, "help ", 5) ) {
      // check if the 'help' command is targetting a specific op
      while ( *cmd != '\0'  &&  *cmd != ' ' ) { cmd++; }
      if ( *cmd == ' '  &&  *(cmd + 1) != '\0' ) { cmd++; }
      else { cmd = op; } // no specific command specified
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

   USAGE( "create",
          "[-s stream-num] -p path [-m mode] [-c ctag]",
          "Create a new file, associated with the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -p path       : Path of the file to be created\n"
          "       -m mode       : Mode value of the file to be created ( octal )\n"
          "                        If omitted, a value of 0600 will be used\n"
          "       -c ctag       : Specified a Client Tag string for a new datastream\n" )

   USAGE( "open",
          "[-s stream-num] -p path -t type",
          "Open a file, associated with the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -p path       : Path of the file to be created\n"
          "       -t type       : Specifies the type of the new stream ( 'read'/'edit )\n" )

   USAGE( "repack",
          "[-s stream-num] -p refpath [-c ctag]",
          "Create a new file, associated with the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -p refpath    : REFERENCE path of the file to be repacked\n"
          "       -c ctag       : Specified a Client Tag string for a new datastream\n" )

   USAGE( "release",
          "[-s stream-num]",
          "Release the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n" )

   USAGE( "close",
          "[-s stream-num]",
          "Close the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n" )

   USAGE( "read",
          "[-s stream-num] [-b bytes] [-o iofile [-@ outputoffset [-f seekfrom] ] ]",
          "Read from the given datastream",
          "       -s stream-num   : Specifies a new stream target for this operation\n"
          "       -b bytes        : Number of bytes to be read\n"
          "       -o iofile       : Specifies a file to output read data to\n"
          "       -@ outputoffset : Specifies an offset within the 'iofile' to output to\n"
          "       -f seekfrom     : Specifies a start location for the output file seek\n"
          "                        ( either 'set', 'cur', or 'end' )\n" )

   USAGE( "write",
          "[-s stream-num] [-b bytes] [-i iofile [-@ inputoffset [-f seekfrom] ] ]",
          "Write to the given datastream",
          "       -s stream-num   : Specifies a new stream target for this operation\n"
          "       -b bytes        : Number of bytes to be written\n"
          "       -i iofile       : Specifies a file to read input data from\n"
          "       -@ outputoffset : Specifies an offset within the 'iofile' to read from\n"
          "       -f seekfrom     : Specifies a start location for the input file seek\n"
          "                        ( either 'set', 'cur', or 'end' )\n" )

   USAGE( "setrecovpath",
          "[-s stream-num] -p path",
          "Set the recovery path of the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -p path       : New path to be included in recovery info\n" )

   USAGE( "seek",
          "[-s stream-num] -@ offset -f seekfrom",
          "Seek the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -@ offset     : Specifies an offset to seek to\n"
          "       -f seekfrom   : Specifies a start location for the seek\n"
          "                      ( either 'set', 'cur', or 'end' )\n" )

   USAGE( "chunkbounds",
          "[-s stream-num] [-n chunknum]",
          "Identify chunk boundries for the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -n chunknum   : Specifies a specific data chunk to query\n" )

   USAGE( "extend",
          "[-s stream-num] -l length",
          "Extend the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -l length     : Byte length to extend the current file to\n" )

   USAGE( "truncate",
          "[-s stream-num] -l length",
          "Truncate the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -l length     : Byte length to truncate the current file to\n" )

   USAGE( "utime",
          "[-s stream-num] -i inputfile",
          "Truncate the given datastream",
          "       -s stream-num : Specifies a new stream target for this operation\n"
          "       -i inputfile  : Name of a posix file to use as an atime/mtime source\n" )

   USAGE( "streamlist",
          "[-s stream-num]",
          "Print a list of all active datastream references", "" )

   USAGE( "ns",
          "[-p ns-path]",
          "Print or update the current MarFS namespace target of this program",
          "       -p ns-path    : Path to a new NS target" )

   USAGE( "ls",
          "[-p dirpath]",
          "Output contents of the current NS or target subdirectory",
          "       -p dirpath    : Path to list contents of" )

   USAGE( "mkdir",
          "-p dirpath [-m modeval]",
          "Create a new subdirectory of the current NS",
          "       -p dirpath    : Path of the directory to be created"
          "       -m mode       : Mode value of the dir to be created ( octal )\n"
          "                        If omitted, a value of 0700 will be used\n" )

   USAGE( "( exit | quit )",
          "",
          "Terminate ( active streams will be released )", "" )

   USAGE( "help", "[CMD]", "Print this usage info",
          "       CMD : A specific command, for which to print detailed info\n" )

   printf("\n");

#undef USAGE
}

int ns_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check args
   if ( !(opts->path ) ) {
      // print our current NS target
      printf( "Current Namespace Target: \"%s\"\n\n", pos->ns->idstr );
      return 0;
   }
   // update our position to target the new path
   char* modpath = strdup( opts->pathval );
   if ( modpath == NULL ) {
      printf( OUTPREFX "ERROR: Failed to create duplicate \"%s\" path for config traversal\n", opts->pathval );
      return -1;
   }
   marfs_ns* origns = pos->ns;
   int targetdepth;
   if ( (targetdepth = config_traverse( config, pos, &(modpath), 1 )) < 0 ) {
      printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
              opts->pathval );
      free( modpath );
      return -1;
   }
   free( modpath );
   if ( origns != pos->ns ) {
      if ( targetdepth == 0 ) {
         // the target path corresponds exactly to a MarFS NS
         if ( pos->ctxt == NULL ) {
            // our MDAL_CTXT was destroyed, so we must re-establish
            if ( config_fortifyposition( pos ) ) {
               printf( "Failed to establish new MDAL_CTXT for NS: \"%s\"\n", pos->ns->idstr );
               config_abandonposition( pos );
               return -1;
            }
         }
      }
      else {
         printf( "WARNING: Target is not a MarFS NS itself\n" );
      }
      printf( "New Namespace Target: \"%s\"\n\n", pos->ns->idstr );
   }
   else {
      printf( "Namespace Target Unchanged: \"%s\"\n\n", pos->ns->idstr );
   }
   return 0;
}

int create_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->path) ) {
      printf( OUTPREFX "ERROR: 'create' command is missing required '-p' arg\n" );
      usage( "help create" );
      return -1;
   }
   if ( !(opts->mode) ) {
      // populate a default mode value
      opts->modeval = 0600;
   }
   // perform path traversal to identify marfs position
   char* modpath = strdup( opts->pathval );
   if ( modpath == NULL ) {
      printf( OUTPREFX "ERROR: Failed to create duplicate \"%s\" path for config traversal\n", opts->pathval );
      return -1;
   }
   marfs_ns* origns = pos->ns;
   // duplicate our position, so that traversal doesn't destroy it
   marfs_position duppos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if ( config_duplicateposition( pos, &duppos ) ) {
      printf( OUTPREFX "ERROR: Failed to duplicate active MarFS position value\n" );
      free( modpath );
      return -1;
   }
   int targetdepth;
   if ( (targetdepth = config_traverse( config, &duppos, &(modpath), 1 )) < 0 ) {
      printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
              opts->pathval );
      free( modpath );
      return -1;
   }
   if ( origns != duppos.ns ) {
      if ( targetdepth == 0 ) {
         // the target path corresponds exactly to a MarFS NS
         printf( "WARNING: Targetting a MarFS NS path directly\n" );
         if ( duppos.ctxt == NULL ) {
            // our MDAL_CTXT was destroyed and must be re-established
            if ( config_fortifyposition( &duppos ) ) {
               printf( "Failed to establish new MDAL_CTXT for NS: \"%s\"\n", duppos.ns->idstr );
               config_abandonposition( &duppos );
               free( modpath );
               return -1;
            }
         }
      }
   }
   int retval = datastream_create( stream, modpath, &duppos, opts->modeval,
                                   (opts->ctag) ? opts->ctagval : config->ctag );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_create(): %d (%s)\n",
              retval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully created target file \"%s\"\n", opts->pathval );
   }
   printf( "\n" );
   config_abandonposition( &duppos );
   free( modpath );
   return retval;
}

int open_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->path)  ||  !(opts->type) ) {
      printf( OUTPREFX "ERROR: 'open' command is missing required '-p'/'-t' args\n" );
      usage( "help open" );
      return -1;
   }
   // perform path traversal to identify marfs position
   char* modpath = strdup( opts->pathval );
   if ( modpath == NULL ) {
      printf( OUTPREFX "ERROR: Failed to create duplicate \"%s\" path for config traversal\n", opts->pathval );
      return -1;
   }
   marfs_ns* origns = pos->ns;
   // duplicate our position, so that traversal doesn't destroy it
   marfs_position duppos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if ( config_duplicateposition( pos, &duppos ) ) {
      printf( OUTPREFX "ERROR: Failed to duplicate active MarFS position value\n" );
      free( modpath );
      return -1;
   }
   int targetdepth;
   if ( (targetdepth = config_traverse( config, &duppos, &(modpath), 1 )) < 0 ) {
      printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
              opts->pathval );
      config_abandonposition( &duppos );
      free( modpath );
      return -1;
   }
   if ( origns != duppos.ns ) {
      if ( targetdepth == 0 ) {
         // the target path corresponds exactly to a MarFS NS
         printf( "WARNING: Targetting a MarFS NS path directly\n" );
         if ( duppos.ctxt == NULL ) {
            // our MDAL_CTXT was destroyed and must be re-established
            if ( config_fortifyposition( &duppos ) ) {
               printf( "Failed to establish new MDAL_CTXT for NS: \"%s\"\n", duppos.ns->idstr );
               config_abandonposition( &duppos );
               free( modpath );
               return -1;
            }
         }
      }
   }
   int retval = datastream_open( stream, opts->typeval, modpath, &duppos, NULL );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_open(): %d (%s)\n",
              retval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully opened target file \"%s\"\n", opts->pathval );
   }
   printf( "\n" );
   config_abandonposition( &duppos );
   free( modpath );
   return retval;
}

int repack_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->path) ) {
      printf( OUTPREFX "ERROR: 'repack' command is missing required '-p' arg\n" );
      usage( "help repack" );
      return -1;
   }
   int retval;
   if ( !(opts->abort) ) {
      // startup the repack stream
      retval = datastream_repack( stream, opts->pathval, pos, (opts->ctag) ? opts->ctagval : config->ctag );
      if ( retval ) {
         printf( OUTPREFX "ERROR: Failure of datastream_repack(): %d (%s)\n",
                 retval, strerror(errno) );
      }
      else {
         printf( OUTPREFX "Successfully started repack of target file \"%s\"\n", opts->pathval );
      }
   }
   else {
      // verify that the specified path is actually a repack marker
      char entrytype = 0;
      ssize_t fileno = ftag_metainfo( opts->pathval, &(entrytype) );
      if ( fileno < 0 ) {
         printf( OUTPREFX "ERROR: The specified reference path does not appear to be a repack marker: \"%s\"\n",
                 opts->pathval );
         return -1;
      }
      // cleanup the specified repack marker
      retval = datastream_repack_cleanup( opts->pathval, pos );
      if ( retval < 0 ) {
         printf( OUTPREFX "ERROR: Failure of datastream_repack_cleanup(): %d (%s)\n",
                 retval, strerror(errno) );
      }
      else if ( retval ) {
         printf( OUTPREFX "Successfully cleaned up repack marker \"%s\" ( repack was reverted )\n", opts->pathval );
      }
      else {
         printf( OUTPREFX "Successfully cleaned up repack marker \"%s\" ( repack was completed )\n", opts->pathval );
         retval = 1; // set this to one and zero out errno to avoid having the main loop assume we have a new datastream
         errno = 0;
      }
   }
   printf( "\n" );
   return retval;
}

int release_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   int retval = datastream_release( stream );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_release(): %d (%s)\n",
              retval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully released stream\n" );
   }
   printf( "\n" );
   return 0; // always consider the stream as released, even if an error occurred
}

int close_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   int retval = datastream_close( stream );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_close(): %d (%s)\n",
              retval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully closed stream\n" );
   }
   printf( "\n" );
   return 0; // always consider the stream as released, even if an error occurred
}

int read_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // allocate 4K buffer for I/O
   char iobuf[4096] = {0};
   int iofile = -1;
   if ( opts->ofile ) {
      iofile = open( opts->iofileval, O_WRONLY | O_CREAT, 0600 );
      if ( iofile < 1 ) {
         printf( OUTPREFX "ERROR: Failed to open output file: \"%s\" (%s)\n",
                 opts->iofileval, strerror(errno) );
         return -1;
      }
      if ( opts->offset ) {
         if ( lseek( iofile, opts->offsetval,
                     (opts->seekfrom) ? opts->seekfromval : SEEK_SET ) < 0 ) {
            printf( OUTPREFX "ERROR: Failed to seek output file: \"%s\" (%s)\n",
                 opts->iofileval, strerror(errno) );
            return -1;
         }
      }
   }
   // perform read operations until complete
   size_t readbytes = 0;
   while ( !(opts->bytes)  ||  readbytes < opts->bytesval ) {
      // establish the size of this read op
      size_t toread = 4096;
      if ( opts->bytes  &&  (readbytes + toread) > opts->bytesval ) {
         toread = opts->bytesval - readbytes;
      }
      // perform the read
      ssize_t readres = datastream_read( stream, iobuf, toread );
      if ( readres < 0 ) {
         printf( OUTPREFX "ERROR: Read failure after %zu bytes read (%s)\n",
                 readbytes, strerror(errno) );
         if ( opts->ofile ) { close( iofile ); return -1; }
      }
      readbytes += readres;
      // output the read data ( if requested )
      if ( opts->ofile  &&  write( iofile, iobuf, readres ) != readres ) {
         printf( OUTPREFX "ERROR: Failed to output %zd read bytes to ouput file after %zu bytes read (%s)\n", readres, readbytes, strerror(errno) );
         close( iofile );
         return -1;
      }
      // check for an under-read condition
      if ( readres < toread ) { break; } // terminate early
   }

   // close our output file ( if present )
   if ( opts->ofile ) {
      if ( close( iofile ) ) {
         printf( OUTPREFX "WARNING: Failed to close output file\n" );
      }
   }

   // check if we failed to read all requested bytes
   if ( opts->bytes  &&  readbytes < opts->bytesval ) {
      printf( OUTPREFX "ERROR: Read terminated early w/ only %zu bytes read\n", readbytes );
      return -1;
   }

   printf( OUTPREFX "%zu bytes read\n", readbytes );
   printf( "\n" );

   return 0;
}

int write_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->bytes)  &&  !(opts->ifile) ) {
      printf( OUTPREFX "ERROR: 'write' command requires at least a '-b' or '-i' arg\n" );
      usage( "help write" );
      return -1;
   }
   // allocate 4K buffer for I/O
   char iobuf[4096] = {0};
   int iofile = -1;
   if ( opts->ifile ) {
      iofile = open( opts->iofileval, O_RDONLY );
      if ( iofile < 1 ) {
         printf( OUTPREFX "ERROR: Failed to open input file: \"%s\" (%s)\n",
                 opts->iofileval, strerror(errno) );
         return -1;
      }
      if ( opts->offset ) {
         if ( lseek( iofile, opts->offsetval,
                     (opts->seekfrom) ? opts->seekfromval : SEEK_SET ) < 0 ) {
            printf( OUTPREFX "ERROR: Failed to seek input file: \"%s\" (%s)\n",
                    opts->iofileval, strerror(errno) );
            return -1;
         }
      }
   }
   // perform write operations until complete
   size_t writebytes = 0;
   while ( !(opts->bytes)  ||  writebytes < opts->bytesval ) {
      // establish the size of this write op
      size_t towrite = 4096;
      if ( opts->bytes  &&  (writebytes + towrite) > opts->bytesval ) {
         towrite = opts->bytesval - writebytes;
      }
      // read input data ( if requested )
      ssize_t readres = towrite;
      if ( opts->ifile ) {
         readres = read( iofile, iobuf, towrite );
         if ( readres < 0 ) {
            printf( OUTPREFX "ERROR: Input file read failure after %zu bytes writen (%s)\n",
                    writebytes, strerror(errno) );
            if ( opts->ofile ) { close( iofile ); return -1; }
         }
      }
      // write to the target stream
      ssize_t writeres = datastream_write( stream, iobuf, readres );
      if ( writeres != readres ) {
         printf( OUTPREFX "ERROR: Unexpected datastream_write() of %zu bytes after %zu bytes written: %zd (%s)\n", towrite, writebytes, writeres, strerror(errno) );
         if ( opts->ifile ) { close( iofile ); }
         return -1;
      }
      writebytes += writeres;
      // check for an under-read condition
      if ( readres < towrite ) { break; } // terminate early
   }

   // close our input file ( if present )
   if ( opts->ofile ) {
      if ( close( iofile ) ) {
         printf( OUTPREFX "WARNING: Failed to close input file\n" );
      }
   }

   // check if we failed to read all requested bytes
   if ( opts->bytes  &&  writebytes < opts->bytesval ) {
      printf( OUTPREFX "ERROR: Read terminated early w/ only %zu bytes written\n", writebytes );
      return -1;
   }

   printf( OUTPREFX "%zu bytes written\n", writebytes );
   printf( "\n" );


   return 0;
}

int setrecovpath_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->path) ) {
      printf( OUTPREFX "ERROR: 'setrecovpath' command is missing required '-p' arg\n" );
      usage( "help setrecovpath" );
      return -1;
   }
   // perform the setrecoverypath op
   int retval = datastream_setrecoverypath( stream, opts->pathval );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failed to set stream recovery path to \"%s\" (%s)\n",
              opts->pathval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully set recovery path to \"%s\"\n", opts->pathval );
   }
   printf( "\n" );
   return retval;
}

int seek_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->offset)  ||  !(opts->seekfrom) ) {
      printf( OUTPREFX "ERROR: 'seek' command is missing required '-@'/'-f' args\n" );
      usage( "help seek" );
      return -1;
   }
   // perform the seek op
   off_t retval = datastream_seek( stream, opts->offsetval, opts->seekfromval );
   if ( retval < 0 ) {
      printf( OUTPREFX "ERROR: Failed to seek stream(%s)\n",
              strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfull seek to resulting offset %zd\n", retval );
   }
   printf( "\n" );
   return retval;
}

int chunkbounds_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // loop over all appropriate chunks
   printf( "%-5s   %-15s   %-15s\n", "Chunk", "Size", "Offset" );
   errno = 0;
   int chunkindex = 0;
   if ( opts->chunknum ) { chunkindex = opts->chunknumval; }
   while ( !(opts->chunknum)  ||  chunkindex <= opts->chunknumval ) {
      // identify bounds of this chunk
      off_t offset = 0;
      size_t size = 0;
      if ( datastream_chunkbounds( stream, chunkindex, &(offset), &(size) ) ) {
         // if we were specifically asked to target this chunk, this is a problem
         if ( opts->chunknum  ||  errno != EINVAL ) {
            printf( OUTPREFX "ERROR: Failed to identify bounds of chunk %d (%s)\n",
                    chunkindex, strerror(errno) );
            return -1;
         }
         break; // no data chunks remain
      }
      printf( "%-5d   %-15zu   %-15zu\n", chunkindex, size, offset );
      chunkindex++;
   }
   printf( "\n" );
   return 0;
}

int extend_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->length) ) {
      printf( OUTPREFX "ERROR: 'extend' command is missing required '-l' arg\n" );
      usage( "help extend" );
      return -1;
   }
   // perform the extend op
   int retval = datastream_extend( stream, opts->lengthval );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failed to extend stream to %zu bytes (%s)\n",
              opts->lengthval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully extended stream to %zu bytes\n", opts->lengthval );
   }
   printf( "\n" );
   return retval;
}

int truncate_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->length) ) {
      printf( OUTPREFX "ERROR: 'truncate' command is missing required '-l' arg\n" );
      usage( "help truncate" );
      return -1;
   }
   // perform the truncate op
   int retval = datastream_truncate( stream, opts->lengthval );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failed to truncate stream to \"%zu\" (%s)\n",
              opts->lengthval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully truncated stream to %zu bytes\n", opts->lengthval );
   }
   printf( "\n" );
   return retval;
}

int utime_command( marfs_config* config, marfs_position* pos, DATASTREAM* stream, argopts* opts ) {
   printf( "\n" );
   // check for required args
   if ( !(opts->ifile) ) {
      printf( OUTPREFX "ERROR: 'utime' command is missing required '-i' arg\n" );
      usage( "help utime" );
      return -1;
   }
   // stat the input file
   struct stat stval;
   if ( stat( opts->iofileval, &(stval) ) ) {
      printf( OUTPREFX "ERROR: Failed to stat input file: \"%s\" (%s)\n",
              opts->iofileval, strerror(errno) );
      return -1;
   }
   // perform the utimens op
   struct timespec times[2];
   times[0].tv_sec = stval.st_atim.tv_sec;
   times[0].tv_nsec = stval.st_atim.tv_nsec;
   times[1].tv_sec = stval.st_mtim.tv_sec;
   times[1].tv_nsec = stval.st_mtim.tv_nsec;
   int retval = datastream_utimens( stream, times );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failed to set time values on stream to match input file \"%s\" (%s)\n", opts->iofileval, strerror(errno) );
   }
   else {
      printf( OUTPREFX "Successfully set time values on stream\n" );
   }
   printf( "\n" );
   return retval;
}

int command_loop( marfs_config* config ) {
   // allocate list structures
   int streamalloc = 10;
   DATASTREAM* streamlist = calloc( streamalloc, sizeof( DATASTREAM ) );
   if ( streamlist == NULL ) {
      printf( OUTPREFX "ERROR: Failed to allocate DATASTREAM list\n" );
      return -1;
   }
   char** streamdesc = calloc( streamalloc, sizeof( char* ) );
   if ( streamdesc == NULL ) {
      printf( OUTPREFX "ERROR: Failed to allocate stream description list\n" );
      free( streamlist );
      return -1;
   }

   // establish a marfs root position
   marfs_position pos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if ( config_establishposition( &pos, config ) ) {
      printf( OUTPREFX "ERROR: Failed to establish a root NS position\n" );
      free( streamlist );
      free( streamdesc );
      return -1;
   }
   printf( "Initial Namespace Target : \"%s\"\n", pos.ns->idstr );

   // infinite loop, processing user commands
   printf( OUTPREFX "Ready for user commands\n" );
   int tgtstream = 0;
   int retval = 0;
   while ( 1 ) {
      printf( "> " );
      fflush( stdout );
      // read in a new line from stdin ( 4096 char limit )
      char inputline[4097] = {0}; // init to NULL bytes
      if ( scanf( "%4096[^\n]", inputline ) < 0 ) {
         printf( OUTPREFX "ERROR: Failed to read user input\n" );
         retval = -1;
         break;
      }
      fgetc( stdin ); // to clear newline char
      if ( inputline[4095] != '\0' ) {
         printf( OUTPREFX "ERROR: Input command exceeds parsing limit of 4096 chars\n" );
         retval = -1;
         break;
      }

      // parse the input command
      char* parse = inputline;
      char repchar = 0;
      while ( *parse != '\0' ) {
         parse++;
         if ( *parse == ' ' ) { *parse = '\0'; repchar = 1; }
      }
      // check for program exit right away
      if ( strcmp( inputline, "exit" ) == 0  ||  strcmp( inputline, "quit" ) == 0 ) {
         printf( OUTPREFX "Terminating...\n" );
         break;
      }
      // check for 'help' command right away
      if ( strcmp( inputline, "help" ) == 0 ) {
         if ( repchar ) { *parse = ' '; } // revert the input edit
         usage( inputline );
         continue;
      }
      // check for empty command right away
      if ( strcmp( inputline, "") == 0 ) {
         // no-op
         continue;
      }
      if ( repchar ) {
         parse++; // proceed to the char following ' '
      }
      // parse all command arguments
      argopts inputopts = {
         .streamnum = 0,
         .streamnumval = 0,
         .path = 0,
         .pathval = NULL,
         .mode = 0,
         .modeval = 0,
         .ctag = 0,
         .ctagval = NULL,
         .type = 0,
         .typeval = 0,
         .bytes = 0,
         .bytesval = 0,
         .ifile = 0,
         .ofile = 0,
         .iofileval = NULL,
         .offset = 0,
         .offsetval = 0,
         .seekfrom = 0,
         .seekfromval = 0,
         .chunknum = 0,
         .chunknumval = 0,
         .length = 0,
         .lengthval = 0,
         .abort = 0
      };
      char argerror = 0;
      char* argparse = strtok( parse, " " );
      while ( argparse ) {
         char argchar = '\0';
         if ( *argparse != '-' ) {
            printf( OUTPREFX "ERROR: Unrecognized argument: \"%s\"\n", argparse );
            argerror = 1;
            break;
         }
         argchar = *(argparse + 1);
         // proceed to the next arguemnt substring
         argparse = strtok( NULL, " " );
         if ( argparse == NULL ) {
            printf( OUTPREFX "ERROR: '-%c' argument lacks a value\n", argchar );
            argerror = 1;
            break;
         }
         // check arg type
         char* endptr = NULL;
         unsigned long long parseval;
         switch( argchar ) {
            case 's':
               if ( inputopts.streamnum ) {
                  printf( OUTPREFX "ERROR: Duplicate '-s' argument detected\n" );
                  argerror = 1;
                  break;
               }
               parseval = strtoull( argparse, &(endptr), 10 );
               if ( parseval > INT_MAX ) {
                  printf( OUTPREFX "ERROR: Streamnum value exceeds type bounds: \"%s\"\n",
                          argparse );
                  argerror = 1;
                  break;
               }
               inputopts.streamnum = 1;
               inputopts.streamnumval = (int)parseval;
               break;
            case 'p':
               if ( inputopts.path ) {
                  printf( OUTPREFX "ERROR: Duplicate '-p' argument detected\n" );
                  argerror = 1;
                  break;
               }
               inputopts.path = 1;
               inputopts.pathval = strdup( argparse );
               if ( inputopts.pathval == NULL ) {
                  printf( OUTPREFX "ERROR: Failed to allocate space for 'path' value: \"%s\"\n", argparse );
                  argerror = 1;
                  break;
               }
               break;
            case 'm':
               if ( inputopts.mode ) {
                  printf( OUTPREFX "ERROR: Duplicate '-m' argument detected\n" );
                  argerror = 1;
                  break;
               }
               parseval = strtoull( argparse, &(endptr), 8 );
               if ( parseval > 07777 ) {
                  printf( OUTPREFX "ERROR: Mode value exceeds type bounds: \"%s\"\n",
                          argparse );
                  argerror = 1;
                  break;
               }
               inputopts.mode = 1;
               inputopts.modeval = (mode_t)parseval;
               break;
            case 'c':
               if ( inputopts.ctag ) {
                  printf( OUTPREFX "ERROR: Duplicate '-c' argument detected\n" );
                  argerror = 1;
                  break;
               }
               inputopts.ctag = 1;
               inputopts.ctagval = strdup( argparse );
               if ( inputopts.ctagval == NULL ) {
                  printf( OUTPREFX "ERROR: Failed to allocate space for 'ctag' value: \"%s\"\n", argparse );
                  argerror = 1;
                  break;
               }
               break;
            case 't':
               if ( inputopts.type ) {
                  printf( OUTPREFX "ERROR: Duplicate '-t' argument detected\n" );
                  argerror = 1;
                  break;
               }
               inputopts.type = 1;
               if ( strcasecmp( argparse, "edit" ) == 0 ) {
                  inputopts.typeval = EDIT_STREAM;
               }
               else if ( strcasecmp( argparse, "read" ) == 0 ) {
                  inputopts.typeval = READ_STREAM;
               }
               else {
                  printf( OUTPREFX "ERROR: '-t' argument is unrecognized: \"%s\"\n",
                          argparse );
                  printf( OUTPREFX "ERROR: Acceptable values are 'read'/'edit'\n" );
                  argerror = 1;
                  break;
               }
               break;
            case 'b':
               if ( inputopts.bytes ) {
                  printf( OUTPREFX "ERROR: Duplicate '-b' argument detected\n" );
                  argerror = 1;
                  break;
               }
               parseval = strtoull( argparse, &(endptr), 10 );
               if ( parseval > SIZE_MAX ) {
                  printf( OUTPREFX "ERROR: Bytes value exceeds type bounds: \"%s\"\n",
                          argparse );
                  argerror = 1;
                  break;
               }
               inputopts.bytes = 1;
               inputopts.bytesval = (size_t)parseval;
               break;
            case 'i':
               if ( inputopts.ifile  ||  inputopts.ofile ) {
                  printf( OUTPREFX "ERROR: Duplicate 'iofile' argument detected\n" );
                  argerror = 1;
                  break;
               }
               inputopts.ifile = 1;
               inputopts.iofileval = strdup( argparse );
               if ( inputopts.iofileval == NULL ) {
                  printf( OUTPREFX "ERROR: Failed to allocate space for 'iofile' value: \"%s\"\n", argparse );
                  argerror = 1;
                  break;
               }
               break;
            case 'o':
               if ( inputopts.ifile  ||  inputopts.ofile ) {
                  printf( OUTPREFX "ERROR: Duplicate 'iofile' argument detected\n" );
                  argerror = 1;
                  break;
               }
               inputopts.ofile = 1;
               inputopts.iofileval = strdup( argparse );
               if ( inputopts.iofileval == NULL ) {
                  printf( OUTPREFX "ERROR: Failed to allocate space for 'iofile' value: \"%s\"\n", argparse );
                  argerror = 1;
                  break;
               }
               break;
            case '@':
               if ( inputopts.offset ) {
                  printf( OUTPREFX "ERROR: Duplicate '-@' argument detected\n" );
                  argerror = 1;
                  break;
               }
               parseval = strtoull( argparse, &(endptr), 10 );
               if ( parseval > INT_MAX ) {
                  printf( OUTPREFX "ERROR: Offset value exceeds type bounds: \"%s\"\n",
                          argparse );
                  argerror = 1;
                  break;
               }
               inputopts.offset = 1;
               inputopts.offsetval = (off_t)parseval;
               break;
            case 'f':
               if ( inputopts.seekfrom ) {
                  printf( OUTPREFX "ERROR: Duplicate '-f' argument detected\n" );
                  argerror = 1;
                  break;
               }
               inputopts.seekfrom = 1;
               if ( strcasecmp( argparse, "set" ) == 0 ) {
                  inputopts.seekfromval = SEEK_SET;
               }
               else if ( strcasecmp( argparse, "cur" ) == 0 ) {
                  inputopts.seekfromval = SEEK_CUR;
               }
               else if ( strcasecmp( argparse, "end" ) == 0 ) {
                  inputopts.seekfromval = SEEK_END;
               }
               else {
                  printf( OUTPREFX "ERROR: '-f' argument is unrecognized: \"%s\"\n",
                          argparse );
                  printf( OUTPREFX "ERROR: Acceptable values are 'set'/'cur'/'end'\n" );
                  argerror = 1;
                  break;
               }
               break;
            case 'n':
               if ( inputopts.chunknum ) {
                  printf( OUTPREFX "ERROR: Duplicate '-n' argument detected\n" );
                  argerror = 1;
                  break;
               }
               parseval = strtoull( argparse, &(endptr), 10 );
               if ( parseval > INT_MAX ) {
                  printf( OUTPREFX "ERROR: Chunknum value exceeds type bounds: \"%s\"\n",
                          argparse );
                  argerror = 1;
                  break;
               }
               inputopts.chunknum = 1;
               inputopts.chunknumval = (int)parseval;
               break;
            case 'l':
               if ( inputopts.length ) {
                  printf( OUTPREFX "ERROR: Duplicate '-l' argument detected\n" );
                  argerror = 1;
                  break;
               }
               parseval = strtoull( argparse, &(endptr), 10 );
               if ( parseval > SIZE_MAX ) {
                  printf( OUTPREFX "ERROR: Length value exceeds type bounds: \"%s\"\n",
                          argparse );
                  argerror = 1;
                  break;
               }
               inputopts.length = 1;
               inputopts.lengthval = (size_t)parseval;
               break;
            case 'A':
               if ( inputopts.abort ) {
                  printf( OUTPREFX "ERROR: Duplicate '-A' argument detected\n" );
                  argerror = 1;
                  break;
               }
               inputopts.abort = 1;
               break;
            default:
               printf( OUTPREFX "ERROR: Unrecognized argument: \"-%c\"\n", argchar );
               argerror = 1;
               break;
         }
         // abort if we've hit an error
         if ( argerror ) { break; }
         // proceed to the next arguemnt substring
         argparse = strtok( NULL, " " );
      }

      // check for any errors parsing arguments
      if ( argerror ) {
         printf( OUTPREFX "ERROR: Skipping command execution due to previous errors\n" );
         usage( inputline );
         free_argopts( &(inputopts) );
         retval = -1; // remember any errors
         continue;
      }

      // resolve a target stream, if specified
      if ( inputopts.streamnum ) {
         tgtstream = inputopts.streamnumval;
      }
      if ( tgtstream + 1 > streamalloc ) {
         // need to expand our stream allocation
         printf( OUTPREFX "Expanding stream list to accomodate %d entries...\n",
                 (tgtstream + 1) );
         streamlist = realloc( streamlist, (tgtstream + 1) * sizeof(DATASTREAM) );
         if ( streamlist == NULL ) {
            printf( OUTPREFX "ERROR: Failed to allocate expanded stream list\n" );
            free( streamdesc );
            free_argopts( &(inputopts) );
            config_abandonposition( &pos );
            return -1;
         }
         streamdesc = realloc( streamdesc, (tgtstream + 1) * sizeof(char*) );
         if ( streamdesc == NULL ) {
            printf( OUTPREFX "ERROR: Failed to allocate expanded stream description list\n" );
            free( streamlist );
            free_argopts( &(inputopts) );
            config_abandonposition( &pos );
            return -1;
         }
         // zero out any added entries
         bzero( streamlist + streamalloc, sizeof(DATASTREAM) * ( (tgtstream + 1) - streamalloc ) );
         bzero( streamdesc + streamalloc, sizeof(char*) * ( (tgtstream + 1) - streamalloc ) );
         streamalloc = (tgtstream + 1);
      }

      // command execution
      if ( strcmp( inputline, "create" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
                   inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
                   inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: Specified args are not supported for a 'create' op\n" );
            usage( "help create" );
         }
         else if ( create_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            // create success, update stream description
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
            int strlen = snprintf( NULL, 0, "CREATE: \"%s\"", inputopts.pathval );
            *(streamdesc + tgtstream) = malloc( sizeof(char) * (strlen + 1) );
            if ( *(streamdesc + tgtstream) == NULL ) {
               printf( OUTPREFX "ERROR: Failed to allocate stream description string\n" );
            }
            else {
               snprintf( *(streamdesc + tgtstream), strlen + 1, "CREATE: \"%s\"",
                         inputopts.pathval );
               retval = 0; // note success
            }
         }
         else if ( errno == EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "open" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.mode  ||  inputopts.bytes  ||  inputopts.ifile ||
                   inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
                   inputopts.chunknum  ||  inputopts.length  ||  inputopts.ctag  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: Specified args are not supported for an 'open' op\n" );
            usage( "help open" );
         }
         else if ( open_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            // open success, update stream description
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
            int strlen;
            if ( inputopts.typeval == READ_STREAM ) {
               strlen = snprintf( NULL, 0, "READ: \"%s\"", inputopts.pathval );
            }
            else {
               strlen = snprintf( NULL, 0, "EDIT: \"%s\"", inputopts.pathval );
            }
            *(streamdesc + tgtstream) = malloc( sizeof(char) * (strlen + 1) );
            if ( *(streamdesc + tgtstream) == NULL ) {
               printf( OUTPREFX "ERROR: Failed to allocate stream description string\n" );
            }
            else {
               if ( inputopts.typeval == READ_STREAM ) {
                  snprintf( *(streamdesc + tgtstream), strlen + 1, "READ: \"%s\"",
                            inputopts.pathval );
               }
               else {
                  snprintf( *(streamdesc + tgtstream), strlen + 1, "EDIT: \"%s\"",
                            inputopts.pathval );
               }
               retval = 0; // note success
            }
         }
         else if ( errno == EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "repack" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||  inputopts.mode  ||
                   inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
                   inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: Specified args are not supported for a 'repack' op\n" );
            usage( "help repack" );
         }
         else if ( repack_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            // repack success, update stream description
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
            int strlen = snprintf( NULL, 0, "REPACK: \"%s\"", inputopts.pathval );
            *(streamdesc + tgtstream) = malloc( sizeof(char) * (strlen + 1) );
            if ( *(streamdesc + tgtstream) == NULL ) {
               printf( OUTPREFX "ERROR: Failed to allocate stream description string\n" );
            }
            else {
               snprintf( *(streamdesc + tgtstream), strlen + 1, "REPACK: \"%s\"",
                         inputopts.pathval );
               retval = 0; // note success
            }
         }
         else if ( errno == EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "release" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'release' op supports only the '-s' arg\n" );
            usage( "help release" );
         }
         else if ( release_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            // release success, delete stream description
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "close" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'close' op supports only the '-s' arg\n" );
            usage( "help close" );
         }
         else if ( close_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            // close success, delete stream description
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "read" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.ifile  ||  inputopts.type  ||  inputopts.chunknum  ||
              inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'read' op does not support all provided args\n" );
            usage( "help read" );
         }
         else if ( !(inputopts.ofile) &&  inputopts.offset ) {
            printf( OUTPREFX "ERROR: The 'offset' arg requires the 'iofile' arg\n" );
         }
         else if ( !(inputopts.offset)  &&  inputopts.seekfrom ) {
            printf( OUTPREFX "ERROR: The 'seekfrom' arg requires the 'offset' arg\n" );
         }
         else if ( read_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno == EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "write" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.ofile  ||  inputopts.type  ||  inputopts.chunknum  ||
              inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'write' op does not support all provided args\n" );
            usage( "help write" );
         }
         else if ( !(inputopts.ifile) &&  inputopts.offset ) {
            printf( OUTPREFX "ERROR: The 'offset' arg requires the 'iofile' arg\n" );
         }
         else if ( !(inputopts.offset)  &&  inputopts.seekfrom ) {
            printf( OUTPREFX "ERROR: The 'seekfrom' arg requires the 'offset' arg\n" );
         }
         else if ( write_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno == EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "setrecovpath" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'setrecovpath' op supports only the '-s'/'-p' args\n" );
            usage( "help setrecovpath" );
         }
         else if ( setrecovpath_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "seek" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  || inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'seek' op supports only '-s'/'-@'/'-f' args\n" );
            usage( "help seek" );
         }
         else if ( seek_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno == EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "chunkbounds" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'chunkbounds' op supports only '-s'/'-c' args\n" );
            usage( "help chunkbounds" );
         }
         else if ( chunkbounds_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "extend" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'extend' op supports only the '-s'/'-l' args\n" );
            usage( "help extend" );
         }
         else if ( extend_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno == EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "truncate" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'truncate' op supports only the '-s'/'-l' args\n" );
            usage( "help truncate" );
         }
         else if ( truncate_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "utime" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'utime' op supports only the '-s'/'-i' args\n" );
            usage( "help utime" );
         }
         else if ( utime_command( config, &(pos), streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "streamlist" ) == 0 ) {
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'streamlist' op supports only the '-s' arg\n" );
            usage( "help streamlist" );
         }
         else {
            int ibreadth = numdigits_unsigned( streamalloc - 1 );
            printf( "\n%d Stream Postions Allocated -- \n", streamalloc );
            int streamindex = 0;
            int streammax = streamalloc;
            if ( inputopts.streamnum ) { streamindex = tgtstream; streammax = tgtstream + 1; } // just print the requested stream
            for( streamindex = 0; streamindex < streammax; streamindex++ ) {
               if ( streamindex == tgtstream ) {
                  printf( " -> %*d -- %s\n", -(ibreadth), streamindex, *(streamdesc + streamindex) );
               }
               else if ( *(streamlist + streamindex)  ||  *(streamdesc + streamindex) ) {
                  printf( "    %*d -- %s\n", -(ibreadth), streamindex, *(streamdesc + streamindex) );
               }
            }
            if ( inputopts.streamnum  &&  ( *(streamlist + tgtstream) ) != NULL ) {
               // print out extra info, if a stream was explicitly specified
               printf( "      -- Client   : \"%s\"\n", ( *(streamlist + tgtstream) )->ctag );
               printf( "      -- StreamID : \"%s\"\n", ( *(streamlist + tgtstream) )->streamid );
               marfs_ns* curns = ( *(streamlist + tgtstream) )->ns;
               printf( "      -- Namespace : \"%s\"\n", curns->idstr );
               printf( "      -- FileNo : %zu\n", ( *(streamlist + tgtstream) )->fileno );
               printf( "      -- ObjNo  : %zu\n", ( *(streamlist + tgtstream) )->objno );
               printf( "      -- Offset : %zu\n", ( *(streamlist + tgtstream) )->offset );
               printf( "      -- Excess Offset : %zu\n", ( *(streamlist + tgtstream) )->excessoffset );
               size_t curfile = ( *(streamlist + tgtstream) )->curfile;
               printf( "      -- Active File Index : %zu\n", curfile );
               HASH_TABLE reftable = curns->prepo->metascheme.reftable;
               if ( ( *(streamlist + tgtstream) )->files[curfile].ftag.refbreadth != curns->prepo->metascheme.refbreadth  ||
                    ( *(streamlist + tgtstream) )->files[curfile].ftag.refdepth != curns->prepo->metascheme.refdepth  ||
                    ( *(streamlist + tgtstream) )->files[curfile].ftag.refdigits != curns->prepo->metascheme.refdigits ) {
                  printf( "      -- NOTE -- Active stream has non-standard reference structure\n" );
                  reftable = config_genreftable( NULL, NULL,
                                                 ( *(streamlist + tgtstream) )->files[curfile].ftag.refbreadth,
                                                 ( *(streamlist + tgtstream) )->files[curfile].ftag.refdepth,
                                                 ( *(streamlist + tgtstream) )->files[curfile].ftag.refdigits );
                  if ( reftable == NULL ) {
                     printf( OUTPREFX "ERROR: Failed to generate a temporary reference table\n" );
                     free_argopts( &(inputopts) );
                     continue;
                  }
               }
               char* actrpath = datastream_genrpath( &( ( *(streamlist + tgtstream) )->files[curfile].ftag ), reftable );
               if ( actrpath ) {
                  printf( "      -- Active Reference Path : %s\n", actrpath );
                  free( actrpath );
               }
               else {
                   printf( OUTPREFX "ERROR: Failed to generate a reference path for the active file\n" );
               }
               // potentially free our tmp hash table
               if ( reftable != curns->prepo->metascheme.reftable ) {
                  HASH_NODE* nodelist = NULL;
                  size_t nodecount = 0;
                  if ( hash_term( reftable, &(nodelist), &(nodecount) ) ) {
                     printf( OUTPREFX "ERROR: Failed to terminate temporary reference table\n" );
                  }
                  else {
                     size_t index = 0;
                     for ( ; index < nodecount; index++ ) {
                        if ( (nodelist + index)->name ) { free( (nodelist + index)->name ); }
                        if ( (nodelist + index)->content ) { free( (nodelist + index)->content ); }
                     }
                     free( nodelist );
                  }
               }
            }
            printf( "\n" );
         }
      }
      else if ( strcmp( inputline, "ns" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.streamnum  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'ns' op supports only the '-p' arg\n" );
            usage( "help ns" );
         }
         else if ( ns_command( config, &(pos), NULL, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "ls" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.streamnum  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'ls' op supports only the '-p' arg\n" );
            usage( "help ls" );
         }
         else {
            // identify a target path
            char* lstgt = (inputopts.path) ? strdup(inputopts.pathval) : strdup(".");
            if ( lstgt == NULL ) {
               printf( OUTPREFX "ERROR: Failed to duplicate path value\n" );
               free_argopts( &(inputopts) );
               continue;
            }
            marfs_position tmppos = {0};
            if ( config_duplicateposition( &(pos), &(tmppos) ) ) {
               printf( OUTPREFX "ERROR: Failed to duplicate MarFS position prior to 'ls' path traversal\n" );
               free( lstgt );
               free_argopts( &(inputopts) );
               continue;
            }
            int targetdepth;
            if ( (targetdepth = config_traverse( config, &(tmppos), &(lstgt), 1 )) < 0 ) {
               printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n", lstgt );
               config_abandonposition( &(tmppos) );
               free( lstgt );
               free_argopts( &(inputopts) );
               continue;
            }
            // attempt to open a dir handle for the target path
            MDAL curmdal = tmppos.ns->prepo->metascheme.mdal;
            MDAL_DHANDLE lsdir = NULL;
            if ( targetdepth == 0 ) {
               if ( tmppos.ctxt == NULL ) {
                  // ignore our current path
                  if ( lstgt ) { free( lstgt ); }
                  // identify the full path of the NS target
                  if ( config_nsinfo( tmppos.ns->idstr, NULL, &(lstgt) ) ) {
                     printf( OUTPREFX "ERROR: Failed to identify path of NS target: \"%s\" ( %s )\n",
                             tmppos.ns->idstr, strerror( errno ) );
                     config_abandonposition( &(tmppos) );
                     free_argopts( &(inputopts) );
                     continue;
                  }
                  lsdir = curmdal->opendirnamespace( curmdal->ctxt, lstgt );
               }
               else {
                  lsdir = curmdal->opendirnamespace( tmppos.ctxt, lstgt );
               }
            }
            else { lsdir = curmdal->opendir( tmppos.ctxt, lstgt ); }
            if ( lsdir == NULL ) {
               printf( OUTPREFX "ERROR: Failed to open a dir handle for tgt path: \"%s\" ( %s )\n",
                       lstgt, strerror( errno ) );
               config_abandonposition( &(tmppos) );
               free( lstgt );
               free_argopts( &(inputopts) );
               continue;
            }
            // dump subspaces first, if appropriate
            if ( targetdepth == 0 ) {
               size_t index = 0;
               for ( ; index < tmppos.ns->subnodecount; index++ ) {
                  printf( "   %s\n", tmppos.ns->subnodes[index].name );
               }
            }
            // readdir contents
            errno = 0;
            struct dirent* dirret = NULL;
            while ( errno == 0 ) {
               dirret = curmdal->readdir( lsdir );
               if ( dirret  &&  ( ( targetdepth == 0  &&  curmdal->pathfilter( dirret->d_name ) == 0 )  ||  targetdepth ) ) {
                  printf( "   %s\n", dirret->d_name );
               }
               else if ( dirret == NULL ) { break; }
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
            printf( "\n" );
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "mkdir" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.streamnum  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length  ||  inputopts.abort ) {
            printf( OUTPREFX "ERROR: The 'mkdir' op supports only the '-p' and '-m' args\n" );
            usage( "help mkdir" );
         }
         else if ( !(inputopts.path) ) {
            printf( OUTPREFX "ERROR: The 'mkdir' op requires a '-p' arg\n" );
            usage( "help mkdir" );
         }
         else {
            // identify target path
            char* mkdirtgt = strdup(inputopts.pathval);
            if ( mkdirtgt == NULL ) {
               printf( OUTPREFX "ERROR: Failed to duplicate path value\n" );
               free_argopts( &(inputopts) );
               continue;
            }
            marfs_position tmppos = {0};
            if ( config_duplicateposition( &(pos), &(tmppos) ) ) {
               printf( OUTPREFX "ERROR: Failed to duplicate MarFS position prior to 'ls' path traversal\n" );
               free( mkdirtgt );
               free_argopts( &(inputopts) );
               continue;
            }
            int targetdepth;
            if ( (targetdepth = config_traverse( config, &(tmppos), &(mkdirtgt), 1 )) < 0 ) {
               printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n", mkdirtgt );
               config_abandonposition( &(tmppos) );
               free( mkdirtgt );
               free_argopts( &(inputopts) );
               continue;
            }
            // identify a mode value
            mode_t mkdirmode = (inputopts.mode) ? inputopts.modeval : 0700;
            // attempt dir creation
            MDAL curmdal = tmppos.ns->prepo->metascheme.mdal;
            if ( curmdal->mkdir( tmppos.ctxt, mkdirtgt, mkdirmode ) ) {
               printf( OUTPREFX "ERROR: Failed to create directory: \"%s\" ( %s )\n", inputopts.pathval, strerror(errno) );
            }
            else {
               retval = 0; // note success
            }
            if ( config_abandonposition( &(tmppos) ) ) {
               printf( OUTPREFX "ERROR: Failed to abandon temporary MarFS position ( %s )\n", strerror( errno ) );
            }
            free( mkdirtgt );
         }
      }
      else {
         printf( OUTPREFX "ERROR: Unrecognized command: \"%s\"\n", inputline );
      }


      // free inputopts
      free_argopts( &(inputopts) );
   }

   // release active streams and cleanup descriptions
   for ( tgtstream = 0; tgtstream < streamalloc; tgtstream++ ) {
      if ( *(streamlist + tgtstream) ) {
         printf( OUTPREFX "Releasing stream %d...\n", tgtstream );
         if ( release_command( config, &(pos), streamlist + tgtstream, NULL ) == 0 ) {
            retval = -1; // note failure
         }
      }
      if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
   }

   // cleanup
   free( streamlist );
   free( streamdesc );
   if ( config_abandonposition( &pos ) ) {
      printf( OUTPREFX "WARNING: Failed to destory MDAL CTXT\n" );
   }
   return retval;
}


int main(int argc, const char **argv)
{
   errno = 0; // init to zero (apparently not guaranteed)
   char *config_path = NULL;

   char pr_usage = 0;
   int c;
   // parse all position-independent arguments
   while ((c = getopt(argc, (char *const *)argv, "c:h")) != -1)
   {
      switch (c)
      {
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
   if ( pr_usage ) {
      printf( OUTPREFX "Usage info --\n" );
      printf( OUTPREFX "%s -c configpath [-h]\n", PROGNAME );
      printf( OUTPREFX "   -c : Path of the MarFS config file\n" );
      printf( OUTPREFX "   -h : Print this usage info\n" );
      return -1;
   }

   // verify that a config was defined
   if ( config_path == NULL ) {
      printf( OUTPREFX "no config path defined ( '-c' arg )\n" );
      return -1;
   }

   // read in the marfs config
   marfs_config* config = config_init( config_path );
   if ( config == NULL ) {
      printf( OUTPREFX "ERROR: Failed to initialize config: \"%s\" ( %s )\n",
              config_path, strerror(errno) );
      return -1;
   }
   printf( OUTPREFX "marfs config loaded...\n" );

   // enter the main command loop
   int retval = 0;
   if ( command_loop( config ) ) {
      retval = -1;
   }

   // terminate the marfs config
   if ( config_term( config ) ) {
      printf( OUTPREFX "WARNING: Failed to properly terminate MarFS config ( %s )\n",
              strerror(errno) );
      return -1;
   }

   return retval;
}

