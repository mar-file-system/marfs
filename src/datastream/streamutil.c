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


#define PROGNAME "marfs_streamutil"
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

#define USAGE(CMD, ARGS, DESC)        \
   printf("  %2s %-11s %s\n%s", \
            (!strncmp(op, CMD, 11) ? "->" : ""), \
            (CMD), (ARGS), (!strncmp(op, "help", 5) ? "      " DESC "\n" : "") )

   USAGE( "create",
          "[-s stream-num] -p path -m mode [-c ctag]",
          "Create a new file, associated with the given datastream" );

   USAGE( "open",
          "[-s stream-num] -t type -p path",
          "Open a file, associated with the given datastream" );

   USAGE( "release",
          "[-s stream-num]",
          "Release the given datastream" );

   USAGE( "close",
          "[-s stream-num]",
          "Close the given datastream" );

   USAGE( "read",
          "[-s stream-num] [-b bytes] [-o iofile [-@ outputoffset [-f seekfrom] ] ]",
          "Read from the given datastream" );

   USAGE( "write",
          "[-s stream-num] [-b bytes] [-i iofile [-@ inputoffset [-f seekfrom] ] ]",
          "Write to the given datastream" );

   USAGE( "setrpath",
          "[-s stream-num] -p path",
          "Set the recovery path of the given datastream" );

   USAGE( "seek",
          "[-s stream-num] -@ offset -f seekfrom",
          "Seek the given datastream" );

   USAGE( "chunkbounds",
          "[-s stream-num] [-c chunknum]",
          "Identify chunk boundries for the given datastream" );

   USAGE( "extend",
          "[-s stream-num] -l length",
          "Extend the given datastream" );

   USAGE( "truncate",
          "[-s stream-num] -l length",
          "Truncate the given datastream" );

   USAGE( "utime",
          "[-s stream-num] -i inputfile",
          "Truncate the given datastream" );

   USAGE( "streamlist",
          "[-s stream-num]",
          "Truncate the given datastream" );

   USAGE( "( exit | quit )",
          "",
          "Terminate ( active streams will be released )" );

   USAGE( "help", "", "Print this usage info" );

   printf("\n");

#undef USAGE
}

int create_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   // check for required args
   if ( !(opts->path)  ||  !(opts->mode) ) {
      printf( OUTPREFX "ERROR: 'create' command is missing required '-p'/'-m' args\n" );
      return -1;
   }
   // perform path traversal to identify marfs position
   char* modpath = strdup( opts->pathval );
   if ( modpath == NULL ) {
      printf( OUTPREFX "ERROR: Failed to create duplicate \"%s\" path for config traversal\n", opts->pathval );
      return -1;
   }
   marfs_position pos = {
      .ns = config->rootns,
      .depth = 0,
      .ctxt = config->rootns->prepo->metascheme.mdal->newctxt( "/.", config->rootns->prepo->metascheme.mdal->ctxt )
   };
   if ( pos.ctxt == NULL ) {
      printf( OUTPREFX "ERROR: Failed to establish MDAL ctxt for 'create' command\n" );
      free( modpath );
      return -1;
   }
   if ( config_traverse( config, &(pos), &(modpath), 1 ) < 0 ) {
      printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
              opts->pathval );
      free( modpath );
      config->rootns->prepo->metascheme.mdal->destroyctxt( pos.ctxt );
      return -1;
   }
   int retval = datastream_create( stream, modpath, &(pos), opts->modeval,
                                   (opts->ctag) ? opts->ctagval : config->ctag );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_create(): %d (%s)\n",
              retval, strerror(errno) );
   }
   free( modpath );
   if ( config->rootns->prepo->metascheme.mdal->destroyctxt( pos.ctxt ) ) {
      // just complain
      printf( OUTPREFX "WARNING: Failed to destory MDAL CTXT\n" );
   }
   return retval;
}

int open_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   // check for required args
   if ( !(opts->path)  ||  !(opts->type) ) {
      printf( OUTPREFX "ERROR: 'open' command is missing required '-p'/'-t' args\n" );
      return -1;
   }
   // perform path traversal to identify marfs position
   char* modpath = strdup( opts->pathval );
   if ( modpath == NULL ) {
      printf( OUTPREFX "ERROR: Failed to create duplicate \"%s\" path for config traversal\n", opts->pathval );
      return -1;
   }
   marfs_position pos = {
      .ns = config->rootns,
      .depth = 0,
      .ctxt = config->rootns->prepo->metascheme.mdal->newctxt( "/.", config->rootns->prepo->metascheme.mdal->ctxt )
   };
   if ( pos.ctxt == NULL ) {
      printf( OUTPREFX "ERROR: Failed to establish MDAL ctxt for 'open' command\n" );
      free( modpath );
      return -1;
   }
   if ( config_traverse( config, &(pos), &(modpath), 1 ) < 0 ) {
      printf( OUTPREFX "ERROR: Failed to identify config subpath for target: \"%s\"\n",
              opts->pathval );
      free( modpath );
      config->rootns->prepo->metascheme.mdal->destroyctxt( pos.ctxt );
      return -1;
   }
   int retval = datastream_open( stream, opts->typeval, modpath, &(pos) );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_open(): %d (%s)\n",
              retval, strerror(errno) );
   }
   free( modpath );
   if ( config->rootns->prepo->metascheme.mdal->destroyctxt( pos.ctxt ) ) {
      // just complain
      printf( OUTPREFX "WARNING: Failed to destory MDAL CTXT\n" );
   }
   return retval;
}

int release_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   int retval = datastream_release( stream );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_release(): %d (%s)\n",
              retval, strerror(errno) );
   }
   return retval;
}

int close_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   int retval = datastream_close( stream );
   if ( retval ) {
      printf( OUTPREFX "ERROR: Failure of datastream_close(): %d (%s)\n",
              retval, strerror(errno) );
   }
   return retval;
}

int read_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
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

   return 0;
}

int write_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
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
         if ( opts->ifile ) { close( iofile ); return -1; }
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


   return 0;
}

int setrpath_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   return 0;
}

int seek_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   return 0;
}

int chunkbounds_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   return 0;
}

int extend_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   return 0;
}

int truncate_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   return 0;
}

int utime_command( marfs_config* config, DATASTREAM* stream, argopts* opts ) {
   return 0;
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
         .lengthval = 0
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
            free_argopts( &(inputopts) );
            return -1;
         }
         streamdesc = realloc( streamdesc, (tgtstream + 1) * sizeof(char*) );
         if ( streamdesc == NULL ) {
            printf( OUTPREFX "ERROR: Failed to allocate expanded stream description list\n" );
            free( streamlist );
            free_argopts( &(inputopts) );
            return -1;
         }
         // zero out any added entries
         bzero( streamlist + streamalloc, sizeof(DATASTREAM) * ( (tgtstream + 1) - streamalloc ) );
         bzero( streamdesc + streamalloc, sizeof(char*) * ( (tgtstream + 1) - streamalloc ) );
         streamalloc = (tgtstream + 1);
      }

      // TODO command execution
      if ( strcmp( inputline, "create" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path == 0  ||  inputopts.mode == 0 ) {
            printf( OUTPREFX "ERROR: Missing required arguments for a 'create' op\n" );
            usage( inputline );
         }
         else if ( inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
                   inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
                   inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: Specified args are not supported for a 'create' op\n" );
            usage( inputline );
         }
         else if ( create_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
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
         else if ( errno = EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "open" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path == 0  ||  inputopts.type == 0 ) {
            printf( OUTPREFX "ERROR: Missing required arguments for an 'open' op\n" );
            usage( inputline );
         }
         else if ( inputopts.mode  ||  inputopts.bytes  ||  inputopts.ifile ||
                   inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
                   inputopts.chunknum  ||  inputopts.length  ||  inputopts.ctag ) {
            printf( OUTPREFX "ERROR: Specified args are not supported for an 'open' op\n" );
            usage( inputline );
         }
         else if ( open_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
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
         else if ( errno = EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
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
              inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'release' op supports only the '-s' arg\n" );
            usage( inputline );
         }
         else if ( release_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
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
              inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'close' op supports only the '-s' arg\n" );
            usage( inputline );
         }
         else if ( close_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
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
              inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'read' op does not support all provided args\n" );
            usage( inputline );
         }
         else if ( !(inputopts.ofile) &&  inputopts.offset ) {
            printf( OUTPREFX "ERROR: The 'offset' arg requires the 'iofile' arg\n" );
         }
         else if ( !(inputopts.offset)  &&  inputopts.seekfrom ) {
            printf( OUTPREFX "ERROR: The 'seekfrom' arg requires the 'offset' arg\n" );
         }
         else if ( read_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno = EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
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
              inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'write' op does not support all provided args\n" );
            usage( inputline );
         }
         else if ( !(inputopts.ifile) &&  inputopts.offset ) {
            printf( OUTPREFX "ERROR: The 'offset' arg requires the 'iofile' arg\n" );
         }
         else if ( !(inputopts.offset)  &&  inputopts.seekfrom ) {
            printf( OUTPREFX "ERROR: The 'seekfrom' arg requires the 'offset' arg\n" );
         }
         else if ( write_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno = EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
            printf( OUTPREFX "ERROR: Stream %d has been rendered unusable\n", tgtstream );
            if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
            *(streamdesc + tgtstream) = NULL;
         }
      }
      else if ( strcmp( inputline, "setrpath" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'setrpath' op supports only the '-s'/'-p' args\n" );
            usage( inputline );
         }
         else if ( setrpath_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "seek" ) == 0 ) {
         errno = 0;
         retval = -1; // assume failure
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  || inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'seek' op supports only '-s'/'-@'/'-f' args\n" );
            usage( inputline );
         }
         else if ( seek_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno = EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
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
              inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'chunkbounds' op supports only '-s'/'-c' args\n" );
            usage( inputline );
         }
         else if ( chunkbounds_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
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
              inputopts.chunknum ) {
            printf( OUTPREFX "ERROR: The 'extend' op supports only the '-s'/'-l' args\n" );
            usage( inputline );
         }
         else if ( extend_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
         else if ( errno = EBADFD  &&  *(streamlist + tgtstream) == NULL ) {
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
              inputopts.chunknum ) {
            printf( OUTPREFX "ERROR: The 'truncate' op supports only the '-s'/'-l' args\n" );
            usage( inputline );
         }
         else if ( truncate_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
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
              inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'utime' op supports only the '-s'/'-i' args\n" );
            usage( inputline );
         }
         else if ( utime_command( config, streamlist + tgtstream, &(inputopts) ) == 0 ) {
            retval = 0; // note success
         }
      }
      else if ( strcmp( inputline, "streamlist" ) == 0 ) {
         // validate arguments
         if ( inputopts.path  ||  inputopts.mode  ||  inputopts.ctag  ||
              inputopts.type  ||  inputopts.bytes  ||  inputopts.ifile ||
              inputopts.ofile  ||  inputopts.offset  ||  inputopts.seekfrom  ||
              inputopts.chunknum  ||  inputopts.length ) {
            printf( OUTPREFX "ERROR: The 'streamlist' op supports only the '-s' arg\n" );
            usage( inputline );
         }
         else {
            int ibreadth = numdigits_unsigned( streamalloc - 1 );
            printf( "\n%d Stream Postions Allocated -- \n", streamalloc );
            int streamindex;
            for( streamindex = 0; streamindex < streamalloc; streamindex++ ) {
               if ( streamindex == tgtstream ) {
                  printf( " -> %*d -- %s\n", -(ibreadth), streamindex, *(streamdesc + streamindex) );
               }
               else if ( *(streamlist + streamindex)  ||  *(streamdesc + streamindex) ) {
                  printf( "    %*d -- %s\n", -(ibreadth), streamindex, *(streamdesc + streamindex) );
               }
            }
            printf( "\n" );
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
         if ( release_command( config, streamlist + tgtstream, NULL ) == 0 ) {
            retval = -1; // note failure
         }
      }
      if ( *(streamdesc + tgtstream) ) { free( *(streamdesc + tgtstream) ); }
   }

   // cleanup
   free( streamlist );
   free( streamdesc );
   return retval;
}


int main(int argc, const char **argv)
{
   errno = 0; // init to zero (apparently not guaranteed)
   char *config_path = NULL;
   char config_v = 0;

   char pr_usage = 0;
   int c;
   // parse all position-independent arguments
   while ((c = getopt(argc, (char *const *)argv, "c:vh")) != -1)
   {
      switch (c)
      {
         case 'c':
            config_path = optarg;
            break;
         case 'v':
            config_v = 1;
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
      printf( OUTPREFX "%s -c configpath [-v] [-h]\n", PROGNAME );
      printf( OUTPREFX "   -c : Path of the MarFS config file\n" );
      printf( OUTPREFX "   -v : Validate the MarFS config\n" );
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

   // validate the config, if requested
   if ( config_v ) {
      if ( config_validate( config ) ) {
         printf( OUTPREFX "ERROR: Failed to validate config: \"%s\" ( %s )\n",
                 config_path, strerror(errno) );
         config_term( config );
         return -1;
      }
      printf( OUTPREFX "config validated...\n" );
   }

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

