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


#include "marfs_auto_config.h"
#if defined(DEBUG_ALL)  ||  defined(DEBUG_RECOVERY)
   #define DEBUG 1
#endif
#define LOG_PREFIX "recovery"

#include <logging.h>
#include "recovery.h"

#include <errno.h>
#include <stdlib.h>

//   -------------   INTERNAL DEFINITIONS    -------------

typedef struct recovery_struct {
   RECOVERY_HEADER header;
   size_t filecount;
   size_t curfile;
   RECOVERY_FINFO* fileinfo;
   void** filebuffers;
   size_t* buffersizes;
}* RECOVERY;


//   -------------   INTERNAL FUNCTIONS    -------------

void* locate_finfo_start( void* finfotail, size_t bufsize ) {
   // parse over the string, in reverse, never extending beyond the beginning of the buffer
   char* parse = (char*)finfotail;
   char section = 0; // 0 -> verifying finfo msg tail
                     // 1 -> looking for finfo type definition
                     // 2 -> verifying finfo msg header
   const char* cmpstr = RECOVERY_MSGTAIL;
   const char* cmpto = cmpstr + ( strlen(cmpstr) - 1 ); // last character of the cmpstr
   size_t parsed = 0;
   while ( parsed < bufsize ) {
      // compare to expected char
      if ( *parse == *cmpto ) {
         // establish our next expected char
         if ( cmpto != cmpstr ) { cmpto--; } // just reverse through our cmpstr
         else {
            // we have completely verified the comparison string
            // progress to the next string
            if ( section == 2 ) {
               // we have found the start of the finfo string
               return (void*)parse;
            }
            switch ( section ) {
               case 0:
                  cmpstr = RECOVERY_FINFO_TYPE;
                  break;
               case 1:
                  cmpstr = RECOVERY_MSGHEAD;
                  break;
            }
            section++;
            cmpto = cmpstr + ( strlen(cmpstr) - 1 ); // last character of the cmpstr
         }
      }
      else {
         // comparison failure behavior depends on what section we are in
         if ( section != 1 ) {
            // broken msg header/tail is a fatal error condition
            LOG( LOG_ERR, "Improper format of RECOVERY_FINFO %s string\n",
                          (section) ? "header" : "tail" );
            errno = EINVAL;
            return NULL;
         }
         // this is expected, as the msg body may contain many similar substrings
         // just restart our comparison
         cmpto = cmpstr + ( strlen(cmpstr) - 1 ); // last character of the cmpstr
      }
      // progress, in reverse, to the next character of the finfo string
      parse--;
      parsed++;
   }
   // exiting the loop means we failed to locate the start of the string
   LOG( LOG_ERR, "Failed to locate start of RECOVERY_FINFO string within %zu chars\n", parsed );
   errno = EINVAL;
   return NULL;
}

void* parse_recov_header( void* headerbuf, size_t bufsize, RECOVERY_HEADER* header ) {
   char* parse = (char*)headerbuf;
   // validate the msg header string
   size_t complen = strlen( RECOVERY_MSGHEAD );
   if ( bufsize < complen  ||  strncmp( parse, RECOVERY_MSGHEAD, complen ) ) {
      LOG( LOG_ERR, "Failed to validate msg header of RECOVERY_HEADER string\n" );
      errno = EINVAL;
      return NULL;
   }
   parse += complen;
   bufsize -= complen;
   // validate the header type string
   complen = strlen( RECOVERY_HEADER_TYPE );
   if ( bufsize < complen  ||  strncmp( parse, RECOVERY_HEADER_TYPE, complen ) ) {
      LOG( LOG_ERR, "Failed to validate the RECOVERY_HEADER type string\n" );
      errno = EINVAL;
      return NULL;
   }
   parse += complen;
   bufsize -= complen;
   // parse the major version number
   if ( bufsize < (UINT_DIGITS+1) ) {
      LOG( LOG_ERR, "String terminates prior to complete major version\n" );
      errno = EINVAL;
      return NULL;
   }
   char* endptr = NULL;
   unsigned long long parseval = strtoull( parse, &(endptr), 10 );
   if ( *endptr != '.'  ||  parseval > UINT_MAX ) {
      LOG( LOG_ERR, "Failed to parse the RECOVERY_HEADER major version number\n" );
      errno = EINVAL;
      return NULL;
   }
   header->majorversion = parseval;
   bufsize -= ( endptr - parse ) + 1;
   parse = endptr + 1; // skip the '.' seperator
   // parse the minor version number
   if ( bufsize < (UINT_DIGITS+1) ) {
      LOG( LOG_ERR, "String terminates prior to complete minor version\n" );
      errno = EINVAL;
      return NULL;
   }
   parseval = strtoull( parse, &(endptr), 10 );
   if ( *endptr != '|'  ||  parseval > UINT_MAX ) {
      LOG( LOG_ERR, "Failed to parse the RECOVERY_HEADER minor version number\n" );
      errno = EINVAL;
      return NULL;
   }
   header->minorversion = parseval;
   bufsize -= ( endptr - parse ) + 1;
   parse = endptr + 1; // skip the '|' seperator
   // parse the client tag string
   char* ctagstart = parse;
   while ( *parse != '\0'  &&  *parse != '|'  &&  bufsize ) { bufsize--; parse++; }
   if ( bufsize == 0  ||  *parse == '\0' ) {
      LOG( LOG_ERR, "String terminates prior to completion of client tag value\n" );
      errno = EINVAL;
      return NULL;
   }
   size_t ctaglen = parse - ctagstart;
   if ( ctaglen > INT_MAX ) {
      LOG( LOG_ERR, "Client tag length exceeds memory bounds\n" );
      errno = ERANGE;
      return NULL;
   }
   header->ctag = malloc( sizeof(char) * (ctaglen + 1) );
   if ( header->ctag == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new client tag string (len=%zu)\n", ctaglen );
      return NULL;
   }
   if ( snprintf( header->ctag, ctaglen+1, "%.*s", (int)ctaglen, ctagstart ) != ctaglen ) {
      LOG( LOG_ERR, "Failed to populate new client tag string\n" );
      free( header->ctag );
      header->ctag = NULL;
      errno = EFAULT;
      return NULL;
   }
   parse++;
   bufsize--; // skip over the '|' seperator
   // parse the streamID string
   char* stidstart = parse;
   char* tailstr = RECOVERY_MSGTAIL;
   size_t taillen = strlen( tailstr );
   while ( *parse != '\0'  &&  bufsize ) {
      if ( *parse == *tailstr ) {
         if ( bufsize < taillen ) {
            LOG( LOG_ERR, "RECOVERY_HEADER string terminates without tail marker\n" );
            free( header->ctag );
            header->ctag = NULL;
            errno = EINVAL;
            return NULL;
         }
         if ( strncmp( parse, tailstr, taillen ) == 0 ) { break; } // done parsing
      }
      parse++;
      bufsize--;
   }
   if ( bufsize == 0  ||  *parse == '\0' ) {
      LOG( LOG_ERR, "RECOVERY_HEADER string lacks tail marker\n" );
      free( header->ctag );
      header->ctag = NULL;
      errno = EINVAL;
      return NULL;
   }
   size_t stidlen = parse - stidstart;
   if ( stidlen > INT_MAX ) {
      LOG( LOG_ERR, "StreamID length exceeds memory bounds\n" );
      free( header->ctag );
      header->ctag = NULL;
      errno = ERANGE;
      return NULL;
   }
   header->streamid = malloc( sizeof(char) * (stidlen + 1) );
   if ( header->streamid == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new streamID string\n" );
      free( header->ctag );
      header->ctag = NULL;
      header->streamid = NULL;
      errno = EINVAL;
      return NULL;
   }
   if ( snprintf( header->streamid, stidlen+1, "%.*s", (int)stidlen, stidstart ) != stidlen ) {
      LOG( LOG_ERR, "Failed to populate new streamID string\n" );
      free( header->ctag );
      header->ctag = NULL;
      free( header->streamid );
      header->streamid = NULL;
      errno = EFAULT;
      return NULL;
   }
   // finally done, return a reference to the final tail character
   return parse + (taillen - 1);
}

void* parse_recov_finfo( void* finfobuf, size_t bufsize, RECOVERY_FINFO* finfo ) {
   char* parse = (char*)finfobuf;
   // validate the msg header string
   size_t complen = strlen( RECOVERY_MSGHEAD );
   if ( bufsize < complen  ||  strncmp( parse, RECOVERY_MSGHEAD, complen ) ) {
      LOG( LOG_ERR, "Failed to validate msg header of RECOVERY_FINFO string\n" );
      errno = EINVAL;
      return NULL;
   }
   parse += complen;
   bufsize -= complen;
   // validate the finfo type string
   complen = strlen( RECOVERY_FINFO_TYPE );
   if ( bufsize < complen  ||  strncmp( parse, RECOVERY_FINFO_TYPE, complen ) ) {
      LOG( LOG_ERR, "Failed to validate the RECOVERY_FINFO type string\n" );
      errno = EINVAL;
      return NULL;
   }
   parse += complen;
   bufsize -= complen;
   // parse and populate all finfo values
   char* tailstr = RECOVERY_MSGTAIL;
   size_t taillen = strlen( tailstr );
   // keep track of all populated values
   char ival = 0;
   char mval = 0;
   char oval = 0;
   char gval = 0;
   char sval = 0;
   char tval = 0;
   char eval = 0;
   char pval = 0;
   while ( bufsize >= taillen  &&  strncmp( parse, tailstr, taillen ) ) {
      unsigned long long parseval;
      char* endptr = parse;
      // identify the length of this value element string
      // This allows us to avoid extending beyond end-of-buffer while parsing later on
      size_t elemlen = 0;
      while ( (elemlen + taillen) <= bufsize  &&  *endptr != '\0'  &&  *endptr != '|' ) {
         // the tail string is also a terminating condition
         if ( *endptr == *tailstr  &&  strncmp( endptr, tailstr, taillen ) == 0 ) { break; }
         endptr++;
         elemlen++;
      }
      if ( (elemlen + taillen) > bufsize  ||  elemlen < 2  ||  elemlen > INT_MAX  ||  *endptr == '\0' ) {
         LOG( LOG_ERR, "Encountered RECOVERY_FINFO value string with unexpected format\n" );
         LOG( LOG_ERR, "Problem element string: %.*s\n", elemlen, parse );
         errno = EINVAL;
         return NULL;
      }
      // parse the value string, according to its initial character 'tag'
      switch ( *parse ) {
         case 'i':
            if ( ival ) {
               LOG( LOG_ERR, "Detected duplicate inode value string\n" );
               errno = EINVAL;
               return NULL;
            }
            parseval = strtoull( parse+1, &(endptr), 10 );
            if ( *endptr == '|' ) { endptr++; } // skip over the '|' seperator
            else if ( *endptr != *tailstr ) {
               LOG( LOG_ERR, "Inode string of RECOVERY_FINFO terminates unexpectedly\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( parseval > ULONG_MAX ) {
               LOG( LOG_ERR, "Inode value exceeds type bounds\n" );
               errno = ERANGE;
               return NULL;
            }
            finfo->inode = (ino_t)parseval;
            bufsize -= (endptr - parse);
            parse = endptr;
            ival = 1;
            break;
         case 'm':
            if ( mval ) {
               LOG( LOG_ERR, "Detected duplicate mode value string\n" );
               errno = EINVAL;
               return NULL;
            }
            parseval = strtoull( parse+1, &(endptr), 8 );
            if ( *endptr == '|' ) { endptr++; } // skip over the '|' seperator
            else if ( *endptr != *tailstr ) {
               LOG( LOG_ERR, "Mode string of RECOVERY_FINFO terminates unexpectedly\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( parseval > UINT_MAX ) {
               LOG( LOG_ERR, "Mode value exceeds type bounds\n" );
               errno = ERANGE;
               return NULL;
            }
            finfo->mode = (mode_t)parseval;
            bufsize -= (endptr - parse);
            parse = endptr;
            mval = 1;
            break;
         case 'o':
            if ( oval ) {
               LOG( LOG_ERR, "Detected duplicate owner value string\n" );
               errno = EINVAL;
               return NULL;
            }
            parseval = strtoull( parse+1, &(endptr), 10 );
            if ( *endptr == '|' ) { endptr++; } // skip over the '|' seperator
            else if ( *endptr != *tailstr ) {
               LOG( LOG_ERR, "Owner string of RECOVERY_FINFO terminates unexpectedly\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( parseval > UINT_MAX ) {
               LOG( LOG_ERR, "Owner value exceeds type bounds\n" );
               errno = ERANGE;
               return NULL;
            }
            finfo->owner = (uid_t)parseval;
            bufsize -= (endptr - parse);
            parse = endptr;
            oval = 1;
            break;
         case 'g':
            if ( gval ) {
               LOG( LOG_ERR, "Detected duplicate group value string\n" );
               errno = EINVAL;
               return NULL;
            }
            parseval = strtoull( parse+1, &(endptr), 10 );
            if ( *endptr == '|' ) { endptr++; } // skip over the '|' seperator
            else if ( *endptr != *tailstr ) {
               LOG( LOG_ERR, "Group string of RECOVERY_FINFO terminates unexpectedly\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( parseval > UINT_MAX ) {
               LOG( LOG_ERR, "Group value exceeds type bounds\n" );
               errno = ERANGE;
               return NULL;
            }
            finfo->group = (gid_t)parseval;
            bufsize -= (endptr - parse);
            parse = endptr;
            gval = 1;
            break;
         case 's':
            if ( sval ) {
               LOG( LOG_ERR, "Detected duplicate size value string\n" );
               errno = EINVAL;
               return NULL;
            }
            parseval = strtoull( parse+1, &(endptr), 10 );
            if ( *endptr == '|' ) { endptr++; } // skip over the '|' seperator
            else if ( *endptr != *tailstr ) {
               LOG( LOG_ERR, "Size string of RECOVERY_FINFO terminates unexpectedly\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( parseval > SIZE_MAX ) {
               LOG( LOG_ERR, "Size value exceeds type bounds\n" );
               errno = ERANGE;
               return NULL;
            }
            finfo->size = (size_t)parseval;
            bufsize -= (endptr - parse);
            parse = endptr;
            sval = 1;
            break;
         case 't':
            if ( tval ) {
               LOG( LOG_ERR, "Detected duplicate timestamp value string\n" );
               errno = EINVAL;
               return NULL;
            }
            parseval = strtoull( parse+1, &(endptr), 10 );
            if ( *endptr != '.' ) {
               LOG( LOG_ERR, "Timestamp string of RECOVERY_FINFO has unexpected format\n" );
               errno = EINVAL;
               return NULL;
            }
            finfo->mtime.tv_sec = (time_t)parseval;
            // ensure we haven't had any weird assignment overflow issues
            if ( (unsigned long long)(finfo->mtime.tv_sec) != parseval ) {
               LOG( LOG_ERR, "Second timestamp value exceeds type bounds\n" );
               errno = ERANGE;
               return NULL;
            }
            parse = endptr;
            parseval = strtoull( parse+1, &(endptr), 10 );
            if ( *endptr == '|' ) { endptr++; } // skip over the '|' seperator
            else if ( *endptr != *tailstr ) {
               LOG( LOG_ERR, "Timestamp string of RECOVERY_FINFO terminates unexpectedly\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( parseval > LONG_MAX ) {
               LOG( LOG_ERR, "Nanosecond timestamp value exceeds type bounds\n" );
               errno = ERANGE;
               return NULL;
            }
            finfo->mtime.tv_nsec = (long)parseval;
            bufsize -= (endptr - parse);
            parse = endptr;
            tval = 1;
            break;
         case 'e':
            if ( eval ) {
               LOG( LOG_ERR, "Detected duplicate EOF value string\n" );
               errno = EINVAL;
               return NULL;
            }
            parseval = strtoull( parse+1, &(endptr), 10 );
            if ( *endptr == '|' ) { endptr++; } // skip over the '|' seperator
            else if ( *endptr != *tailstr ) {
               LOG( LOG_ERR, "EOF string of RECOVERY_FINFO terminates unexpectedly\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( parseval  &&  parseval != 1 ) {
               LOG( LOG_ERR, "Unexpected EOF value of %llu\n", parseval );
               errno = ERANGE;
               return NULL;
            }
            finfo->eof = (char)parseval;
            bufsize -= (endptr - parse);
            parse = endptr;
            eval = 1;
            break;
         case 'p':
            if ( pval ) {
               LOG( LOG_ERR, "Detected duplicate path value string\n" );
               errno = EINVAL;
               return NULL;
            }
            if ( *(parse+1) != ':' ) {
               LOG( LOG_ERR, "Path string of RECOVERY_FINFO has an unexpected format\n" );
               errno = EINVAL;
               return NULL;
            }
            // skip over the 'p:' prefix
            parse += 2;
            bufsize -= 2;
            elemlen -= 2;
            if ( elemlen < 1 ) {
               LOG( LOG_ERR, "Path string of RECOVERY_FINFO has no content\n" );
               errno = EINVAL;
               return NULL;
            }
            // allocate and populate a new path string
            finfo->path = malloc( sizeof(char) * (elemlen + 1) );
            if ( finfo->path == NULL ) {
               LOG( LOG_ERR, "Failed to allocate new RECOVERY_FINFO path string of length %zu\n", elemlen );
               return NULL;
            }
            if ( snprintf( finfo->path, elemlen+1, "%.*s", (int)elemlen, parse ) != elemlen ) {
               LOG( LOG_ERR, "Failed to populate RECOVERY_FINFO path string\n" );
               errno = EFAULT;
               return NULL;
            }
            parse += elemlen;
            bufsize -= elemlen;
            if ( *parse == '|' ) {
               // skip over any '|' seperator char
               parse++;
               bufsize--;
            }
            pval = 1;
            break;
         default:
            LOG( LOG_ERR, "Encountered unrecognized RECOVERY_FINFO value: '%c'\n", *parse );
            errno = EINVAL;
            return NULL;
      }
   }
   if ( bufsize < taillen ) {
      LOG( LOG_ERR, "RECOVERY_FINFO buffer terminates without tail string\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( !( ival  &&  mval  &&  oval  &&  gval  &&  sval  &&  tval  &&  eval  &&  pval ) ) {
      LOG( LOG_ERR, "RECOVERY_FINFO is missing some expected values\n" );
      errno = EINVAL;
      return NULL;
   }
   // finally done, return a reference to the final character of the string
   return parse + (taillen - 1);
}

int populate_recovery( RECOVERY recov, void* headerend, size_t objsize ) {
   // traverse files in reverse, populating references as we go
   recov->curfile = 0;
   char errorcond = 0;
   void* curpos = headerend + objsize; // start at the final byte of the buffer
   while ( objsize ) {
      // check if we need to extend our lists
      if ( recov->curfile >= recov->filecount ) {
         // realloc all lists to longer buffers
         recov->filecount = ( recov->curfile + 1024 ); // allocate in batches of 1024 files
         RECOVERY_FINFO* newfileinfo = realloc( recov->fileinfo, sizeof(RECOVERY_FINFO) * recov->filecount );
         if ( newfileinfo == NULL ) {
            LOG( LOG_ERR, "Failed to allocate %zu RECOVERY_FINFO references\n",
                           recov->filecount );
            errorcond = 1;
            break;
         }
         recov->fileinfo = newfileinfo;
         void** newfilebuffers = realloc( recov->filebuffers, sizeof(void*) * recov->filecount );
         if ( newfilebuffers == NULL ) {
            LOG( LOG_ERR, "Failed to allocate %zu buffer postion references\n",
                           recov->filecount );
            errorcond = 1;
            break;
         }
         recov->filebuffers = newfilebuffers;
         size_t* newbuffersizes = realloc( recov->buffersizes, sizeof(size_t) * recov->filecount );
         if ( newbuffersizes == NULL ) {
            LOG( LOG_ERR, "Failed to allocate %zu buffer size values\n", recov->filecount );
            errorcond = 1;
            break;
         }
         recov->buffersizes = newbuffersizes;
      }
      // locate the start of the current FINFO string
      void* finfostart = locate_finfo_start( curpos, objsize );
      if ( finfostart == NULL ) {
         LOG( LOG_ERR, "Failed to locate the start of finfo %zu\n", recov->curfile );
         errorcond = 1;
         break;
      }
      size_t finfostrlen = ((curpos - finfostart) + 1);
      objsize -= finfostrlen;
      // parse the FINFO string
      RECOVERY_FINFO* curfinfo = recov->fileinfo + recov->curfile;
      if ( parse_recov_finfo( finfostart, finfostrlen, curfinfo ) != curpos ) {
         LOG( LOG_ERR, "Failed to parse FINFO string: \"%.*s\"\n", finfostrlen, finfostart );
         errorcond = 1;
         break;
      }
      // locate the start of this file's data buffer
      size_t datainobj = ( (curfinfo->size > objsize) ? objsize : curfinfo->size );
      void* databuf = finfostart - datainobj;
      recov->filebuffers[ recov->curfile ] = databuf;
      recov->buffersizes[ recov->curfile ] = datainobj;
      // proceed to the next file
      recov->curfile++;
      curpos = databuf - 1;
      objsize -= datainobj;
   }
   // Post-loop error handling
   if ( errorcond ) {
      while ( recov->curfile ) {
         free( recov->fileinfo[ recov->curfile - 1 ].path );
         recov->curfile--;
      }
      // Leave our per-file lists alone, in case we want to use them later
      return -1;
   }
   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------


/**
 * Produce a string representation of the given recovery header
 * @param const RECOVERY_HEADER* header : Reference to the header to be encoded
 * @param char* tgtstr : Reference to the string buffer to be populated
 * @param size_t len : Size of the provided buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t recovery_headertostr( const RECOVERY_HEADER* header, char* tgtstr, size_t size ) {
   // check for NULL references
   if ( header == NULL ) {
      LOG( LOG_ERR, "Received a NULL header reference\n" );
      return 0;
   }
   if ( tgtstr == NULL  &&  size != 0 ) {
      LOG( LOG_ERR, "Asked to populate a NULL tgtstr\n" );
      return 0;
   }
   // construct the output string, tracking total length
   int prres = snprintf( tgtstr, size, "%s%s%.*u.%.*u|%s|%s%s", 
                         RECOVERY_MSGHEAD,
                         RECOVERY_HEADER_TYPE,
                         UINT_DIGITS, header->majorversion,
                         UINT_DIGITS, header->minorversion,
                         header->ctag,
                         header->streamid,
                         RECOVERY_MSGTAIL );
   if ( prres < 0 ) { return 0; }
   return (size_t)prres;
}

/**
 * Produce a string representation of the given recovery FINFO
 * @param const RECOVERY_FINFO* finfo : Reference to the FINFO to be encoded
 * @param char* tgtstr : Reference to the string buffer to be populated
 * @param size_t len : Size of the provided buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t recovery_finfotostr( const RECOVERY_FINFO* finfo, char* tgtstr, size_t size ) {
   // check for NULL references
   if ( finfo == NULL ) {
      LOG( LOG_ERR, "Received a NULL finfo reference\n" );
      return 0;
   }
   if ( tgtstr == NULL  &&  size != 0 ) {
      LOG( LOG_ERR, "Asked to populate a NULL tgtstr\n" );
      return 0;
   }
   // construct the output string, tracking total length
   int prres = snprintf( tgtstr, size, "%s%si%lu|m%o|o%u|g%u|s%zu|t%llu.%llu|e%d|p:%s%s", 
                         RECOVERY_MSGHEAD,
                         RECOVERY_FINFO_TYPE,
                         (unsigned long)finfo->inode,
                         (unsigned int)finfo->mode,
                         (unsigned int)finfo->owner,
                         (unsigned int)finfo->group,
                         finfo->size,
                         (unsigned long long)finfo->mtime.tv_sec,
                         (unsigned long long)finfo->mtime.tv_nsec,
                         (int)finfo->eof,
                         finfo->path,
                         RECOVERY_MSGTAIL );
   if ( prres < 0 ) { return 0; }
   return (size_t)prres;
}

/**
 * Initialize a RECOVERY reference for a data stream, based on the given object data,
 * and populate a RECOVERY_HEADER reference with the stream info
 * @param void* objbuffer : Reference to the data content of an object to produce a
 *                          recovery reference for
 * @param size_t objsize : Size of the previous data buffer argument
 * @param RECOVERY_HEADER* header : Reference to a RECOVERY_HEADER struct to be populated,
 *                                  ignored if NULL
 * @return RECOVERY : Newly created RECOVERY reference, or NULL if a failure occurred
 */
RECOVERY recovery_init( void* objbuffer, size_t objsize, RECOVERY_HEADER* header ) {
   // check for NULL refs
   if ( objbuffer == NULL ) {
      LOG( LOG_ERR, "Received a NULL object buffer\n" );
      return NULL;
   }
   // create our RECOVERY struct
   RECOVERY recov = malloc( sizeof( struct recovery_struct ) );
   if ( recov == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a RECOVERY struct\n" );
      return NULL;
   }
   // attempt to parse in the header info
   void* headerend = NULL;
   if ( (headerend = parse_recov_header( objbuffer, objsize, &(recov->header) )) == NULL ) {
      LOG( LOG_ERR, "Failed to parse the RECOVERY_HEADER of the object buffer\n" );
      free( recov );
      return NULL;
   }
   // populate the caller's header struct, if provided
   if ( header ) {
      header->majorversion = recov->header.majorversion;
      header->minorversion = recov->header.minorversion;
      header->ctag = strdup( recov->header.ctag );
      header->streamid = strdup( recov->header.streamid );
      if ( header->ctag == NULL  ||  header->streamid == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate header strings into caller struct\n" );
         if ( header->ctag ) { free( header->ctag ); }
         if ( header->streamid ) { free( header->streamid ); }
         header->ctag = NULL;
         header->streamid = NULL;
         free( recov->header.ctag );
         free( recov->header.streamid );
         free( recov );
         return NULL;
      }
   }
   objsize -= (( headerend - objbuffer ) + 1);
   // populate per-file info
   recov->filecount = 0;
   recov->fileinfo = NULL;
   recov->filebuffers = NULL;
   recov->buffersizes = NULL;
   if ( populate_recovery( recov, headerend, objsize ) ) {
      LOG( LOG_ERR, "Failed to populate per-file recovery info\n" );
      if ( recov->fileinfo ) { free( recov->fileinfo ); }
      if ( recov->filebuffers ) { free( recov->filebuffers ); }
      if ( recov->buffersizes ) { free( recov->buffersizes ); }
      free( recov->header.ctag );
      free( recov->header.streamid );
      free( recov );
      if ( header ) {
         if ( header->ctag ) { free( header->ctag ); }
         if ( header->streamid ) { free( header->streamid ); }
         header->ctag = NULL;
         header->streamid = NULL;
      }
      return NULL;
   }
   // all done
   return recov;
}

/**
 * Shift a given RECOVERY reference to the content of a new object
 * @param RECOVERY recovery : RECOVERY reference to be updated
 * @param void* objbuffer : Reference to the data content of the new object
 * @param size_t objsize : Size of the previous data buffer argument
 * @return int : Zero on success, or -1 if a failure occurred
 *               NOTE -- an error condition will be produced if the given object buffer
 *               includes differing RECOVERY_HEADER info
 */
int recovery_cont( RECOVERY recovery, void* objbuffer, size_t objsize ) {
   // check for NULL refs
   if ( objbuffer == NULL ) {
      LOG( LOG_ERR, "Received a NULL object buffer\n" );
      return -1;
   }
   if ( recovery == NULL ) {
      LOG( LOG_ERR, "Received a NULL RECOVERY reference\n" );
      return -1;
   }
   // attempt to parse in the header info
   RECOVERY_HEADER newheader;
   void* headerend = NULL;
   if ( (headerend = parse_recov_header( objbuffer, objsize, &(newheader) )) == NULL ) {
      LOG( LOG_ERR, "Failed to parse the RECOVERY_HEADER of the object buffer\n" );
      return -1;
   }
   objsize -= (( headerend - objbuffer ) + 1);
   // verify that header info hasn't changed in this new object
   if ( newheader.majorversion != recovery->header.majorversion ||
        newheader.minorversion != recovery->header.minorversion ||
        strcmp( newheader.ctag, recovery->header.ctag )  ||
        strcmp( newheader.streamid, recovery->header.streamid ) ) {
      LOG( LOG_ERR, "Header info differs in new object buffer\n" );
      free( newheader.ctag );
      free( newheader.streamid );
   }
   // we're done with new header info
   free( newheader.ctag );
   free( newheader.streamid );
   // populate per-file info
   if ( populate_recovery( recovery, headerend, objsize ) ) {
      LOG( LOG_ERR, "Failed to populate per-file recovery info\n" );
      return -1;
   }
   // all done
   return 0;
}

/**
 * Iterate over file info and content included in the current object data buffer
 * @param RECOVERY recovery : RECOVERY reference to iterate over
 * @param RECOVERY_FINFO* : Reference to the RECOVERY_FINFO struct to be populated with
 *                          info for the next file in the stream; ignored if NULL
 * @param void** databuf : Reference to a void*, to be updated with a reference to the data
 *                         content of the next recovery file; ignored if NULL
 * @param size_t* bufsize : Size of the data content buffer of the next recovery file;
 *                          ignored if NULL
 * @return int : One, if another set of file info was produced;
 *               Zero, if no files remain in the current recovery object;
 *               -1, if a failure occurred.
 */
int recovery_nextfile( RECOVERY recovery, RECOVERY_FINFO* finfo, void** databuf, size_t* bufsize ) {
   // check for NULL refs
   if ( recovery == NULL ) {
      LOG( LOG_ERR, "Received a NULL RECOVERY reference\n" );
      return -1;
   }
   // check if any files remain
   if ( recovery->curfile == 0 ) {
      LOG( LOG_INFO, "No files remain in this recovery object\n" );
      return 0;
   }
   // populate the next file info set
   if ( finfo ) {
      finfo->inode = recovery->fileinfo[ recovery->curfile - 1 ].inode;
      finfo->mode = recovery->fileinfo[ recovery->curfile - 1 ].mode;
      finfo->owner = recovery->fileinfo[ recovery->curfile - 1 ].owner;
      finfo->group = recovery->fileinfo[ recovery->curfile - 1 ].group;
      finfo->size = recovery->fileinfo[ recovery->curfile - 1 ].size;
      finfo->mtime.tv_sec = recovery->fileinfo[ recovery->curfile - 1 ].mtime.tv_sec;
      finfo->mtime.tv_nsec = recovery->fileinfo[ recovery->curfile - 1 ].mtime.tv_nsec;
      finfo->eof = recovery->fileinfo[ recovery->curfile - 1 ].eof;
      finfo->path = strdup( recovery->fileinfo[ recovery->curfile - 1 ].path );
      if ( finfo->path == NULL ) {
         LOG( LOG_ERR, "Failed to populate path of caller's finfo struct\n" );
         return -1;
      }
   }
   if ( databuf ) {
      *databuf = recovery->filebuffers[ recovery->curfile - 1 ];
   }
   if ( bufsize ) {
      *bufsize = recovery->buffersizes[ recovery->curfile - 1 ];
   }
   // free the path string of the returned file
   free( recovery->fileinfo[ recovery->curfile - 1 ].path );
   // finally, decrement our count, to permanently progress to the next file
   recovery->curfile--;
   return 1;
}

/**
 * Close the given RECOVERY reference
 * @param RECOVERY recovery : RECOVERY reference to be closed
 * @param int : Zero on success, or -1 if a failure occurred
 */
int recovery_close( RECOVERY recovery ) {
   // check for NULL refs
   if ( recovery == NULL ) {
      LOG( LOG_ERR, "Received a NULL RECOVERY reference\n" );
      return -1;
   }
   // free all allocated memory
   while ( recovery->curfile ) {
      free( recovery->fileinfo[ recovery->curfile - 1 ].path );
      recovery->curfile--;
   }
   if ( recovery->fileinfo ) { free( recovery->fileinfo ); }
   if ( recovery->filebuffers ) { free( recovery->filebuffers ); }
   if ( recovery->buffersizes ) { free( recovery->buffersizes ); }
   free( recovery->header.ctag );
   free( recovery->header.streamid );
   free( recovery );
   return 0;
}


