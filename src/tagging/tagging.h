#ifndef _TAGGING_H
#define _TAGGING_H
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

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/


#define FTAG_CURRENT_MAJORVERSION 0
#define FTAG_CURRENT_MINORVERSION 1
#define FTAG_MINORVERSION_PADDING 3

#define FTAG_NAME "MARFS-FTAG"

typedef enum 
{
   // Data Object State Indicators ( Every file will be in one of the following states )
   FTAG_INIT = 0,  // initial state   -- no file data exists
   FTAG_SIZED = 1, // sized state     -- known lower bound on file size
   FTAG_FIN = 2,   // finalized state -- known total file size
   FTAG_COMP = 3,  // completed state -- all data synced

   // State Flag values ( These may or may not be set )
   FTAG_WRITABLE = 4,  // Writable flag -- file's data is writable by arbitrary procs
   FTAG_READABLE = 8,  // Readable flag -- file's data is readable by arbitrary procs
} FTAG_STATE;


typedef struct ftag_struct {
   // version info
   unsigned int majorversion;
   unsigned int minorversion;
   // stream identification info
   char* ctag;
   char* streamid;
   // stream structure info
   size_t objfiles;
   size_t objsize;
   // file position info
   size_t fileno;
   size_t objno;
   char   endofstream;
   size_t offset;
   ne_location location;
   // data content info
   ne_erasure protection;
   size_t bytes;
   size_t availbytes;
   size_t recoverybytes;
   size_t directbytes;
   FTAG_STATE state;
} FTAG;

/**
 * Populate the given ftag struct based on the content of the given ftag string
 * @param FTAG* ftag : Reference to the ftag struct to be populated
 * @param const char* ftagstr : String value to be parsed for structure values
 * @return int : Zero on success, or -1 if a failure occurred
 */
int ftag_initstr( const char* ftagstr, FTAG* ftag );


ssize_t ftag_tostr( const FTAG* ftag, char* str, size_t size );


ssize_t ftag_objectname( const FTAG* ftag, char* str, size_t size );


ssize_t ftag_metaname( const FTAG* ftag, char* str, size_t size );


// ftag_copy = just assignment

#endif // _TAGGING_H

