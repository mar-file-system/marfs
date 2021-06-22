#ifndef _STREAM_H
#define _STREAM_H
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


typedef enum
{
   WRITE_STREAM,
   READ_STREAM
} STREAM_TYPE;

#define RECOVERY_CURRENT_MAJORVERSION 0
#define RECOVERY_CURRENT_MINORVERSION 1
#define RECOVERY_MSGHEAD "RECOV( "    // ALTERING THIS MAY HORRIBLY BREAK PREVIOUS RECOVERY INFO
#define RECOVERY_MSGTAIL " )\n"       // ALTERING THIS MAY HORRIBLY BREAK PREVIOUS RECOVERY INFO
typedef struct recovery_header_struct {
   unsigned int majorversion;
   unsigned int minorversion;
} RECOVERY_HEADER;
#define RECOVERY_HEADER "HEADER : "
typedef struct recovery_finfo_struct {
   ino_t  inode;
   mode_t mode;
   uid_t  owner;
   gid_t  group;
   size_t size;
   struct timespec mtime;
   char   eof;
   char*  path;
} RECOVERY_FINFO;
#define RECOVERY_FINFO "FINFO : "


typedef struct streamfile_struct {
   MDAL_FHANDLE    metahandle;
   FTAG            ftag;
   struct timespec times[2];
   size_t          size;
   STREAMFILE      next;
}* STREAMFILE;


typedef struct stream_struct {
   STREAM_TYPE type;
   ne_handle   datahandle;
   MDAL        mdal;
   MDAL_CTXT   mdalctxt;
   RECOVERY_FINFO finfo;
   STREAMFILE  files;
   size_t      filecount;
}* STREAM;


STREAM stream_write_init( const char* path, mode_t mode, MDAL mdal, MDAL_CTXT mdalctxt );
STREAM stream_write_cont( STREAM stream, const char* path, mode_t mode );
STREAM stream_write_resume( const char* path, MDAL mdal, MDAL_CTXT mdalctxt );

size_t stream_read( STREAM stream, void* buffer, size_t size );
size_t stream_write( STREAM stream, const void* buffer, size_t size );


RECOVERY recovery_init( void* objectbuffer, size_t objectsize, RECOVERY_HEADER* header );
const void* recovery_nextfile( RECOVERY recovery, RECOVERY_FINFO* finfo, size_t* buffsize );


#endif // _STREAM_H

