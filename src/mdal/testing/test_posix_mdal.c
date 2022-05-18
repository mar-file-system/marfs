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

#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
// directly including the C file allows more flexibility for these tests
#include "mdal/posix_mdal.c"


int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error contions which result in immediate termination

   // test the namespace path generation
   char nspath[64];
   if ( namespacepath( "/abs/", nspath, 64 ) != ( 1 + strlen(PMDAL_SUBSP) + 1 + 3 ) ) {
      printf( "unexpected length of nspath for \"/abs/\": \"%s\"\n", nspath );
      return -1;
   }
   if ( strcmp( nspath, "/"PMDAL_SUBSP"/abs" ) ) {
      printf( "unexpected result of nspath for \"/abs/\": \"%s\"\n", nspath );
      return -1;
   }
   if ( namespacepath( "rel", nspath, 64 ) != ( 3 + strlen(PMDAL_SUBSP) + 1 + 3 ) ) {
      printf( "unexpected length of nspath for \"rel\": \"%s\"\n", nspath );
      return -1;
   }
   if ( strcmp( nspath, "../"PMDAL_SUBSP"/rel" ) ) {
      printf( "unexpected result of nspath for \"rel\": \"%s\"\n", nspath );
      return -1;
   }
   if ( namespacepath( "..///", nspath, 64 ) != ( 3 + 3 + 2 ) ) {
      printf( "unexpected length of nspath for \"..///\": \"%s\"\n", nspath );
      return -1;
   }
   if ( strcmp( nspath, "../../.." ) ) {
      printf( "unexpected result of nspath for \"..///\": \"%s\"\n", nspath );
      return -1;
   }
   if ( namespacepath( "/////.//", nspath, 64 ) != ( 2 ) ) {
      printf( "unexpected length of nspath for \"/.\": \"%s\"\n", nspath );
      return -1;
   }
   if ( strcmp( nspath, "/." ) ) {
      printf( "unexpected result of nspath for \"/.\": \"%s\"\n", nspath );
      return -1;
   }

   // create a subdir to be used by this test
   errno = 0;
   if ( mkdir( "test_posix_mdal_nsroot", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to produce nsroot subdir\n" );
      return -1;
   }

   // Initialize the libxml lib and check for API mismatches
   LIBXML_TEST_VERSION

   // open the test config file and produce an XML tree
   xmlDoc* doc = xmlReadFile("./testing/posix_config.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse file %s\n", "./testing/posix_config.xml");
      return -1;
   }
   xmlNode* root_element = xmlDocGetRootElement(doc);

   // Initialize a posix mdal instance
   MDAL mdal = init_mdal( root_element );
   if ( mdal == NULL ) {
      printf( "failed to initialize posix mdal\n" );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();

   // create a root NS
   if ( mdal->createnamespace( mdal->ctxt, "/." ) ) {
      printf( "failed to create namespace \"/.\"\n" );
      return -1;
   }

   // verify EINVAL case
   errno = 0;
   if ( mdal->createnamespace( mdal->ctxt, "." ) == 0  ||  errno != EINVAL ) {
      printf( "expected EINVAL for relative NS path of unset ctxt\n" );
      return -1;
   }

   // create a few subspaces
   if ( mdal->createnamespace( mdal->ctxt, "/subsp1" ) ) {
      printf( "failed to create subsp1\n" );
      return -1;
   }
   if ( mdal->createnamespace( mdal->ctxt, "/subsp2" ) ) {
      printf( "failed to create subsp2\n" );
      return -1;
   }
   if ( mdal->createnamespace( mdal->ctxt, "/subsp1/subsp1" ) ) {
      printf( "failed to create subsp1/subsp1\n" );
      return -1;
   }

   // create a new context, referencing the root NS
   MDAL_CTXT rootctxt = mdal->newctxt( "/.", mdal->ctxt );
   if ( rootctxt == NULL ) {
      printf( "failed to create new ctxt referencing \"/.\"\n" );
      return -1;
   }

   // create a reference dir for this NS
   if ( mdal->createrefdir( rootctxt, "ref0", S_IRWXU ) ) {
      printf( "failed to create ref0 for the rootNS\n" );
      return -1;
   }

   // set the data and inode usage of the NS
   if ( mdal->setdatausage( rootctxt, 1048576 ) ) {
      printf( "failed to set data usage of root NS\n" );
      return -1;
   }
   if ( mdal->setinodeusage( rootctxt, 1024 ) ) {
      printf( "failed to set inode usage of root NS\n" );
      return -1;
   }

   // duplicate the base ctxt, then set it to the root NS as well
   MDAL_CTXT dupctxt = mdal->dupctxt( mdal->ctxt );
   if ( dupctxt == NULL ) {
      printf( "failed to dup root ctxt\n" );
      return -1;
   }
   if ( mdal->setnamespace( dupctxt, "/." ) ) {
      printf( "failed to set dup ctxt to root NS\n" );
      return -1;
   }

   // check for EEXIST from the same ref dir
   errno = 0;
   if ( mdal->createrefdir( dupctxt, "ref0", S_IRWXU ) == 0  ||  errno != EEXIST ) {
      printf( "expected EEXIST for dup creation of ref0\n" );
      return -1;
   }

   // verify previous data / inode usage values
   if ( mdal->getdatausage( dupctxt ) != 1048576 ) {
      printf( "dupctxt recieved unexpected data usage value\n" );
      return -1;
   }
   if ( mdal->getinodeusage( dupctxt ) != 1024 ) {
      printf( "dupctxt recieved unexpected inode usage value\n" );
      return -1;
   }

   // destroy a NS by relative path
   if ( mdal->destroynamespace( dupctxt, "subsp2" ) ) {
      printf( "failed to destory subsp2 NS\n" );
      return -1;
   }

   // verify ENOTEMPTY for subsp1
   errno = 0;
   if ( mdal->destroynamespace( dupctxt, "subsp1" ) == 0  ||  errno != ENOTEMPTY ) {
      printf( "expected ENOTEMPTY for destruction of subsp1\n" );
      return -1;
   }

   // destroy subsp1/subsp1 by absolute path
   if ( mdal->destroynamespace( mdal->ctxt, "/subsp1/subsp1" ) ) {
      printf( "failed to destroy \"/subsp1/subsp1\"\n" );
      return -1;
   }

   // actually destroy subsp1 by relative path
   if ( mdal->destroynamespace( dupctxt, "subsp1" ) ) {
      printf( "failed to destroy subsp1\n" );
      return -1;
   }

   // destroy the dup ctxt
   if ( mdal->destroyctxt( dupctxt ) ) {
      printf( "failed to destroy dup ctxt\n" );
      return -1;
   }

   // create a reference file in the root NS
   MDAL_FHANDLE rootfh = mdal->openref( rootctxt, "ref0/reffile", O_CREAT | O_EXCL | O_WRONLY, S_IRWXU );
   if ( !(rootfh) ) {
      printf( "failed to open \"ref0/reffile\" in the root NS\n" );
      return -1;
   }
   // truncate to a new size
   if ( mdal->ftruncate( rootfh, 10234 ) ) {
      printf( "failed to truncate reffile to new length\n" );
      return -1;
   }
   // seek to the end of the file
   if ( mdal->lseek( rootfh, 0, SEEK_END ) != 10234 ) {
      printf( "unexpected offset for end of reffile\n" );
      return -1;
   }
   // write to the end of the file
   if ( mdal->write( rootfh, "CONTENT", (sizeof(char) * 8) ) != (sizeof(char) * 8) ) {
      printf( "unexpected return for write of \"CONTENT\" to reffile\n" );
      return -1;
   }
   // set a user visible xattr
   if ( mdal->fsetxattr( rootfh, 0, "user.testname", "testnamecontent", sizeof(char) * 16, XATTR_CREATE ) ) {
      printf( "failed to set testname xattr on reffile\n" );
      return -1;
   }
   // set a hidden xattr
   if ( mdal->fsetxattr( rootfh, 1, "hidename", "hidenamecontent", sizeof(char) * 16, 0 ) ) {
      printf( "failed to set hidename hidden xattr on reffile\n" );
      return -1;
   }
   // set utime values
   struct timespec ftimevals[2];
   ftimevals[0].tv_sec = 123456;
   ftimevals[0].tv_nsec = 0;
   ftimevals[1].tv_sec = 654321;
   ftimevals[1].tv_nsec = 1;
   if ( mdal->futimens( rootfh, ftimevals ) ) {
      printf( "failed to set timevals on reffile\n" );
      return -1;
   }
   // close the file handle
   if ( mdal->close( rootfh ) ) {
      printf( "failed to close reffile\n" );
      return -1;
   }

   // link the reference file into the user namespace
   if ( mdal->linkref( rootctxt, 0, "ref0/reffile", "userfile" ) ) {
      printf( "failed to link \"ref0/reffile\" to user path \"userfile\"\n" );
      return -1;
   }

   // stat the reference file directly
   struct stat stbuf;
   if ( mdal->statref( rootctxt, "ref0/reffile", &(stbuf) ) ) {
      printf( "failed to stat \"ref0/reffile\"\n" );
      return -1;
   }
   // verify link count
   if ( stbuf.st_nlink != 2 ) {
      printf( "\"ref0/reffile\" has unexpected link count\n" );
      return -1;
   }
   // verify file size
   if ( stbuf.st_size != (10234 + 8) ) {
      printf( "reffline has unexpected size\n" );
      return -1;
   }
   // verify perms
   if ( (stbuf.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) != S_IRWXU ) {
      printf( "unexpected perms for reffile\n" );
      return -1;
   }
   // verify mtime values
   if ( stbuf.st_mtime != ftimevals[1].tv_sec  ||  stbuf.st_mtim.tv_nsec != ftimevals[1].tv_nsec ) {
      printf( "reffile has unexpected mtime values\n" );
      return -1;
   }
   

   // verify ENOTEMPTY for ref0
   errno = 0;
   if ( mdal->destroyrefdir( rootctxt, "ref0" ) == 0  ||  errno != ENOTEMPTY ) {
      printf( "expected ENOTEMPTY for destruction of ref0\n" );
      return -1;
   }

   // open a scanner for ref0
   MDAL_SCANNER sref0 = mdal->openscanner( rootctxt, "ref0" );
   if ( !(sref0) ) {
      printf( "failed to open scanner for ref0\n" );
      return -1;
   }

   // scan over the ref dir
   struct dirent* entry;
   errno = 0;
   while ( (entry = mdal->scan( sref0 )) != NULL ) {
      // look for '.', '..', and 'reffile'
      if ( strncmp( ".", entry->d_name, 2 ) == 0  ||  strncmp( "..", entry->d_name, 3 ) == 0 ) {
         continue;
      }
      if ( strncmp( "reffile", entry->d_name, 8 ) ) {
         printf( "expected \"reffile\" scanner entry, but found \"%s\"\n", entry->d_name );
         return -1;
      }
   }
   if ( errno ) {
      printf( "expected zero errno value following scan of ref0\n" );
      return -1;
   }

   // open the file via the scanner, stat it via the file handle, and compare to the original stat struct
   struct stat verstat;
   MDAL_FHANDLE sfh = mdal->sopen( sref0, "reffile" );
   if ( !(sfh) ) {
      printf( "failed to open reffile via scanner\n" );
      return -1;
   }
   if ( mdal->fstat( sfh, &(verstat) ) ) {
      printf( "failed to stat reffile via scanner handle\n" );
      return -1;
   }
   if ( memcmp( &(verstat), &(stbuf), sizeof(struct stat) ) ) {
      printf( "scanner stat does not match reference stat for reffile\n" );
      return -1;
   }
   // directly stat the userspace link, and verify the stat struct matches
   if ( mdal->stat( rootctxt, "userfile", &(verstat), AT_SYMLINK_NOFOLLOW ) ) {
      printf( "failed to stat userfile via rootctxt\n" );
      return -1;
   }
   if ( memcmp( &(verstat), &(stbuf), sizeof(struct stat) ) ) {
      printf( "userfile stat does not match reference stat\n" );
      return -1;
   }
   // seek to EOF minus 8, and verify the CONTENT string
   if ( mdal->lseek( sfh, 10234, SEEK_SET ) != 10234 ) {
      printf( "failed to seek to 10234 of scanner reffile\n" );
      return -1;
   }
   char buf[64];
   if ( mdal->read( sfh, buf, 8 ) != 8 ) {
      printf( "failed to read 8bytes from end of scanner reffile\n" );
      return -1;
   }
   if ( strncmp( buf, "CONTENT", 8 ) ) {
      printf( "retrieved bytes do not match CONTENT\n" );
      return -1;
   }
   // list xattrs on the file and verify
   ssize_t xlistsz = mdal->flistxattr( sfh, 0, buf, 64 );
   ssize_t origxlsz = xlistsz;
   if ( xlistsz < 1 ) {
      printf( "list xattrs on scanner reffile gave unexpected return\n" );
      return -1;
   } 
   char testxattrfound = 0;
   char* parse = buf;
   while ( xlistsz > 0 ) {
      if ( strncmp( parse, "user.testname", 64 ) == 0 ) { testxattrfound = 1; }
      xlistsz -= strlen( parse ) + 1;
      parse += strlen( parse ) + 1;
   }
   if ( !(testxattrfound) ) {
      printf( "xattr list does not contain \"user.testname\"\n" );
      return -1;
   }
   // list hidden values on the file and verify
   if ( mdal->flistxattr( sfh, 1, buf, 64 ) != 9 ) {
      printf( "list hidden xattrs on scanner reffile gave unexpected return\n" );
      return -1;
   } 
   if ( strncmp( buf, "hidename", 64 ) ) {
      printf( "hidden xattr list does not match \"hidename\"\n" );
      return -1;
   }
   // retrieve the xattr val and verify
   if ( mdal->fgetxattr( sfh, 0, "user.testname", buf, 64 ) != 16 ) {
      printf( "value of user.testname has an unexpected length\n" );
      return -1;
   }
   if ( strncmp( buf, "testnamecontent", 64 ) ) {
      printf( "user.testname had unexpected content\n" );
      return -1;
   }
   // retrieve the hidden xattr val and verify
   if ( mdal->fgetxattr( sfh, 1, "hidename", buf, 64 ) != 16 ) {
      printf( "value of hidename has an unexpected length\n" );
      return -1;
   }
   if ( strncmp( buf, "hidenamecontent", 64 ) ) {
      printf( "hidename had unexpected content\n" );
      return -1;
   }
   // remove both values
   if ( mdal->fremovexattr( sfh, 0, "user.testname" ) ) {
      printf( "failed to remove user.testname value\n" );
      return -1;
   }
   if ( mdal->fremovexattr( sfh, 1, "hidename" ) ) {
      printf( "failed to remove hidename value\n" );
      return -1;
   }
   // confirm absence of values
   if ( mdal->flistxattr( sfh, 0, buf, 64 ) >= origxlsz ) {
      printf( "expected absence of any xattr vals\n" );
      return -1;
   }
   if ( mdal->flistxattr( sfh, 1, buf, 64 ) ) {
      printf( "expected absence of any hidden vals\n" );
      return -1;
   }
   // close the file handle
   if ( mdal->close( sfh ) ) {
      printf( "failed to close scanner handle\n" );
      return -1;
   }

   // unlink the reference file path
   if ( mdal->unlinkref( rootctxt, "ref0/reffile" ) ) {
      printf( "failed to unlink reffile\n" );
      return -1;
   }

   // destroy the reference dir
   if ( mdal->destroyrefdir( rootctxt, "ref0" ) ) {
      printf( "failed to destroy ref0 dir\n" );
      return -1;
   }

   // issue some ops directly against userfile
   if ( mdal->chown( rootctxt, "userfile", geteuid(), getegid(), 0 ) ) {
      printf( "failed to chown userfile\n" );
      return -1;
   }
   if ( mdal->chmod( rootctxt, "userfile", S_IRWXG, 0 ) ) {
      printf( "failed to chmod userfile\n" );
      return -1;
   }

   // cleanup all previous state
   if ( mdal->unlink( rootctxt, "userfile" ) ) {
      printf( "failed to unlink userfile\n" );
      return -1;
   }
   if ( mdal->setdatausage( rootctxt, 0 ) ) {
      printf( "failed to zero out root NS data usage\n" );
      return -1;
   }
   if ( mdal->setinodeusage( rootctxt, 0 ) ) {
      printf( "failed to zero out root NS inode usage\n" );
      return -1;
   }
   if ( mdal->destroyctxt( rootctxt ) ) {
      printf( "failed to destroy rootctxt\n" );
      return -1;
   }
   mdal->destroynamespace( mdal->ctxt, "/." ); // expected to fail

   // free the mdal itself
   mdal->cleanup( mdal );

   // cleanup the root dir
   rmdir( "./test_posix_mdal_nsroot" );

   return 0;
}
