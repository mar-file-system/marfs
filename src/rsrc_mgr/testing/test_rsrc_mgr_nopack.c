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

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <limits.h>

#include "config/config.h"
#include "datastream/datastream.h"
#include "tagging/tagging.h"

#define MAXSIZE 256 // Must be a power of 2

int main(int argc, char** argv) {
  // create the dirs necessary for DAL/MDAL initialization (ignore EEXIST)
  errno = 0;
  if (mkdir("./test_rmgr_topdir", S_IRWXU) && errno != EEXIST) {
    printf("failed to create test_rmgr_topdir\n");
    return -1;
  }
  system("rm -rf ./test_rmgr_topdir/dal_root/*");
  errno = 0;
  if (mkdir("./test_rmgr_topdir/dal_root", S_IRWXU) && errno != EEXIST) {
    printf("failed to create test_rmgr_topdir/dal_root\n");
    return -1;
  }
  system("rm -rf ./test_rmgr_topdir/mdal_root/*");
  errno = 0;
  if (mkdir("./test_rmgr_topdir/mdal_root", S_IRWXU) && errno != EEXIST) {
    printf("failed to create \"./test_rmgr_topdir/mdal_root\"\n");
    return -1;
  }

  // establish a new marfs config
  marfs_config* config = config_init("./testing/config_nopack.xml");
  if (config == NULL) {
    printf("Failed to initialize marfs config\n");
    return -1;
  }

  // create all namespaces associated with the config
  if (config_verify(config, ".././campaign", 1, 1, 1, 1)) {
    printf("Failed to validate the marfs config\n");
    return -1;
  }

  // establish a rootNS position
  MDAL rootmdal = config->rootns->prepo->metascheme.mdal;
  marfs_position pos = {
     .ns = config->rootns,
     .depth = 0,
     .ctxt = rootmdal->newctxt("/.", rootmdal->ctxt)
  };
  if (pos.ctxt == NULL) {
    printf("Failed to establish root MDAL_CTXT for position\n");
    return -1;
  }

  char buf[MAXSIZE];
  bzero(buf, MAXSIZE);
  DATASTREAM stream = NULL;
  char fname[6 + ((int)log10(INT_MAX))];
  int i;
  for (i = 1; i <= MAXSIZE; i *= 2) {
    snprintf(fname, 6 + ((int)log10(INT_MAX)), "file%d", i);

    if (datastream_create(&stream, fname, &pos, 0744, "")) {
      printf("failed to create %s\n", fname);
      return -1;
    }

    errno = 0;
    if (datastream_write(&stream, buf, i) != i) {
      printf("failed to write to %s (%s)\n", fname, strerror(errno));
      return -1;
    }
  }

  if (datastream_close(&stream)) {
    printf("failed to close stream\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  marfs_ms* ms = &(config->rootns->prepo->metascheme);
  marfs_ds* ds = &(config->rootns->prepo->datascheme);

  char* ns_path;
  if (config_nsinfo(config->rootns->idstr, NULL, &ns_path)) {
    printf("failed to retrieve root NS path\n");
    return -1;
  }

  MDAL_CTXT ctxt;
  if ((ctxt = ms->mdal->newctxt(ns_path, ms->mdal->ctxt)) == NULL) {
    printf("failed to create MDAL ctxt\n");
    return -1;
  }

  off_t dusage = ms->mdal->getdatausage(ctxt);
  off_t iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != (MAXSIZE * 2) - 1 || iusage != log2(MAXSIZE) + 1) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  MDAL_FHANDLE fh = ms->mdal->open(ctxt, "file1", O_RDONLY);
  if (fh == NULL) {
    printf("failed to open file1 for xattr access\n");
    return -1;
  }

  char* xattrstr = NULL;
  errno = 0;
  ssize_t xattrlen = ms->mdal->fgetxattr(fh, 1, FTAG_NAME, xattrstr, 0);
  if (xattrlen <= 0) {
    printf("failed to get FTAG xattr (%s)\n", strerror(errno));
    return -1;
  }

  xattrstr = calloc(1, xattrlen + 1);
  if (xattrstr == NULL) {
    printf("failed to allocate xattrstr\n");
    return -1;
  }

  if (ms->mdal->fgetxattr(fh, 1, FTAG_NAME, xattrstr, xattrlen) != xattrlen) {
    printf("FTAG value for xattr changed while reading\n");
    return -1;
  }

  if (ms->mdal->close(fh)) {
    printf("failed to close file1\n");
    return -1;
  }

  FTAG ftag;
  if (ftag_initstr(&ftag, xattrstr)) {
    printf("failed to parse ftag from string\n");
    return -1;
  }

  char* rpath;
  char* objname;
  ne_erasure epat;
  ne_location loc;
  for (i = 0; i <= log2(MAXSIZE); i++) {
    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    errno = 0;
    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st)) {
      printf("failed to stat ref %d (%s)\n", i, strerror(errno));
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if (ne_stat(ds->nectxt, objname, loc) == NULL) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove one file in the middle of the stream
  if (ms->mdal->unlink(ctxt, "file8")) {
    printf("failed to delete file8\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != (MAXSIZE * 2) - 9 || iusage != log2(MAXSIZE)) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  int del;
  for (i = 0; i <= log2(MAXSIZE); i++) {
    if (i == 3) {
      del = 1;
    }
    else {
      del = 0;
    }

    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -del) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if ((ne_stat(ds->nectxt, objname, loc) == NULL) != del) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove a second file in the middle of the stream
  if (ms->mdal->unlink(ctxt, "file16")) {
    printf("failed to delete file16\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != (MAXSIZE * 2) - 25 || iusage != log2(MAXSIZE) - 1) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  for (i = 0; i <= log2(MAXSIZE); i++) {
    if (i == 3 || i == 4) {
      del = 1;
    }
    else {
      del = 0;
    }

    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -del) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if ((ne_stat(ds->nectxt, objname, loc) == NULL) != del) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove a file at the end of the stream
  if (ms->mdal->unlink(ctxt, "file256")) {
    printf("failed to delete file256\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != MAXSIZE - 25 || iusage != log2(MAXSIZE) - 2) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  for (i = 0; i <= log2(MAXSIZE); i++) {
    if (i == 3 || i == 4 || i == log2(MAXSIZE)) {
      del = 1;
    }
    else {
      del = 0;
    }

    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -del) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if ((ne_stat(ds->nectxt, objname, loc) == NULL) != del) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove a file immediately before the end of the stream
  if (ms->mdal->unlink(ctxt, "file128")) {
    printf("failed to delete file128\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != (MAXSIZE / 2) - 25 || iusage != log2(MAXSIZE) - 3) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  for (i = 0; i <= log2(MAXSIZE); i++) {
    if (i == 3 || i == 4 || i >= log2(MAXSIZE) - 1) {
      del = 1;
    }
    else {
      del = 0;
    }

    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -del) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if ((ne_stat(ds->nectxt, objname, loc) == NULL) != del) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove a file at the beginning of the stream
  if (ms->mdal->unlink(ctxt, "file1")) {
    printf("failed to delete file1\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != (MAXSIZE / 2) - 26 || iusage != log2(MAXSIZE) - 4) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  for (i = 0; i <= log2(MAXSIZE); i++) {
    if (i == 0 || i == 3 || i == 4 || i >= log2(MAXSIZE) - 1) {
      del = 1;
    }
    else {
      del = 0;
    }

    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -(del && i)) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if ((ne_stat(ds->nectxt, objname, loc) == NULL) != del) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove a file right after the beginning of the stream
  if (ms->mdal->unlink(ctxt, "file2")) {
    printf("failed to delete file2\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != (MAXSIZE / 2) - 28 || iusage != log2(MAXSIZE) - 5) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  for (i = 0; i <= log2(MAXSIZE); i++) {
    if (i <= 1 || i == 3 || i == 4 || i >= log2(MAXSIZE) - 1) {
      del = 1;
    }
    else {
      del = 0;
    }

    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -(del && i)) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if ((ne_stat(ds->nectxt, objname, loc) == NULL) != del) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove a file bridging two previously removed files
  if (ms->mdal->unlink(ctxt, "file4")) {
    printf("failed to delete file4\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != (MAXSIZE / 2) - 32 || iusage != log2(MAXSIZE) - 6) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  for (i = 0; i <= log2(MAXSIZE); i++) {
    if (i <= 4 || i >= log2(MAXSIZE) - 1) {
      del = 1;
    }
    else {
      del = 0;
    }

    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -(del && i)) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if ((ne_stat(ds->nectxt, objname, loc) == NULL) != del) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  // remove all remaining files
  if (ms->mdal->unlink(ctxt, "file32")) {
    printf("failed to delete file32\n");
    return -1;
  }
  if (ms->mdal->unlink(ctxt, "file64")) {
    printf("failed to delete file64\n");
    return -1;
  }

  if (system("mpirun --oversubscribe --allow-run-as-root -np 2 ./marfs-rsrc_mgr -c ./testing/config_nopack.xml -d") < 0) {
    printf("rsrc_mgr failed\n");
    return -1;
  }

  dusage = ms->mdal->getdatausage(ctxt);
  iusage = ms->mdal->getinodeusage(ctxt);

  if (dusage != 0 || iusage != 0) {
    printf("incorrect quota values. data: %lu B inodes: %lu\n", dusage, iusage);
    return -1;
  }

  for (i = 0; i <= log2(MAXSIZE); i++) {
    ftag.fileno = i;
    ftag.objno = i;
    if ((rpath = datastream_genrpath(&ftag, ms->reftable)) == NULL) {
      printf("failed to create rpath\n");
      return -1;
    }

    struct stat st;
    if (ms->mdal->statref(ctxt, rpath, &st) != -1) {
      printf("failed to stat ref %d\n", i);
      return -1;
    }

    if (datastream_objtarget(&ftag, ds, &objname, &epat, &loc)) {
      printf("failed to generate object name\n");
      return -1;
    }

    if (ne_stat(ds->nectxt, objname, loc) != NULL) {
      printf("failed to stat object %i\n", i);
      return -1;
    }

    free(rpath);
    free(objname);
  }

  return 0;
}
