#Copyright (c) 2015, Los Alamos National Security, LLC
#All rights reserved.
#
#Copyright 2015.  Los Alamos National Security, LLC. This software was produced
#under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
#Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
#the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
#and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
#SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
#FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
#works, such modified software should be clearly marked, so as not to confuse it
#with the version available from LANL.
#
#Additionally, redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#1. Redistributions of source code must retain the above copyright notice, this
#list of conditions and the following disclaimer.
#
#2. Redistributions in binary form must reproduce the above copyright notice,
#this list of conditions and the following disclaimer in the documentation
#and/or other materials provided with the distribution.
#3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
#Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
#used to endorse or promote products derived from this software without specific
#prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
#"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
#THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
#ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
#CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
#OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
#SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
#INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
#STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
#OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#-----
#NOTE:
#-----
#Although these files reside in a seperate repository, they fall under the MarFS copyright and license.
#
#MarFS is released under the BSD license.
#
#MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
#LA-CC-15-039.
#
#These erasure utilites make use of the Intel Intelligent Storage Acceleration Library (Intel ISA-L), which can be found at https://github.com/01org/isa-l and is under its own license.
#
#MarFS uses libaws4c for Amazon S3 object communication. The original version
#is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
#LANL added functionality to the original work. The original work plus
#LANL contributions is found at https://github.com/jti-lanl/aws4c.
#
#GNU licenses can be found at http://www.gnu.org/licenses/.


# automake requires '=' before '+=', even for these built-in vars
AM_CPPFLAGS = -I ${top_srcdir}/src
AM_CFLAGS   =
AM_LDFLAGS  =


# define sources used by many programs as noinst libraries, to avoid multiple compilations
noinst_LTLIBRARIES = libdal.la

SIDE_LIBS = ../logging/liblog.la

if S3DAL
S3_SOURCES = s3_dal.c
endif

libdal_la_SOURCES = posix_dal.c dal.c fuzzing_dal.c $(S3_SOURCES) timer_dal.c noop_dal.c metainfo.c
libdal_la_CFLAGS = $(XML_CFLAGS)
DAL_LIB = libdal.la

bin_PROGRAMS = dalverify emerg_reb
dalverify_SOURCES = dalverify.c
dalverify_LDADD = $(DAL_LIB) $(SIDE_LIBS)
dalverify_CFLAGS = $(XML_CFLAGS)

emerg_reb_CC = mpicc
emerg_reb_SOURCES = emergency_rebuild.c metainfo.c ../thread_queue/thread_queue.c
emerg_reb_LDADD = $(SIDE_LIBS)
emerg_reb_CFLAGS = $(XML_CFLAGS)

# ---
POSIX_TESTS = test_dal_verify test_dal test_dal_abort test_dal_migrate test_dal_oflags
FUZZING_TESTS = test_dal_fuzzing test_dal_fuzzing_put
if S3DAL
S3_TESTS = test_dal_s3_verify test_dal_s3 test_dal_s3_abort test_dal_s3_multipart test_dal_s3_migrate
endif
TIMER_TESTS = test_dal_timer test_dal_timer_abort test_dal_timer_migrate
NOOP_TESTS = test_dal_noop
check_PROGRAMS = $(POSIX_TESTS) $(FUZZING_TESTS) $(S3_TESTS) $(TIMER_TESTS) $(NOOP_TESTS)

test_dal_SOURCES = testing/test_dal.c
test_dal_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_CFLAGS= $(XML_CFLAGS)

test_dal_abort_SOURCES = testing/test_dal_abort.c
test_dal_abort_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_abort_CFLAGS= $(XML_CFLAGS)

test_dal_migrate_SOURCES = testing/test_dal_migrate.c
test_dal_migrate_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_migrate_CFLAGS= $(XML_CFLAGS)

test_dal_verify_SOURCES = testing/test_dal_verify.c
test_dal_verify_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_verify_CFLAGS= $(XML_CFLAGS)

test_dal_oflags_SOURCES = testing/test_dal_oflags.c
test_dal_oflags_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_oflags_CFLAGS= $(XML_CFLAGS)

test_dal_fuzzing_SOURCES = testing/test_dal_fuzzing.c
test_dal_fuzzing_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_fuzzing_CFLAGS= $(XML_CFLAGS)

test_dal_fuzzing_put_SOURCES = testing/test_dal_fuzzing_put.c
test_dal_fuzzing_put_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_fuzzing_put_CFLAGS= $(XML_CFLAGS)

if S3DAL
test_dal_s3_SOURCES = testing/test_dal_s3.c
test_dal_s3_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_CFLAGS= $(XML_CFLAGS)

test_dal_s3_abort_SOURCES = testing/test_dal_s3_abort.c
test_dal_s3_abort_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_abort_CFLAGS= $(XML_CFLAGS)

test_dal_s3_multipart_SOURCES = testing/test_dal_s3_multipart.c
test_dal_s3_multipart_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_multipart_CFLAGS= $(XML_CFLAGS)

test_dal_s3_migrate_SOURCES = testing/test_dal_s3_migrate.c
test_dal_s3_migrate_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_migrate_CFLAGS= $(XML_CFLAGS)

test_dal_s3_verify_SOURCES = testing/test_dal_s3_verify.c
test_dal_s3_verify_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_verify_CFLAGS= $(XML_CFLAGS)
endif

test_dal_timer_SOURCES = testing/test_dal_timer.c
test_dal_timer_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_timer_CFLAGS= $(XML_CFLAGS)

test_dal_timer_abort_SOURCES = testing/test_dal_timer_abort.c
test_dal_timer_abort_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_timer_abort_CFLAGS= $(XML_CFLAGS)

test_dal_timer_migrate_SOURCES = testing/test_dal_timer_migrate.c
test_dal_timer_migrate_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_timer_migrate_CFLAGS= $(XML_CFLAGS)

test_dal_noop_SOURCES = testing/test_dal_noop.c
test_dal_noop_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_noop_CFLAGS= $(XML_CFLAGS)

TESTS = $(POSIX_TESTS) $(FUZZING_TESTS) $(S3_TESTS) $(TIMER_TESTS) $(NOOP_TESTS)
