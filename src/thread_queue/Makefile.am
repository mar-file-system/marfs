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


include_HEADERS = thread_queue.h

# define sources used by many programs as noinst libraries, to avoid multiple compilations
lib_LTLIBRARIES = libTQ.la

SIDE_LIBS = ../logging/liblog.la

libTQ_la_SOURCES = thread_queue.c
TQ_LIB = libTQ.la

# ---
check_PROGRAMS = test_threadqueue test_threadqueue_enqueue test_threadqueue_getopts test_threadqueue_getflags test_threadqueue_noprod test_threadqueue_nocons test_threadqueue_mastercons test_threadqueue_masterprod


test_threadqueue_SOURCES = testing/test_threadqueue.c
test_threadqueue_LDADD = $(TQ_LIB) $(SIDE_LIBS)

test_threadqueue_enqueue_SOURCES = testing/test_threadqueue_enqueue.c
test_threadqueue_enqueue_LDADD = $(TQ_LIB) $(SIDE_LIBS)

test_threadqueue_getopts_SOURCES = testing/test_threadqueue_getopts.c
test_threadqueue_getopts_LDADD = $(TQ_LIB) $(SIDE_LIBS)

test_threadqueue_getflags_SOURCES = testing/test_threadqueue_getflags.c
test_threadqueue_getflags_LDADD = $(TQ_LIB) $(SIDE_LIBS)

test_threadqueue_noprod_SOURCES = testing/test_threadqueue_noprod.c
test_threadqueue_noprod_LDADD = $(TQ_LIB) $(SIDE_LIBS)

test_threadqueue_nocons_SOURCES = testing/test_threadqueue_nocons.c
test_threadqueue_nocons_LDADD = $(TQ_LIB) $(SIDE_LIBS)

test_threadqueue_mastercons_SOURCES = testing/test_threadqueue_mastercons.c
test_threadqueue_mastercons_LDADD = $(TQ_LIB) $(SIDE_LIBS)

test_threadqueue_masterprod_SOURCES = testing/test_threadqueue_masterprod.c
test_threadqueue_masterprod_LDADD = $(TQ_LIB) $(SIDE_LIBS)

TESTS = test_threadqueue test_threadqueue_enqueue test_threadqueue_getopts test_threadqueue_getflags test_threadqueue_noprod test_threadqueue_nocons test_threadqueue_mastercons test_threadqueue_masterprod


