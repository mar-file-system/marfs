# 
# Copyright 2015. Triad National Security, LLC. All rights reserved.
# 
# Full details and licensing terms can be found in the License file in the main development branch
# of the repository.
# 
# MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
#

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


