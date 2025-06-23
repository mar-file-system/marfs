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


include_HEADERS = ne.h


# define sources used by many programs as noinst libraries, to avoid multiple compilations
lib_LTLIBRARIES = libne.la

libne_la_SOURCES = ne.c
libne_la_LIBADD  = ../logging/liblog.la ../dal/libdal.la ../io/libioqueue.la ../io/libiothreads.la ../thread_queue/libTQ.la
libne_la_CFLAGS  = $(XML_CFLAGS)
NE_LIBS = libne.la

bin_PROGRAMS = neutil erasurePerf
neutil_SOURCES = neutil.c
neutil_LDADD   = $(NE_LIBS)
neutil_CFLAGS  = $(XML_CFLAGS)

erasurePerf_SOURCES = erasurePerf.c
erasurePerf_LDADD   = $(NE_LIBS)
erasurePerf_CFLAGS  = $(XML_CFLAGS)

# ---

if S3DAL
S3TESTS=testing/test_libne_s3
endif

check_PROGRAMS = testing/test_libne_io testing/test_libne_seek testing/test_libne_fuzzing $(S3TESTS) testing/test_libne_timer testing/test_libne_noop #data_shredder

testing_test_libne_io_SOURCES = testing/test_libne_io.c
testing_test_libne_io_LDADD   = $(NE_LIBS)
testing_test_libne_io_CFLAGS  = $(XML_CFLAGS)

testing_test_libne_seek_SOURCES = testing/test_libne_seek.c
testing_test_libne_seek_LDADD   = $(NE_LIBS)
testing_test_libne_seek_CFLAGS  = $(XML_CFLAGS)

testing_test_libne_fuzzing_SOURCES = testing/test_libne_fuzzing.c
testing_test_libne_fuzzing_LDADD   = $(NE_LIBS)
testing_test_libne_fuzzing_CFLAGS  = $(XML_CFLAGS)

if S3DAL
testing_test_libne_s3_SOURCES = testing/test_libne_s3.c
testing_test_libne_s3_LDADD   = $(NE_LIBS)
testing_test_libne_s3_CFLAGS  = $(XML_CFLAGS)
endif

testing_test_libne_timer_SOURCES = testing/test_libne_timer.c
testing_test_libne_timer_LDADD   = $(NE_LIBS)
testing_test_libne_timer_CFLAGS  = $(XML_CFLAGS)

testing_test_libne_noop_SOURCES = testing/test_libne_noop.c
testing_test_libne_noop_LDADD   = $(NE_LIBS)
testing_test_libne_noop_CFLAGS  = $(XML_CFLAGS)

check_SCRIPTS = testing/erasureTest

#data_shredder_SOURCES = testing/data_shredder.c

TESTS = testing/test_libne_io testing/test_libne_seek testing/test_libne_fuzzing $(S3TESTS) testing/erasureTest testing/test_libne_timer testing/test_libne_noop


