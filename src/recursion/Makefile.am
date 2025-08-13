# 
# Copyright 2015. Triad National Security, LLC. All rights reserved.
# 
# Full details and licensing terms can be found in the License file in the main development branch
# of the repository.
# 
# MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
#

# automake requires '=' before '+=', even for these built-in vars
AM_CPPFLAGS = -I ${top_srcdir}/src -DRECURSION
AM_CFLAGS   =
AM_LDFLAGS  =


# define sources used by many programs as noinst libraries, to avoid multiple compilations
noinst_LTLIBRARIES = librec.la

SIDE_LIBS = ../logging/liblog.la

THREAD_QUEUE_SRC = thread_queue/thread_queue.c
if S3DAL
DAL_SRC = dal/posix_dal.c dal/dal.c dal/metainfo.c dal/fuzzing_dal.c dal/s3_dal.c dal/rec_dal.c dal/timer_dal.c dal/noop_dal.c
else
DAL_SRC = dal/posix_dal.c dal/dal.c dal/metainfo.c dal/fuzzing_dal.c dal/rec_dal.c dal/timer_dal.c dal/noop_dal.c
endif
IO_SRC = io/ioqueue.c io/iothreads.c
NE_SRC = ne/ne.c
librec_la_SOURCES = $(THREAD_QUEUE_SRC) $(DAL_SRC) $(IO_SRC) $(NE_SRC)
librec_la_CFLAGS = $(XML_CFLAGS)
REC_LIB = librec.la

# ---
check_PROGRAMS = test_dal_rec test_dal_rec_abort test_libne_rec

test_dal_rec_SOURCES = testing/test_dal_rec.c
test_dal_rec_LDADD = $(REC_LIB) $(SIDE_LIBS)
test_dal_rec_CFLAGS = $(XML_CFLAGS)

test_dal_rec_abort_SOURCES = testing/test_dal_rec_abort.c
test_dal_rec_abort_LDADD = $(REC_LIB) $(SIDE_LIBS)
test_dal_rec_abort_CFLAGS = $(XML_CFLAGS)

test_libne_rec_SOURCES = testing/test_libne_rec.c
test_libne_rec_LDADD = $(REC_LIB) $(SIDE_LIBS)
test_libne_rec_CFLAGS = $(XML_CFLAGS)

TESTS = test_dal_rec test_dal_rec_abort test_libne_rec
