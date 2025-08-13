# 
# Copyright 2015. Triad National Security, LLC. All rights reserved.
# 
# Full details and licensing terms can be found in the License file in the main development branch
# of the repository.
# 
# MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
#

# automake requires '=' before '+=', even for these built-in vars
AM_CPPFLAGS = -I ${top_srcdir}/src -DRMAN_USE_MPI=1
AM_CFLAGS   =
AM_LDFLAGS  =

CC = mpicc

lib_LTLIBRARIES = libResourceLog.la libResourceCore.la

# Standalone Resource Log library
libResourceLog_la_SOURCES = resourcelog.c logline.c
libResourceLog_la_LIBADD = ../config/libConfig.la ../tagging/libTagging.la ../thread_queue/libTQ.la ../ne/libne.la ../logging/liblogging.la
libResourceLog_la_CFLAGS = $(XML_CFLAGS)

# Combination of all code required for Resource Manager other than entry point
libResourceCore_la_SOURCES = \
    findoldlogs.c            \
    manager.c                \
    outputinfo.c             \
    parse_program_args.c     \
    repack.c                 \
    resourceinput.c          \
    resourcemanager.c        \
    resourceprocessing.c     \
    resourcethreads.c        \
    rmanstate.c              \
    streamwalker.c           \
    summary_log_setup.c      \
    work.c                   \
    worker.c
libResourceCore_la_LIBADD = libResourceLog.la
libResourceCore_la_CFLAGS = $(XML_CFLAGS)

bin_PROGRAMS = marfs-rman quota

marfs_rman_SOURCES =         \
    resourcemanager.c
marfs_rman_LDADD = libResourceCore.la ../datastream/libDatastream.la
marfs_rman_CFLAGS = $(XML_CFLAGS)

quota_SOURCES =              \
    quota.c
quota_LDADD = libResourceCore.la ../datastream/libDatastream.la
quota_CFLAGS = $(XML_CFLAGS)

# ---

check_PROGRAMS = test_resourcelog test_resourceprocessing test_resourcethreads

test_resourcelog_SOURCES = testing/test_resourcelog.c
test_resourcelog_LDADD = libResourceLog.la
test_resourcelog_CFLAGS = $(XML_CFLAGS)

test_resourceprocessing_SOURCES = testing/test_resourceprocessing.c repack.c streamwalker.c
test_resourceprocessing_LDADD = libResourceLog.la ../datastream/libDatastream.la
test_resourceprocessing_CFLAGS = $(XML_CFLAGS)

test_resourcethreads_SOURCES = testing/test_resourcethreads.c repack.c resourceinput.c resourceprocessing.c streamwalker.c
test_resourcethreads_LDADD = libResourceLog.la ../datastream/libDatastream.la
test_resourcethreads_CFLAGS = $(XML_CFLAGS)

TESTS = test_resourcelog test_resourceprocessing test_resourcethreads
