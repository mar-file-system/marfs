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


# define sources used by many programs as noinst libraries, to avoid multiple compilations
noinst_LTLIBRARIES = libDatastream.la

libDatastream_la_SOURCES = datastream.c
libDatastream_la_LIBADD  = ../config/libConfig.la ../recovery/libRecovery.la ../tagging/libTagging.la ../ne/libne.la
libDatastream_la_CFLAGS  = $(XML_CFLAGS)
DATASTREAM_LIB = libDatastream.la

bin_PROGRAMS = marfs-streamutil marfs-streamwalker
marfs_streamutil_SOURCES = streamutil.c
marfs_streamutil_LDADD   = $(DATASTREAM_LIB)
marfs_streamutil_CFLAGS  = $(XML_CFLAGS)

marfs_streamwalker_SOURCES = streamwalker.c
marfs_streamwalker_LDADD   = $(DATASTREAM_LIB)
marfs_streamwalker_CFLAGS  = $(XML_CFLAGS)

# ---

check_PROGRAMS = test_datastream test_datastream_repack test_datastream_rebuilds

test_datastream_SOURCES = testing/test_datastream.c
test_datastream_CFLAGS = $(XML_CFLAGS)
test_datastream_LDADD = $(DATASTREAM_LIB)

test_datastream_repack_SOURCES = testing/test_datastream_repack.c
test_datastream_repack_CFLAGS = $(XML_CFLAGS)
test_datastream_repack_LDADD = $(DATASTREAM_LIB)

test_datastream_rebuilds_SOURCES = testing/test_datastream_rebuilds.c
test_datastream_rebuilds_CFLAGS = $(XML_CFLAGS)
test_datastream_rebuilds_LDADD = $(DATASTREAM_LIB)

TESTS = test_datastream test_datastream_repack test_datastream_rebuilds


