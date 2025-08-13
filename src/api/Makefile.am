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

include_HEADERS = marfs.h
lib_LTLIBRARIES = libmarfs.la

libmarfs_la_SOURCES = marfs.c
libmarfs_la_LIBADD  = ../datastream/libDatastream.la
libmarfs_la_CFLAGS  = $(XML_CFLAGS)
MARFS_LIB = libmarfs.la

# ---

check_PROGRAMS = test_marfsapi

test_marfsapi_SOURCES = testing/test_marfsapi.c
test_marfsapi_CFLAGS = $(XML_CFLAGS)
test_marfsapi_LDADD = $(MARFS_LIB) ../logging/liblogging.la

TESTS = test_marfsapi
