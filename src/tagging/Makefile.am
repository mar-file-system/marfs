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
noinst_LTLIBRARIES = libTagging.la

libTagging_la_SOURCES = tagging.c
libTagging_la_CFLAGS  = $(XML_CFLAGS)
TAGGING_LIB = libTagging.la

# ---

check_PROGRAMS = test_tagging

test_tagging_SOURCES = testing/test_tagging.c
test_tagging_CFLAGS  = $(XML_CFLAGS)
test_tagging_LDADD = $(TAGGING_LIB) ../logging/liblogging.la

TESTS = test_tagging
