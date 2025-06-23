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
noinst_LTLIBRARIES = libMDAL.la

libMDAL_la_SOURCES = mdal.c posix_mdal.c
libMDAL_la_CFLAGS = $(XML_CFLAGS)
MDAL_LIB = libMDAL.la

# ---

check_PROGRAMS = test_posix_mdal

test_posix_mdal_SOURCES = testing/test_posix_mdal.c
test_posix_mdal_CFLAGS = $(XML_CFLAGS)
test_posix_mdal_LDADD = $(MDAL_LIB) ../logging/liblogging.la

TESTS = test_posix_mdal
