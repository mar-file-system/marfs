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


include_HEADERS = hash.h

# define sources used by many programs as noinst libraries, to avoid multiple compilations
noinst_LTLIBRARIES = libHash.la

libHash_la_SOURCES = hash.c
Hash_LIB = libHash.la

# ---

check_PROGRAMS = test_hash_lookup test_hash_distribution

test_hash_lookup_SOURCES = testing/test_hash_lookup.c
test_hash_lookup_LDADD = $(Hash_LIB) ../logging/liblogging.la

test_hash_distribution_SOURCES = testing/test_hash_distribution.c
test_hash_distribution_LDADD = $(Hash_LIB) ../logging/liblogging.la

TESTS = test_hash_lookup test_hash_distribution
