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
noinst_LTLIBRARIES = libConfig.la

libConfig_la_SOURCES = config.c
libConfig_la_LIBADD  = ../hash/libHash.la ../mdal/libMDAL.la ../ne/libne.la
libConfig_la_CFLAGS  = $(XML_CFLAGS)
CONFIG_LIB = libConfig.la

bin_PROGRAMS = marfs-verifyconf
marfs_verifyconf_SOURCES = verifyconf.c
marfs_verifyconf_LDADD   = $(CONFIG_LIB)
marfs_verifyconf_CFLAGS  = $(XML_CFLAGS)

# ---

check_PROGRAMS = test_config

test_config_SOURCES = testing/test_config.c
test_config_CFLAGS = $(XML_CFLAGS)
test_config_LDADD = $(CONFIG_LIB)

TESTS = test_config


