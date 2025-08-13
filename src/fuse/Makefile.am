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

bin_PROGRAMS = marfs-fuse

marfs_fuse_SOURCES = fuse.c change_user.c
marfs_fuse_LDADD  = ../api/libmarfs.la ../ne/libne.la
marfs_fuse_CFLAGS  = $(XML_CFLAGS) -D_FILE_OFFSET_BITS=64

# ---

#check_PROGRAMS = test_marfsapi
#
#test_marfsapi_SOURCES = testing/test_marfsapi.c
#test_marfsapi_CFLAGS = $(XML_CFLAGS)
#test_marfsapi_LDADD = $(MARFS_LIB)
#
#TESTS = test_marfsapi


