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
noinst_LTLIBRARIES = libioqueue.la libiothreads.la

SIDE_LIBS= ../dal/libdal.la ../logging/liblog.la

libioqueue_la_SOURCES = ioqueue.c
libioqueue_la_CFLAGS  = $(XML_CFLAGS)
IOQ_LIB = libioqueue.la

libiothreads_la_SOURCES = iothreads.c
libiothreads_la_CFLAGS  = $(XML_CFLAGS)
IOT_LIB = libiothreads.la

# ---
check_PROGRAMS = test_ioqueue test_iothreads

test_ioqueue_SOURCES = testing/test_ioqueue.c
test_ioqueue_LDADD   = $(IOQ_LIB) $(SIDE_LIBS)
test_ioqueue_CFLAGS  = $(XML_CFLAGS)

test_iothreads_SOURCES = testing/test_iothreads.c
test_iothreads_LDADD   = $(IOT_LIB) $(IOQ_LIB) $(SIDE_LIBS)
test_iothreads_CFLAGS  = $(XML_CFLAGS)

TESTS = test_ioqueue test_iothreads

