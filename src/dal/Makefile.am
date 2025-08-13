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
noinst_LTLIBRARIES = libdal.la

SIDE_LIBS = ../logging/liblog.la

if S3DAL
S3_SOURCES = s3_dal.c
endif

libdal_la_SOURCES = posix_dal.c dal.c fuzzing_dal.c $(S3_SOURCES) timer_dal.c noop_dal.c metainfo.c
libdal_la_CFLAGS = $(XML_CFLAGS)
DAL_LIB = libdal.la

bin_PROGRAMS = dalverify emerg_reb
dalverify_SOURCES = dalverify.c
dalverify_LDADD = $(DAL_LIB) $(SIDE_LIBS)
dalverify_CFLAGS = $(XML_CFLAGS)

emerg_reb_CC = mpicc
emerg_reb_SOURCES = emergency_rebuild.c metainfo.c ../thread_queue/thread_queue.c
emerg_reb_LDADD = $(SIDE_LIBS)
emerg_reb_CFLAGS = $(XML_CFLAGS)

# ---
POSIX_TESTS = test_dal_verify test_dal test_dal_abort test_dal_migrate test_dal_oflags
FUZZING_TESTS = test_dal_fuzzing test_dal_fuzzing_put
if S3DAL
S3_TESTS = test_dal_s3_verify test_dal_s3 test_dal_s3_abort test_dal_s3_multipart test_dal_s3_migrate
endif
TIMER_TESTS = test_dal_timer test_dal_timer_abort test_dal_timer_migrate
NOOP_TESTS = test_dal_noop
check_PROGRAMS = $(POSIX_TESTS) $(FUZZING_TESTS) $(S3_TESTS) $(TIMER_TESTS) $(NOOP_TESTS)

test_dal_SOURCES = testing/test_dal.c
test_dal_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_CFLAGS= $(XML_CFLAGS)

test_dal_abort_SOURCES = testing/test_dal_abort.c
test_dal_abort_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_abort_CFLAGS= $(XML_CFLAGS)

test_dal_migrate_SOURCES = testing/test_dal_migrate.c
test_dal_migrate_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_migrate_CFLAGS= $(XML_CFLAGS)

test_dal_verify_SOURCES = testing/test_dal_verify.c
test_dal_verify_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_verify_CFLAGS= $(XML_CFLAGS)

test_dal_oflags_SOURCES = testing/test_dal_oflags.c
test_dal_oflags_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_oflags_CFLAGS= $(XML_CFLAGS)

test_dal_fuzzing_SOURCES = testing/test_dal_fuzzing.c
test_dal_fuzzing_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_fuzzing_CFLAGS= $(XML_CFLAGS)

test_dal_fuzzing_put_SOURCES = testing/test_dal_fuzzing_put.c
test_dal_fuzzing_put_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_fuzzing_put_CFLAGS= $(XML_CFLAGS)

if S3DAL
test_dal_s3_SOURCES = testing/test_dal_s3.c
test_dal_s3_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_CFLAGS= $(XML_CFLAGS)

test_dal_s3_abort_SOURCES = testing/test_dal_s3_abort.c
test_dal_s3_abort_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_abort_CFLAGS= $(XML_CFLAGS)

test_dal_s3_multipart_SOURCES = testing/test_dal_s3_multipart.c
test_dal_s3_multipart_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_multipart_CFLAGS= $(XML_CFLAGS)

test_dal_s3_migrate_SOURCES = testing/test_dal_s3_migrate.c
test_dal_s3_migrate_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_migrate_CFLAGS= $(XML_CFLAGS)

test_dal_s3_verify_SOURCES = testing/test_dal_s3_verify.c
test_dal_s3_verify_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_s3_verify_CFLAGS= $(XML_CFLAGS)
endif

test_dal_timer_SOURCES = testing/test_dal_timer.c
test_dal_timer_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_timer_CFLAGS= $(XML_CFLAGS)

test_dal_timer_abort_SOURCES = testing/test_dal_timer_abort.c
test_dal_timer_abort_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_timer_abort_CFLAGS= $(XML_CFLAGS)

test_dal_timer_migrate_SOURCES = testing/test_dal_timer_migrate.c
test_dal_timer_migrate_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_timer_migrate_CFLAGS= $(XML_CFLAGS)

test_dal_noop_SOURCES = testing/test_dal_noop.c
test_dal_noop_LDADD = $(DAL_LIB) $(SIDE_LIBS)
test_dal_noop_CFLAGS= $(XML_CFLAGS)

TESTS = $(POSIX_TESTS) $(FUZZING_TESTS) $(S3_TESTS) $(TIMER_TESTS) $(NOOP_TESTS)
