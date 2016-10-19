if GPFS_UTILITIES
UTIL_COMMON = gpfs/utilities_common.c gpfs/utilities_common.h

# TODO: this will need to be moved outside of the 'if' once we
# implement generic utilities.
bin_PROGRAMS = marfs_gc marfs_quota marfs_packer marfs_repack

marfs_gc_SOURCES = gpfs/marfs_gc.c gpfs/marfs_gc.h $(UTIL_COMMON)
marfs_quota_SOURCES = gpfs/marfs_quota.c gpfs/marfs_quota.h $(UTIL_COMMON)
marfs_packer_SOURCES = gpfs/marfs_packer.c gpfs/marfs_packer.h $(UTIL_COMMON)
marfs_repack_SOURCES = gpfs/marfs_repack.c gpfs/marfs_repack.h $(UTIL_COMMON)

marfs_gc_LDFLAGS = -lgpfs
marfs_quota_LDFLAGS = -lgpfs
marfs_packer_LDFLAGS = -lgpfs
marfs_repack_LDFLAGS = -lgpfs

# note: Alfred's makefiles define USE_STDOUT if DEUBG is defined. we
# may need to do that here.

endif

AM_CPPFLAGS = -I../fuse/src -I../common/log/src -I../common/configuration/src -I$(AWS4C) -D_XOPEN_SOURCE=700

# TODO: Add debugging as an AM_CONDITIONAL set by configure.
if USE_SYSLOG
AM_CPPFLAGS += -DUSE_SYSLOG
endif

if USE_STDOUT
AM_CPPFLAGS += -DUSE_STDOUT
endif

if USE_DAL
AM_CPPFLAGS += -DUSE_DAL=1
endif

if USE_MDAL
AM_CPPFLAGS += -DUSE_MDAL=1
endif

if USE_MC
AM_CPPFLAGS += -DUSE_MC=1
# I actually think we may not actually need this here, since libne
# is statically added to libmarfs.a
#libmarfs_a_LIBADD += $(LIBNE)/libne.a
endif

LDADD = ../fuse/src/libmarfs.a ../common/configuration/src/libconfig.a $(AWS4C)/libaws4c.a $(AWS4C)/libaws4c_extra.a
