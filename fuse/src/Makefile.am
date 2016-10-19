lib_LIBRARIES = libmarfs.a
libmarfs_a_SOURCES = marfs_base.c marfs_base.h marfs_ops.c marfs_ops.h common.c common.c object_stream.c object_stream.h push_user.c push_user.h mdal.c mdal.h dal.c dal.h

include_HEADERS = marfs_base.h marfs_ops.h mdal.h dal.h

#noinst_LIBRARIES = libconfig.a
#libconfig_a_LIBADD = $(srcdir)/../../common/congiguration/logging.o $(srcdir)/../../common/configuration/marfs_configuration.o
# we need to use pa2x to generate a parser in some local directory, then include the compiled .o files for that pasert in libconfig_a... perhaps this should be done by the configure script
# it is possible that something like this will work:
#
# libconfig_a_SOURCES = logging.c marfs_configuration.c parser
#
# then define the parser target as it is defined in the libconfig Makefile.
#
# see http://stackoverflow.com/questions/19165715/call-external-program-from-autoconf-generated-makefile as a reference
#
# supposedly if we build this as a "noinst" target, automake will bake
# it in to libmarfs.a, which will reduce the number of libraries
# anything that links libmarfs.a will also have to link.

LDADD = $(srcdir)/../../common/configuration/src/libconfig.a $(AWS4C)/libaws4c.a $(AWS4C)/libaws4c_extra.a

libmarfs_a_LIBADD = $(LDADD)

AM_CPPFLAGS = -I$(srcdir)/../../common/log/src -I$(srcdir)/../../common/configuration/src -I$(AWS4C) -D_XOPEN_SOURCE=700

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

# Use our own polite spin locks
if SPINLOCKS
libmarfs_a_SOURCES += spinlock.h spinlock.c spinlock_asm.s
AM_CPPFLAGS += -DSPINLOCKS
endif

if USE_MC
AM_CPPFLAGS += -DUSE_MC=1
libmarfs_a_LIBADD += $(LIBNE)/libne.a
endif

if WANT_FUSE
bin_PROGRAMS = marfs_fuse
marfs_fuse_SOURCES = main.c
marfs_fuse_LDADD = $(LDADD) $(srcdir)/libmarfs.a
marfs_fuse_LDFLAGS = -lfuse
AM_CPPFLAGS += -D_FILE_OFFSET_BITS=64
endif

# --------------------------------------------------------------------
# Useful development targets

# TODO: FUSE_FLAGS, EXEC, PID
FUSE_FLAGS = -o allow_other,direct_io,use_ino,intr
PID = marfs_fuse.pid
# ifeq ($(DEBUG),2)
#	FUSE_FLAGS += -d -f
# endif



# convenience targets for fuse.
_mnt:
	@ echo
	@ echo "mounting at $(MARFS_MNT)"
	$(EXEC) ./marfs_fuse $(FUSE_FLAGS) $(MARFS_MNT) $(LOG) &
	@ echo "sleeping to give background fuse a chance to start"
	sleep 1
	@ ps -elf | grep marfs_fuse | grep $(MARFS_MNT) | grep -v grep | awk '{print $$4}' > $(PID)
	@ echo "PID: " `cat $(PID)`

mnt: mnt.std

mnt.std: marfs_fuse umnt.std
	@ $(MAKE) _mnt DEBUG=$(DEBUG)

mnt.dbg: marfs_fuse umnt.dbg
	@ $(MAKE) _mnt MARFS_MNT=$(MARFS_MNT)_dbg FUSE_FLAGS="$(FUSE_FLAGS) -d -f"

_umnt:
	@ echo -n "unmounting $(MARFS_MNT): ..."
	@ (fusermount -q -u $(MARFS_MNT) && echo "unmounted") || echo "already unmounted ?"
	@ rm -f $(PID)

umnt: umnt.std

umnt.std: _umnt

umnt.dbg:
	@ $(MAKE) _umnt MARFS_MNT=$(MARFS_MNT)_dbg

umnt.force:
	@ cat $(PID) | xargs -r echo kill -KILL
	@ cat $(PID) | xargs -r kill -KILL
	@ rm -f $(PID)
	@ $(MAKE) umnt.std
