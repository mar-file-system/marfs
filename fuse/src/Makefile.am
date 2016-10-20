lib_LIBRARIES = libmarfs.a
libmarfs_a_SOURCES = marfs_base.c marfs_base.h marfs_ops.c marfs_ops.h common.c object_stream.c object_stream.h push_user.c push_user.h mdal.c mdal.h dal.c dal.h marfs_configuration.h marfs_configuration.c $(top_srcdir)/common/log/src/logging.h $(top_srcdir)/common/log/src/logging.c

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

LDADD = $(top_srcdir)/common/configuration/src/libconfig.a $(AWS4C)/libaws4c.a $(AWS4C)/libaws4c_extra.a

libmarfs_a_LIBADD = $(LDADD)

AM_CPPFLAGS = -I$(top_srcdir)/common/log/src -I$(top_srcdir)/common/configuration/src -I$(AWS4C) -D_XOPEN_SOURCE=700 -D_GNU_SOURCE -DDATAPARSE

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

#if PIPETOOL
#bin_PROGRAMS = marfs_pipe
#marfs_pipe_SOURCES = marfs_pipe_main.c
#endif

bin_PROGRAMS = marfs_config test_marfs_configuration
marfs_config_SOURCES = marfs_config.c
marfs_config_LDADD = libmarfs.a $(LDADD)

test_marfs_configuration_SOURCES = test_marfs_configuration.c
test_marfs_configuration_LDADD = libmarfs.a $(LDADD)

if WANT_FUSE
bin_PROGRAMS += marfs_fuse
marfs_fuse_SOURCES = main.c
marfs_fuse_LDADD = libmarfs.a $(LDADD)
marfs_fuse_LDFLAGS = -lfuse
AM_CPPFLAGS += -D_FILE_OFFSET_BITS=64
endif

# --------------------------------------------------------------------
# Useful development targets

# ...........................................................................
# "marfspipe" is an application that lets you pipe into a marfs file
# without going through fuse.  For example:
#
#    tar -czvf - /scratch/mydir | marfspipe /marfs/project/mydir.tgz
#
# ...........................................................................

marfs_pipe: marfs_pipe_main.c
	@ $(MAKE) _app_marfs_pipe_main
	mv marfs_pipe_main marfs_pipe
	chmod u+s marfs_pipe

# ---------------------------------------------------------------------------
# tool to allow scripts to get access to configuration details
#
# for example:
#
#   $ marfs_config -n           [print namespace names]
#   $ marfs_config -n jti       [print contents of namespace with name 'jti']
#   $ marfs_config -r           [print repo names]
#   $ marfs_config -r s3        [print contents of repo with name 's3']
#   $ marfs_config t            [print top-level config details]
#   $ marfs_config t mnt_top    [print value of top-level "mnt_top" setting]
#
# NOTE: This was formerly built in libconfig, but had to come here during
#       the refactoring of libmarfs vs libconfig.
# ---------------------------------------------------------------------------

#marfs_config: marfs_config.c
#	@ $(MAKE) _app_$@


# Brett's configuration tester
#test_marfs_configuration: test_marfs_configuration.c
#	@ $(MAKE) _app_$@

# ...........................................................................
# various testing
# ...........................................................................

# test app that links with assembly-code
test_lock: test_lock.c spinlock_asm.s
	$(CC) -g -o $@ $^ -lpthread -lrt

test_lock2: test_lock2.c spinlock_asm.o spinlock.h spinlock.o
	$(CC) -g -o $@ $^ -lpthread -lrt

test_lock2b: test_lock2b.c spinlock_asm.o spinlock.o
	$(CC) -g -o $@ $^ -lpthread -lrt

# test-apps that may potentially use any of the various marfs support libs
# demo_%: demo_%.o $(OBJS)
# 	@ # $(CC) -o $@ $< $(LDFLAGS) $(LIBS) $(OBJS)
# 	@ echo "demo: $*"
# 	@ $(MAKE) _app_demo_$* LINK_LIBFUSE=1

# test_%: test_%.c
# 	@ echo "test: $*"
# 	@ $(MAKE) _app_test_$* LINK_LIBFUSE=1

# _app_%: libmarfs.a
# 	@ echo "making: _app_$*"
# 	$(CC) $(CFLAGS) -o $* $(LDFLAGS) $(LDADD) $(srcdir)/libmarfs.a -L$(PWD) $*.c $(LFLAGS)

gdb.core:
	ls core.* | tail -n -1 | xargs exec gdb marfs_fuse

gdb:
	@ exec gdb marfs_fuse `cat "$(PID)"`
#	@ exec gdb marfs_fuse `ps -elf | egrep 'marfs_fuse(.*)/marfs' | grep -v grep | awk '{print $$4}'`

fuse_pid:
	ps -elf | grep marfs_fuse | grep $(MARFS_MNT) | grep -v grep | awk '{print $$4}'

# fusectl filesystem.  Under the fusectl mount-point, each existing
# connection has a numbered subdirectory.  There's a special file named
# "abort" there.  Writing anything to that file aborts the connection.
fusectl:
	@ if [[ -n `mount | grep fusectl` ]]; then \
		  echo "fusectl already mounted" ; \
		else \
		  mount -t fusectl none /sys/fs/fuse/connections ; \
		  echo "mounted fusectl" ; \
		fi

abort: fusectl
	for A in `find /sys/fs/fuse/connections/ -name abort`; do echo $$A; echo 1 > $$A;  done
	@ $(MAKE) umnt


#run-%: %
#	RUN $* $(MNT)/test00


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
