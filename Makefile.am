# At build-time, the PA2X package generates the file
# $PARSE_DIR/parse-inc/config-structs.h (where environment-var
# $PARSE_DIR points to the PA2X installation).  At run-time, the PA2X
# parser loads "./parse-inc/config-structs.h", but at run-time there
# might not be any PA2X installation.  So, the build will now copy
# that header-file to include/parse-inc/config-structs.h, under the
# install-dir.  Thus, you need to build with '--prefix=' a destination
# that will also exist at run-time, but you no longer need the PA2X
# install at run-time.
AM_CPPFLAGS = -Icommon/configuration/src -Icommon/log/src -I$(AWS4C) \
	      -Ifuse/src  -D_GNU_SOURCE -D_XOPEN_SOURCE=700          \
	      -DDATAPARSE -DBUILD_INC_DIR=@includedir@ -I$(PARSE_DIR)

AM_CFLAGS =
AM_LDFLAGS =
LDADD =

if GPROF
AM_CFLAGS  += -pg
AM_LDFLAGS += -pg
endif

if DEBUG
# Configure has -g enabled by default
AM_CPPFLAGS += -DDEBUG=2
endif

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

if NO_AWSAUTH
AM_CPPFLAGS += -DUSE_SPROXYD=1
endif


#if PIPETOOL
#bin_PROGRAMS = marfs_pipe
#marfs_pipe_SOURCES = marfs_pipe_main.c
#endif

PARSE_OBJ_Files = confpars.o path-switch.o parsedata.o
PARSE_OBJ = $(PARSE_DIR)/confpars.o $(PARSE_DIR)/path-switch.o $(PARSE_DIR)/parsedata.o

LDADD += libmarfs.a $(AWS4C)/libaws4c.a $(AWS4C)/libaws4c_extra.a

# Generally we will always want to build the config tester program.
bin_PROGRAMS = marfs_config
lib_LIBRARIES = libmarfs.a

libmarfs_a_SOURCES = parser fuse/src/common.c fuse/src/common.h        \
					 fuse/src/marfs_base.c fuse/src/marfs_base.h       \
					 fuse/src/marfs_ops.c fuse/src/marfs_ops.h         \
					 fuse/src/push_user.c fuse/src/push_user.h         \
					 fuse/src/object_stream.c fuse/src/object_stream.h \
					 fuse/src/dal.c fuse/src/dal.h                     \
					 fuse/src/mdal.c fuse/src/mdal.h                   \
					 fuse/src/marfs_locks.h fuse/src/marfs_locks.c     \
					 common/log/src/logging.c common/log/src/logging.h \
					 common/configuration/src/PA2X_interface.c         \
					 common/configuration/src/PA2X_interface.h         \
					 fuse/src/marfs_configuration.c                    \
					 fuse/src/marfs_configuration.h

libmarfs_a_LIBADD = $(PARSE_OBJ)

# Use our own polite spin locks
if SPINLOCKS
libmarfs_a_SOURCES += fuse/src/spinlock.h fuse/src/spinlock.c \
					  fuse/src/spinlock_asm.s
AM_CPPFLAGS += -DSPINLOCKS
endif


# --- additions for multi-component builds
if USE_MC
AM_CPPFLAGS += -I$(LIBNE)/include -DUSE_MC=1
AM_LDFLAGS  += -L$(LIBNE)/lib
LDADD       += -L$(LIBNE)/lib -lne

all-local : utilities/multi-component/rebuilder.c
	@ mpicc ${CPPFLAGS} ${AM_CPPFLAGS} -I. ${LDFLAGS} ${AM_LDFLAGS} -o rebuilder $^ ${LDADD} ${LIBS}

clean-local :
	@ rm -f rebuilder

endif


# --- marfs_config
include_HEADERS = fuse/src/marfs_base.h \
                  fuse/src/object_stream.h \
                  fuse/src/common.h \
                  fuse/src/marfs_ops.h \
                  fuse/src/xdal_common.h \
                  fuse/src/mdal.h \
                  fuse/src/dal.h \
				  fuse/src/marfs_locks.h \
                  fuse/src/marfs_configuration.h \
                  common/log/src/logging.h

marfs_config_SOURCES = fuse/src/marfs_config.c                   \
					   fuse/src/marfs_configuration.c            \
					   fuse/src/marfs_configuration.h            \
                       common/configuration/src/PA2X_interface.c \
					   common/configuration/src/PA2X_interface.h

# The PA2X_interface needs to look in additionSal places for its #include files.
#common/configuration/src/PA2X_interface.o: common/configuration/src/PA2X_interface.c common/configuration/src/PA2X_interface.h
#	$(COMPILE) -I$(PARSE_DIR) -DPARSE_DIR=$(PARSE_DIR) $(AM_CPPFLAGS) -o $@ -c $<

# NOTE: we could build marfs_config just from marfs_config.c and add
#       the necessary objects with LDADD like so:
# marfs_config_LDADD = marfs_configuration.o PA2X_Interface.o $(PARSE_OBJ)
#
# Of just link libmarfs:
#
# marfs_config_SOURCES = fuse/src/marfs_config.c
# marfs_config_LDADD = libmarfs.a



# ............................................................................
# FUSE
# ............................................................................

if WANT_FUSE
bin_PROGRAMS += marfs_fuse
marfs_fuse_SOURCES = fuse/src/main.c
AM_CPPFLAGS += -D_FILE_OFFSET_BITS=64
endif

AM_CPPFLAGS += -Iutilities/common/src
# ............................................................................
# GPFS Utilities
# ............................................................................
if GPFS_UTILITIES

bin_PROGRAMS += marfs_gc marfs_quota marfs_packer marfs_repack

marfs_gc_SOURCES = utilities/gpfs/marfs_gc.c utilities/gpfs/marfs_gc.h \
				   utilities/gpfs/utilities_common.c                   \
				   utilities/gpfs/utilities_common.h   \
                                   utilities/common/src/hash_table.c   \
                                   utilities/common/src/hash_table.h

marfs_quota_SOURCES = utilities/gpfs/marfs_quota.c utilities/gpfs/marfs_quota.h \
					  utilities/gpfs/utilities_common.c                         \
					  utilities/gpfs/utilities_common.h

marfs_packer_SOURCES = utilities/gpfs/marfs_packer.c     \
					   utilities/gpfs/marfs_packer.h     \
					   utilities/gpfs/utilities_common.c \
					   utilities/gpfs/utilities_common.h 

marfs_repack_SOURCES = utilities/gpfs/marfs_repack.c     \
					   utilities/gpfs/marfs_repack.h     \
					   utilities/gpfs/utilities_common.c \
					   utilities/gpfs/utilities_common.h

marfs_gc_LDFLAGS     = -lgpfs
marfs_quota_LDFLAGS  = -lgpfs
marfs_packer_LDFLAGS = -lgpfs
marfs_repack_LDFLAGS = -lgpfs

endif

# ............................................................................
# Parser
# ............................................................................
PARSE_OPT = -DDATAPARSE
BLUEPRINT = $(abs_top_srcdir)/common/configuration/src/marfs_configuration_blueprint.cfg

PARSE_H_file = config-structs.h
PARSE_H      = $(PARSE_DIR)/parse-inc/$(PARSE_H_file)

.PHONY: parser
# This generates a 'datapars' in PARSE_DIR, using the BLUEPRINT config.
# This parser should then be able to parse MarFS configs that are
# consistent with the blueprint.
#
# NOTE: If you get errors about "redefining struct namespace",
#     You probably have a mistake in the tags in your config.
#
parser:
	@ echo; echo "#-- cleaning $(PARSE_DIR)"
	@ $(MAKE) -C $(PARSE_DIR) pristine

	@ echo; echo "#-- making 'base'"
	@ # $(MAKE) -C $(PARSE_DIR) confpars-d
	@ $(MAKE) -C $(PARSE_DIR) base

	@ echo; echo "#-- running confpars with $(BLUEPRINT)"
	@ # (cd $(PARSE_DIR); ./confpars $(BLUEPRINT); echo 0 )
	(cd $(PARSE_DIR); mkdir -p parse-inc; ./confpars $(BLUEPRINT) )

	@ echo; echo "#-- making datapars"
	@ # $(MAKE) -C $(PARSE_DIR) datapars-d   [fails]
	@ $(MAKE) -C $(PARSE_DIR) datapars

	@ echo; echo "#-- running datapars with $(CONFIG)"
	(cd $(PARSE_DIR); ./datapars $(CONFIG) )

	@ # NOTE: This is the only time PA2X's Makefile will honor our CFLAGS settings.
	@ echo; echo "#-- making $(PARSE_DIR)/*.o (again, because makefile deleted them)"
	@ $(MAKE) -C $(PARSE_DIR) $(PARSE_OBJ_Files) CFLAGS="$(OPTIONS) $(PARSE_OPT)"

#	@ echo; echo "#-- making a symlink to $(PARSE_H)"
#	ln -fs $(PARSE_H) $(PARSE_H_file)


# a super nasty way to make sure that parser is always built first.
BUILT_SOURCES = parser
CLEANFILES = $(PARSE_H_file)


### include_HEADERS += $(PARSE_H)
otherincludedir = $(includedir)/parse-inc
otherinclude_HEADERS = $(PARSE_H)

# ............................................................................
# Various Testing
# ............................................................................
check_PROGRAMS = test_marfs_configuration

# test_lock test_lock2 test_lock2b

test_lock: test_lock.c spinlock_asm.s
	$(CC) -g -o $@ $^ -lpthread -lrt

test_lock2: test_lock2.c spinlock_asm.o spinlock.h spinlock.o
	$(CC) -g -o $@ $^ -lpthread -lrt

test_lock2b: test_lock2b.c spinlock_asm.o spinlock.o
	$(CC) -g -o $@ $^ -lpthread -lrt

test_marfs_configuration_SOURCES = fuse/src/test_marfs_configuration.c

# ............................................................................
# Useful development targets
# ............................................................................
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



# NOTE: NFS-exporting (v3 or v4) a fuse-mount mounted with 'direct_io'
#       causes all reads received at fuse to have size=4K.  Without
#       direct_io, read sizes in this scenario are 128K.

FUSE_FLAGS = -o allow_other,use_ino,intr
# ifeq ($(DEBUG),2)
#	FUSE_FLAGS += -d -f
# endif

PID = marfs_fuse.pid
LOG =  >fuse.log 2>&1


# convenience targets for fuse.
# TODO: re-introduce the old EXEC, which allowed e.g. gprof, valgrind, etc.

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

