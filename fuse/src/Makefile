# This file is part of MarFS, which is released under the BSD license.
# 
# 
# Copyright (c) 2015, Los Alamos National Security (LANS), LLC
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
# 
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# 
# -----
# NOTE:
# -----
# MarFS uses libaws4c for Amazon S3 object communication. The original version
# is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
# LANS, LLC added functionality to the original work. The original work plus
# LANS, LLC contributions is found at https://github.com/jti-lanl/aws4c.
# 
# GNU licenses can be found at <http://www.gnu.org/licenses/>.
# 
# 
# From Los Alamos National Security, LLC:
# LA-CC-15-039
# 
# Copyright (c) 2015, Los Alamos National Security, LLC All rights reserved.
# Copyright 2015. Los Alamos National Security, LLC. This software was produced
# under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
# Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
# the U.S. Department of Energy. The U.S. Government has rights to use,
# reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
# ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
# ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
# modified to produce derivative works, such modified software should be
# clearly marked, so as not to confuse it with the version available from
# LANL.
# 
# THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
# OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE.




# Lots of testing/support crap in here, for now.
#
# 'make'           rebuild the fuse client
# 'make remount'   unmount [if needed],  mount
# 'make redo'      unmount [if needed], clean, build, WIPE-SYSLOG!, mount

all: fuse


# non-useless shell
SHELL = /bin/bash

# logging.* has now been moved to here
LOGGING = $(PWD)/../../common/log/src

# Brett's configuration reader is (temporarily) here
CONFIG = $(PWD)/../../common/configuration/src

# remote libs.  Re-link, if any of these are more recent than our last build.
FUSE_DEPS =


# These are files where .c, .o, .h all make sense
FNAME_ROOTS = marfs_base common object_stream marfs_ops push_user mdal dal

OBJS =            logging.o $(patsubst %, %.o, $(FNAME_ROOTS))
SRCS = $(LOGGING)/logging.c $(patsubst %, %.c, $(FNAME_ROOTS))
H    = $(LOGGING)/logging.h $(patsubst %, %.h, $(FNAME_ROOTS))


# pipe-to-logger works, but without -d, all fuse stdout/stderr is lost
# LOG  = 2>&1 | logger
LOG  = > fuse.log 2>&1



MNT := /marfs


# file
PID = marfs_fuse.pid



# ---------------------------------------------------------------------------
# CFLAGS, LDFLAGS, etc
#
# Flags for the compiler and linker depend on what target we're building, 
# ---------------------------------------------------------------------------


# c99 needed for gcc 4.4.7 to know snprintf(), and floorf()
CFLAGS  += -Wall -std=c99 -I.

# This adds runtime checking against smashing the stack
CFLAGS += -fstack-protector-all

# Default to gcc
CC=gcc

# 700 = Single UNIX Spec v4.
#
# We use:
#  -- strptime()
#  -- readdir_r()
#  -- utimensat(... AT_SYMLINK_NOFOLLOW)
DEFS += _XOPEN_SOURCE=700


# environment-variable DEBUG controls compile-flags:
# NOTE: -DDEBUG w/value > 1 turns on curl-conversation logging
#
#   DEBUG = undef/null    --    -O3
#   DEBUG = 0             --    -g  -DEBUG=0    (-g, but no logging)
#   DEBUG = 1             --    -O3 -DEBUG=1    (-O3, plus logging)
#   DEBUG = <n>           --    -g  -DEBUG=<n>  (libcurl transactions to stdout)
#
# environment-var DEBUG also controls command-line flags for fuse launch.
#
#   DEBUG = undef/null/0  --    
#   DEBUG = 1             --    
#   DEBUG = <n>           --    -d -f  [-f means foreground, without threads]
#

FUSE_FLAGS += -o allow_other,direct_io,use_ino,intr

ifdef DEBUG
	ifeq ($(DEBUG),)
		CFLAGS += -O3

	else ifeq ($(DEBUG),0)
		CFLAGS += -g

	else ifeq ($(DEBUG),1)
		CFLAGS += -O3
		DEFS   += DEBUG=$(DEBUG)

	else
		CFLAGS += -g
		DEFS   += DEBUG=$(DEBUG)
		FUSE_FLAGS += -d -f
	endif
else
	CFLAGS += -O3
endif


# When mounted, this will accumulate profiling info into /gmon.out
# After unmounting, you can analyze the profiling info like this:
#
#   gprof ./marfs_fuse /gmon.out > gmon.txt
#
# NOTE: gprof sucks if threads or kernel are involved.  Ours involves both.
ifdef GPROF
	# CFLAGS = -pg
	CFLAGS  += -pg -g
	LDFLAGS += -pg -g
endif

# For use with valgrind:
#
#   make clean && make grind
#   make mnt.grind      -->  valgrind --tool=callgrind [callgrind opts] marfs_fuse [fuse opts]
#   [run exercises]
#   make umnt
#   callgrind_annotate [options] callgrind.out.`cat $PID`
#   # NOTE: there are actually 2 out-files.
#   
ifdef GRIND
	CFLAGS  += -g
	LDFLAGS += -g
	# EXEC     = valgrind --leak-check=yes
	EXEC     = valgrind --tool=callgrind
endif

ifdef HELGRIND
	CFLAGS  += -g
	LDFLAGS += -g
	EXEC     = valgrind --tool=helgrind
endif



# For testing new MDAL code
#
ifdef USE_MDAL
	DEFS += USE_MDAL=$(USE_MDAL)
endif


ifdef USE_DAL
	DEFS += USE_DAL=$(USE_DAL)
endif



# NOTE: You should define LIBAWS4C in your environment, using one of the
#       following methods:
#
#       [bash]  export LIBAWS4C=/my/path/to/aws4c; make ...
#       [tcsh]  setenv LIBAWS4C /my/path/to/aws4c; make ...
#
#       [any]   make ... LIBAWS4C=/my/path/to/aws4c
CFLAGS    += -I$(LIBAWS4C)
LDFLAGS   += -L$(LIBAWS4C)
LIBS      += -laws4c -laws4c_extra -lcurl -lxml2 -lcrypto -lpthread -lrt
FUSE_DEPS += $(LIBAWS4C)/libaws4c.a



# Logging support has been moved its own spot in the git repo
CFLAGS  += -I$(LOGGING)

# This tells logging.h to write via syslog(), instead of printf()
# If USE_SYSLOG is undef, and DEBUG is undef, there is no logging output
ifdef USE_SYSLOG
	DEFS += USE_SYSLOG
endif

ifdef USE_STDOUT
	DEFS += USE_STDOUT
endif

# Tells marfs_read_internal() that any out-of-order (discontiguous) reads
# can be enqueued, because NFS will eventually requests the intervening
# data, and the read will then be in order, and allowed to proceed.
#
# NOT A GOOD IDEA.  NFS does scattered reads across file-handles as well as
# threads, so this causes reads on some file-handles with a single NFS
# read-thread to stall unnecessarily.  They might've been better off
# waiting until they had detected multiple readers dynamically.  Leaving it
# in, in case there's some path forward where this becomes a good idea,
# again.
ifdef NFS_THREADS
   DEFS += NFS_THREADS
endif

# needs extra quoting to survive being read by the shell, on gcc command-lines
ifdef LOG_PREFIX
	DEFS += LOG_PREFIX='"$(LOG_PREFIX)"'
endif


# NOTE: Confusingly, this DOES NOT control whether SPROXYD will be used.
#    currently, this just tells fuse that it's okay if ~/.awsAuth doesn't exist
ifdef USE_SPROXYD
	DEFS += USE_SPROXYD=1
endif


# This affects thread-locking in object-streams.c (controlling interaction
# between object-streams and curl callbacks to op-thread).  Without this,
# we'll use semaphores, which seem to incur huge context-switching
# overhead.  With this, we'll use our PoliteSpinLock, which doesn't
# actually burn a lot of CPU.
ifdef SPINLOCKS
	DEFS += SPINLOCKS
	OBJS += spinlock.o spinlock_asm.o
	H    += spinlock.h
	LIBS += -lrt
endif


# link with the config-reader library.
CFLAGS    += -I$(CONFIG)
LDFLAGS   += -L$(CONFIG)
LIBS      += -lconfig
FUSE_DEPS += $(CONFIG)/libconfig.a


# If we are building the 'lib' targets for others (e.g. pftool, GC, quota,
# etc), then we don't want to have to link with fuse proper.  For those
# targets, LINK_LIBFUSE will not be defined.  On the other hand, when building
# fuse, we will link with libfuse.
CFLAGS  += $(shell pkg-config fuse --cflags)
ifdef LINK_LIBFUSE
	LDFLAGS += $(shell pkg-config fuse --libs-only-L)
	LIBS    += $(shell pkg-config fuse --libs-only-l) -lm
	DEFS    += LINK_LIBFUSE
endif

ifdef USE_MC
	OBJS += $(LIBNE)/erasure.o
	CFLAGS += -I$(LIBNE)
#	LDFLAGS += -L$(LIBNE)
#	LIBS += -lisal -lne
endif

# assimilate all the DEFS we've added, into CFLAGS
CFLAGS  += $(patsubst %, -D%, $(DEFS))


# ...........................................................................
# basics
# ...........................................................................

logging.o: $(LOGGING)/logging.c $(LOGGING)/logging.h
	$(CC) -c $(CFLAGS) -o $@ $<

marfs_configuration.o: $(CONFIG)/marfs_configuration.c $(CONFIG)/marfs_configuration.h
	$(CC) -c $(CFLAGS) -o $@ $<


%: %.o
	$(CC) -o $@ $< $(LDFLAGS) $(LIBS)

%.o: %.c %.h
	$(CC) -c $(CFLAGS) -o $@ $<

%.o: %.s
	$(CC) -c $(CFLAGS) -o $@ $<


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*


# for every '*.c' file, if there is an executable file without the '.c',
# then delete that executable file.
# Doesn't work.  Why not?
cleanx:
	ls *.c | sed -e 's/\.c$$//' | xargs -r -L 1 bash -c '[[ -x $$0 ]] && rm $$0'

clean: # cleanx
	$(RM) *.{o,i,E}
	$(RM) *~
	$(RM) marfs_fuse
	$(RM) marfs_pipe
	$(RM) core.*
	$(RM) libmarfs.a
	$(RM) libmarfs_*mdal.a


# ...........................................................................
# fuse
# ...........................................................................

# Instead of generating cryptic error-messages from gcc, we can inform the
# user about certain basics they might be missing.
#
# (a) Make sure that either the default value for LIBAWS4C is valid, or
#     user has overridden it with something valid.
.PHONY: pre_req
pre_req:
ifeq ($(LIBAWS4C),)
	$(error $$LIBAWS4C is empty.  Consider 'make ... LIBAWS4C=<DIR>')
else ifeq ($(wildcard $(LIBAWS4C)/libaws4c.a),)
	$(error couldnt find $$LIBAWS4C (in $(LIBAWS4C)/*).  Consider 'make ... LIBAWS4C=<DIR>')
endif




# Don't make this directly; make one of the 'fuse.foo' targets below.
marfs_fuse: $(H) $(OBJS) main.c $(FUSE_DEPS) libmarfs.a
	@# $(CC) $(CFLAGS) -o $@ $(LDFLAGS) -Wl,--verbose main.c $(OBJS) $(LIBS)
	@# $(CC) $(CFLAGS) -o $@ $(LDFLAGS) main.c $(OBJS) $(LIBS)
	$(CC) $(CFLAGS) -o $@ $(LDFLAGS) -L$(PWD) main.c -lmarfs $(LIBS)

fuse:  fuse.std
fast:  fuse.fast
lean:  fuse.lean
dbg:   fuse.dbg
gprof: fuse.gprof
grind: fuse.grind




# --- specialized builds
fuse.std: pre_req
	@ # $(MAKE) marfs_fuse LINK_LIBFUSE=1 USE_SYSLOG=1 DEBUG=2
	@ $(MAKE) marfs_fuse LINK_LIBFUSE=1 USE_SYSLOG=1

fuse.fast: pre_req
	@ $(MAKE) marfs_fuse LINK_LIBFUSE=1 USE_SYSLOG=1

fuse.lean: pre_req
	@ $(MAKE) marfs_fuse LINK_LIBFUSE=1 

fuse.dbg: pre_req
	@ $(MAKE) marfs_fuse LINK_LIBFUSE=1 USE_STDOUT=1 DEBUG=2 USE_SPROXYD=1 LOG_PREFIX=marfs_fuse.dbg

fuse.gprof: pre_req
	@ $(MAKE) marfs_fuse LINK_LIBFUSE=1 GPROF=1

fuse.grind: pre_req
	@ $(MAKE) marfs_fuse LINK_LIBFUSE=1 GRIND=1

fuse.helgrind: pre_req
	@ $(MAKE) marfs_fuse LINK_LIBFUSE=1 HELGRIND=1



# --- "redo" means: unmount, make clean, rebuild, clean up, mount

redo: redo.std

# compile -O3, verbose logging
redo.std:  umnt.std  clean reset_syslog fuse.std  mnt.std

# compile -O3, no logging
redo.lean: umnt.lean clean              fuse.lean mnt.lean

# This makes a different mount-point (at /marfs_dbg), with verbose logging to syslog
redo.dbg:  umnt.dbg  clean reset_syslog fuse.dbg  mnt.dbg




# --- easy way to restart fuse, if it crashes.
remount: umnt
	@ make _mnt


# ...........................................................................
# library, containing everything except main()
# ...........................................................................

libmarfs.a: $(OBJS) | pre_req
	ar -cvr $@ $^


.PHONY: lib
lib: libmarfs.a | pre_req



# ...........................................................................
# mount
# ...........................................................................

# NOTE: You should do this, if you want to see core-files.
#       Otherwise, they might end up in /var/tmp
#       [default core-pattern was "/var/tmp/core-%e-%p-%t"]
#
#  $ ulimit -c unlimited
#  $ echo "core.%e-%t" > /proc/sys/kernel/core_pattern
#  $ echo 1 > /proc/sys/fs/suid_dumpable

# _mnt:
# 	@ echo
# 	@ echo "mounting at $(MNT)"
# 	marfs_fuse -f $(MNT) &
# 	echo $! > $(PID)


# # TBD: Do (something like) this, instead of the "sleep", in _mnt
#
# while (! `mount | grep fuse | grep -v fusectl | wc -l`) ; do \
#    (echo "waiting for fuse to start"; sleep 1) \
# done

# NOTE: Need to 'eval' because $(LOG) is now "> file 2>&1" or "2>&1 | logger"
_mnt:
	@ echo
	@ echo "mounting at $(MNT)"
	$(EXEC) ./marfs_fuse $(FUSE_FLAGS) $(MNT) $(LOG) &
	@ echo "sleeping to give background fuse a chance to start"
	sleep 1
	@ ps -elf | grep marfs_fuse | grep $(MNT) | grep -v grep | awk '{print $$4}' > $(PID)
	@ echo "PID: " `cat $(PID)`



mnt: mnt.std

mnt.std: fuse.std umnt.std
	@ $(MAKE) _mnt DEBUG=$(DEBUG)

mnt.lean: fuse.lean umnt
	@ $(MAKE) _mnt LOG="> /dev/null 2>&1"

mnt.grind: fuse.grind umnt
	@ $(MAKE) _mnt GRIND=1

mnt.helgrind: fuse.helgrind umnt
	@ $(MAKE) _mnt HELGRIND=1

mnt.dbg: fuse.dbg umnt.dbg
	@ $(MAKE) _mnt MNT=$(MNT)_dbg DEBUG=2 LOG="> fuse.log_dbg 2>&1"




# ...........................................................................
# un-mount
# ...........................................................................

# okay to run this even if not mounted
_umnt:
	@ echo -n "unmounting $(MNT): ... "
	@ (fusermount -q -u $(MNT) && echo "unmounted") || echo "already unmounted ?"
	@ rm -f $(PID)


umnt: umnt.std

umnt.std: _umnt

umnt.dbg:
	@ $(MAKE) _umnt MNT=$(MNT)_dbg




# umnt.force:
# 	@ ps -elf | grep marfs_fuse | grep -v grep | awk '{print $$4}' | xargs -r echo kill -KILL
# 	@ ps -elf | grep marfs_fuse | grep -v grep | awk '{print $$4}' | xargs -r kill -KILL
# 	@ $(MAKE) umnt.simple

umnt.force:
	@ cat $(PID) | xargs -r echo kill -KILL
	@ cat $(PID) | xargs -r kill -KILL
	@ rm -f $(PID)
	@ $(MAKE) umnt.std

test_umnt:
	@ (fusermount -q -u $(MNT) && echo "unmounted") || ( (( `ps -elf | grep fuse_mount | grep -v grep` )) && echo "wedged" ) || echo "already unmounted"


# ...........................................................................
# reset everything
#
# When testing against an S3 repo, it's easy to "wipe marfs".  You can ask
# the S3 server for all the objects (matching some pattern), and delete
# those.  Then, you also wipe out your MD contents and you're in pristine
# state.  Now that we've moved on to Scality sproxyd, this is less
# applicable.
#
# Yes, you could just look at the MDFS, and delete all the corresponding
# objects.  That works fine if you don't have any bugs in your metadata.
# During testing, we *do* have bugs, and the point of this was to make
# things fresh, by brute force.
# ...........................................................................


# # wipe all objects under  http://10.140.0.15:9020/emcS3_00.-test00/
# reset_obj:
# 	@ ls_obj_w_prefix emcS3_00.-test00 ver | xargs -r -L 1 rm_obj

# on marfs-gpfs-003, syslog-ng is disabled by default.  We're the
# only ones using it, so this seems reasonable
reset_syslog:
	service syslog-ng stop
	rm -f /var/log/messages
	service syslog-ng start



# ...........................................................................
# debugging
# ...........................................................................

# TBD: develop a unit-test script to run ops on the mount-point
# unit tests of the FUSE mount
# unit: unit.c

# test: marfs_fuse unit
# 	$(MAKE) mount
# 	unit $(MNT)
# 	$(MAKE) umount



# I have things setup so core-files sort chronologically.  (See above)
gdb.core:
	ls core.* | tail -n -1 | xargs exec gdb marfs_fuse

gdb:
	# @ exec gdb marfs_fuse `cat "$(PID)"`
	@ exec gdb marfs_fuse `ps -elf | egrep 'marfs_fuse(.*)/marfs' | grep -v grep | awk '{print $$4}'`


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


run-%: %
	RUN $* $(MNT)/test00


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
demo_%: demo_%.o $(OBJS)
	@ # $(CC) -o $@ $< $(LDFLAGS) $(LIBS) $(OBJS)
	@ echo "demo: $*"
	@ $(MAKE) _app_demo_$* LINK_LIBFUSE=1

test_%: test_%.c
	@ echo "test: $*"
	@ $(MAKE) _app_test_$* LINK_LIBFUSE=1

_app_%: libmarfs.a
	@ echo "making: _app_$*"
	$(CC) $(CFLAGS) -o $* $(LDFLAGS) -L$(PWD) $*.c -lmarfs $(LIBS)


# lines-of-code
CODE = $(H) $(SRCS) main.c \
	$(CONFIG)/marfs_configuration.{c,h} $(CONFIG)/marfs_config.c

# lines-of-code (including libaws4c)
CODE2 = $(CODE) \
	$(LIBAWS4C)/aws4c*.{c,h} $(LIBAWS4C)/test_aws.c

# --- line-of code (include blank lines)
loc:
	cat $(CODE) | wc -l

loc2: pre_req
	cat $(CODE2) | wc -l

# --- non-blank lines
nb_loc:
	cat $(CODE) | egrep -v '^$$' | wc -l

nb_loc2: pre_req
	cat $(CODE2) | egrep -v '^$$' | wc -l





# test shell-syntax for checking Makefile-variables
var_%:
	@ ([ -n "$($*)" ] && echo "value: $($*)" ) || echo "empty: $*"

printenv:
	printenv

inc_path:
	$(CC) -Wp,-v $(CFLAGS) -c -o marfs_base.o marfs_base.c

subshell_uid:
	( id )

fuse_pid:
	ps -elf | egrep 'marfs_fuse(.*)/marfs' | grep -v grep | awk '{print $$4}'

locals:
	@ echo "CFLAGS     = $(CFLAGS)"
	@ echo "LDFLAGS    = $(LDFLAGS)"
	@ echo "FUSE_FLAGS = $(FUSE_FLAGS)"
	@ echo "DEBUG      = $(DEBUG)"
	@ echo "LIBAWS4C   = $(LIBAWS4C)"
