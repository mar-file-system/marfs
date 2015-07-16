# Lots of testing/support crap in here, for now.

all: marfs_fuse

# non-useless shell
SHELL = /bin/bash

OBJS = logging.o marfs_base.o common.o object_stream.o
SRCS = logging.c marfs_base.c common.c object_stream.c
H    = logging.h marfs_base.h common.h object_stream.h
LOG  = fuse.log



MNT    := /marfs

GPFS = /gpfs/marfs-gpfs/fuse

# DO NOT COMMENT ANY OF THESE OUT !!!
# look at reset_md
MDFS   = $(GPFS)/test00/mdfs
FSINFO = $(GPFS)/test00/fsinfo
TRASH  = $(GPFS)/test00/trash

PID = marfs_fuse.pid

# LIBAWS4C := ~/projects/git/aws4c
LIBAWS4C := $(shell readlink -f ~/projects/ecs_hobo/tools/aws4c)


# # passed in the environment by a recursive make.  (See e.g. 'fuse.dbg')
# DEFS := $(RECURSIVE_DEFS)


# if you set the environment variable DEBUG, we'll add '-g', and define
# DEBUG for the pre-processor.  Use DEBUG=0 to turn off diagnostic output,
# but still compile with -g.  For optimization, you must undef DEBUG, or
# define it empty.
#
# environment-var DEBUG also controls command-line flags for fuse launch.
# DEBUG = 0 (or undef, or empty)
#   run as standard background task, w/out debug flags
# DEBUG >= 1
#   run in foreground, with debug flags  (NOTE: foreground is single-threaded)
#
# Summary:
#    DEBUG undef/null/0  --   -O3
#    DEBUG <n>           --   -g -DDEBUG=<n> / launch w/ '-d -f'

FUSE_FLAGS += -o allow_other
ifdef DEBUG
	ifeq ($(DEBUG),)
		CFLAGS += -O3
	else ifeq ($(DEBUG),0)
		CFLAGS += -O3
	else
		CFLAGS += -g
		DEFS   += DEBUG=$(DEBUG)
		FUSE_FLAGS += -d -f
	endif
else
	CFLAGS += -O3
endif

# currently, this just tells fuse that it's okay if ~/.awsAuth doesn't exist
ifdef USE_SPROXYD
	DEFS += USE_SPROXYD=1
endif

# This tells logging.h to write via syslog(), instead of printf()
ifdef USE_SYSLOG
	DEFS += USE_SYSLOG
endif

# needs extra quoting to survive being read by the shell, on gcc command-lines
ifdef LOG_PREFIX
	DEFS += LOG_PREFIX='"$(LOG_PREFIX)"'
endif


# c99 needed for gcc 4.4.7 to know snprintf(), and floorf()
CFLAGS  += -Wall -std=c99 $(shell pkg-config fuse --cflags) -I.
CFLAGS  += $(patsubst %, -D%, $(DEFS))

# LDFLAGS += `pkg-config fuse --cflags --libs` -lm -L $(LIBAWS4C) -laws4c
LDFLAGS += $(shell pkg-config fuse --libs-only-L)
LIBS    += $(shell pkg-config fuse --libs-only-l) -lm

# stuff for aws4c
### LIBAWS4C = /users/jti/projects/ecs_hobo/tools/aws4c
### LIBAWS4C = `readlink -f ~/projects/ecs_hobo/tools/aws4c`
### LIBAWS4C := `readlink -f ~/projects/git/aws4c`
CFLAGS  += -I$(LIBAWS4C)
LDFLAGS += -L$(LIBAWS4C)
LIBS    += -laws4c -laws4c_extra -lcurl -lxml2 -lcrypto -lpthread



# ...........................................................................
# back and forth to test node
#
# [see ./init_test_node, to initialize a test node with everything I need]
# ...........................................................................

# WORK = 10.146.0.3
WORK = 10.146.0.2

# only works on ccstargate, or cc-gate
2work:
	rsync -Pvaurl ~/projects/marfs/git/ root@$(WORK):/root/projects/marfs/git
	# rsync -Pvaurl ~/projects/ecs_hobo/tools/aws4c/ root@$(WORK):/root/projects/ecs_hobo/tools/aws4c
	# rsync -Pvaurl ~/projects/ecs_hobo/tools/curl_tools/ root@$(WORK):/root/projects/ecs_hobo/tools/curl_tools


# only works on ccstargate, or cc-gate
fromwork:
	rsync -Pvaurl root@$(WORK):/root/projects/marfs/git/ ~/projects/marfs/git
	rsync -Pvaurl root@$(WORK):/root/projects/ecs_hobo/tools/aws4c/ ~/projects/ecs_hobo/tools/aws4c
	rsync -Pvaurl root@$(WORK):/root/projects/ecs_hobo/tools/curl_tools/ ~/projects/ecs_hobo/tools/curl_tools



# ...........................................................................
# fuse
# ...........................................................................

# None of this crap works, to do a simple effing test for existence of user's $LIBAWS4C
# in a way that would let me print a simple effing message.  Makefiles suck.
#
# # make sure that either the default value for LIBAWS4C is valid, or user
# # has overridden it with something valid.
# # This approach doesn't work.  Why not?
# check_libaws4c:
# 	@ [[ -d $(LIBAWS4C) ]] || $(error "can't find $(LIBAWS4C).  Consider 'make ... LIBAWS4C=...'")
# 
# # tells make where to look for libaws4c*
# vpath libaws4c% $(LIBAWS4C)
# 
# # the "-laws4c" is Makefile crap to go look for the library.
# # If not found, you should 'make ... LIBAWS4C=<libaws4c_install_dir>"
# marfs_fuse: libaws4c.a $(H) marfs_fuse.c marfs_fuse.h $(OBJS)
# 	gcc $(CFLAGS) -o $@ $(LDFLAGS) $(OBJS) marfs_fuse.c $(LIBS)

marfs_fuse: $(H) marfs_fuse.c marfs_fuse.h $(OBJS)
	@# gcc $(CFLAGS) -o $@ $(LDFLAGS) -Wl,--verbose marfs_fuse.c $(OBJS) $(LIBS)
	gcc $(CFLAGS) -o $@ $(LDFLAGS) marfs_fuse.c $(OBJS) $(LIBS)


fuse: fuse.std

# --- specialized builds
fuse.std:
	@ $(MAKE) marfs_fuse USE_SYSLOG=1

fuse.lean:
	@ $(MAKE) marfs_fuse

fuse.dbg:
	@ $(MAKE) marfs_fuse USE_SYSLOG=1 USE_SPROXYD=1 DEBUG=2 \
                        LOG_PREFIX=marfs_fuse.dbg
#	@ $(MV) marfs_fuse marfs_fuse.dbg


# --- "install" means: unmount, rebuild, clean up, mount

install: install.std

# compile -O3, verbose logging
install.std:  umnt.std  clean reset_syslog fuse.std  mnt.std

# compile -O3, no logging
install.lean: umnt.lean clean              fuse.lean mnt.lean

# This makes a different mount-point (at /marfs_dbg), with verbose logging to syslog
install.dbg:  umnt.dbg  clean reset_syslog fuse.dbg  mnt.dbg


# ...........................................................................
# basics
# ...........................................................................

%: %.o
	gcc -o $@ $< $(LDFLAGS) $(LIBS)

%.o: %.c %.h
	gcc -c $(CFLAGS) -o $@ $<


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
	$(RM) *.{o,i}
	$(RM) *~
	$(RM) marfs_fuse
	$(RM) core.*



# ...........................................................................
# mount
# ...........................................................................

# NOTE: You should do this, if you want to see core-files.
#       Otherwise, they might end up in /var/tmp
#       [default core-pattern was "/var/tmp/core-%e-%p-%t"]
#
#  $ ulimit -c unlimited
#  $ echo "core.%e-%t" > /proc/sys/kernel/core_pattern


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

_mnt:
	@ echo
	@ echo "mounting at $(MNT)"
	( ./marfs_fuse $(FUSE_FLAGS) $(MNT) > $(LOG) 2>&1 & echo $$! > $(PID) )
	@ echo "PID: " `cat $(PID)`
	@ echo "sleeping to give background fuse a chance to start"
	sleep 1

test.mnt:
	@ # ( bash -c "(sleep 10; echo done)" & echo $$! )
	@ bash -c "(echo $$$$; sleep 10; echo done)" & echo "pid: $$!"; echo "ppid: $$PPID"





mnt: mnt.std

mnt.std: fuse.std umnt.std
	@ $(MAKE) _mnt LOG=/dev/null

mnt.lean: fuse.lean umnt.lean
	@ $(MAKE) _mnt LOG=/dev/null

mnt.dbg: fuse.dbg umnt.dbg
	@ $(MAKE) _mnt MNT=$(MNT)_dbg




# ...........................................................................
# un-mount
# ...........................................................................

# okay to run this even if not mounted
_umnt:
	@ echo -n "unmounting $(MNT): "
	@ (fusermount -q -u $(MNT) && echo "unmounted") || echo "already unmounted"


umnt: umnt.std

umnt.std: _umnt

umnt.lean: _umnt

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

# wipe all state from MDFS
# Create a dummy fsinfo file, for now.
#
# CAREFUL!!! Think what happens if one of these variables is undefined.
#            That's why this is in a makefile.
reset_md:
	( [ -n "$(MDFS)" ]  && rm -rf $(MDFS)/* )
	( [ -n "$(TRASH)" ] && rm -f $(TRASH)/* )
	( [ -n "$(FSINFO)" ]  && rm -f $(FSINFO) )
	touch $(FSINFO)

# wipe all objects under  http://10.140.0.15:9020/emcS3_00.-test00/
reset_obj:
	@ ls_obj_w_prefix emcS3_00.-test00 ver | xargs -r -L 1 rm_obj

# on the marfs-gpfs-003, syslog-ng is disabled by default.  We're the
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

# after edits: unmount, rebuild (if needed), wipe MDFS, mount, run test, show the log
redo: umnt fuse reset_md reset_obj mnt

redo2: umnt fuse reset_syslog reset_md reset_obj mnt



# I have things setup so core-files sort chronologically.  (See above)
gdb.core:
	ls core.* | tail -n -1 | xargs exec gdb marfs_fuse

gdb:
	# @ exec gdb marfs_fuse `cat "$(PID)"`
	@ exec gdb marfs_fuse `ps -elf | egrep 'marfs_fuse(.*)/marfs' | grep -v grep | awk '{print $$4}'`


# fusectl filesystem.  Under the mounted point, each existing connection
# has a numbered subdirectory.  There's a special file named "abort" there.
# Writing anything to that file aborts the connection.
fusectl:
	mount -t fusectl none /sys/fs/fuse/connections

abort: fusectl
	for A in `find /sys/fs/fuse/connections/ -name abort`; do echo $$A; echo 1 > $$A;  done


run-%: %
	RUN $* $(MNT)/test00



# show quick diagnostics of the guts of an MDFS file
#  e.g. 'make mdfs-foo' shows ...filesys/mdfs/test00/foo
mdfs-%:
	@ echo
	@ echo "overview of metadata for $(MDFS)/$*"
	@ ls -l   $(MDFS)/$*
	@ echo
	@ # echo "xattrs:"
	@ # (attr -l $(MDFS)/$* | grep marfs_) || echo "(none)"
	@ echo
	@ ( attr -g marfs_objid $(MDFS)/$* 2>/dev/null || echo "N/A")
	@ echo
	@ ( attr -g marfs_post  $(MDFS)/$* 2>/dev/null || echo "N/A")


# ...........................................................................
# various testing
# ...........................................................................

# hello3: hello3.o $(OBJS)
# 	gcc $(CFLAGS) -o $@ $(LDFLAGS) $^ $(LIBS)

# For testing with repo.chunk_size=2048, and sizeof(RecoveryInfo)+8=1080 That
# leaves 968 of user-data, per object.  We want the individual objects to
# have recognizable contents, so when we read the contents back, it will be
# obvious that correct contents were written into each object.  That means,
# we want successive 968-byte chunks of input to have different values.
.PHONY: foo.multi.4096
foo.multi.4096:
	dd if=/dev/zero bs=968 count=1 | tr '\0' '0' >  foo.multi.4096
	dd if=/dev/zero bs=968 count=1 | tr '\0' '1' >> foo.multi.4096
	dd if=/dev/zero bs=968 count=1 | tr '\0' '2' >> foo.multi.4096
	dd if=/dev/zero bs=968 count=1 | tr '\0' '3' >> foo.multi.4096
	dd if=/dev/zero bs=224 count=1 | tr '\0' '4' >> foo.multi.4096


# aplications that link with various marfs support libs
demo_%: demo_%.o $(OBJS)
	gcc -o $@ $< $(LDFLAGS) $(LIBS) $(OBJS)

# lines-of-code
loc:
	wc -l $(H) $(SRCS)

# lines-of-code (including libaws4c)
loc2:
	wc -l $(H) $(SRCS)  $(LIBAWS4C)/*.{c,h}

# test shell-syntax for checking Makefile-variables
var_%:
	@ ([ -n "$($*)" ] && echo "value: $($*)" ) || echo "empty: $*"

show_env:
	printenv

show_inc_path:
	gcc -Wp,-v $(CFLAGS) -c -o marfs_base.o marfs_base.c

show_subshell_uid:
	( id )

show_pid:
	ps -elf | egrep 'marfs_fuse(.*)/marfs' | grep -v grep | awk '{print $$4}'

show_locals:
	@ echo "CFLAGS     = $(CFLAGS)"
	@ echo "LDFLAGS    = $(LDFLAGS)"
	@ echo "FUSE_FLAGS = $(FUSE_FLAGS)"

.PHONY: show_objects
show_objects:
	./show_objects

.PHONY: show_trash
show_trash:
	./show_trash
