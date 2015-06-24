# Lots of testing/support crap in here, for now.

all: marfs_fuse


OBJS = logging.o marfs_base.o common.o object_stream.o
SRCS = logging.c marfs_base.c common.c object_stream.c
H    = logging.h marfs_base.h common.h object_stream.h
LOG  = fuse.log


GIT = `readlink -f ~/projects/marfs/git`

### SYS = `readlink -f ~/projects/marfs/filesys`
### 
### MNT  = $(SYS)/mnt
### MDFS = $(SYS)/mdfs/test00


GPFS = /gpfs/marfs-gpfs/fuse

MNT    = /marfs

# DO NOT COMMENT ANY OF THESE OUT !!!
# look at reset_md
MDFS   = $(GPFS)/test00/mdfs
FSINFO = $(GPFS)/test00/fsinfo
TRASH  = $(GPFS)/test00/trash




# if you set the environment variable DEBUG, we'll add '-g', and define
# DEBUG for the pre-processor.  Use DEBUG=0 to turn off diagnostic output,
# but still compile with -g.  For optimization, you must undef DEBUG, or
# define it empty.
#
# DEBUG undef  -- full optimization, no -DDEBUG
# DEBUG null   -- full optimization, no -DDEBUG
# DEBUG <n>    -- compile w/ "-g -DDEBUG=<n>"
ifdef DEBUG
	ifeq ($(DEBUG),)
		CFLAGS += -O3
	else
      # NOTE: this also applies if DEBUG = 0
		CFLAGS += -g
		DEFS   += DEBUG=$(DEBUG)
	endif
else
	CFLAGS += -O3
endif

# environment-var DEBUG also controls command-line flags for fuse launch.
# DEBUG = 0 (or undef, or empty)
#   run as standard background task, w/out debug flags
# DEBUG >= 1
#   run in foreground, with debug flags  (NOTE: foreground is single-threaded)
FUSE_FLAGS += -d -f
ifndef DEBUG
	FUSE_FLAGS =
	# what would this do? ...
	# FUSE_FLAGS = -o intr
else ifeq ($(DEBUG),)
	FUSE_FLAGS =
else ifeq ($(DEBUG),0)
	FUSE_FLAGS =
	# # performance-tuning experiments (these don't seem to help)
	# FUSE_FLAGS = -d -f -o max_write="$$((1024 * 1024))"
	# FUSE_FLAGS = -d -f -o noatime
	# FUSE_FLAGS = -o noatime
endif


# This tells logging.h to write via syslog(), instead of printf()
ifdef USE_SYSLOG
	DEFS += USE_SYSLOG
endif



# c99 needed for gcc 4.4.7 to know snprintf(), and floorf()
CFLAGS  += -Wall -std=c99 `pkg-config fuse --cflags` -I.
CFLAGS  += $(patsubst %, -D%, $(DEFS))

# LDFLAGS += `pkg-config fuse --cflags --libs` -lm -L $(LIBAWS4C) -laws4c
LDFLAGS += `pkg-config fuse --libs-only-L`
LIBS    += `pkg-config fuse --libs-only-l` -lm

# stuf for aws4c
### LIBAWS4C = /users/jti/projects/ecs_hobo/tools/aws4c
LIBAWS4C = `readlink -f ~/projects/ecs_hobo/tools/aws4c`
CFLAGS  += -I $(LIBAWS4C)
LDFLAGS += -L$(LIBAWS4C)
LIBS    += -laws4c -laws4c_extra -lcurl -lxml2 -lcrypto -lpthread


# ...........................................................................
# back and forth to test node
#
# [see ./init_test_node, to initialize a test node with everything I need]

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




# currently a simple top-level FUSE daemon
marfs_fuse: $(OBJS) marfs_fuse.c $(OBJS)
	gcc $(CFLAGS) -o marfs_fuse $(LDFLAGS) $(OBJS) marfs_fuse.c $(LIBS)

# aplications that link with various marfs support libs
demo_%: demo_%.o $(OBJS)
	gcc -o $@ $< $(LDFLAGS) $(LIBS) $(OBJS)



PID = marfs_fuse.pid


# NOTE: You should do this, if you want to see core-files.
#       Otherwise, they might end up in /var/tmp
#       [default core-pattern was "/var/tmp/core-%e-%p-%t"]
#
#  $ ulimit -c unlimited
#  $ echo "core.%e-%t" > /proc/sys/kernel/core_pattern

mnt: marfs_fuse umnt
	@ echo
	@ echo "mounting at $(MNT)"
	@ echo "core.%e-%t" > /proc/sys/kernel/core_pattern
	( marfs_fuse $(FUSE_FLAGS) $(MNT) > $(LOG) 2>&1 & echo "$$!" > $(PID) )
	@ echo "PID: " `cat $(PID)`
	@
	@ # TBD: while (! `mount | grep fuse | grep -v fusectl | wc -l`) ; do (echo sleeping; sleep 1) done
	@ echo "sleeping to give background fuse a chance to start"
	sleep 1


mnt.simple: marfs_fuse
	@ # marfs_fuse $(MNT)
	@ echo
	@ echo "mounting at $(MNT)"
	marfs_fuse -f $(MNT) &
	echo $! > $(PID)





# okay to run this even if not mounted
umnt: umnt.simple

# umnt.force:
# 	@ ps -elf | grep marfs_fuse | grep -v grep | awk '{print $$4}' | xargs -r echo kill -KILL
# 	@ ps -elf | grep marfs_fuse | grep -v grep | awk '{print $$4}' | xargs -r kill -KILL
# 	@ $(MAKE) umnt.simple

umnt.force:
	@ cat $(PID) | xargs -r echo kill -KILL
	@ cat $(PID) | xargs -r kill -KILL
	@ rm -f $(PID)
	@ $(MAKE) umnt.simple

umnt.simple:
	@ (fusermount -q -u $(MNT) && echo "unmounted") || echo "already unmounted"

test_umnt:
	@ (fusermount -q -u $(MNT) && echo "unmounted") || ( (( `ps -elf | grep fuse_mount | grep -v grep` )) && echo "wedged" ) || echo "already unmounted"



# test shell-syntax for checking Makefile-variables
var_%:
	@ ([ -n "$($*)" ] && echo "value: $($*)" ) || echo "empty: $*"

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



test_wr:
	@ echo "testing 1 2 3" > $(MNT)/test00/foo || echo "failed"
	@ $(MAKE) show_results

test_rd:
	@ cat $(MNT)/test00/foo || echo "failed"
	@ $(MAKE) show_results

show_results:
	@ echo "sleeping to let logging complete"
	sleep 1
	@ echo "tail of $(LOG) (trimmed to match window-size):"
	@ tail -20 $(LOG) | cut -c-`tput cols`
	@ echo
	@ $(MAKE) mdfs-foo
	@ ./show_trash

# TBD: develop a unit-test script to run ops on the mount-point
# unit tests of the FUSE mount
# unit: unit.c

# test: marfs_fuse unit
# 	$(MAKE) mount
# 	unit $(MNT)
# 	$(MAKE) umount

# after edits: unmount, rebuild (if needed), wipe MDFS, mount, run test, show the log
redo: umnt marfs_fuse reset_md reset_obj mnt

redo2: umnt marfs_fuse reset_syslog reset_md reset_obj mnt


# I have things setup so core-files sort chronologically.  (See above)
gdb.core:
	ls core.* | tail -n -1 | xargs exec gdb marfs_fuse

gdb:
	@ exec gdb marfs_fuse `cat "$(PID)"`


%: %.o
	gcc -o $@ $< $(LDFLAGS) $(LIBS)

%.o: %.c %.h
	gcc $(CFLAGS) -c -o $@ $<


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*



run-%: %
	RUN $* $(MNT)/test00

# for every '*.c' file, if there is an executable file without the '.c',
# then delete that executable file.
cleanx:
	ls *.c | sed -e 's/\.c$$//' | xargs -L 1 bash -c '[[ -x $$0 ]] && rm $$0'

clean:
	$(RM) *.{o,i}
	$(RM) *~
	$(RM) marfs_fuse
	$(RM) core.*


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


# -- various testing

shell_test:
	[ -e foo.pid ] \
	&& [ -n "`cat foo.pid | xargs ps --no-headers`" ] \
	&& ( eval "cat foo.pid | xargs kill -TERM" ; \
		eval "cat foo.pid | xargs echo killed" ; \
		rm foo.pid )

hello3: hello3.o $(OBJS)
	gcc $(CFLAGS) -o $@ $(LDFLAGS) $^ $(LIBS)

# for testing with repo.chunk_size=2048, and sizeof(RecoveryInfo)=1080 That
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



# lines-of-code
loc:
	wc -l $(H) $(SRCS)

loc2:
	wc -l $(H) $(SRCS)  $(LIBAWS4C)/*.{c,h}


show_inc_path:
	gcc -Wp,-v $(CFLAGS) -c -o marfs_base.o marfs_base.c

show_env:
	printenv

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
