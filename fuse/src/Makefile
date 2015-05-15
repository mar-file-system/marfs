all: marfs_fuse


GIT = `readlink -f ~/projects/marfs/git`
SYS = `readlink -f ~/projects/marfs/filesys`

OBJS = marfs_base.o common.o object_stream.o
H    = marfs_base.h common.h object_stream.h

# MNT  = ./filesys/mnt
MNT  = $(SYS)/mnt
MDFS = $(SYS)/mdfs/test00




# if you set the environment variable DEBUG, we'll add '-g', and defined
# DEBUG for the pre-processor.
ifeq ($(DEBUG),1)
	CFLAGS += -g
	DEFS   += DEBUG=1
else ifeq ($(DEBUG),2)
	CFLAGS += -ggdb -save-temps
	DEFS   += DEBUG=1
else ifeq ($(DEBUG),3)
	CFLAGS += -pg
	DEFS   += DEBUG=2
else
	CFLAGS += -O3
	DEFS   += DEBUG=0
endif



# c99 needed for gcc 4.4.7 to know snprintf(), and floorf()
CFLAGS += -Wall -std=c99 `pkg-config fuse --cflags` -I $(LIBAWS4C) -I.
CFLAGS += $(patsubst %, -D%, $(DEFS))

# LDFLAGS += `pkg-config fuse --cflags --libs` -lm -L $(LIBAWS4C) -laws4c
LDFLAGS += `pkg-config fuse --libs-only-L`
LIBS    += `pkg-config fuse --libs-only-l` -lm

# stuf for aws4c
### LIBAWS4C = /users/jti/projects/ecs_hobo/tools/aws4c
LIBAWS4C = `readlink -f ~/projects/ecs_hobo/tools/aws4c`
LDFLAGS += -L$(LIBAWS4C)
LIBS    += -laws4c -laws4c_extra -lcurl -lxml2 -lcrypto -lpthread


# ...........................................................................
# back and forth to test node
#
# [see ./init_test_node, to initialize a test node with everything I need]

# WORK = 10.135.0.2
WORK = 10.146.0.3

# only works on ccstargate, or cc-gate
2work:
	rsync -Pvaurl ~/projects/marfs/git/ root@$(WORK):/root/projects/marfs/git
	# rsync -Pvaurl ~/projects/ecs_hobo/tools/aws4c/ root@$(WORK):/root/projects/ecs_hobo/tools/aws4c

# only works on ccstargate, or cc-gate
fromwork:
	rsync -Pvaurl root@$(WORK):/root/projects/marfs/git/ ~/projects/marfs/git
	rsync -Pvaurl root@$(WORK):/root/projects/ecs_hobo/tools/aws4c/ ~/projects/ecs_hobo/tools/aws4c

# ...........................................................................




# currently a simple top-level FUSE daemon
marfs_fuse: $(OBJS) marfs_fuse.c $(OBJS)
	gcc $(CFLAGS) -o marfs_fuse $(LDFLAGS) $(OBJS) marfs_fuse.c $(LIBS)

# aplications that link with various marfs support libs
demo_%: demo_%.o $(OBJS)
	gcc -o $@ $< $(LDFLAGS) $(LIBS) $(OBJS)



PID = marfs_fuse.pid

mnt.simple: marfs_fuse
	@ # marfs_fuse $(MNT)
	@ echo
	@ echo "mounting at $(MNT)"
	marfs_fuse -f $(MNT) &

# NOTE: You should do this, if you want to see core-files:
#  # [default core-pattern was "/var/tmp/core-%e-%p-%t"]
#
#  $ ulimit -c unlimited
#  $ echo "core.%e-%t" > /proc/sys/kernel/core_pattern

mnt: marfs_fuse umnt
	@ echo
	@ echo "mounting at $(MNT)"
	@ echo "core.%e-%t" > /proc/sys/kernel/core_pattern
	marfs_fuse -d -f $(MNT) > foo.log 2>&1 &
	@
	@ # TBD: while (! `mount | grep fuse | grep -v fusectl | wc -l`) ; do (echo sleeping; sleep 1) done
	@ echo "sleeping to give background fuse a chance to start"
	sleep 1

# okay to run this even if not mounted
umnt:
	@ (fusermount -q -u $(MNT) && echo "unmounted") || echo "already unmounted"


# wipe all state from MDFS and object-store
reset:
	rm -f $(MDFS)/foo


# TBD: develop a unit-test script to run ops on the mount-point
# unit tests of the FUSE mount
# unit: unit.c

# test: marfs_fuse unit
# 	$(MAKE) mount
# 	unit $(MNT)
# 	$(MAKE) umount

# after edits: unmount, rebuild (if needed), wipe MDFS, mount, run test, show the log
redo: umnt marfs_fuse reset mnt
	echo "foo" > $(MNT)/test00/foo || echo "failed"
	@ echo
	@ echo "tail of foo.log (trimmed to match window-size):"
	@ tail -20 foo.log | cut -c-`tput cols`
	@ echo
	$(MAKE) mdfs-foo

gdb:
	ls core.* | tail -n -1 | xargs exec gdb marfs_fuse

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
	@ echo "xattrs:"
	@ (attr -l $(MDFS)/$* | grep marfs_) || echo "(none)"


# -- various testing

shell_test:
	[ -e foo.pid ] \
	&& [ -n "`cat foo.pid | xargs ps --no-headers`" ] \
	&& ( eval "cat foo.pid | xargs kill -TERM" ; \
		eval "cat foo.pid | xargs echo killed" ; \
		rm foo.pid )

hello3: hello3.o $(OBJS)
	gcc $(CFLAGS) -o $@ $(LDFLAGS) $^ $(LIBS)


# lines-of-code
loc:
	wc -l marfs_base.{h,c} common.{h,c} marfs_fuse.{h,c} 

show_inc_path:
	gcc -Wp,-v $(CFLAGS) -c -o marfs_base.o marfs_base.c

show_cflags:
	@ echo "CFLAGS = $(CFLAGS)"

show_env:
	printenv
