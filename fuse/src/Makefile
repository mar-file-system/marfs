all: marfs_fuse

GIT = ~/projects/marfs/git

OBJS = marfs_base.o common.o

# MNT  = ./filesys/mnt
MNT  = $(GIT)/fuse/filesys/mnt

# c99 needed for gcc 4.4.7 to know snprintf(), and floorf()
CFLAGS += -Wall -std=c99 `pkg-config fuse --cflags`
# CFLAGS += -Wall

LDFLAGS += `pkg-config fuse --cflags --libs` -lm


# ...........................................................................
# back and forth to HB's test node with XFS (supporting xattrs) at 10.135.0.2

# only works from ccstargate
2hb:
	rsync -Pvaurl ~/projects/marfs/git/fuse/src/ root@10.135.0.2:/root/projects/marfs/git/fuse/src

# only works from ccstargate
fromhb:
	rsync -Pvaurl root@10.135.0.2:/root/projects/marfs/git/fuse/src/ ~/projects/marfs/git/fuse/src

# ...........................................................................




# currently a simple top-level FUSE daemon
marfs_fuse: $(OBJS) marfs_fuse.c
	gcc $(CFLAGS) -o marfs_fuse $(LDFLAGS) $(OBJS) marfs_fuse.c



PID = marfs_fuse.pid

mnt: marfs_fuse
	@ # marfs_fuse $(MNT)
	@ echo
	@ echo "mounting at $(MNT)"
	marfs_fuse -f $(MNT) &

mntlog: marfs_fuse
	@ echo
	@ echo "mounting at $(MNT)"
	marfs_fuse -f $(MNT) > foo.log 2>&1 &

# TBD should succeed silently if not mounted
umnt:
	fusermount -u $(MNT)


reset:
	rm -f /mnt/xfs/jti/filesys/mdfs/test00/foo


# TBD: develop a unit-test script to run ops on the mount-point
# unit tests of the FUSE mount
# unit: unit.c

# test: marfs_fuse unit
# 	$(MAKE) mount
# 	unit $(MNT)
# 	$(MAKE) umount

# test: umnt clean marfs_fuse reset mntlog
test: clean marfs_fuse reset mntlog
	echo foo > ../filesys/mnt/test00/foo



%: %.o
	gcc -o $@ $< $(LDFLAGS)

%.o: %.c
	gcc $(CFLAGS) -c -o $@ $<


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*



run-%: %
	RUN $* $(MNT)/test00

clean:
	$(RM) *.{o,i}
	$(RM) *~
	$(RM) marfs_fuse
	$(RM) core.*




# -- various testing

shell_test:
	[ -e foo.pid ] \
	&& [ -n "`cat foo.pid | xargs ps --no-headers`" ] \
	&& ( eval "cat foo.pid | xargs kill -TERM" ; \
		eval "cat foo.pid | xargs echo killed" ; \
		rm foo.pid )

hello3: hello3.o $(OBJS)
	gcc $(CFLAGS) -o $@ $(LDFLAGS) $^


# lines-of-code
loc:
	wc -l marfs_base.{h,c} common.{h,c} marfs_fuse.{h,c} 

show_inc_path:
	gcc -Wp,-v $(CFLAGS) -c -o marfs_base.o marfs_base.c


