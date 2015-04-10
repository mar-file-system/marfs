all: fuse

OBJS = config.o common.o
MNT  = ./filesys/mnt

# currently a simple top-level FUSE daemon
fuse: $(OBJS) fuse.c
	gcc -Wall fuse.c `pkg-config fuse --cflags --libs` -o fuse

# unit tests of the FUSE mount
# unit: unit.c


mount: fuse
	fuse $(MNT)

umount:
	fusermount -u $(MNT)

# TBD: develop a unit-test script to run ops on the mount-point
test: fuse unit
	$(MAKE) mount
	unit $(MNT)
	$(MAKE) umount



%: %.o
	gcc -o $@ `pkg-config fuse --libs` $<

%.o: %.c
	gcc -Wall -c -o $@ `pkg-config fuse --cflags` $<

# seriously?  You can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*



hello3: hello3.o common.o config.o
	gcc -Wall -o $@ `pkg-config fuse --cflags --libs` $^






run-%: %
	RUN $* $(MNT)

clean:
	$(RM) *.o
