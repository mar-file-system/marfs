all: marfs_gc

#OBJS = marfs_gc.o

CFLAGS += -Wall -lgpfs

# currently a simple top-level FUSE daemon
marfs_gc: marfs_gc.c
	gcc $(CFLAGS) -o marfs_gc $(LDFLAGS) $(OBJS) marfs_gc.c






%: %.o
	gcc -o $@ $< $(LDFLAGS)

%.o: %.c
	gcc $(CFLAGS) -c -o $@ $<


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*


clean:
	$(RM) *.{o,i}
	$(RM) *~
	$(RM) marfs_gc   
[root@dsu-sn45 gc]# 

