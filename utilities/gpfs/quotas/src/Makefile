ll: marfs_quota

#OBJS = marfs_quota.o

CFLAGS += -Wall -lgpfs

# currently a simple top-level FUSE daemon
marfs_quota: marfs_quota.c
	gcc $(CFLAGS) -o marfs_quota $(LDFLAGS) $(OBJS) marfs_quota.c






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
	$(RM) marfs_quota

